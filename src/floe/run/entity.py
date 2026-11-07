"""One entity run: inputs, per-file checks, split, outputs, `run.json` (Rust: `run/entity/*.rs`, local and S3 storage)."""

import io
import tempfile
import time
from collections.abc import Generator, Iterator
from contextlib import closing, nullcontext
from dataclasses import dataclass, replace
from functools import partial
from itertools import chain
from pathlib import Path
from typing import Any

import polars as pl

from floe.checks.cast import cast_column
from floe.checks.normalize import (
    normalize_name,
    output_column_mapping,
    resolve_strategy,
    source_names,
)
from floe.checks.rows import (
    Formatter,
)
from floe.checks.unique import (
    UniqueResult,
    UniqueTracker,
    resolve_unique_keys,
)
from floe.config.storage import CatalogsConfig, StoragesConfig
from floe.config.types import EntityConfig, with_partition_columns
from floe.errors import FloeConfigError, FloeRunError
from floe.extras import require
from floe.report.entity import build_report
from floe.run.events import NO_EVENTS, Emitter
from floe.run.file import (
    Plan,
    output,
    run_file,
    sink_write,
    source_columns,
    timed,
    unspill,
)
from floe.run.incremental import Incremental, skipped_report
from floe.run.sources import archive_location, list_source, resolve_source
from floe.storage.archive import archive_input_file
from floe.storage.inputs import (
    InputFile,
    ResolvedInputs,
    input_hash,
)
from floe.storage.partitioning import (
    advance,
    partition_date,
    partition_of,
    source_root,
)
from floe.storage.prefetch import localized_inputs
from floe.storage.resolve import resolve_path, storage_client
from floe.storage.types import Downloader, StorageClient
from floe.write.outputs import (
    AcceptedWrite,
    part_uris,
    write_accepted,
)

# #1156: the entity's cumulative wall-clock phases (`timings_ms` in run.json); a phase that did not run is null.
# ponytail: no commit_ms: `write_iceberg` commits inside PyIceberg's append, so `accepted_write_ms` includes the commit.
PHASES = ("list_ms", "download_wait_ms", "read_validate_ms", "rejected_write_ms", "accepted_write_ms", "archive_ms")  # fmt: skip


def _pulled(
    frames: Iterator[pl.DataFrame], spent: dict[str, float]
) -> Iterator[pl.DataFrame]:
    """`frames`, counting the time the sink waits for each chunk (reading and validating the next file) under `pulled`."""
    while True:
        with timed(spent, "pulled"):
            frame = next(frames, None)
        if frame is None:
            return
        yield frame


@dataclass(frozen=True)
class EntityRun:
    report: dict[str, Any]
    aborted: bool


def _real(uri: str) -> str:
    """A local path through a symlink is judged by where it lands."""
    return Path(uri.removeprefix("local://")).resolve().as_posix() if uri.startswith("local://") else uri  # fmt: skip


def _refuse_overlap(
    who: str, sink: str, inputs: list[InputFile], what: str = "sink.accepted.path"
) -> None:
    """D34: an input under a Parquet sink (accepted, or a Parquet rejected sink) would be deleted or re-read as one of its parts."""
    root = _real(sink).rstrip("/") + "/"
    for f in inputs:
        if _real(f.source_uri).startswith(root):
            raise FloeConfigError(f"{who} source file {f.source_uri} lies under {what} {sink}; the sink would overwrite or re-read it")  # fmt: skip


def _archive(plan: Plan, file: InputFile) -> str | None:
    """Move the source file to `<archive>/<entity>/<stem>__run-…__src-….<ext>` (Rust `archive_input`).

    D29: with `source.partitioning`, its partition sub-path is kept: `<archive>/<entity>/yyyy/MM/dd/<batch>/<stem>__run-…`.
    """
    if plan.archive is None:
        return None
    base, client = plan.archive
    if file.source_uri.startswith(f"{base}/"):
        return None  # D24: an archived input stays put
    # Rust: copy, then delete the source; the report shows a local target as a path
    sub = file.source_uri.removeprefix(f"{plan.root}/").rpartition("/")[0] if plan.root else ""  # fmt: skip
    uri = archive_input_file(client, plan.run_id, plan.entity.name, lambda rel: f"{base}/{rel}", file, sub)  # fmt: skip
    return uri.removeprefix("local://")


def _key_cache(plan: Plan, client: StorageClient, base: str, temp: Path) -> pl.LazyFrame | None:  # fmt: skip
    """Append mode: the key columns of the target's existing data, written once to local disk (#1057).

    Each chunk then joins its keys against this cache, so the target is read once per entity and
    memory follows the chunk, not the history (Rust `seed_unique_tracker_for_append`).
    """
    cache = temp / "unique-keys"
    cache.mkdir()
    if plan.table is not None:
        from floe.iceberg.unique import cache_table_keys

        cache_table_keys(plan.table, plan.tracker.keys, plan.out_map, cache / "table.parquet")  # fmt: skip
    else:
        columns = sorted({plan.out_map.get(n, n) for k in plan.tracker.keys for n in k})  # the sink holds output names  # fmt: skip
        for i, uri in enumerate(part_uris(client, base.rstrip("/"), "parquet")):
            part = uri.removeprefix("local://")
            try:  # only the key columns are read
                body = client.read(uri)
                keys = pl.read_parquet(io.BytesIO(body.body if body else b""), columns=columns)  # fmt: skip
            except pl.exceptions.PolarsError as err:
                raise FloeRunError(f"entity.name={plan.entity.name} cannot seed unique keys from {part}: {err}") from err  # fmt: skip
            keys.write_parquet(cache / f"{i}.parquet")
    return pl.scan_parquet(cache / "*.parquet") if any(cache.iterdir()) else None


def _report_names(
    r: UniqueResult, key: list[str], runtime: dict[str, str]
) -> UniqueResult:
    """The report names the schema columns, in `columns` and in the sample values."""
    back = {runtime.get(c, c): c for c in key}
    samples = [replace(x, values={back.get(k, k): v for k, v in x.values.items()}) for x in r.samples]  # fmt: skip
    return replace(r, columns=key, samples=samples)


def _derived(entity: EntityConfig, source_uri: str, archive: str | None, files: list[InputFile]) -> dict[str, list[pl.Expr]]:  # fmt: skip
    """D28: each input's date (and batch) column values, from its path."""
    p = entity.source.partitioning
    if p is None:
        return {}
    root, out = source_root(source_uri), dict[str, list[pl.Expr]]()
    for f in files:
        archived = archive and f.source_uri.startswith(f"{archive}/")  # D29: a full refresh  # fmt: skip
        part = partition_of(p, entity.name, f"{archive}/{entity.name}" if archived else root, f.source_uri)  # fmt: skip
        out[f.source_uri] = [pl.lit(part.when).alias(p.date_column)]
        if p.batch_column:
            out[f.source_uri].append(
                pl.lit(part.batch, pl.String).alias(p.batch_column)
            )
    return out


def run_entity(
    entity: EntityConfig,
    config_dir: Path,
    spec_version: str,
    formatter: Formatter = "json",
    storages: StoragesConfig | None = None,
    emit: Emitter = NO_EVENTS,
    full_refresh: bool = False,
    catalogs: CatalogsConfig | None = None,
    listed: ResolvedInputs | None = None,
    sinks: dict[str, str] | None = None,
) -> EntityRun:
    """Run one entity on local or S3 storage, with Parquet or Iceberg sinks; `config_dir` is absolute and normalised."""
    inc = Incremental(entity, config_dir, emit.run_id, full_refresh, storages)
    try:
        run = _run_entity(
            entity,
            config_dir,
            spec_version,
            formatter,
            storages,
            emit,
            inc,
            catalogs,
            listed,
            sinks,
        )
    except BaseException:
        if inc.committed is None:
            inc.release()  # a failed entity leaves no claims behind
        else:  # #915: the sink holds the rows, so a release would let a rerun write them again
            inc.finish(inc.committed)
        raise
    inc.finish(run.report["files"])
    return run


def _run_entity(
    entity: EntityConfig,
    config_dir: Path,
    spec_version: str,
    formatter: Formatter,
    storages: StoragesConfig | None,
    emit: Emitter,
    inc: Incremental,
    catalogs: CatalogsConfig | None,
    listed: ResolvedInputs | None,
    sinks: dict[str, str] | None,
) -> EntityRun:
    entity = with_partition_columns(
        entity
    )  # D28: the derived columns are schema columns
    merging = entity.sink.write_mode in ("merge_scd1", "merge_scd2")
    configured = entity  # the sink still checks a merge's key against the table
    # #1091: a full refresh rebuilds a merge_scd1 table from every input in one commit
    if inc.full_refresh and entity.sink.write_mode == "merge_scd1":
        entity = entity.model_copy(update={"sink": entity.sink.model_copy(update={"write_mode": "overwrite"})})  # fmt: skip
    sink = entity.sink
    if sink.accepted.format not in ("parquet", "iceberg"):
        raise FloeRunError(f"entity.name={entity.name} unsupported sink format: {sink.accepted.format}")  # fmt: skip
    iceberg = sink.accepted.format == "iceberg"
    who = f"entity.name={entity.name}"
    source_uri, client = resolve_source(entity, config_dir, storages)
    rejected_iceberg = bool(sink.rejected and sink.rejected.format == "iceberg")
    target = rejected_table = None
    if iceberg or rejected_iceberg:
        # a manifest run skips validation: name the missing extra before the import fails
        require(
            "iceberg",
            "pyiceberg",
            f"{who} sink.{'accepted' if iceberg else 'rejected'}.format=iceberg",
        )
        from floe.iceberg.sink import (
            resolve_target,
        )  # lazy: PyIceberg is the optional iceberg extra

        target = (
            resolve_target(storages, catalogs, config_dir, entity) if iceberg else None
        )
        rejected_table = resolve_target(storages, catalogs, config_dir, entity, "rejected") if rejected_iceberg else None  # fmt: skip
    if target and rejected_table and (target.catalog, target.namespace, target.table) == (rejected_table.catalog, rejected_table.namespace, rejected_table.table):  # fmt: skip
        raise FloeConfigError(
            f"{who} sink.rejected.iceberg must not write to the accepted table"
        )
    # PyIceberg writes an Iceberg table's files, not Floe's storage clients
    base, accepted = resolve_path(storages, config_dir, sink.accepted.storage, sink.accepted.path, f"{who} sink.accepted.storage")  # fmt: skip
    rejected = sink.rejected and resolve_path(storages, config_dir, sink.rejected.storage, sink.rejected.path, f"{who} sink.rejected.storage")  # fmt: skip
    strategy = resolve_strategy(entity.schema_)
    columns = entity.schema_.columns
    names = source_names(columns, strategy)
    runtime = {c.name: n for c, n in zip(columns, names, strict=True)}
    keys = resolve_unique_keys(
        entity.schema_.unique_keys,
        [c.name for c in columns if c.unique],
        entity.schema_.primary_key,
    )
    primary = {runtime.get(k.strip(), k.strip()) for k in entity.schema_.primary_key or []}  # fmt: skip
    plan = Plan(
        entity, config_dir, formatter, strategy, names,
        output_column_mapping(columns, strategy),
        {n: c.source for n, c in zip(names, columns, strict=True) if c.source},
        [n for n, c in zip(names, columns, strict=True) if c.nullable is False or n in primary],
        UniqueTracker([[runtime.get(k, k) for k in key] for key in keys]),
        {
            c.name: n
            for c in columns
            if (n := c.name if c.source else normalize_name(c.name, strategy)) != c.name
        },
        run_id=emit.run_id, rejected=rejected and (rejected[0], storage_client(rejected[1])),
        rejected_table=rejected_table,
    )  # fmt: skip
    if sinks is not None:
        plan.sinks = sinks
    location = archive_location(entity, config_dir, storages)
    if location and entity.incremental_mode in ("none", "archive"):
        plan.archive = location[0], storage_client(location[1])
        plan.root = source_root(source_uri) if entity.source.partitioning else None
    if merging and entity.schema_.primary_key:
        pk = [runtime.get(k.strip(), k.strip()) for k in entity.schema_.primary_key]
        plan.merge_key = (
            pk[0] if len(pk) == 1 else f"[{','.join(pk)}]"
        )  # the tracker's check name
    accepted_client = storage_client(accepted)
    if sink.write_mode == "append" and target is not None:
        from floe.iceberg.sink import existing_table

        plan.table = existing_table(target)  # fmt: skip  # Rust `seed_unique_tracker`, and the retry guard

    def source() -> ResolvedInputs:
        # `listed`: the run's pre-check already listed the source (#864)
        return (
            listed
            if listed is not None
            else list_source(
                entity, config_dir, storages, source_uri, client, inc.full_refresh
            )
        )

    with timed(plan.spent, "list_ms") if listed is None else nullcontext():
        inputs = source()
    if inputs.warning:  # D30
        emit("log", log_level="warn", code=None, message=inputs.warning, entity=entity.name, input=None)  # fmt: skip
    if entity.source.partitioning and entity.incremental_mode == "file":
        dated = entity.source.partitioning, entity.name, source_root(source_uri)
        inc.advance = partial(advance, *dated)
        inc.dated = partial(partition_date, *dated)
    archive = plan.archive  # a full refresh lists it too
    plan.derived = _derived(entity, source_uri, archive and archive[0], inputs.files)
    # D34 (#931): same store only (a bucket name repeats across endpoints); before the claim, so a refusal leaves nothing to release
    if target is None and client.endpoint == accepted_client.endpoint:  # fmt: skip
        _refuse_overlap(who, base, inputs.files)
    if plan.rejected and sink.rejected:
        parquet = sink.rejected.format == "parquet"
        rows = f"{plan.rejected[0].rstrip('/')}/rows"
        if parquet and client.endpoint == plan.rejected[1].endpoint:  # fmt: skip
            _refuse_overlap(who, rows, inputs.files, "sink.rejected.path")
        # accepted parts are `part-*.parquet`: in the rejected directory they would overwrite its raw file copies (any rejected format), in `rows/` its row parts
        shared = {
            _real(d).rstrip("/")
            for d in (plan.rejected[0], *([rows] if parquet else []))
        }
        if target is None and accepted_client.endpoint == plan.rejected[1].endpoint and _real(base).rstrip("/") in shared:  # fmt: skip
            raise FloeConfigError(f"{who} sink.accepted.path must differ from sink.rejected.path" + (f" and its rows directory {rows}" if parquet else ""))  # fmt: skip
    pending, files = inc.claim(inputs.files, emit)
    if done := inc.skip_committed(plan.table, pending, emit):
        files += done
        skip = {r["input_file"] for r in done}
        # `archive` mode: the archive is the marker
        plan.to_archive += [f for f in pending if f.source_uri in skip]
        pending = [f for f in pending if f.source_uri not in skip]
    plan.input_hash = input_hash(entity, pending)
    # #545: the snapshot's fingerprint names only the files it holds in full, never an aborted batch
    complete: list[InputFile] = []
    skipped = len(files)
    downloader = (
        client if isinstance(client, Downloader) else None
    )  # a local client downloads nothing
    failure: list[Exception] = []
    # #1090: a merge keeps a key's latest version, so the newest file is checked first; the first row of a file still wins.
    # Not under abort: a duplicate aborts there anyway, and newer files committed ahead of a pending older one would let it merge stale rows later
    latest_first = merging and entity.policy.severity != "abort"

    def accepted_frames() -> Generator[pl.DataFrame]:
        """Files are read as the sink pulls (#1056); `files` and `complete` are final once it is exhausted."""
        try:
            queue = pending[::-1] if latest_first else pending
            # Rust `localize_input`: a remote input is read from a local copy; #1102: downloaded ahead, and closed before the temp dir goes
            with (
                tempfile.TemporaryDirectory(prefix="floe-") as temp_dir,
                closing(localized_inputs(queue, downloader, Path(temp_dir))) as inputs,
            ):
                if pending and sink.write_mode == "append" and plan.tracker.keys and (target is None or plan.table is not None):  # fmt: skip
                    plan.history = _key_cache(plan, accepted_client, base, Path(temp_dir))  # fmt: skip
                for f in queue:
                    emit("file_started", entity=entity.name, input=f.source_uri)
                    timer = time.monotonic()
                    with timed(plan.spent, "download_wait_ms"):
                        local = next(inputs)
                    rejecting = plan.spent.get("rejected_write_ms", 0.0)
                    parsing = time.monotonic()
                    report, kept = run_file(plan, f, local.local_path, Path(temp_dir))
                    plan.spent["read_validate_ms"] = plan.spent.get("read_validate_ms", 0.0) + time.monotonic() - parsing - (plan.spent.get("rejected_write_ms", 0.0) - rejecting)  # fmt: skip
                    if local.is_ephemeral:
                        local.local_path.unlink()
                    emit("file_finished", entity=entity.name, input=f.source_uri, status=report["status"],
                         rows=report["row_count"], accepted=report["accepted_count"],
                         rejected=report["rejected_count"], elapsed_ms=int((time.monotonic() - timer) * 1000))  # fmt: skip
                    files.append(report)
                    yield from kept
                    if report["status"] == "aborted":
                        break
                    complete.append(f)
                # before the accepted commit; append-only, a retry appends again
                if plan.rejected_spills:
                    assert plan.rejected_table is not None
                    frames = (output(plan, unspill(p)).with_columns(source_columns(plan, uri)) for p, uri in plan.rejected_spills)  # fmt: skip
                    from floe.iceberg.sink import write_rejected_iceberg

                    with sink_write(plan, "rejected", "rejected_write_ms"):
                        write_rejected_iceberg(entity, plan.rejected_table, frames, [plan.out_map.get(n, n) for n in names], emit.run_id, input_hash(entity, complete))  # fmt: skip
        except Exception as err:
            # the sinks wrap what they read; this error reaches the caller as it is
            failure.append(err)
            raise

    table: dict[str, Any] | None = None
    merge: dict[str, Any] | None = None
    added: list[str] = []
    stream = accepted_frames()
    try:
        first = next(stream, None)  # Rust writes nothing when every input was skipped
        if pending and (first is not None or sink.write_mode == "overwrite"):
            empty = pl.DataFrame(schema=dict.fromkeys(names, pl.String)).select(
                cast_column(n, c.column_type)[0]
                for n, c in zip(names, columns, strict=True)
            )
            frames = chain(
                [output(plan, empty) if first is None else first],
                _pulled(stream, plan.spent),
            )
            del first  # the sink holds the chunk it is on, not the first
            sink_started = time.monotonic()
            plan.sinks["accepted"] = "unknown"
            if target is None:
                write = write_accepted(frames, accepted_client, base, sink.write_mode, sink.accepted.options)  # fmt: skip
            else:
                from floe.iceberg.sink import write_iceberg

                outputs = [plan.out_map.get(n, n) for n in names]
                result, table, merge, added = write_iceberg(configured, target, frames, outputs, sink.write_mode, emit.run_id, lambda: input_hash(entity, complete))  # fmt: skip
                write = AcceptedWrite(result.files_written, result.file_paths, result.total_bytes_written,
                                      result.avg_file_size_mb, result.small_files_count)  # fmt: skip
                if added:
                    emit("schema_evolution_applied", entity=entity.name, mode="add_columns", added_columns=added)  # fmt: skip
                if result.already_committed:  # #920: the commit conflicted and the guard found this batch in the table
                    done = {f.source_uri for f in complete}
                    files[:] = [skipped_report(r["input_file"], "already_committed") if r["input_file"] in done else r for r in files]  # fmt: skip
            # the sink's time between pulls: the writes, and the Iceberg commit after the last one
            plan.spent["accepted_write_ms"] = time.monotonic() - sink_started - plan.spent.pop("pulled", 0.0)  # fmt: skip
            plan.sinks["accepted"] = "committed"
        else:
            write = AcceptedWrite(0, [], 0, 0.0, 0)
    except Exception:
        if not failure:
            raise
        raise failure[0] from failure[0].__cause__
    finally:
        stream.close()
    if latest_first:  # the report lists the files in file order
        files[skipped:] = files[skipped:][::-1]
    files[skipped:] = sorted(files[skipped:], key=lambda r: r["input_file"] not in plan.prechecked)  # fmt: skip
    inc.committed = files  # #915: from here on a failure promotes the written files
    # #914: archive only once the sink committed, so a failed commit leaves the inputs in the source (Rust archives before)
    by_uri = {r["input_file"]: r for r in files}
    order = {f.source_uri: i for i, f in enumerate(inputs.files)}
    # #1090: oldest first, so a partial archive leaves the newest batch pending, never only an older one
    for f in sorted(plan.to_archive, key=lambda f: order[f.source_uri]) if latest_first else plan.to_archive:  # fmt: skip
        with timed(plan.spent, "archive_ms") if plan.archive else nullcontext():
            by_uri[f.source_uri]["output"]["archived_path"] = _archive(plan, f)
    unique = [_report_names(r, key, runtime) for r, key in zip(plan.tracker.results(), keys, strict=True)]  # fmt: skip
    report = build_report(
        entity,
        spec_version,
        inputs,
        files,
        write,
        source_uri=source_uri,
        accepted_uri=target.accepted_uri if target else base,
        rejected_uri=rejected[0] if rejected else None,
        unique=unique,
        table_root_uri=target.location if target and table else None,
        table=table,
        merge=merge,
        added_columns=added,
    )
    report["timings_ms"] = {k: int(plan.spent[k] * 1000) if k in plan.spent else None for k in PHASES}  # fmt: skip
    return EntityRun(report, any(f["status"] == "aborted" for f in files))
