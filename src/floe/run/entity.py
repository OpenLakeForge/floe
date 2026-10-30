"""One entity run: inputs, per-file checks, split, outputs, `run.json` (Rust: `run/entity/*.rs`, local and S3 storage)."""

import io
import tempfile
import threading
import time
from collections.abc import Callable, Generator, Iterable, Iterator
from contextlib import closing, suppress
from dataclasses import dataclass, field, replace
from datetime import datetime
from functools import partial
from itertools import chain
from pathlib import Path
from typing import Any

import polars as pl
from pyiceberg.table import Table

from floe.checks.cast import CAST_ERROR_MESSAGE, cast_column
from floe.checks.mismatch import (
    MismatchOutcome,
    apply_mismatch,
    declared_names,
    json_input_columns,
    plan_mismatch,
)
from floe.checks.normalize import (
    normalize_columns,
    normalize_name,
    output_column_mapping,
    resolve_strategy,
    source_names,
)
from floe.checks.rows import (
    ERRORS,
    ROW_INDEX,
    Formatter,
    RowCheck,
    not_null_check,
    split_rows,
)
from floe.checks.unique import (
    UniqueResult,
    UniqueTracker,
    resolve_unique_keys,
    seed_from_history,
)
from floe.config.storage import CatalogsConfig, StorageDefinition, StoragesConfig
from floe.config.types import EntityConfig, with_partition_columns
from floe.errors import FloeConfigError, FloeIoError, FloeRunError
from floe.iceberg.sink import (
    existing_table,
    input_hash,
    resolve_target,
    write_iceberg,
)
from floe.iceberg.unique import cache_table_keys
from floe.iceberg.write import find_committed
from floe.pii import apply_pii_masking
from floe.read.avro import read_avro
from floe.read.csv import RAGGED_MESSAGE, read_csv
from floe.read.json import read_json
from floe.read.orc import read_orc
from floe.read.parquet import read_parquet
from floe.read.xlsx import read_xlsx
from floe.read.xml import read_xml, xml_columns
from floe.report.entity import build_report, file_report
from floe.report.summary import run_outcome
from floe.run.events import NO_EVENTS, Emitter
from floe.state import (
    CLAIM_TTL_SECONDS,
    Claim,
    EntityState,
    FileState,
    Loaded,
    claim_all,
    claim_inputs,
    promote,
    read_state,
    release,
    renew,
)
from floe.storage.archive import archive_input_file
from floe.storage.inputs import (
    InputFile,
    NoInputsError,
    ResolvedInputs,
    resolve_inputs,
    suffixes_for_format,
)
from floe.storage.local import LocalClient
from floe.storage.partitioning import (
    advance,
    in_load_order,
    listing_start,
    partition_of,
    source_root,
    window,
)
from floe.storage.paths import normalize_local_path
from floe.storage.prefetch import localized_inputs
from floe.storage.resolve import resolve_path, storage_client
from floe.storage.s3 import S3Client
from floe.storage.types import StorageClient
from floe.write.outputs import (
    AcceptedWrite,
    part_uris,
    write_accepted,
    write_rejected,
)

Chunks = Iterator[tuple[int, pl.DataFrame, pl.Series | None]]  # fmt: skip  # offset, frame, ragged rows


@dataclass(frozen=True)
class EntityRun:
    report: dict[str, Any]
    aborted: bool


def _chunks(entity: EntityConfig, path: Path, names: list[str]) -> Chunks:
    src = entity.source
    match src.format:
        case "csv" | "tsv":
            for c in read_csv(path, src.options, names):
                yield c.offset, c.df, c.ragged
        case "parquet":
            for p in read_parquet(path):
                yield p.offset, p.df, None
        case "avro":
            for a in read_avro(path):
                yield a.offset, a.df, None
        case "orc":
            for o in read_orc(path):
                yield o.offset, o.df, None
        case "xlsx":
            yield 0, read_xlsx(path, src.options), None
        case "xml":
            for x in read_xml(path, src.options, entity.schema_.columns[: len(names)]):
                yield x.offset, x.df, None
        case "json":  # #998
            for j in read_json(path, src.options, entity.schema_.columns[: len(names)], src.cast_mode):  # fmt: skip
                yield j.offset, j.df, None
        case other:
            raise FloeRunError(
                f"entity.name={entity.name} unsupported source format: {other}"
            )


def _read_rule(fmt: str, message: str) -> str:
    """Rust's `FileReadError.rule`; the readers carry only the message (the XLSX ones prefix the rule)."""
    head, _, _ = message.partition(": ")
    if fmt == "xlsx" and head.startswith("xlsx_"):
        return head
    if fmt == "xml" and message.startswith(
        ("invalid selector", "duplicate xml selector")
    ):
        return "xml_selector_invalid"
    if fmt == "json":  # Rust `JsonReadError.rule`
        if message.startswith(("invalid selector", "duplicate json selector")):
            return "json_selector_invalid"
        if message.startswith("non-scalar value for selector"):
            return "json_selector_non_scalar"
        return "json_parse_error"
    return "xml_parse_error" if fmt == "xml" else f"{fmt}_read_error"


def _read_message(fmt: str, message: str) -> str:
    head, _, rest = message.partition(": ")
    return rest if fmt == "xlsx" and head.startswith("xlsx_") else message


def _failed_file(
    entity: EntityConfig,
    uri: str,
    declared: int,
    plan: MismatchOutcome | None,
    error: FloeIoError,
    rejected_path: str | None,
) -> dict[str, Any]:
    """A file that could not be read: `rejected` (`aborted` under abort), no rows."""
    fmt = entity.source.format
    action = "aborted" if entity.policy.severity == "abort" else "rejected_file"
    base = plan.report if plan else {}
    report: dict[str, Any] = {
        "declared_columns_count": declared,
        "input_columns_count": base.get("input_columns_count", 0),
        "missing_columns": base.get("missing_columns", []),
        "extra_columns": base.get("extra_columns", []),
        "mismatch_action": action,
        "error": {
            "rule": _read_rule(fmt, str(error)),
            "message": f"entity.name={entity.name} {_read_message(fmt, str(error))}",
        },
    }
    if "warning" in base:
        report["warning"] = base["warning"]
    outcome = MismatchOutcome(
        report, False, False, plan.warnings if plan else 0, 1 + (plan.errors if plan else 0),
        [], [], False, False,
    )  # fmt: skip
    out = file_report(entity, uri, 0, 0, [], outcome, rejected_path)
    out["status"] = "aborted" if action == "aborted" else "rejected"
    return out


def _rejected(plan: "_Plan", severity: str) -> tuple[str, StorageClient]:
    """The rejected sink's URI and client."""
    if plan.rejected is None:
        raise FloeConfigError(f"sink.rejected is required for {severity} severity")
    return plan.rejected


def _put(plan: "_Plan", severity: str, name: str, body: bytes) -> str:
    """Write `<rejected>/<name>`; the report shows a local target as a path, as Rust."""
    base, client = _rejected(plan, severity)
    uri = f"{base.rstrip('/')}/{name}"
    client.write(uri, body)
    return uri.removeprefix("local://")


def _copy_raw(plan: "_Plan", source: Path, severity: str, name: str) -> str:
    # ponytail: the raw file is read into memory for the put; stream it (upload_file) if rejected files get large
    return _put(plan, severity, name, source.read_bytes())


def _error_report(plan: "_Plan", stem: str, rows: list[tuple[int, str]]) -> str:
    """`<stem>_reject_errors.json`, Rust's `write_error_report`."""
    items = ",".join(f'{{"row_index":{i},"errors":{e}}}' for i, e in rows)
    return _put(plan, plan.entity.policy.severity, f"{stem}_reject_errors.json", f"[{items}]".encode())  # fmt: skip


@dataclass
class _Plan:
    """Per-entity values every file needs."""

    entity: EntityConfig
    config_dir: Path
    formatter: Formatter
    strategy: str | None
    names: list[str]
    out_map: dict[str, str]
    sources: dict[str, str]
    required: list[str]
    tracker: UniqueTracker
    pii_map: dict[str, str]
    rejected_writes: int = 0
    prechecked: set[str] = field(
        default_factory=set[str]
    )  # files the precheck settles (unreadable header, rejected or aborted mismatch); Rust settles them before it reads any file
    archive: tuple[str, StorageClient] | None = None  # Rust `archive_enabled()`
    to_archive: list[InputFile] = field(default_factory=list[InputFile])  # fmt: skip  # moved after the sink commit (#914)
    run_id: str = ""
    input_hash: str = (
        ""  # of the claimed files; names the rejected parts so a retry overwrites them
    )
    rejected: tuple[str, StorageClient] | None = None  # `sink.rejected` URI and client
    table: Table | None = (
        None  # the Iceberg target whose keys seed the tracker (append)
    )
    history: pl.LazyFrame | None = (
        None  # the target's key columns, cached on local disk (append, #1057)
    )
    merge_key: str | None = (
        None  # the merge key's unique check; its duplicates are rejected even under warn (Rust `enforce_reject`)
    )
    derived: dict[str, list[pl.Expr]] = field(default_factory=dict[str, list[pl.Expr]])  # D28: by input URI  # fmt: skip
    root: str | None = None  # D29: the source root, when `source.partitioning` is set


def _refuse_overlap(who: str, sink: str, inputs: list[InputFile]) -> None:
    """D34: an input under the accepted Parquet sink would be deleted or re-read as one of its parts."""

    def real(
        uri: str,
    ) -> str:  # a local path through a symlink is judged by where it lands
        return Path(uri.removeprefix("local://")).resolve().as_posix() if uri.startswith("local://") else uri  # fmt: skip

    root = real(sink).rstrip("/") + "/"
    for f in inputs:
        if real(f.source_uri).startswith(root):
            raise FloeConfigError(f"{who} source file {f.source_uri} lies under sink.accepted.path {sink}; the sink would overwrite or re-read it")  # fmt: skip


def _archive(plan: _Plan, file: InputFile) -> str | None:
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


def _with_archived(e: EntityConfig, archive: tuple[str, StorageClient], source: Callable[[], ResolvedInputs]) -> ResolvedInputs:  # fmt: skip
    """D24: a full refresh reads `<archive>/<entity>/` first (by name; D29: in load order), then the source inputs; either may be empty, not both.

    Only "no inputs matched" counts as empty: any other listing error fails the refresh rather than rebuild from part of the inputs.
    """
    base, client = archive
    p = e.source.partitioning
    path = e.name if base.startswith("local://") else f"{base}/{e.name}/"
    try:
        old = resolve_inputs(Path(base.removeprefix("local://")), path, e.source.format, e.name, e.source.storage or "local", client, recursive=p is not None)  # fmt: skip
    except NoInputsError:
        return (
            source()
        )  # nothing archived: the source's "no input files matched" stands
    if p is not None:  # D29: the archive keeps the partition sub-paths
        old = in_load_order(p, e.name, f"{base}/{e.name}", old, archived=True)
    try:
        new = source()
    except NoInputsError:  # no new source file: rebuild from the archive alone
        return old
    seen = set(old.listed)  # an S3 source prefix also lists an archive nested under it
    files = [f for f in new.files if f.source_uri not in seen]
    return ResolvedInputs(old.files + files, old.listed + [u for u in new.listed if u not in seen], new.mode)  # fmt: skip


def _output(plan: _Plan, df: pl.DataFrame) -> pl.DataFrame:
    """Output column names, then PII masking."""
    df = df.rename(plan.out_map)
    pii = plan.entity.pii
    return apply_pii_masking(df, pii.columns, plan.pii_map) if pii else df


def _typed(
    plan: _Plan, raw: pl.DataFrame, mismatch: MismatchOutcome, derived: list[pl.Expr]
) -> tuple[pl.DataFrame, list[RowCheck]]:
    entity, src = plan.entity, plan.entity.source
    # D28 (#913): the derived columns join after the file's own columns are normalised and matched
    df = apply_mismatch(normalize_columns(raw, plan.strategy), mismatch)
    if derived:
        df = df.with_columns(derived)
    if df.columns != plan.names:
        df = df.select(
            pl.col(n) if n in df.columns else pl.lit(None, pl.String).alias(n)
            for n in plan.names
        )
    columns = entity.schema_.columns
    tag = "_e_cast_"  # the mask columns' prefix: no declared column may start with it
    while any(n.startswith(tag) for n in plan.names):
        tag = f"_{tag}"
    try:
        casts = [
            cast_column(n, c.column_type, src.cast_mode, df.schema[n])
            for n, c in zip(plan.names, columns, strict=True)
        ]
        # one plan for both; unlike select, with_columns broadcasts the constant masks
        masks = [m.alias(f"{tag}{n}") for n, (_, m) in zip(plan.names, casts, strict=True)]  # fmt: skip
        both = df.with_columns([t for t, _ in casts] + masks)
        typed = both[plan.names]
    except pl.exceptions.InvalidOperationError as err:
        raise FloeRunError(f"entity.name={entity.name} cast failed: {err}") from err
    cast_checks = [
        RowCheck("cast_error", n, CAST_ERROR_MESSAGE, both[f"{tag}{n}"])
        for n, c in zip(plan.names, columns, strict=True)
        if c.column_type != "string"
    ]  # fmt: skip
    if entity.policy.severity == "abort" and any(c.mask.any() for c in cast_checks):
        return typed, cast_checks  # fmt: skip  # Rust: abort-mode cast errors skip not_null and unique
    required = [not_null_check(typed, n) for n in plan.required]
    if plan.history is not None:
        seed_from_history(plan.tracker, plan.history, typed, plan.out_map)
    return typed, [*required, *cast_checks, *plan.tracker.check(typed)]


def _unspill(path: Path) -> pl.DataFrame:
    df = pl.read_ipc(path)
    path.unlink()
    return df


def _replay(plan: _Plan, held: list[pl.DataFrame | Path]) -> Iterator[pl.DataFrame]:
    while held:
        item = held.pop(0)
        yield _output(plan, item if isinstance(item, pl.DataFrame) else _unspill(item))


def _run_file(plan: _Plan, file: InputFile, path: Path, spill: Path) -> tuple[dict[str, Any], Iterable[pl.DataFrame]]:  # fmt: skip
    """The file's report and its accepted frames, one by one (none when it aborted the run); `path` is the local copy.

    A file's rows are accepted only once it is read to the end (a late read error or an abort drops them all), so
    every chunk after the first waits in `spill`, not in memory.
    """
    entity, uri = plan.entity, file.source_uri
    severity = entity.policy.severity
    derived = plan.derived.get(uri, [])
    # D28: the derived columns come last in the schema and are not in the file
    file_names = plan.names[: len(plan.names) - len(derived)]
    derived = [e.alias(n) for e, n in zip(derived, plan.names[len(file_names) :], strict=True)]  # fmt: skip
    declared = [n for n in declared_names(entity) if n not in plan.names[len(file_names) :]]  # fmt: skip
    chunks = _chunks(entity, path, file_names)
    keys = plan.tracker.snapshot()

    def failed(err: FloeIoError, mismatch: MismatchOutcome | None) -> tuple[dict[str, Any], Iterable[pl.DataFrame]]:  # fmt: skip
        # Rust skips the raw copy under PII: the file cannot be masked
        plan.tracker.restore(keys)  # the unreadable file leaves no keys behind
        keep = entity.pii is None and entity.sink.rejected is not None
        raw = _copy_raw(plan, path, severity, file.source_name) if keep else None
        return _failed_file(entity, uri, len(declared) + len(derived), mismatch, err, raw), []  # fmt: skip

    try:
        if entity.source.format == "xml":
            first, probe = [], xml_columns(path, entity.source.options)
        elif entity.source.format == "json":
            # D40: every record's top-level keys; D41: only a file without records has nothing to compare
            json_keys = json_input_columns(path, entity.source.options.json_mode or "array")  # fmt: skip
            first, probe = [], declared if json_keys is None else json_keys
        else:  # the first chunk's columns are the input columns the plan compares
            first = [next(chunks)]
            probe = list(first[0][1].columns)
    except FloeIoError as err:
        plan.prechecked.add(uri)  # Rust `run_precheck` settles an unreadable header too
        return failed(err, None)
    mismatch = plan_mismatch(entity, declared, probe)
    # D28: the derived columns are declared (through partitioning), not input columns
    mismatch.report["declared_columns_count"] += len(derived)
    if mismatch.rejected or mismatch.aborted:
        plan.prechecked.add(uri)
        raw = _copy_raw(plan, path, "abort" if mismatch.aborted else "reject", file.source_name)  # fmt: skip
        plan.to_archive.append(file)
        return file_report(entity, uri, 0, 0, [], mismatch, raw), []

    rows, bad_total, forced_total = 0, 0, 0
    masks: dict[tuple[str, str, str], list[pl.Series]] = {}
    accepted: list[pl.DataFrame | Path] = []
    rejected: list[pl.DataFrame] = []
    errors: list[tuple[int, str]] = []
    try:
        for offset, raw_df, ragged in chain(first, chunks):
            typed, checks = _typed(plan, raw_df, mismatch, derived)
            if ragged is not None and ragged.any():
                checks.insert(0, RowCheck("csv_read_error", "", RAGGED_MESSAGE, ragged))
            good, bad = split_rows(typed, checks, plan.formatter, plan.sources)
            bad = bad.with_columns(pl.col(ROW_INDEX) + offset)
            rows += typed.height
            bad_total += bad.height
            for c in checks:
                masks.setdefault((c.rule, c.column, c.message), []).append(c.mask)
            errors += zip(bad[ROW_INDEX].to_list(), bad[ERRORS].to_list(), strict=True)
            forced = next((c.mask for c in checks if c.rule == "unique" and c.column == plan.merge_key), None)  # fmt: skip
            if severity == "warn" and forced is not None:
                forced = forced.fill_null(False)
                forced_total += int(forced.sum())
                typed = typed.filter(~forced)
            frame = good if severity == "reject" else typed
            if accepted:
                frame.write_ipc(spilled := spill / f"{len(accepted)}.ipc")
                accepted.append(spilled)
            else:
                accepted.append(frame)
            if severity == "reject" and bad.height:
                rejected.append(bad)
    except FloeIoError as err:
        return failed(err, mismatch)

    merged = [RowCheck(r, c, m, pl.concat(s)) for (r, c, m), s in masks.items()]
    rejected_path = errors_path = None
    if bad_total and severity == "reject":
        mode = entity.sink.write_mode
        if mode == "overwrite" and plan.rejected_writes:
            mode = "append"
        plan.rejected_writes += 1
        target, client = _rejected(plan, "reject")
        written = write_rejected(_output(plan, pl.concat(rejected)), client, target, mode, f"{plan.input_hash}:{file.source_uri}")  # fmt: skip
        rejected_path = written.removeprefix("local://")
    elif bad_total and severity == "abort":
        rejected_path = _copy_raw(plan, path, "abort", file.source_name)
    if bad_total and entity.sink.rejected and severity != "reject":
        errors_path = _error_report(plan, file.source_stem, errors)
    plan.to_archive.append(file)
    out = file_report(entity, uri, rows, bad_total, merged, mismatch, rejected_path, None, errors_path, forced_total)  # fmt: skip
    return out, [] if out["status"] == "aborted" else _replay(plan, accepted)


def _key_cache(plan: _Plan, client: StorageClient, base: str, temp: Path) -> pl.LazyFrame | None:  # fmt: skip
    """Append mode: the key columns of the target's existing data, written once to local disk (#1057).

    Each chunk then joins its keys against this cache, so the target is read once per entity and
    memory follows the chunk, not the history (Rust `seed_unique_tracker_for_append`).
    """
    cache = temp / "unique-keys"
    cache.mkdir()
    if plan.table is not None:
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


RENEW_INTERVAL_S = float(min(max(CLAIM_TTL_SECONDS // 3, 1), 300))
_SEPARATORS = "/\\"


def _parent(value: str) -> str | None:
    i = max(value.rfind(s) for s in _SEPARATORS)
    return None if i < 0 else value[: max(i, 1)]


def _source_root(raw: str, fmt: str, resolved: Path | None) -> str:
    """Rust `derive_source_root`: the directory the default state lives under."""
    trimmed = raw.rstrip(_SEPARATORS)
    if not trimmed:
        return ""
    cut = next((i for i, c in enumerate(trimmed) if c in "*?["), -1)
    if cut >= 0:
        prefix = trimmed[:cut]
        done = prefix if not prefix or prefix[-1] in _SEPARATORS else _parent(prefix) or prefix  # fmt: skip
        return done.rstrip(_SEPARATORS)
    if resolved and resolved.is_dir():  # a remote source has no local path to probe
        return trimmed
    if (resolved and resolved.is_file()) or trimmed.lower().endswith(
        tuple(suffixes_for_format(fmt))
    ):
        # #813: a bare filename lives in the config dir (Rust 0.7 used the file itself)
        return (_parent(trimmed) or ".").rstrip(_SEPARATORS)
    return trimmed


def state_uri(entity: EntityConfig, config_dir: Path, storages: StoragesConfig | None = None) -> tuple[str, StorageClient]:  # fmt: skip
    """`state.path` if set (a URI on the source storage, else local), else `<source root>/.floe/state/<domain>/<name>/state.json` on the source storage (Rust `resolve_entity_state_path`)."""
    src, field = entity.source, f"entity.name={entity.name} entity.state.path"
    custom = entity.state and entity.state.path
    if custom and "://" not in custom:
        return f"local://{normalize_local_path(config_dir / custom)}", LocalClient()
    if not custom:
        source_uri, _ = resolve_path(storages, config_dir, src.storage, src.path, field)
        local = source_uri.startswith("local://")
        root = _source_root(src.path, src.format, Path(source_uri.removeprefix("local://")) if local else None)  # fmt: skip
        ident = f"{entity.domain}/{entity.name}" if entity.domain and entity.id != entity.name else entity.name  # fmt: skip
        tail = f".floe/state/{ident}/state.json"
        custom = f"{root}/{tail}" if root and root != "." else tail
    uri, definition = resolve_path(storages, config_dir, src.storage, custom, field)
    return uri, storage_client(definition)


def skipped_report(uri: str, reason: str, warning: str | None = None) -> dict[str, Any]:
    mismatch: dict[str, Any] = {
        "declared_columns_count": 0,
        "input_columns_count": 0,
        "missing_columns": [],
        "extra_columns": [],
        "mismatch_action": "none",
    }
    if warning:
        mismatch["warning"] = warning
    return {
        "input_file": uri,
        "status": "skipped",
        "row_count": 0,
        "accepted_count": 0,
        "rejected_count": 0,
        "skip_reason": reason,
        "mismatch": mismatch,
        "output": {
            "accepted_path": None,
            "rejected_path": None,
            "errors_path": None,
            "archived_path": None,
        },
        "validation": {"errors": 0, "warnings": int(bool(warning)), "rules": []},
    }


def skipped_reports(
    entity: EntityConfig,
    already: list[tuple[InputFile, FileState]],
    active: list[str],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for f, rec in already:
        if (rec.size, rec.mtime) == (f.source_size, f.source_mtime):
            out.append(skipped_report(f.source_uri, "already_ingested"))
            continue
        msg = (
            f"entity.name={entity.name} incremental_mode=file skipping previously ingested file with changed metadata: {f.source_uri} "
            f"(recorded size={rec.size}, mtime={rec.mtime}; current size={f.source_size}, mtime={f.source_mtime})"
        )  # fmt: skip
        out.append(skipped_report(f.source_uri, "already_ingested_changed", msg))
    out += [
        skipped_report(
            u, "active_claim",
            f"entity.name={entity.name} incremental_mode=file skipping file claimed by another active run: {u}",
        )
        for u in active
    ]  # fmt: skip
    return out


def _emit_skipped(emit: Emitter, entity: str, reports: list[dict[str, Any]]) -> None:
    for r in reports:
        emit("file_finished", entity=entity, input=r["input_file"], status="skipped",
             skip_reason=r["skip_reason"], rows=0, accepted=0, rejected=0, elapsed_ms=0)  # fmt: skip


class Incremental:
    """Claims the entity's inputs, renews the claims while it runs, then promotes or releases them."""

    def __init__(self, entity: EntityConfig, config_dir: Path, run_id: str, full_refresh: bool = False, storages: StoragesConfig | None = None) -> None:  # fmt: skip
        self.entity, self.config_dir, self.run_id = entity, config_dir, run_id
        self.storages = storages
        self.full_refresh = full_refresh
        self.client: StorageClient = LocalClient()
        self.uri = ""
        self.claimed: Loaded | None = None
        self.expired: dict[str, Claim] = {}  # the dead runs' claims this run took over
        # the file reports, set once the sink step is done (#915)
        self.committed: list[dict[str, Any]] | None = None
        self.advance: Callable[[EntityState, list[str]], None] | None = None  # D30
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def claim(self, inputs: list[InputFile], emit: Emitter) -> tuple[list[InputFile], list[dict[str, Any]]]:  # fmt: skip
        """The inputs left to process and the reports of the skipped ones; `row` mode and a full refresh never skip (Rust 0.7)."""
        e = self.entity
        if e.incremental_mode != "file":
            return inputs, []
        self.uri, self.client = state_uri(e, self.config_dir, self.storages)
        if self.full_refresh:
            self._hold(claim_all(self.client, self.uri, e.name, self.run_id, inputs))
            return inputs, []
        got = claim_inputs(self.client, self.uri, e.name, self.run_id, inputs, e.sink.write_mode.startswith("merge"))  # fmt: skip
        reports = skipped_reports(e, got.already_processed, got.active_claims)
        _emit_skipped(emit, e.name, reports)
        self.expired = got.expired
        if got.claimed:
            self._hold(got.claimed)
        return got.pending, reports

    def skip_committed(self, table: Table | None, pending: list[InputFile], emit: Emitter) -> list[dict[str, Any]]:  # fmt: skip
        """#545 retry guard (append): the reports of the pending inputs a dead run already committed, before any is read.

        `file` mode re-derives each dead run's batch from its expired claims (by `run_id`, with their size and mtime),
        so a retry that also holds new inputs still matches; it looks back to that run's claim. Other modes match the
        whole set.
        """
        e = self.entity
        if table is None or not pending or self.full_refresh:
            return []
        if e.incremental_mode != "file":
            done = pending if find_committed(table, input_hash(e, pending)) else []
            return self._skipped(done, emit)
        ours = {f.source_uri for f in pending}
        runs: dict[str, dict[str, Claim]] = {}
        for u, c in self.expired.items():
            runs.setdefault(c.run_id, {})[u] = c
        committed: set[str] = set()
        for claims in runs.values():
            if ours.isdisjoint(claims):
                continue  # new inputs, or a reload after `floe state reset`
            since = min(datetime.fromisoformat(c.acquired_at) for c in claims.values())
            batch = [InputFile(u, "", "", c.size, c.mtime) for u, c in claims.items()]
            found = find_committed(table, input_hash(e, batch), since)
            # fail closed; `run_entity` releases, which puts the dead runs' claims back so the next run fails too
            if found is None:
                raise FloeRunError(
                    f"entity.name={e.name} cannot tell whether a run that died after claiming its inputs at {since} "
                    "committed them: the table's snapshots since then were expired (retention floor: 7 days and at least "
                    "10 snapshots); run `floe state reset` for the entity, then rerun"
                )  # fmt: skip
            if found:
                committed |= claims.keys() & ours
        # promote now, so a later failure of the other inputs can't release them
        if committed and self.claimed:
            promote(self.client, self.uri, self.run_id, self.claimed, only=committed, advance=self.advance)  # fmt: skip
        return self._skipped([f for f in pending if f.source_uri in committed], emit)

    def _skipped(self, files: list[InputFile], emit: Emitter) -> list[dict[str, Any]]:
        reports = [skipped_report(f.source_uri, "already_committed") for f in files]
        _emit_skipped(emit, self.entity.name, reports)
        return reports

    def _hold(self, claimed: Loaded) -> None:
        self.claimed = claimed
        self._thread = threading.Thread(target=self._beat, daemon=True)
        self._thread.start()

    def _beat(self) -> None:
        while not self._stop.wait(RENEW_INTERVAL_S):
            with suppress(Exception):  # Rust ignores renewal errors
                if self.claimed:
                    renew(self.client, self.uri, self.run_id, self.claimed)

    def _end(self) -> Loaded | None:
        self._stop.set()
        if self._thread:
            self._thread.join()
        claimed, self.claimed = self.claimed, None
        return claimed

    def release(self) -> None:
        """The dead runs' claims this run took over go back: the next run checks them again (#545)."""
        if claimed := self._end():
            # D30: an older day's released file stays listed for a retry
            release(self.client, self.uri, self.run_id, claimed, self.expired, self.advance is not None and not self.full_refresh)  # fmt: skip

    def finish(self, files: list[dict[str, Any]]) -> None:
        """Promote unless the entity failed (Rust commits on success, warnings and rejected).

        #918: an abort still writes the files before it (Rust 0.7), so it promotes those and releases the rest;
        under `overwrite` it releases them all, as the retry rebuilds the output from every file.
        """
        status, _ = run_outcome([f["status"] for f in files])
        if status == "failed" or (status == "aborted" and self.entity.sink.write_mode == "overwrite"):  # fmt: skip
            return self.release()
        if claimed := self._end():
            only = {f["input_file"] for f in files if f["status"] != "aborted"} if status == "aborted" else None  # fmt: skip
            promote(self.client, self.uri, self.run_id, claimed, self.full_refresh, only, self.advance)  # fmt: skip
            if (
                only is not None
            ):  # D30: the aborted day's released files stay listed for a retry
                release(self.client, self.uri, self.run_id, claimed, self.expired, self.advance is not None and not self.full_refresh)  # fmt: skip


def resolve_source(entity: EntityConfig, config_dir: Path, storages: StoragesConfig | None) -> tuple[str, LocalClient | S3Client]:  # fmt: skip
    """`source.path` as a URI, and the client of its storage."""
    src = entity.source
    uri, definition = resolve_path(storages, config_dir, src.storage, src.path, f"entity.name={entity.name} source.storage")  # fmt: skip
    return uri, storage_client(definition)


def archive_location(entity: EntityConfig, config_dir: Path, storages: StoragesConfig | None) -> tuple[str, StorageDefinition | None] | None:  # fmt: skip
    """`sink.archive` as a URI (no trailing `/`) and its storage definition."""
    archive, src = entity.sink.archive, entity.source
    if archive is None:
        return None
    uri, definition = resolve_path(storages, config_dir, archive.storage or src.storage, archive.path, f"entity.name={entity.name} sink.archive.storage")  # fmt: skip
    return uri.rstrip("/"), definition


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


def list_source(entity: EntityConfig, config_dir: Path, storages: StoragesConfig | None, uri: str, client: StorageClient, full_refresh: bool = False) -> ResolvedInputs:  # fmt: skip
    """The source inputs; a local path keeps its globs relative to the config.

    D31: the archive is never listed as source input; only a full refresh reads it, explicitly (D24).
    D30: `file` mode with partitioning lists only from the state's window.
    """
    src, archive = entity.source, archive_location(entity, config_dir, storages)
    p, root, state = src.partitioning, source_root(uri), None
    if p and entity.incremental_mode == "file" and not full_refresh:
        at, state_client = state_uri(entity, config_dir, storages)
        state = read_state(state_client, at, entity.name)
    start = window(p, entity.name, root, state) if p else None
    after = listing_start(p, start) if p and start else None

    def source() -> ResolvedInputs:
        inputs = resolve_inputs(
            config_dir, src.path if uri.startswith("local://") else uri, src.format, entity.name,
            src.storage or "local", client, glob_override=src.options.glob, exclude=archive and archive[0],
            recursive=bool(src.options.recursive) or p is not None,  # D28: partitioning lists recursively
            after=f"{root}/{after}" if after else "",
        )  # fmt: skip
        if not p:
            return inputs
        inputs = in_load_order(p, entity.name, root, inputs, start=start)
        if start is None or state is None:
            return inputs
        late = f"entity.name={entity.name} incremental_mode=file lists source.partitioning dates from {start} (watermark {state.watermark}, lookback_days {p.lookback_days}); a late file in an older day is not ingested until --full-refresh"  # fmt: skip
        return replace(inputs, warning=late)

    if full_refresh and archive and entity.incremental_mode in ("none", "archive"):
        return _with_archived(entity, (archive[0], storage_client(archive[1])), source)
    return source()


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
    target = resolve_target(storages, catalogs, config_dir, entity) if iceberg else None
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
    plan = _Plan(
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
    )  # fmt: skip
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

    inputs = source()
    if inputs.warning:  # D30
        emit("log", log_level="warn", code=None, message=inputs.warning, entity=entity.name, input=None)  # fmt: skip
    if entity.source.partitioning and entity.incremental_mode == "file":
        dated = entity.source.partitioning, entity.name, source_root(source_uri)
        inc.advance = partial(advance, *dated)
    archive = plan.archive  # a full refresh lists it too
    plan.derived = _derived(entity, source_uri, archive and archive[0], inputs.files)
    # D34 (#931): same store only (a bucket name repeats across endpoints); before the claim, so a refusal leaves nothing to release
    if target is None and getattr(client, "endpoint", None) == getattr(accepted_client, "endpoint", None):  # fmt: skip
        _refuse_overlap(who, base, inputs.files)
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
    downloader = None if isinstance(client, LocalClient) else client
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
                    local = next(inputs)
                    report, kept = _run_file(plan, f, local.local_path, Path(temp_dir))
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
            frames = chain([_output(plan, empty) if first is None else first], stream)
            del first  # the sink holds the chunk it is on, not the first
            if target is None:
                write = write_accepted(frames, accepted_client, base, sink.write_mode, sink.accepted.options)  # fmt: skip
            else:
                outputs = [plan.out_map.get(n, n) for n in names]
                result, table, merge, added = write_iceberg(configured, target, frames, outputs, sink.write_mode, emit.run_id, lambda: input_hash(entity, complete))  # fmt: skip
                write = AcceptedWrite(result.files_written, result.file_paths, result.total_bytes_written,
                                      result.avg_file_size_mb, result.small_files_count)  # fmt: skip
                if added:
                    emit("schema_evolution_applied", entity=entity.name, mode="add_columns", added_columns=added)  # fmt: skip
                if result.already_committed:  # #920: the commit conflicted and the guard found this batch in the table
                    done = {f.source_uri for f in complete}
                    files[:] = [skipped_report(r["input_file"], "already_committed") if r["input_file"] in done else r for r in files]  # fmt: skip
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
    return EntityRun(report, any(f["status"] == "aborted" for f in files))
