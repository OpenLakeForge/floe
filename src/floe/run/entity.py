"""One entity run: inputs, per-file checks, split, outputs, `run.json` (Rust: `run/entity/*.rs`, local and S3 storage)."""

import io
import tempfile
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import suppress
from dataclasses import dataclass, field, replace
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
from floe.checks.unique import UniqueResult, UniqueTracker, resolve_unique_keys
from floe.config.storage import CatalogsConfig, StorageDefinition, StoragesConfig
from floe.config.types import EntityConfig, with_partition_columns
from floe.errors import FloeConfigError, FloeIoError, FloeRunError
from floe.iceberg.sink import (
    existing_table,
    input_hash,
    resolve_target,
    write_iceberg,
)
from floe.iceberg.unique import seed_from_table
from floe.pii import apply_pii_masking
from floe.read.avro import read_avro
from floe.read.csv import RAGGED_MESSAGE, read_csv
from floe.read.orc import read_orc
from floe.read.parquet import read_parquet
from floe.read.xlsx import read_xlsx
from floe.read.xml import read_xml, xml_columns
from floe.report.entity import build_report, file_report
from floe.report.summary import run_outcome
from floe.run.events import NO_EVENTS, Emitter
from floe.state import (
    CLAIM_TTL_SECONDS,
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
    localize_input,
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
    merge_key: str | None = (
        None  # the merge key's unique check; its duplicates are rejected even under warn (Rust `enforce_reject`)
    )
    derived: dict[str, list[pl.Expr]] = field(default_factory=dict[str, list[pl.Expr]])  # D28: by input URI  # fmt: skip


def _archive(plan: _Plan, file: InputFile) -> str | None:
    """Move the source file to `<archive>/<entity>/<stem>__run-…__src-….<ext>` (Rust `archive_input`)."""
    if plan.archive is None:
        return None
    base, client = plan.archive
    if file.source_uri.startswith(f"{base}/"):
        return None  # D24: an archived input stays put
    # Rust: copy, then delete the source; the report shows a local target as a path
    uri = archive_input_file(client, plan.run_id, plan.entity.name, lambda rel: f"{base}/{rel}", file)  # fmt: skip
    return uri.removeprefix("local://")


def _with_archived(e: EntityConfig, archive: tuple[str, StorageClient], source: Callable[[], ResolvedInputs]) -> ResolvedInputs:  # fmt: skip
    """D24: a full refresh reads `<archive>/<entity>/` first (by name), then the source inputs; either may be empty, not both.

    Only "no inputs matched" counts as empty: any other listing error fails the refresh rather than rebuild from part of the inputs.
    """
    base, client = archive
    path = e.name if base.startswith("local://") else f"{base}/{e.name}/"
    try:
        old = resolve_inputs(Path(base.removeprefix("local://")), path, e.source.format, e.name, e.source.storage or "local", client)  # fmt: skip
    except NoInputsError:
        return (
            source()
        )  # nothing archived: the source's "no input files matched" stands
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
    plan: _Plan, raw: pl.DataFrame, mismatch: MismatchOutcome
) -> tuple[pl.DataFrame, list[RowCheck]]:
    entity, src = plan.entity, plan.entity.source
    df = apply_mismatch(normalize_columns(raw, plan.strategy), mismatch)
    df = df.select(
        pl.col(n) if n in df.columns else pl.lit(None, pl.String).alias(n)
        for n in plan.names
    )
    columns = entity.schema_.columns
    try:
        casts = [
            cast_column(n, c.column_type, src.cast_mode, df.schema[n])
            for n, c in zip(plan.names, columns, strict=True)
        ]
        typed = df.with_columns(t for t, _ in casts)
        masks = df.with_columns(
            m for _, m in casts
        )  # unlike select, broadcasts constant masks
    except pl.exceptions.InvalidOperationError as err:
        raise FloeRunError(f"entity.name={entity.name} cast failed: {err}") from err
    cast_checks = [
        RowCheck("cast_error", n, CAST_ERROR_MESSAGE, masks[f"_e_cast_{n}"])
        for n, c in zip(plan.names, columns, strict=True)
        if c.column_type != "string"
    ]  # fmt: skip
    if entity.policy.severity == "abort" and any(c.mask.any() for c in cast_checks):
        return typed, cast_checks  # fmt: skip  # Rust: abort-mode cast errors skip not_null and unique
    required = [not_null_check(typed, n) for n in plan.required]
    if plan.table is not None:
        # ponytail: one key scan of the table per chunk; a DuckDB anti-join (ADR) if large histories make it slow
        seed_from_table(plan.tracker, plan.table, typed, plan.out_map)
    return typed, [*required, *cast_checks, *plan.tracker.check(typed)]


def _run_file(plan: _Plan, file: InputFile, path: Path) -> tuple[dict[str, Any], list[pl.DataFrame]]:  # fmt: skip
    """The file's report and its accepted frames (none when it aborted the run); `path` is the local copy."""
    entity, uri = plan.entity, file.source_uri
    severity = entity.policy.severity
    derived = plan.derived.get(uri, [])
    added = [e.meta.output_name() for e in derived]
    # D28: the derived columns come last; they are declared, whatever their name, but not in the file
    derived_names = [normalize_name(n, plan.strategy) for n in added]
    declared = [
        n for n in declared_names(entity) if n not in derived_names
    ] + derived_names
    chunks = _chunks(entity, path, plan.names[: len(plan.names) - len(derived)])
    keys = plan.tracker.snapshot()

    def failed(err: FloeIoError, mismatch: MismatchOutcome | None) -> tuple[dict[str, Any], list[pl.DataFrame]]:  # fmt: skip
        # Rust skips the raw copy under PII: the file cannot be masked
        plan.tracker.restore(keys)  # the unreadable file leaves no keys behind
        keep = entity.pii is None and entity.sink.rejected is not None
        raw = _copy_raw(plan, path, severity, file.source_name) if keep else None
        return _failed_file(entity, uri, len(declared), mismatch, err, raw), []

    try:
        if entity.source.format == "xml":
            first, probe = [], xml_columns(path, entity.source.options)
        else:  # the first chunk's columns are the input columns the plan compares
            first = [next(chunks)]
            probe = list(first[0][1].columns)
    except FloeIoError as err:
        plan.prechecked.add(uri)  # Rust `run_precheck` settles an unreadable header too
        return failed(err, None)
    mismatch = plan_mismatch(entity, declared, probe + added)
    if derived:  # D28: the input columns are the file's own
        mismatch.report["input_columns_count"] = len(probe)
    if mismatch.rejected or mismatch.aborted:
        plan.prechecked.add(uri)
        raw = _copy_raw(plan, path, "abort" if mismatch.aborted else "reject", file.source_name)  # fmt: skip
        plan.to_archive.append(file)
        return file_report(entity, uri, 0, 0, [], mismatch, raw), []

    rows, bad_total, forced_total = 0, 0, 0
    masks: dict[tuple[str, str, str], list[pl.Series]] = {}
    accepted: list[pl.DataFrame] = []
    rejected: list[pl.DataFrame] = []
    errors: list[tuple[int, str]] = []
    try:
        for offset, raw_df, ragged in chain(first, chunks):
            typed, checks = _typed(plan, raw_df.with_columns(derived), mismatch)
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
            accepted.append(good if severity == "reject" else typed)
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
    return out, [] if out["status"] == "aborted" else [_output(plan, df) for df in accepted]  # fmt: skip


def _seed_unique(plan: _Plan, client: StorageClient, base: str, columns: list[str]) -> None:  # fmt: skip
    """Append mode: the keys already in the sink's parts count as seen (Rust `seed_unique_tracker_for_append`)."""
    if not columns:
        return  # no unique keys: never read the history
    stored = {n: plan.out_map.get(n, n) for n in columns}  # the sink holds output names
    for uri in part_uris(client, base.rstrip("/"), "parquet"):
        part = uri.removeprefix("local://")
        try:  # only the key columns are read
            body = client.read(uri)
            keys = pl.read_parquet(io.BytesIO(body.body if body else b""), columns=list(stored.values()))  # fmt: skip
        except pl.exceptions.PolarsError as err:
            raise FloeRunError(f"entity.name={plan.entity.name} cannot seed unique keys from {part}: {err}") from err  # fmt: skip
        plan.tracker.seed(keys.rename({v: k for k, v in stored.items()}))


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


class Incremental:
    """Claims the entity's inputs, renews the claims while it runs, then promotes or releases them."""

    def __init__(self, entity: EntityConfig, config_dir: Path, run_id: str, full_refresh: bool = False, storages: StoragesConfig | None = None) -> None:  # fmt: skip
        self.entity, self.config_dir, self.run_id = entity, config_dir, run_id
        self.storages = storages
        self.full_refresh = full_refresh
        self.client: StorageClient = LocalClient()
        self.uri = ""
        self.claimed: Loaded | None = None
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
        got = claim_inputs(self.client, self.uri, e.name, self.run_id, inputs)
        reports = skipped_reports(e, got.already_processed, got.active_claims)
        for r in reports:
            emit("file_finished", entity=e.name, input=r["input_file"], status="skipped",
                 skip_reason=r["skip_reason"], rows=0, accepted=0, rejected=0, elapsed_ms=0)  # fmt: skip
        if got.claimed:
            self._hold(got.claimed)
        return got.pending, reports

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
        if claimed := self._end():
            release(self.client, self.uri, self.run_id, claimed)

    def finish(self, files: list[dict[str, Any]]) -> None:
        """Promote unless the entity failed or aborted (Rust commits on success, warnings and rejected)."""
        status, _ = run_outcome([f["status"] for f in files])
        if status in ("failed", "aborted"):
            return self.release()
        if claimed := self._end():
            promote(self.client, self.uri, self.run_id, claimed, self.full_refresh, self.advance)  # fmt: skip


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
        if archive and f.source_uri.startswith(f"{archive}/"):  # a full refresh
            raise FloeRunError(f"entity.name={entity.name} archived inputs keep their partition path only with D29 (#884): {f.source_uri}")  # fmt: skip
        part = partition_of(p, entity.name, root, f.source_uri)
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
        inputs = in_load_order(p, entity.name, root, inputs, start)
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
    sink = entity.sink
    if sink.accepted.format not in ("parquet", "iceberg"):
        raise FloeRunError(f"entity.name={entity.name} unsupported sink format: {sink.accepted.format}")  # fmt: skip
    iceberg = sink.accepted.format == "iceberg"
    if iceberg and sink.write_mode == "merge_scd2":  # #654
        raise FloeRunError(f"entity.name={entity.name} sink.write_mode={sink.write_mode} is not yet supported for iceberg")  # fmt: skip
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
    if sink.write_mode in ("merge_scd1", "merge_scd2") and entity.schema_.primary_key:
        pk = [runtime.get(k.strip(), k.strip()) for k in entity.schema_.primary_key]
        plan.merge_key = (
            pk[0] if len(pk) == 1 else f"[{','.join(pk)}]"
        )  # the tracker's check name
    accepted_client = storage_client(accepted)
    if sink.write_mode == "append" and target is None:
        _seed_unique(
            plan,
            accepted_client,
            base,
            sorted({runtime.get(c, c) for k in keys for c in k}),
        )
    elif sink.write_mode == "append" and target is not None and keys:
        plan.table = existing_table(
            target
        )  # Rust `seed_unique_tracker`; no table, nothing to seed

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
        inc.advance = partial(advance, entity.source.partitioning, entity.name, source_root(source_uri))  # fmt: skip
    archive = plan.archive  # a full refresh lists it too
    plan.derived = _derived(entity, source_uri, archive and archive[0], inputs.files)
    pending, files = inc.claim(inputs.files, emit)
    plan.input_hash = input_hash(entity, pending)
    frames: list[pl.DataFrame] = []
    skipped = len(files)
    downloader = None if isinstance(client, LocalClient) else client
    # Rust `localize_input`: a remote input is read from a local copy
    with tempfile.TemporaryDirectory(prefix="floe-") as temp_dir:
        for f in pending:
            emit("file_started", entity=entity.name, input=f.source_uri)
            timer = time.monotonic()
            local = localize_input(f, downloader, Path(temp_dir))
            report, kept = _run_file(plan, f, local.local_path)
            if local.is_ephemeral:
                local.local_path.unlink()
            emit("file_finished", entity=entity.name, input=f.source_uri, status=report["status"],
                 rows=report["row_count"], accepted=report["accepted_count"],
                 rejected=report["rejected_count"], elapsed_ms=int((time.monotonic() - timer) * 1000))  # fmt: skip
            files.append(report)
            frames += kept
            if report["status"] == "aborted":
                break
    files[skipped:] = sorted(files[skipped:], key=lambda r: r["input_file"] not in plan.prechecked)  # fmt: skip

    table: dict[str, Any] | None = None
    merge: dict[str, Any] | None = None
    if pending and (
        frames or sink.write_mode == "overwrite"
    ):  # Rust writes nothing when every input was skipped
        empty = pl.DataFrame(schema=dict.fromkeys(names, pl.String)).select(
            cast_column(n, c.column_type)[0]
            for n, c in zip(names, columns, strict=True)
        )
        frames = frames or [_output(plan, empty)]
        if target is None:
            write = write_accepted(pl.concat(frames), accepted_client, base, sink.write_mode, sink.accepted.options)  # fmt: skip
        else:
            outputs = [plan.out_map.get(n, n) for n in names]
            result, table, merge = write_iceberg(entity, target, frames, outputs, sink.write_mode, emit.run_id, plan.input_hash)  # fmt: skip
            write = AcceptedWrite(result.files_written, result.file_paths, result.total_bytes_written,
                                  result.avg_file_size_mb, result.small_files_count)  # fmt: skip
    else:
        write = AcceptedWrite(0, [], 0, 0.0, 0)
    inc.committed = files  # #915: from here on a failure promotes the written files
    # #914: archive only once the sink committed, so a failed commit leaves the inputs in the source (Rust archives before)
    by_uri = {r["input_file"]: r for r in files}
    for f in plan.to_archive:
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
    )
    return EntityRun(report, any(f["status"] == "aborted" for f in files))
