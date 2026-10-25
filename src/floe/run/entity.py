"""One entity run: inputs, per-file checks, split, outputs, `run.json` (Rust: `run/entity/*.rs`, local sources and sinks)."""

import shutil
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import suppress
from dataclasses import dataclass, replace
from itertools import chain
from pathlib import Path
from typing import Any

import polars as pl

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
from floe.config.storage import CatalogsConfig, StoragesConfig
from floe.config.types import EntityConfig
from floe.errors import FloeConfigError, FloeIoError, FloeRunError
from floe.iceberg.sink import input_hash, resolve_target, write_iceberg
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
    FileState,
    Loaded,
    claim_all,
    claim_inputs,
    promote,
    release,
    renew,
)
from floe.storage.archive import archive_input_file
from floe.storage.inputs import (
    InputFile,
    ResolvedInputs,
    resolve_inputs,
    suffixes_for_format,
)
from floe.storage.local import LocalClient
from floe.storage.paths import normalize_local_path
from floe.storage.resolve import require_local
from floe.storage.types import StorageClient
from floe.write.outputs import AcceptedWrite, write_accepted, write_rejected

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
            for x in read_xml(path, src.options, entity.schema_.columns):
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


def _rejected_dir(entity: EntityConfig, config_dir: Path, severity: str) -> Path:
    if entity.sink.rejected is None:
        raise FloeConfigError(f"sink.rejected is required for {severity} severity")
    return normalize_local_path(config_dir / entity.sink.rejected.path)


def _copy_raw(entity: EntityConfig, config_dir: Path, source: Path, severity: str) -> str:  # fmt: skip
    target = _rejected_dir(entity, config_dir, severity) / source.name
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, target)
    return str(target)


def _error_report(
    config_dir: Path, entity: EntityConfig, stem: str, rows: list[tuple[int, str]]
) -> str:
    """`<stem>_reject_errors.json`, Rust's `write_error_report`."""
    target = _rejected_dir(entity, config_dir, entity.policy.severity)
    target.mkdir(parents=True, exist_ok=True)
    path = target / f"{stem}_reject_errors.json"
    items = ",".join(f'{{"row_index":{i},"errors":{e}}}' for i, e in rows)
    path.write_text(f"[{items}]")
    return str(path)


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
    archive_dir: Path | None = None  # Rust `archive_enabled()`
    run_id: str = ""


def _archive(plan: _Plan, uri: str, path: Path) -> str | None:
    """Move the source file to `<archive>/<entity>/<stem>__run-…__src-….<ext>` (Rust `archive_input`)."""
    archive = plan.archive_dir
    if archive is None or path.is_relative_to(archive):
        return None  # D24: an archived input stays put
    file = InputFile(uri, path.name, path.stem, None, None)
    return archive_input_file(LocalClient(), plan.run_id, plan.entity.name, lambda rel: str(archive / rel), file)  # fmt: skip


def _with_archived(plan: _Plan, archive: Path, source: Callable[[], ResolvedInputs]) -> ResolvedInputs:  # fmt: skip
    """D24: a full refresh reads `<archive>/<entity>/` first (by name), then the source inputs; either may be empty, not both."""
    e = plan.entity
    try:
        old = resolve_inputs(archive, e.name, e.source.format, e.name, e.source.storage or "local", LocalClient())  # fmt: skip
    except FloeRunError:
        return (
            source()
        )  # nothing archived: the source's "no input files matched" stands
    try:
        new = source()
    except FloeRunError:  # no new source file: rebuild from the archive alone
        return old
    return ResolvedInputs(old.files + new.files, old.listed + new.listed, new.mode)


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
    return typed, [*required, *cast_checks, *plan.tracker.check(typed)]


def _run_file(plan: _Plan, uri: str, path: Path) -> tuple[dict[str, Any], list[pl.DataFrame]]:  # fmt: skip
    """The file's report and its accepted frames (none when it aborted the run)."""
    entity = plan.entity
    severity = entity.policy.severity
    declared = declared_names(entity)
    chunks = _chunks(entity, path, plan.names)
    keys = plan.tracker.snapshot()

    def failed(err: FloeIoError, mismatch: MismatchOutcome | None) -> tuple[dict[str, Any], list[pl.DataFrame]]:  # fmt: skip
        # Rust skips the raw copy under PII: the file cannot be masked
        plan.tracker.restore(keys)  # the unreadable file leaves no keys behind
        keep = entity.pii is None and entity.sink.rejected is not None
        raw = _copy_raw(entity, plan.config_dir, path, severity) if keep else None
        return _failed_file(entity, uri, len(declared), mismatch, err, raw), []

    try:
        if entity.source.format == "xml":
            first, probe = [], xml_columns(path, entity.source.options)
        else:  # the first chunk's columns are the input columns the plan compares
            first = [next(chunks)]
            probe = list(first[0][1].columns)
    except FloeIoError as err:
        return failed(err, None)
    mismatch = plan_mismatch(entity, declared, probe)
    if mismatch.rejected or mismatch.aborted:
        raw = _copy_raw(
            entity, plan.config_dir, path, "abort" if mismatch.aborted else "reject"
        )
        return file_report(entity, uri, 0, 0, [], mismatch, raw, _archive(plan, uri, path)), []  # fmt: skip

    rows, bad_total = 0, 0
    masks: dict[tuple[str, str, str], list[pl.Series]] = {}
    accepted: list[pl.DataFrame] = []
    rejected: list[pl.DataFrame] = []
    errors: list[tuple[int, str]] = []
    try:
        for offset, raw_df, ragged in chain(first, chunks):
            typed, checks = _typed(plan, raw_df, mismatch)
            if ragged is not None and ragged.any():
                checks.insert(0, RowCheck("csv_read_error", "", RAGGED_MESSAGE, ragged))
            good, bad = split_rows(typed, checks, plan.formatter, plan.sources)
            bad = bad.with_columns(pl.col(ROW_INDEX) + offset)
            rows += typed.height
            bad_total += bad.height
            for c in checks:
                masks.setdefault((c.rule, c.column, c.message), []).append(c.mask)
            errors += zip(bad[ROW_INDEX].to_list(), bad[ERRORS].to_list(), strict=True)
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
        target = _rejected_dir(entity, plan.config_dir, "reject")
        written = write_rejected(_output(plan, pl.concat(rejected)), target, mode)
        rejected_path = str(written)
    elif bad_total and severity == "abort":
        rejected_path = _copy_raw(entity, plan.config_dir, path, "abort")
    if bad_total and entity.sink.rejected and severity != "reject":
        stem = path.stem
        errors_path = _error_report(plan.config_dir, entity, stem, errors)
    archived = _archive(
        plan, uri, path
    )  # Rust: after the rejected outputs, before the rows reach the sink
    out = file_report(entity, uri, rows, bad_total, merged, mismatch, rejected_path, archived, errors_path)  # fmt: skip
    return out, [] if out["status"] == "aborted" else [_output(plan, df) for df in accepted]  # fmt: skip


def _seed_unique(plan: _Plan, base: Path, columns: list[str]) -> None:
    """Append mode: the keys already in the sink's parts count as seen (Rust `seed_unique_tracker_for_append`)."""
    stored = {n: plan.out_map.get(n, n) for n in columns}  # the sink holds output names
    for part in sorted(base.glob("part-*.parquet")):
        try:  # only the key columns are read
            keys = pl.read_parquet(part, columns=list(stored.values()))
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


def _source_root(raw: str, fmt: str, resolved: Path) -> str:
    """Rust `derive_source_root`: the directory the default state lives under."""
    trimmed = raw.rstrip(_SEPARATORS)
    if not trimmed:
        return ""
    cut = next((i for i, c in enumerate(trimmed) if c in "*?["), -1)
    if cut >= 0:
        prefix = trimmed[:cut]
        done = prefix if not prefix or prefix[-1] in _SEPARATORS else _parent(prefix) or prefix  # fmt: skip
        return done.rstrip(_SEPARATORS)
    if resolved.is_dir():
        return trimmed
    if resolved.is_file() or trimmed.lower().endswith(tuple(suffixes_for_format(fmt))):
        # #813: a bare filename lives in the config dir (Rust 0.7 used the file itself)
        return (_parent(trimmed) or ".").rstrip(_SEPARATORS)
    return trimmed


def state_uri(entity: EntityConfig, config_dir: Path) -> str:
    """`state.path` if set, else `<source root>/.floe/state/<domain>/<name>/state.json`; local only."""
    custom = entity.state and entity.state.path
    if custom:
        if "://" in custom:
            raise FloeConfigError(f"entity.name={entity.name} entity.state.path must be a local path (got {custom})")  # fmt: skip
        return f"local://{normalize_local_path(config_dir / custom)}"
    src = entity.source
    root = _source_root(
        src.path, src.format, normalize_local_path(config_dir / src.path)
    )
    ident = f"{entity.domain}/{entity.name}" if entity.domain and entity.id != entity.name else entity.name  # fmt: skip
    tail = f".floe/state/{ident}/state.json"
    return f"local://{normalize_local_path(config_dir / (f'{root}/{tail}' if root and root != '.' else tail))}"  # fmt: skip


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

    def __init__(self, entity: EntityConfig, config_dir: Path, run_id: str, full_refresh: bool = False) -> None:  # fmt: skip
        self.entity, self.config_dir, self.run_id = entity, config_dir, run_id
        self.full_refresh = full_refresh
        self.client: StorageClient = LocalClient()
        self.uri = ""
        self.claimed: Loaded | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def claim(self, inputs: list[InputFile], emit: Emitter) -> tuple[list[InputFile], list[dict[str, Any]]]:  # fmt: skip
        """The inputs left to process and the reports of the skipped ones; `row` mode and a full refresh never skip (Rust 0.7)."""
        e = self.entity
        if e.incremental_mode != "file":
            return inputs, []
        self.uri = state_uri(e, self.config_dir)
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

    def finish(self, report: dict[str, Any]) -> None:
        """Promote unless the entity failed or aborted (Rust commits on success, warnings and rejected)."""
        status, _ = run_outcome([f["status"] for f in report["files"]])
        if status in ("failed", "aborted"):
            return self.release()
        if claimed := self._end():
            promote(self.client, self.uri, self.run_id, claimed, self.full_refresh)


def run_entity(
    entity: EntityConfig,
    config_dir: Path,
    spec_version: str,
    formatter: Formatter = "json",
    storages: StoragesConfig | None = None,
    emit: Emitter = NO_EVENTS,
    full_refresh: bool = False,
    catalogs: CatalogsConfig | None = None,
) -> EntityRun:
    """Run one entity with local sources and local or Iceberg sinks; `config_dir` is absolute and normalised."""
    inc = Incremental(entity, config_dir, emit.run_id, full_refresh)
    try:
        run = _run_entity(
            entity, config_dir, spec_version, formatter, storages, emit, inc, catalogs
        )
    except BaseException:
        inc.release()  # a failed entity leaves no claims behind
        raise
    inc.finish(run.report)
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
) -> EntityRun:
    src, sink = entity.source, entity.sink
    if sink.accepted.format not in ("parquet", "iceberg"):
        raise FloeRunError(f"entity.name={entity.name} unsupported sink format: {sink.accepted.format}")  # fmt: skip
    iceberg = sink.accepted.format == "iceberg"
    if iceberg and sink.write_mode not in ("overwrite", "append"):
        raise FloeRunError(f"entity.name={entity.name} sink.write_mode={sink.write_mode} is not yet supported for iceberg")  # fmt: skip
    who = f"entity.name={entity.name}"
    require_local(storages, src.storage, f"{who} source.storage")
    target = resolve_target(storages, catalogs, config_dir, entity) if iceberg else None
    if target is None:  # PyIceberg writes the table's files, not Floe's storage clients
        require_local(storages, sink.accepted.storage, f"{who} sink.accepted.storage")
    if sink.rejected:
        require_local(storages, sink.rejected.storage, f"{who} sink.rejected.storage")
    # sink.archive.storage must equal source.storage (validated), so the source check covers it
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
        run_id=emit.run_id,
    )  # fmt: skip
    if sink.archive and entity.incremental_mode in ("none", "archive"):
        plan.archive_dir = normalize_local_path(config_dir / sink.archive.path)
    base = normalize_local_path(config_dir / sink.accepted.path)
    if sink.write_mode == "append" and target is None:
        _seed_unique(plan, base, sorted({runtime.get(c, c) for k in keys for c in k}))
    client = LocalClient()

    def source() -> ResolvedInputs:
        return resolve_inputs(
            config_dir, src.path, src.format, entity.name, src.storage or "local", client,
            recursive=bool(src.options.recursive), glob_override=src.options.glob,
        )  # fmt: skip

    archive = plan.archive_dir
    inputs = _with_archived(plan, archive, source) if inc.full_refresh and archive else source()  # fmt: skip
    pending, files = inc.claim(inputs.files, emit)
    frames: list[pl.DataFrame] = []
    for f in pending:
        emit("file_started", entity=entity.name, input=f.source_uri)
        timer = time.monotonic()
        report, kept = _run_file(plan, f.source_uri, Path(f.source_uri.removeprefix("local://")))  # fmt: skip
        emit("file_finished", entity=entity.name, input=f.source_uri, status=report["status"],
             rows=report["row_count"], accepted=report["accepted_count"],
             rejected=report["rejected_count"], elapsed_ms=int((time.monotonic() - timer) * 1000))  # fmt: skip
        files.append(report)
        frames += kept
        if report["status"] == "aborted":
            break

    table: dict[str, Any] | None = None
    if pending and (
        frames or sink.write_mode == "overwrite"
    ):  # Rust writes nothing when every input was skipped
        empty = pl.DataFrame(schema=dict.fromkeys(names, pl.String)).select(
            cast_column(n, c.column_type)[0]
            for n, c in zip(names, columns, strict=True)
        )
        frames = frames or [_output(plan, empty)]
        if target is None:
            write = write_accepted(pl.concat(frames), base, sink.write_mode, sink.accepted.options)  # fmt: skip
        else:
            mode = "overwrite" if sink.write_mode == "overwrite" else "append"
            outputs = [plan.out_map.get(n, n) for n in names]
            result, table = write_iceberg(entity, target, frames, outputs, mode, emit.run_id, input_hash(entity, pending))  # fmt: skip
            write = AcceptedWrite(result.files_written, result.file_paths, result.total_bytes_written,
                                  result.avg_file_size_mb, result.small_files_count)  # fmt: skip
    else:
        write = AcceptedWrite(0, [], 0, 0.0, 0)
    rejected = sink.rejected and normalize_local_path(config_dir / sink.rejected.path)
    unique = [_report_names(r, key, runtime) for r, key in zip(plan.tracker.results(), keys, strict=True)]  # fmt: skip
    report = build_report(
        entity,
        spec_version,
        inputs,
        files,
        write,
        source_uri=f"local://{normalize_local_path(config_dir / src.path)}",
        accepted_uri=target.accepted_uri if target else f"local://{base}",
        rejected_uri=f"local://{rejected}" if rejected else None,
        unique=unique,
        table_root_uri=target.location if target and table else None,
        table=table,
    )
    return EntityRun(report, any(f["status"] == "aborted" for f in files))
