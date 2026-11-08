"""Per-file pipeline of an entity run: read, check, split and write each input file."""

import time
from collections.abc import Generator, Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from itertools import chain
from pathlib import Path
from typing import TYPE_CHECKING, Any

import polars as pl

from floe.checks.cast import CAST_ERROR_MESSAGE, cast_column
from floe.checks.mismatch import (
    MismatchOutcome,
    apply_mismatch,
    declared_names,
    json_probe,
    plan_mismatch,
)
from floe.checks.normalize import (
    normalize_columns,
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
    UniqueTracker,
    seed_from_history,
)
from floe.config.types import EntityConfig
from floe.errors import FloeConfigError, FloeIoError, FloeRunError
from floe.pii import apply_pii_masking
from floe.read.avro import read_avro
from floe.read.csv import RAGGED_MESSAGE, read_csv
from floe.read.json import JSON_CHUNK_ROWS, JSON_SINGLE_PASS_BYTES, read_json
from floe.read.orc import read_orc
from floe.read.parquet import read_parquet
from floe.read.xml import read_xml, xml_columns
from floe.report.entity import file_report
from floe.storage.inputs import (
    InputFile,
    input_hash,
)
from floe.storage.types import StorageClient
from floe.write.outputs import (
    write_rejected,
    write_rejected_parquet,
)

if TYPE_CHECKING:
    from pyiceberg.table import Table

    from floe.iceberg.sink import IcebergTarget

Chunks = Iterator[tuple[int, pl.DataFrame, pl.Series | None]]  # fmt: skip  # offset, frame, ragged rows


@contextmanager
def timed(spent: dict[str, float], phase: str) -> Generator[None]:
    started = time.monotonic()
    try:
        yield
    finally:
        spent[phase] = spent.get(phase, 0.0) + time.monotonic() - started


@contextmanager
def sink_write(plan: "Plan", sink: str, phase: str) -> Generator[None]:
    """`timed`, and the sink's commit state for `run.failure.json`: unknown once the write starts, committed only once it returned (#1155)."""
    plan.sinks[sink] = "unknown"
    with timed(plan.spent, phase):
        yield
    plan.sinks[sink] = "committed"


def _chunks(entity: EntityConfig, path: Path, names: list[str], head: list[tuple[str, Any]] | None = None) -> Chunks:  # fmt: skip
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
            from floe.read.xlsx import read_xlsx  # the xlsx extra

            yield 0, read_xlsx(path, src.options), None
        case "xml":
            for x in read_xml(path, src.options, entity.schema_.columns[: len(names)]):
                yield x.offset, x.df, None
        case "json":  # #998
            for j in read_json(path, src.options, entity.schema_.columns[: len(names)], src.cast_mode, JSON_CHUNK_ROWS, head):  # fmt: skip
                yield j.offset, j.df, None
        case other:
            raise FloeRunError(
                f"entity.name={entity.name} unsupported source format: {other}"
            )


def _read_rule(fmt: str, error: FloeIoError) -> str:
    """Rust's `FileReadError.rule`: the reader's own, else its format's parse-error rule."""
    return error.rule or {"xml": "xml_parse_error", "json": "json_parse_error"}.get(fmt, f"{fmt}_read_error")  # fmt: skip


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
            "rule": (rule := _read_rule(fmt, error)),
            "message": f"entity.name={entity.name} {str(error).removeprefix(f'{rule}: ')}",
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


def _rejected(plan: "Plan", severity: str) -> tuple[str, StorageClient]:
    """The rejected sink's URI and client."""
    if plan.rejected is None:
        raise FloeConfigError(f"sink.rejected is required for {severity} severity")
    return plan.rejected


def _put(plan: "Plan", severity: str, name: str, body: bytes) -> str:
    """Write `<rejected>/<name>`; the report shows a local target as a path, as Rust."""
    base, client = _rejected(plan, severity)
    uri = f"{base.rstrip('/')}/{name}"
    with sink_write(plan, "rejected", "rejected_write_ms"):
        client.write(uri, body)
    return uri.removeprefix("local://")


def _copy_raw(plan: "Plan", source: Path, severity: str, name: str) -> str:
    # ponytail: the raw file is read into memory for the put; stream it (upload_file) if rejected files get large
    return _put(plan, severity, name, source.read_bytes())


def _error_report(plan: "Plan", stem: str, rows: list[tuple[int, str]]) -> str:
    """`<stem>_reject_errors.json`, Rust's `write_error_report`."""
    items = ",".join(f'{{"row_index":{i},"errors":{e}}}' for i, e in rows)
    return _put(plan, plan.entity.policy.severity, f"{stem}_reject_errors.json", f"[{items}]".encode())  # fmt: skip


@dataclass
class Plan:
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
    sinks: dict[str, str] = field(default_factory=lambda: {"rejected": "not_started", "accepted": "not_started"})  # fmt: skip  # #1155: `run()` passes its own, to report a failure
    spent: dict[str, float] = field(
        default_factory=dict[str, float]
    )  # seconds per phase, see PHASES
    prechecked: set[str] = field(
        default_factory=set[str]
    )  # files the precheck settles (unreadable header, rejected or aborted mismatch); Rust settles them before it reads any file
    archive: tuple[str, StorageClient] | None = None  # Rust `archive_enabled()`
    to_archive: list[InputFile] = field(default_factory=list[InputFile])  # fmt: skip  # moved after the sink commit (#914)
    run_id: str = ""
    rejected: tuple[str, StorageClient] | None = None  # `sink.rejected` URI and client
    table: "Table | None" = (
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
    rejected_table: "IcebergTarget | None" = None  # an Iceberg `sink.rejected`
    rejected_spills: list[tuple[Path, str]] = field(default_factory=list[tuple[Path, str]])  # fmt: skip  # its spilled chunks with their source file; one commit writes them all


def output(plan: Plan, df: pl.DataFrame) -> pl.DataFrame:
    """Output column names, then PII masking."""
    df = df.rename(plan.out_map)
    pii = plan.entity.pii
    return apply_pii_masking(df, pii.columns, plan.pii_map) if pii else df


_CSV_TYPES = {
    pl.String,
    pl.Boolean,
    pl.Null,
    pl.Date,
    pl.Datetime,
    pl.Time,
    pl.Categorical,
    pl.Enum,
}


def _parquet_rejected(plan: Plan) -> bool:
    return plan.entity.sink.rejected is not None and plan.entity.sink.rejected.format == "parquet"  # fmt: skip


def _raw_rejected(plan: Plan) -> bool:
    """Parquet and Iceberg rejects hold the raw parsed values and spill every chunk."""
    return plan.entity.sink.rejected is not None and plan.entity.sink.rejected.format in ("parquet", "iceberg")  # fmt: skip


def _typed(
    plan: Plan, raw: pl.DataFrame, mismatch: MismatchOutcome, derived: list[pl.Expr]
) -> tuple[pl.DataFrame, pl.DataFrame, list[RowCheck]]:
    """The typed frame, the same rows before the casts (what the rejected sink holds) and the row checks."""
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
    # the rejected CSV holds only these types; any other source column (binary, duration, nested ...) keeps its typed value there. A rejected Parquet keeps the native value as it is; an Iceberg one casts it to string
    if unwritable := [] if _parquet_rejected(plan) else [n for n in plan.names if not (df.schema[n].is_numeric() or df.schema[n].base_type() in _CSV_TYPES)]:  # fmt: skip
        df = df.with_columns(typed[n] for n in unwritable)
    if entity.policy.severity == "abort" and any(c.mask.any() for c in cast_checks):
        return typed, df, cast_checks  # fmt: skip  # Rust: abort-mode cast errors skip not_null and unique
    required = [not_null_check(typed, n) for n in plan.required]
    if plan.history is not None:
        seed_from_history(plan.tracker, plan.history, typed, plan.out_map)
    return typed, df, [*required, *cast_checks, *plan.tracker.check(typed)]


def source_columns(plan: Plan, uri: str) -> list[pl.Expr]:
    return [pl.lit(plan.run_id).alias("__floe_run_id"), pl.lit(uri).alias("__floe_source_uri")]  # fmt: skip


def unspill(path: Path) -> pl.DataFrame:
    df = pl.read_ipc(path)
    path.unlink()
    return df


def _replay(plan: Plan, held: list[pl.DataFrame | Path]) -> Iterator[pl.DataFrame]:
    while held:
        item = held.pop(0)
        yield output(plan, item if isinstance(item, pl.DataFrame) else unspill(item))


def run_file(plan: Plan, file: InputFile, path: Path, spill: Path) -> tuple[dict[str, Any], Iterable[pl.DataFrame]]:  # fmt: skip
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
        # a file removed after listing has nothing to copy: the read error alone is reported
        keep = entity.pii is None and entity.sink.rejected is not None and path.exists()
        raw = _copy_raw(plan, path, severity, file.source_name) if keep else None
        return _failed_file(entity, uri, len(declared) + len(derived), mismatch, err, raw), []  # fmt: skip

    try:
        if entity.source.format == "xml":
            first, probe = [], xml_columns(path, entity.source.options)
        elif entity.source.format == "json":
            # D40: every record's top-level keys; D41: only a file without records has nothing to compare
            # a file listed as small is parsed once: the read reuses the probe's records (limit 0 keeps none)
            json_keys, head = json_probe(path, entity.source.options.json_mode or "array", JSON_CHUNK_ROWS if file.source_size is not None and file.source_size <= JSON_SINGLE_PASS_BYTES else 0)  # fmt: skip
            chunks = _chunks(entity, path, file_names, head)
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
    spilled_rejected: list[Path] = []  # a Parquet rejected sink spills every chunk
    errors: list[tuple[int, str]] = []
    try:
        for offset, raw_df, ragged in chain(first, chunks):
            typed, pre_cast, checks = _typed(plan, raw_df, mismatch, derived)
            if ragged is not None and ragged.any():
                checks.insert(0, RowCheck("csv_read_error", "", RAGGED_MESSAGE, ragged))
            good, bad = split_rows(
                typed, checks, plan.formatter, plan.sources, pre_cast
            )
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
                if _raw_rejected(plan):
                    bad.write_ipc(spilled := spill / f"rejected-{len(plan.rejected_spills) + len(spilled_rejected)}.ipc")  # fmt: skip
                    spilled_rejected.append(spilled)
                else:
                    rejected.append(bad)
    except FloeIoError as err:
        return failed(err, mismatch)

    merged = [RowCheck(r, c, m, pl.concat(s)) for (r, c, m), s in masks.items()]
    rejected_path = errors_path = None
    if bad_total and severity == "reject" and plan.rejected_table is not None:
        plan.rejected_spills += [
            (p, uri) for p in spilled_rejected
        ]  # one commit for the run
        rejected_path = _rejected(plan, "reject")[0].rstrip("/")
    elif bad_total and severity == "reject":
        mode = entity.sink.write_mode
        if mode == "overwrite" and plan.rejected_writes:
            mode = "append"
        plan.rejected_writes += 1
        target, client = _rejected(plan, "reject")
        key = input_hash(
            entity, [file]
        )  # per file, so a retry that claims a larger set overwrites the dead run's parts (#1197)
        with sink_write(plan, "rejected", "rejected_write_ms"):
            if _parquet_rejected(plan):
                frames = (output(plan, unspill(p)).with_columns(source_columns(plan, uri)) for p in spilled_rejected)  # fmt: skip
                written = f"{target.rstrip('/')}/rows"  # apart from the raw copies in the rejected directory; a file can have many parts  # fmt: skip
                write_rejected_parquet(frames, client, written, mode, key)
            else:
                written = write_rejected(output(plan, pl.concat(rejected)), client, target, mode, key)  # fmt: skip
        rejected_path = written.removeprefix("local://")
    elif bad_total and severity == "abort":
        rejected_path = _copy_raw(plan, path, "abort", file.source_name)
    if bad_total and entity.sink.rejected and severity != "reject":
        errors_path = _error_report(plan, file.source_stem, errors)
    plan.to_archive.append(file)
    out = file_report(entity, uri, rows, bad_total, merged, mismatch, rejected_path, None, errors_path, forced_total)  # fmt: skip
    return out, [] if out["status"] == "aborted" else _replay(plan, accepted)
