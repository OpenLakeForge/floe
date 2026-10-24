"""One entity run: inputs, per-file checks, split, outputs, `run.json` (Rust: `run/entity/*.rs`, local sources and sinks)."""

import shutil
from collections.abc import Iterator
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
from floe.checks.unique import UniqueTracker, resolve_unique_keys
from floe.config.types import EntityConfig
from floe.errors import FloeConfigError, FloeIoError, FloeRunError
from floe.pii import apply_pii_masking
from floe.read.avro import read_avro
from floe.read.csv import RAGGED_MESSAGE, read_csv
from floe.read.orc import read_orc
from floe.read.parquet import read_parquet
from floe.read.xlsx import read_xlsx
from floe.read.xml import read_xml, xml_columns
from floe.report.entity import build_report, file_report
from floe.storage.inputs import ResolvedInputs, resolve_inputs
from floe.storage.local import LocalClient
from floe.storage.paths import normalize_local_path
from floe.write.outputs import AcceptedWrite, write_accepted, write_rejected

Chunks = Iterator[tuple[int, pl.DataFrame, pl.Series | None]]  # fmt: skip  # offset, frame, ragged rows


@dataclass(frozen=True)
class EntityRun:
    report: dict[str, Any]
    aborted: bool


def _chunks(entity: EntityConfig, path: Path, names: list[str]) -> Chunks:
    src = entity.source
    match src.format:
        case "csv":
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
        masks = df.select(m for _, m in casts)
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

    def failed(err: FloeIoError, mismatch: MismatchOutcome | None) -> tuple[dict[str, Any], list[pl.DataFrame]]:  # fmt: skip
        # Rust skips the raw copy under PII: the file cannot be masked
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
        out = file_report(entity, uri, 0, 0, [], mismatch, raw)
        out["status"] = "aborted" if mismatch.aborted else "rejected"
        return out, []

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
    out = file_report(entity, uri, rows, bad_total, merged, mismatch, rejected_path)
    out["output"]["errors_path"] = errors_path
    return out, [] if out["status"] == "aborted" else [_output(plan, df) for df in accepted]  # fmt: skip


def run_entity(
    entity: EntityConfig,
    config_dir: Path,
    spec_version: str,
    formatter: Formatter = "json",
) -> EntityRun:
    """Run one entity with local sources and sinks; `config_dir` is absolute and normalised."""
    if entity.sink.accepted.format != "parquet":
        raise FloeRunError(
            f"entity.name={entity.name} unsupported sink format: {entity.sink.accepted.format}"
        )
    if entity.incremental_mode in ("file", "row"):
        # ponytail: incremental state lands with #648; refuse rather than re-ingest every run
        raise FloeRunError(f"entity.name={entity.name} incremental_mode {entity.incremental_mode} is not supported yet")  # fmt: skip
    src, sink = entity.source, entity.sink
    strategy = resolve_strategy(entity.schema_)
    columns = entity.schema_.columns
    names = source_names(columns, strategy)
    runtime = {c.name: n for c, n in zip(columns, names, strict=True)}
    keys = resolve_unique_keys(
        entity.schema_.unique_keys,
        [c.name for c in columns if c.unique],
        entity.schema_.primary_key,
    )
    primary = {runtime.get(k, k) for k in entity.schema_.primary_key or []}
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
    )  # fmt: skip
    client = LocalClient()
    inputs: ResolvedInputs = resolve_inputs(
        config_dir, src.path, src.format, entity.name, src.storage or "local", client,
        recursive=bool(src.options.recursive), glob_override=src.options.glob,
    )  # fmt: skip
    files: list[dict[str, Any]] = []
    frames: list[pl.DataFrame] = []
    for f in inputs.files:
        report, kept = _run_file(plan, f.source_uri, Path(f.source_uri.removeprefix("local://")))  # fmt: skip
        files.append(report)
        frames += kept
        if report["status"] == "aborted":
            break

    base = normalize_local_path(config_dir / sink.accepted.path)
    if frames or sink.write_mode == "overwrite":
        empty = pl.DataFrame(schema=dict.fromkeys(names, pl.String)).select(
            cast_column(n, c.column_type)[0]
            for n, c in zip(names, columns, strict=True)
        )
        frame = pl.concat(frames) if frames else _output(plan, empty)
        write = write_accepted(frame, base, sink.write_mode, sink.accepted.options)
    else:
        write = AcceptedWrite(0, [], 0, 0.0, 0)
    rejected = sink.rejected and normalize_local_path(config_dir / sink.rejected.path)
    unique = [
        replace(r, columns=key)
        for r, key in zip(plan.tracker.results(), keys, strict=True)
    ]  # the report names the schema columns
    report = build_report(
        entity,
        spec_version,
        inputs,
        files,
        write,
        source_uri=f"local://{normalize_local_path(config_dir / src.path)}",
        accepted_uri=f"local://{base}",
        rejected_uri=f"local://{rejected}" if rejected else None,
        unique=unique,
    )
    return EntityRun(report, any(f["status"] == "aborted" for f in files))
