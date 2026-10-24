"""Entity `run.json` vs the recorded Rust 0.7 `expected/reports` (python overlay applied).

Each case goes through the readers, mismatch plan, casts, row checks, split and writers directly
(the pipeline is #655); `floe.report.entity` builds the report from those results and the
serialised text is compared with `conformance/check.py`'s helpers (writer byte metrics ignored).
"""

# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportMissingTypeStubs=false
# ^ pyarrow ships no type stubs; check.py is a script on the conformance sys.path

import json
import re
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from floe.checks.cast import CAST_ERROR_MESSAGE, cast_column
from floe.checks.mismatch import apply_mismatch, declared_names, plan_mismatch
from floe.checks.normalize import (
    normalize_columns,
    resolve_strategy,
    source_names,
)
from floe.checks.rows import RowCheck, not_null_check, split_rows
from floe.checks.unique import UniqueTracker, resolve_unique_keys
from floe.config.parse import parse_config
from floe.config.types import ArchiveTarget, EntityConfig
from floe.read.avro import read_avro
from floe.read.csv import read_csv
from floe.read.orc import read_orc
from floe.read.parquet import read_parquet
from floe.read.xlsx import read_xlsx
from floe.read.xml import read_xml
from floe.report.entity import build_report, file_report, serialize
from floe.storage.inputs import ResolvedInputs
from floe.write.outputs import AcceptedWrite, write_accepted, write_rejected

CONFORMANCE = Path(__file__).parents[3] / "conformance"
sys.path.insert(0, str(CONFORMANCE))
from check import comparable, load_expected  # pyright: ignore[reportMissingImports]  # script on the sys.path entry above

UUID_PART = re.compile(r"part-[0-9a-f]{8}-[0-9a-f-]{27}")


def read(entity: EntityConfig, path: Path) -> pl.DataFrame:
    src, fmt = entity.source.options, entity.source.format
    declared = entity.schema_.columns
    if fmt == "csv":
        return pl.concat(c.df for c in read_csv(path, src))
    chunks: Iterator[Any] = {
        "avro": lambda: read_avro(path),
        "orc": lambda: read_orc(path),
        "parquet": lambda: read_parquet(path),
        "xml": lambda: read_xml(path, src, declared),
    }.get(fmt, lambda: iter([]))()
    return read_xlsx(path, src) if fmt == "xlsx" else next(iter(chunks)).df


def run_entity(case: str, entity: EntityConfig, tmp: Path) -> tuple[str, str]:
    """The entity's report text, and the case-relative report path it is recorded under."""
    base = CONFORMANCE / case
    root = "local://<ROOT>/{}"
    src_path = base / entity.source.path
    paths = sorted(src_path.glob("*.csv")) if src_path.is_dir() else [src_path]
    strategy = resolve_strategy(entity.schema_)
    columns = entity.schema_.columns
    names = source_names(columns, strategy)
    tracker = UniqueTracker(
        resolve_unique_keys(
            entity.schema_.unique_keys,
            [c.name for c in columns if c.unique],
            entity.schema_.primary_key,
        )
    )
    out = tmp / "out"
    files: list[dict[str, Any]] = []
    accepted: list[pl.DataFrame] = []
    for i, path in enumerate(paths):
        df = normalize_columns(read(entity, path), strategy)
        # the XML probe (top-level child names) is not ported: the case's are name, addr, price
        inputs = (
            ["name", "addr", "price"] if entity.source.format == "xml" else df.columns
        )
        plan = plan_mismatch(entity, declared_names(entity), inputs)
        uri = root.format(path.relative_to(base).as_posix())
        if (
            plan.rejected or plan.aborted
        ):  # the raw file goes to the rejected dir, no row is read
            accepted.append(pl.DataFrame())
            raw = f"<ROOT>/out/rejected/{entity.name}/{path.name}"
            files.append(file_report(entity, uri, 0, 0, [], plan, raw))
            continue
        df = apply_mismatch(df, plan)
        df = df.select(
            pl.col(n) if n in df.columns else pl.lit(None, pl.String).alias(n)
            for n in names
        )
        casts = {
            n: cast_column(n, c.column_type, entity.source.cast_mode, df.schema[n])
            for n, c in zip(names, columns, strict=True)
        }
        typed = df.with_columns(c[0] for c in casts.values())
        masks = df.select(c[1] for c in casts.values())
        required = [
            n for n, c in zip(names, columns, strict=True)
            if c.nullable is False or n in (entity.schema_.primary_key or [])
        ]  # fmt: skip
        checks = [not_null_check(typed, n) for n in required]
        checks += [
            RowCheck("cast_error", n, CAST_ERROR_MESSAGE, masks[f"_e_cast_{n}"])
            for n, c in zip(names, columns, strict=True)
            if c.column_type != "string"
        ]  # fmt: skip
        checks += tracker.check(typed)
        good, bad = split_rows(typed, checks)
        accepted.append(good)
        rejected_path = None
        if bad.height:
            mode = "overwrite" if i == 0 else "append"
            written = write_rejected(bad, out / "rejected" / entity.name, mode)
            rejected_path = str(written).replace(str(tmp), "<ROOT>")
            rejected_path = UUID_PART.sub("part-<uuid>", rejected_path)
        files.append(file_report(entity, uri, df.height, bad.height, checks, plan, rejected_path))  # fmt: skip
    write = write_accepted(pl.concat(accepted), out / "accepted" / entity.name)
    rel = Path(entity.source.path).as_posix().removeprefix("./")
    listed = [f["input_file"] for f in files]
    resolved = ResolvedInputs([], listed, "directory" if src_path.is_dir() else "file")
    report = build_report(
        entity,
        "0.3",
        resolved,
        files,
        write,
        source_uri=root.format(rel),
        accepted_uri=root.format(f"out/accepted/{entity.name}"),
        rejected_uri=root.format(f"out/rejected/{entity.name}"),
        unique=tracker.results(),
    )
    parts = (entity.domain, entity.name, "run.json")
    return serialize(report) + "\n", "/".join(p for p in parts if p)


CASES = [
    ("casts", "spike.casts"),
    ("mismatch", "spike.customers"),
    ("mismatch_abort", "spike.orders"),
    ("mismatch_reject_file", "spike.orders"),
    ("unique_pii", "spike.orders"),
    ("example/standalone", "sales.orders"),
    ("avro_read", "spike.avro_read"),
    ("orc_read", "spike.orc_read"),
    ("xlsx", "orders"),
    ("xml", "spike.items"),
    *(("casts_typed", f"spike.typed_{k}") for k in (
        "boolean", "date", "datetime", "float32", "float64", "int8", "int32", "int64",
        "string", "uint32", "uint64",
    )),
]  # fmt: skip


@pytest.mark.parametrize(("case", "entity_id"), CASES)
def test_report_matches_recorded(case: str, entity_id: str, tmp_path: Path) -> None:
    entity = next(
        e for e in parse_config(CONFORMANCE / case / "config.yml").entities if e.id == entity_id
    )  # fmt: skip
    text, rel = run_entity(case, entity, tmp_path)
    expected = load_expected(CONFORMANCE / case, "python")
    (name,) = [k for k in expected if k.startswith("reports/") and k.endswith(rel)]
    assert comparable(name, text) == comparable(name, expected[name])
    # the raw text is serde's pretty print: Python's re-dump of it changes nothing
    assert json.dumps(json.loads(text), indent=2, ensure_ascii=False) + "\n" == text


def _build(
    entity: EntityConfig, parts: list[str], **kw: Any
) -> tuple[dict[str, Any], dict[str, Any]]:
    outcome = plan_mismatch(entity, ["id"], ["id"])
    f = file_report(entity, "a.csv", 2, 0, [], outcome, **kw.pop("file", {}))
    write = AcceptedWrite(len(parts), parts, 0, 0.0, 0)
    inputs = ResolvedInputs([], ["a.csv", "b.csv"], "directory")
    return f, build_report(
        entity, "0.3", inputs, [f], write,
        source_uri="s", accepted_uri="acc", rejected_uri=None, **kw,
    )  # fmt: skip


def _entity() -> EntityConfig:
    path = CONFORMANCE / "example/standalone/config.yml"
    return parse_config(path).entities[0]


def test_accepted_path_only_when_parts_were_written() -> None:
    f, _ = _build(_entity(), [])
    assert f["output"]["accepted_path"] is None
    f, _ = _build(_entity(), ["part-00000.parquet"])
    assert f["output"]["accepted_path"] == "acc"


def test_archive_echo_uses_the_resolved_incremental_mode() -> None:
    entity = _entity()
    archive = ArchiveTarget(path="arch", storage=None)
    sink = entity.sink.model_copy(update={"archive": archive})
    implicit = entity.model_copy(update={"sink": sink})
    assert _build(implicit, [])[1]["sink"]["archive"] == {
        "enabled": True,
        "path": "arch",
    }
    explicit = implicit.model_copy(update={"incremental_mode": "file"})
    assert _build(explicit, [])[1]["sink"]["archive"] == {
        "enabled": False,
        "path": "arch",
    }


def test_archived_path_is_reported() -> None:
    f, _ = _build(_entity(), [], file={"archived_path": "arch/a.csv"})
    assert f["output"]["archived_path"] == "arch/a.csv"


def test_errors_path_is_reported() -> None:
    # warn and abort write an error report (reject writes none); the pipeline passes its URI
    f, _ = _build(_entity(), [], file={"errors_path": "rej/errors.json"})
    assert f["output"]["errors_path"] == "rej/errors.json"


def test_files_total_counts_reported_files() -> None:
    assert _build(_entity(), [])[1]["results"]["files_total"] == 1


def test_write_mode_argument_overrides_the_configured_one() -> None:
    assert _build(_entity(), [])[1]["accepted_output"]["write_mode"] == "overwrite"
    report = _build(_entity(), [], write_mode="append")[1]
    assert report["accepted_output"]["write_mode"] == "append"
