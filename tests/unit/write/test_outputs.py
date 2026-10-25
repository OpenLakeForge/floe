"""Writers vs the recorded Rust 0.7 `expected/accepted` and `expected/rejected` (python overlay applied).

Each case's input goes through the readers' output shape, casts, row checks and split, is written
by `floe.write.outputs` and compared with `conformance/check.py`'s helpers (Arrow-rendered Parquet,
CSV byte for byte).
"""

# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportMissingTypeStubs=false
# ^ pyarrow ships no type stubs; check.py is a script on the conformance sys.path

import re
import sys
from collections.abc import Callable
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from floe.checks.cast import CAST_ERROR_MESSAGE, cast_column
from floe.checks.rows import RowCheck, not_null_check, split_rows
from floe.checks.unique import UniqueTracker
from floe.config.types import SinkOptions, WriteMode
from floe.errors import FloeError
from floe.pii import PiiColumn, apply_pii_masking
from floe.read.avro import read_avro
from floe.read.orc import read_orc
from floe.read.parquet import read_parquet
from floe.write.outputs import write_accepted, write_rejected

CONFORMANCE = Path(__file__).parents[3] / "conformance"
sys.path.insert(0, str(CONFORMANCE))
from check import comparable, load_expected  # pyright: ignore[reportMissingImports]  # script on the sys.path entry above

UUID_PART = re.compile(r"part-[0-9a-f]{8}-[0-9a-f-]{27}")
Columns = list[tuple[str, str]]


def process(
    raw: pl.DataFrame,
    columns: Columns,
    required: list[str],
    tracker: UniqueTracker | None = None,
    pii: list[PiiColumn] | None = None,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Declared columns (missing ones null), casts, not_null, cast_error, unique, split, then PII."""
    raw = raw.select(
        [
            pl.col(n) if n in raw.columns else pl.lit(None, pl.String).alias(n)
            for n, _ in columns
        ]
    )
    casts = {n: cast_column(n, k, "strict", raw.schema[n]) for n, k in columns}
    typed = raw.with_columns(c[0] for c in casts.values())
    masks = raw.select(c[1] for c in casts.values())
    checks = [not_null_check(typed, n) for n in required]
    checks += [
        RowCheck("cast_error", n, CAST_ERROR_MESSAGE, masks[f"_e_cast_{n}"])
        for n, k in columns
        if k != "string"
    ]
    if tracker is not None:
        checks += tracker.check(typed)
    accepted, rejected = split_rows(typed, checks)
    return (
        apply_pii_masking(accepted, pii or [], {}),
        apply_pii_masking(rejected, pii or [], {}),
    )


def emit(
    base: Path,
    entity: str,
    frames: tuple[pl.DataFrame, pl.DataFrame],
    mode: WriteMode = "overwrite",
) -> None:
    accepted, rejected = frames
    write_accepted(accepted, base / "accepted" / entity, mode)
    if rejected.height:
        write_rejected(rejected, base / "rejected" / entity, mode)


def check(case: str, base: Path, entities: list[str]) -> None:
    """Compare what `emit` wrote with the case's `expected/accepted` and `expected/rejected`."""
    expected = {
        k: v
        for k, v in load_expected(CONFORMANCE / case, "python").items()
        if not k.startswith("reports/")
        and k != "manifest.json"
        and k.split("/")[1].removesuffix(".parquet") in entities
    }
    actual: dict[str, object] = {}
    for e in entities:
        parts = sorted((base / "accepted" / e).glob("part-*.parquet"))
        if parts:
            actual[f"accepted/{e}.parquet"] = pa.concat_tables(
                [pq.read_table(p) for p in parts]
            )
        for p in sorted((base / "rejected" / e).glob("*.csv")):
            name = UUID_PART.sub("part-UUID", p.name)
            actual[f"rejected/{e}/{name}"] = p.read_text()
    assert sorted(actual) == sorted(expected)
    for name, value in expected.items():
        assert comparable(name, actual[name]) == comparable(name, value), name


def test_casts(tmp_path: Path) -> None:
    columns = [("id", "string"), ("qty", "int64"), ("price", "number"), ("active", "boolean"),
               ("day", "date"), ("ts", "datetime"), ("small", "int32"), ("note", "string")]  # fmt: skip
    raw = pl.read_csv(CONFORMANCE / "casts/in/casts.csv", infer_schema=False)
    emit(tmp_path, "casts", process(raw, columns, ["id", "qty"]))
    check("casts", tmp_path, ["casts"])


def test_mismatch(tmp_path: Path) -> None:
    columns = [("tenant", "string"), ("code", "int64"), ("phone", "string"),
               ("score", "number"), ("signup", "date"), ("missing_col", "string")]  # fmt: skip
    path = CONFORMANCE / "mismatch/in/customers.csv"
    raw = pl.read_csv(path, separator=";", infer_schema=False)
    frames = process(
        raw,
        columns,
        ["tenant", "code"],
        UniqueTracker([["tenant", "code"]]),
        [PiiColumn("phone", "hash")],
    )
    emit(tmp_path, "customers", frames)
    check("mismatch", tmp_path, ["customers"])


def test_unique_pii_overwrite_then_append(tmp_path: Path) -> None:
    columns = [("customer_id", "int64"), ("email", "string"), ("region", "string"),
               ("order_no", "int64"), ("amount", "number")]  # fmt: skip
    tracker = UniqueTracker([["customer_id"], ["region", "order_no"]])
    pii = [PiiColumn("email", "hash", key="spike-hmac-key")]
    runs: list[tuple[str, WriteMode]] = [("a.csv", "overwrite"), ("b.csv", "append")]
    for name, mode in runs:
        raw = pl.read_csv(CONFORMANCE / "unique_pii/in" / name, infer_schema=False)
        frames = process(raw, columns, ["customer_id", "amount"], tracker, pii)
        emit(tmp_path, "orders", frames, mode)
    check("unique_pii", tmp_path, ["orders"])


def test_casts_typed(tmp_path: Path) -> None:
    [chunk] = list(read_parquet(CONFORMANCE / "casts_typed/in/casts_typed.parquet"))
    accepted = (CONFORMANCE / "casts_typed/expected/accepted").glob("typed_*.parquet")
    # typed_string is left out: the cast layer renders 1e20 as "1e+20", Rust as "1e20" (not a writer matter)
    entities = sorted(p.stem for p in accepted if p.stem != "typed_string")
    for entity in entities:
        kind = entity.removeprefix("typed_")
        names = chunk.df.columns[:5] if kind == "boolean" else chunk.df.columns
        columns = [("id", "string")] + [(c, kind) for c in names[1:]]
        emit(tmp_path, entity, process(chunk.df, columns, ["id"]))
    check("casts_typed", tmp_path, entities)


def test_append_gives_unique_part_names_and_overwrite_clears_them(
    tmp_path: Path,
) -> None:
    df = pl.DataFrame({"a": [1, 2]})
    write_accepted(df, tmp_path, "append")
    write_accepted(df, tmp_path, "append")
    assert len(list(tmp_path.glob("part-*.parquet"))) == 2
    result = write_accepted(df, tmp_path, "overwrite")
    assert [p.name for p in tmp_path.glob("*.parquet")] == ["part-00000.parquet"]
    assert result.part_files == ["part-00000.parquet"]


def test_splits_at_max_size_per_file_and_writes_an_empty_part(tmp_path: Path) -> None:
    df = pl.DataFrame({"a": list(range(100))})
    options = SinkOptions(
        compression="snappy", row_group_size=10, max_size_per_file=8 * 40
    )
    result = write_accepted(df, tmp_path / "big", "overwrite", options)
    assert result.files_written == 3
    assert pl.read_parquet(tmp_path / "big").height == 100
    empty = write_accepted(df.clear(), tmp_path / "empty")
    assert empty.part_files == ["part-00000.parquet"]
    assert pl.read_parquet(tmp_path / "empty").columns == ["a"]


def test_avro_and_orc_read(tmp_path: Path) -> None:
    columns = [("id", "string"), ("qty", "int64"), ("price", "number"),
               ("active", "boolean"), ("note", "string")]  # fmt: skip
    [avro] = list(read_avro(CONFORMANCE / "avro_read/in/avro_read.avro"))
    emit(tmp_path, "avro_read", process(avro.df, columns, ["id", "qty"]))
    check("avro_read", tmp_path, ["avro_read"])
    columns.insert(4, ("day", "date"))
    [orc] = list(read_orc(CONFORMANCE / "orc_read/in/orc_read.orc"))
    emit(tmp_path, "orc_read", process(orc.df, columns, ["id", "qty"]))
    check("orc_read", tmp_path, ["orc_read"])


@pytest.mark.parametrize("write", [write_accepted, write_rejected])
def test_write_oserror_is_a_floe_error(
    tmp_path: Path, write: Callable[..., object]
) -> None:
    """#878: a sink whose parent is a file fails like Rust's `io::Error`, not with a traceback."""
    (tmp_path / "file").write_text("x")
    with pytest.raises(FloeError, match=r"^Not a directory \(os error 20\)$"):
        write(pl.DataFrame({"id": ["1"]}), tmp_path / "file" / "first")


def test_write_oserror_without_errno_keeps_its_message(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Polars raises message-only `OSError`s (no errno): the cause must not become `None (os error None)`."""

    def boom(*_: object, **__: object) -> None:
        raise OSError("Read-only file system (os error 30)")

    monkeypatch.setattr(pl.DataFrame, "write_csv", boom)
    with pytest.raises(FloeError, match=r"^Read-only file system \(os error 30\)$"):
        write_rejected(pl.DataFrame({"id": ["1"]}), tmp_path)
