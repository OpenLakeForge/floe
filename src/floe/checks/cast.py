"""String to typed casts and cast-mismatch errors (Rust: `checks/cast.rs`, `io/format.rs`)."""

import polars as pl

CAST_ERROR_MESSAGE = "invalid value for target type"

_DTYPES: dict[str, pl.DataType] = {
    "int8": pl.Int8(), "int16": pl.Int16(), "int32": pl.Int32(), "int64": pl.Int64(),
    "int": pl.Int64(), "integer": pl.Int64(), "long": pl.Int64(),
    "uint8": pl.UInt8(), "uint16": pl.UInt16(), "uint32": pl.UInt32(), "uint64": pl.UInt64(),
    "float32": pl.Float32(),
    "float64": pl.Float64(), "float": pl.Float64(), "double": pl.Float64(),
    "number": pl.Float64(), "decimal": pl.Float64(),
}  # fmt: skip

# Polars 2.0 has no string -> date/datetime cast, so both are parsed by regex (decision D9, #619):
# groups 1-3 year/month/day, 4-7 hour/minute/second/fraction, 8-10 offset sign/hour/minute.
_DATE = r"\+?([0-9]{1,4})-([0-9]{1,2})-([0-9]{1,2})"
_TIME = r"T([0-9]{1,2}):([0-9]{1,2})(?::([0-9]{1,2})(?:\.([0-9]*))?)?"
_OFFSET = r"(?:[Zz]|([+-])([0-9]{2})(?::?([0-9]{2}))?)?"
_DATE_RE = f"^{_DATE}$"
_DATETIME_RE = f"^{_DATE}{_TIME}{_OFFSET}$"


def _groups(raw: pl.Expr, pattern: str, count: int) -> list[pl.Expr]:
    groups = raw.str.strip_chars().str.extract_groups(pattern)
    return [groups.struct.field(str(i)) for i in range(1, count + 1)]


def _int(field: pl.Expr) -> pl.Expr:
    return field.cast(pl.Int32)


def _ymd(g: list[pl.Expr]) -> pl.Expr:
    return pl.format(
        "{}-{}-{}", g[0].str.zfill(4), g[1].str.zfill(2), g[2].str.zfill(2)
    )


def _date(raw: pl.Expr) -> pl.Expr:
    g = _groups(raw, _DATE_RE, 3)
    return pl.when(_int(g[0]) >= 1).then(_ymd(g).str.to_date("%Y-%m-%d", strict=False))


def _datetime(raw: pl.Expr) -> pl.Expr:
    g = _groups(raw, _DATETIME_RE, 10)
    second = _int(g[5]).fill_null(0)
    # fraction truncated (not rounded) to milliseconds
    millis = _int(g[6].str.slice(0, 3).str.pad_end(3, "0")).fill_null(0)
    off_hour, off_minute = _int(g[8]).fill_null(0), _int(g[9]).fill_null(0)
    off_sign = pl.when(g[7] == "-").then(-1).otherwise(1)
    valid = (_int(g[0]) >= 1) & (second <= 60) & (off_hour <= 23) & (off_minute <= 59)
    local = pl.format(
        "{} {}:{}", _ymd(g), g[3].str.zfill(2), g[4].str.zfill(2)
    ).str.to_datetime("%Y-%m-%d %H:%M", time_unit="ms", strict=False)
    # second 60 rolls over into the next minute; the offset is subtracted to reach UTC
    shift = pl.duration(
        seconds=second,
        milliseconds=millis,
        hours=-off_sign * off_hour,
        minutes=-off_sign * off_minute,
        time_unit="ms",
    )
    utc = local + shift
    # the 1..9999 year range applies to the final value (rollover, offset), not only the input
    return pl.when(valid & utc.dt.year().is_between(1, 9999)).then(utc)


def _boolean(raw: pl.Expr) -> pl.Expr:
    value = raw.str.strip_chars().str.to_lowercase()
    return (
        pl.when(value.is_in(["true", "1"]))
        .then(True)
        .when(value.is_in(["false", "0"]))
        .then(False)
    )


def cast_column(
    name: str, column_type: str, cast_mode: str = "strict"
) -> tuple[pl.Expr, pl.Expr]:
    """Return `(typed, cast_error_mask)` expressions over the string column `name`.

    A value that is present but does not parse becomes null; the mask (named `_e_cast_<name>`)
    flags it, except in `cast_mode: coerce` and for string columns.
    """
    raw = pl.col(name)
    kind = column_type.lower().replace("-", "").replace("_", "")
    if kind in ("string", "str", "text"):
        return raw, pl.lit(False).alias(f"_e_cast_{name}")
    if kind in _DTYPES:
        typed = raw.cast(_DTYPES[kind], strict=False)
    elif kind in ("boolean", "bool"):
        typed = _boolean(raw)
    elif kind == "date":
        typed = _date(raw)
    elif kind in ("datetime", "timestamp"):
        typed = _datetime(raw)
    else:
        raise ValueError(f"unsupported column type: {column_type}")
    typed = typed.alias(name)
    mask = (
        pl.lit(False) if cast_mode == "coerce" else raw.is_not_null() & typed.is_null()
    )
    return typed, mask.alias(f"_e_cast_{name}")


_ESCAPES = str.maketrans(
    {"\\": "\\\\", '"': '\\"', "\n": "\\n", "\r": "\\r", "\t": "\\t"}
)


def _json_escape(value: str) -> str:
    """Rust's `json_escape`: only these five characters are escaped, all others stay raw."""
    return value.translate(_ESCAPES)


def cast_error_entry(column: str) -> str:
    """The `cast_error` entry written to `__floe_errors`, as Rust's `RowError::to_json`."""
    return (
        f'{{"rule":"cast_error","column":"{_json_escape(column)}",'
        f'"message":"{CAST_ERROR_MESSAGE}"}}'
    )
