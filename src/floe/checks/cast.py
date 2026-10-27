"""String to typed casts and cast-mismatch errors (Rust: `checks/cast.rs`, `io/format.rs`)."""

import re

import polars as pl

CAST_ERROR_MESSAGE = "invalid value for target type"

_DTYPES: dict[str, pl.DataType] = {
    "int8": pl.Int8(), "int16": pl.Int16(), "int32": pl.Int32(), "int64": pl.Int64(),
    "int": pl.Int64(), "integer": pl.Int64(), "long": pl.Int64(),
    "uint8": pl.UInt8(), "uint16": pl.UInt16(), "uint32": pl.UInt32(), "uint64": pl.UInt64(),
    "float32": pl.Float32(),
    "float64": pl.Float64(), "float": pl.Float64(), "double": pl.Float64(),
    "number": pl.Float64(),
}  # fmt: skip

_DECIMAL = re.compile(r"decimal\((\d+),\s*(\d+)\)")


def decimal_type(kind: str) -> pl.Decimal | None:
    """`decimal(p,s)` as an exact `pl.Decimal`, or None when `kind` is not one.

    Raises ValueError for a precision outside 1..38 or a scale outside 0..precision.
    """
    if not (m := _DECIMAL.fullmatch(kind)):
        return None
    precision, scale = int(m[1]), int(m[2])
    if not 1 <= precision <= 38 or scale > precision:
        raise ValueError(f"decimal(p,s) needs 1 <= p <= 38 and 0 <= s <= p, got {kind}")
    return pl.Decimal(precision, scale)


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
    name: str,
    column_type: str,
    cast_mode: str = "strict",
    dtype: pl.DataType | type[pl.DataType] = pl.String,
) -> tuple[pl.Expr, pl.Expr]:
    """Return `(typed, cast_error_mask)` expressions over the column `name`, read as `dtype`.

    A value that is present but does not parse becomes null; the mask (named `_e_cast_<name>`)
    flags it, except in `cast_mode: coerce` and for string columns.

    `dtype` is the input column's dtype. Parquet, ORC and Avro keep native types (D15): anything
    but `pl.String` is cast natively, as Rust's `cast_df_to_schema` does. That is Polars'
    non-strict cast, so overflow, a float outside the integer range or NaN, and a date that does
    not fit become null (a `cast_error`), while 2.5 into an int truncates to 2 and an int into a
    bool is `!= 0`. A pair Polars cannot cast (date or datetime into bool, a list) raises, and
    aborts the run as in Rust. D17: unlike Rust, a float with a fraction into an integer type is a
    `cast_error` in strict mode (2.0 is accepted); in coerce mode it truncates as in Rust.
    Timezone-aware datetimes lose the zone and keep their UTC value.
    """
    raw = pl.col(name)
    native = dtype != pl.String
    kind = column_type.lower().replace("-", "").replace("_", "")
    if kind in ("string", "str", "text"):
        text = raw.cast(pl.String) if native else raw
        if native and dtype.is_float():  # Rust writes 1e20, Polars 1e+20
            text = text.str.replace("e+", "e", literal=True)
        return text, pl.lit(False).alias(f"_e_cast_{name}")
    fractional = None
    if (exact := decimal_type(kind)) is not None:
        typed = raw.cast(exact, strict=False)
        if (
            not native
        ):  # strict: digits beyond the scale are a cast_error, not a silent rounding
            fractional = raw.str.contains(
                rf"\.[0-9]{{{exact.scale}}}[0-9]*[1-9]"
            ).fill_null(False)
    elif kind in _DTYPES:
        typed = raw.cast(_DTYPES[kind], strict=False)
        if native and dtype.is_float() and _DTYPES[kind].is_integer():
            # D17; NaN and inf are null already
            fractional = (raw != raw.floor()).fill_null(False)
    elif kind in ("boolean", "bool"):
        typed = raw.cast(pl.Boolean, strict=False) if native else _boolean(raw)
    elif kind == "date":
        typed = raw.cast(pl.Date, strict=False) if native else _date(raw)
    elif kind in ("datetime", "timestamp"):
        typed = raw.cast(pl.Datetime("ms"), strict=False) if native else _datetime(raw)
    else:
        raise ValueError(f"unsupported column type: {column_type}")
    typed = typed.alias(name)
    mask = pl.lit(False)
    if cast_mode != "coerce":
        mask = raw.is_not_null() & typed.is_null()
        if fractional is not None:
            mask = mask | fractional
    return typed, mask.alias(f"_e_cast_{name}")


_ESCAPES = str.maketrans(
    {"\\": "\\\\", '"': '\\"', "\n": "\\n", "\r": "\\r", "\t": "\\t"}
)


def json_escape(value: str) -> str:
    """Rust's `json_escape`: only these five characters are escaped, all others stay raw."""
    return value.translate(_ESCAPES)
