# polars stubs reference numpy, which is not installed, so their member types are partly unknown
# pyright: reportUnknownMemberType=false
import hashlib
import os
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Literal

import duckdb
import polars as pl

from floe.errors import FloeConfigError, FloeRunError

Strategy = Literal["hash", "drop", "nullify", "redact", "mask", "tokenize"]


@dataclass(frozen=True)
class PiiColumn:
    name: str
    strategy: Strategy
    mask_pattern: str | None = None
    redact_value: str | None = None
    key: str | None = None


def apply_pii_masking(
    df: pl.DataFrame,
    columns: Sequence[PiiColumn],
    schema_to_runtime: Mapping[str, str],
) -> pl.DataFrame:
    for col in columns:
        name = schema_to_runtime.get(col.name, col.name)
        if name not in df.columns:
            continue
        match col.strategy:
            case "hash":
                df = df.with_columns(_hash(_as_string(df[name]), name, col.key))
            case "drop":
                df = df.drop(name)
            case "nullify":
                df = df.with_columns(pl.lit(None, dtype=df.schema[name]).alias(name))
            case "redact":
                text = _as_string(df[name])
                df = df.with_columns(
                    text.to_frame().select(
                        _when_set(name, pl.lit(col.redact_value or "[REDACTED]"))
                    )
                )
            case "mask":
                text = _as_string(df[name])
                df = df.with_columns(
                    text.to_frame().select(_mask(name, col.mask_pattern or ""))
                )
            case "tokenize":
                pass  # rejected at config validation; never reached at runtime
    return df


def _as_string(series: pl.Series) -> pl.Series:
    try:
        return series.cast(pl.String)
    except pl.exceptions.PolarsError as e:
        raise FloeRunError(
            f"PII: failed to cast column {series.name} to string: {e}"
        ) from e


def _when_set(name: str, value: pl.Expr) -> pl.Expr:
    return pl.when(pl.col(name).is_not_null()).then(value).alias(name)


def _token_n(pattern: str, token: str) -> int:
    start = pattern.find(token)
    if start < 0:
        return 0
    rest = pattern[start + len(token) :]
    end = rest.find("}")
    digits = rest[:end]
    if end < 0 or not re.fullmatch(r"\+?[0-9]+", digits) or int(digits) >= 2**64:
        return 0
    return int(digits)


def _mask(name: str, pattern: str) -> pl.Expr:
    first_n = _token_n(pattern, "{first")
    last_n = _token_n(pattern, "{last")
    text = pl.col(name)
    chars = text.str.len_chars()
    last = pl.min_horizontal(pl.lit(last_n, pl.UInt32), chars)
    first = pl.min_horizontal(pl.lit(first_n, pl.UInt32), chars - last)
    out = _when_set(name, pl.lit(pattern))  # broadcast the pattern to one row per value
    if first_n:
        out = out.str.replace_all(
            f"{{first{first_n}}}", text.str.slice(0, first), literal=True
        )
    if last_n:
        out = out.str.replace_all(
            f"{{last{last_n}}}", text.str.slice(chars - last, last), literal=True
        )
    return out.alias(name)


def _resolve_key(raw: str, name: str) -> str:
    if "${" not in raw:
        return raw
    if not (raw.startswith("${") and raw.endswith("}")):
        raise FloeConfigError(
            f"pii column {name}: key must be a plain value or a single ${{VAR_NAME}} reference; "
            "mixing literal text with ${...} is not supported"
        )
    var = raw[2:-1]
    if not var or "{" in var or "}" in var:
        raise FloeConfigError(f"pii column {name}: key has invalid placeholder syntax")
    value = os.environ.get(var)
    if value is None:
        raise FloeRunError(
            f"pii column {name}: key references env var {var} which is not set"
        )
    return value


def _hash(values: pl.Series, name: str, key: str | None) -> pl.Series:
    # SHA-256, or HMAC-SHA256 (RFC 2104) composed from DuckDB's vectorised sha256 over blobs
    if key is None:
        sql = "SELECT sha256(v) AS h FROM frame"
        params: list[bytes] = []
    else:
        raw = _resolve_key(key, name).encode()
        if len(raw) > 64:
            raw = hashlib.sha256(raw).digest()
        raw = raw.ljust(64, b"\0")
        params = [bytes(b ^ 0x36 for b in raw), bytes(b ^ 0x5C for b in raw)]
        sql = "SELECT sha256($2 || unhex(sha256($1 || encode(v)))) AS h FROM frame"
    with duckdb.connect() as con:
        con.register("frame", values.to_frame("v"))
        out = con.sql(sql, params=params).pl()["h"]
    return out.alias(name)
