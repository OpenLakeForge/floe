import hashlib
import hmac
from pathlib import Path

import polars as pl
import pytest

from floe.errors import FloeConfigError, FloeRunError
from floe.pii import PiiColumn, Strategy, apply_pii_masking

CASE = Path(__file__).parents[2] / "conformance" / "unique_pii"


def run(values: list[str | None], strategy: Strategy, **kw: str) -> list[str | None]:
    df = pl.DataFrame({"f": values}, schema={"f": pl.String})
    return apply_pii_masking(df, [PiiColumn("f", strategy, **kw)], {})["f"].to_list()


def sha(v: str) -> str:
    return hashlib.sha256(v.encode()).hexdigest()


def mac(key: str, v: str) -> str:
    return hmac.digest(key.encode(), v.encode(), "sha256").hex()


VALUES = ["alice@example.com", "", "héllo wörld", "x" * 1000]


def test_hash_is_sha256_and_keeps_nulls() -> None:
    assert run([*VALUES, None], "hash") == [*map(sha, VALUES), None]
    assert sha("").startswith("e3b0c442")


@pytest.mark.parametrize(
    "key", ["secret", "", "k" * 64, "k" * 65, "k" * 200, "clé-ünï"]
)
def test_hash_with_key_is_hmac_sha256(key: str) -> None:
    assert run([*VALUES, None], "hash", key=key) == [
        *(mac(key, v) for v in VALUES),
        None,
    ]


def test_hash_of_all_null_and_empty_column() -> None:
    assert run([None, None], "hash", key="k") == [None, None]
    assert run([], "hash", key="k") == []


def test_hash_key_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLOE_TEST_PII_KEY", "env-secret")
    assert run(["data"], "hash", key="${FLOE_TEST_PII_KEY}") == [
        mac("env-secret", "data")
    ]


def test_hash_key_mixed_syntax_is_config_error() -> None:
    with pytest.raises(FloeConfigError) as e:
        run(["a"], "hash", key="prefix-${VAR}")
    assert str(e.value) == (
        "pii column f: key must be a plain value or a single ${VAR_NAME} reference; "
        "mixing literal text with ${...} is not supported"
    )


@pytest.mark.parametrize("key", ["${}", "${A{B}", "${A}B}"])
def test_hash_key_invalid_placeholder_is_config_error(key: str) -> None:
    with pytest.raises(FloeConfigError) as e:
        run(["a"], "hash", key=key)
    assert str(e.value) == "pii column f: key has invalid placeholder syntax"


def test_hash_key_missing_env_is_run_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FLOE_MISSING_PII_KEY_XYZ", raising=False)
    with pytest.raises(FloeRunError) as e:
        run(["a"], "hash", key="${FLOE_MISSING_PII_KEY_XYZ}")
    assert (
        str(e.value)
        == "pii column f: key references env var FLOE_MISSING_PII_KEY_XYZ which is not set"
    )


def test_hash_matches_rust_recorded_output() -> None:
    src = pl.concat(
        pl.read_csv(f, infer_schema=False) for f in sorted((CASE / "in").glob("*.csv"))
    )
    got = apply_pii_masking(src, [PiiColumn("email", "hash", key="spike-hmac-key")], {})
    mine = set(got["email"].drop_nulls())
    accepted = pl.read_parquet(CASE / "expected" / "accepted" / "orders.parquet")[
        "email"
    ]
    rejected = pl.read_csv(
        next((CASE / "expected" / "rejected" / "orders").glob("part-0*.csv")),
        infer_schema=False,
    )["email"]
    recorded = {
        *accepted.drop_nulls(),
        *rejected.drop_nulls(),
    }  # Rust masks rejected rows too (#604)
    assert len(recorded) >= 10
    assert recorded <= mine


def test_redact() -> None:
    assert run(["1234", "", None], "redact") == ["[REDACTED]", "[REDACTED]", None]
    assert run(["secret", None], "redact", redact_value="[PII]") == ["[PII]", None]


def test_nullify_keeps_column_and_type() -> None:
    df = pl.DataFrame({"f": [1, 2, None], "g": ["a", "b", "c"]})
    out = apply_pii_masking(df, [PiiColumn("f", "nullify")], {})
    assert out.schema == df.schema
    assert out["f"].to_list() == [None, None, None]
    assert out["g"].to_list() == ["a", "b", "c"]


def test_drop_removes_only_that_column() -> None:
    df = pl.DataFrame({"keep": ["x"], "pii_col": ["s"]})
    assert apply_pii_masking(df, [PiiColumn("pii_col", "drop")], {}).columns == ["keep"]


def test_tokenize_is_a_no_op() -> None:
    df = pl.DataFrame({"f": ["a"]})
    assert apply_pii_masking(df, [PiiColumn("f", "tokenize")], {}).equals(df)


@pytest.mark.parametrize(
    ("pattern", "value", "want"),
    [
        ("****{last4}", "1234567890123456", "****3456"),
        ("****{last4}", "", "****"),
        ("{first4}****{last4}", "4111222233334444", "4111****4444"),
        ("***-**-{last4}", "123-45-6789", "***-**-6789"),
        ("{first2}****{last2}", "hello", "he****lo"),
        ("{first4}****{last4}", "abcdef", "ab****cdef"),  # suffix wins, prefix clamped
        ("{first4}*{last4}", "abc", "*abc"),
        ("{first3}-{last2}", "héllo", "hél-lo"),  # characters, not bytes
        ("{first0}-{last2}", "hello", "{first0}-lo"),  # n=0 is not a token
        ("{first2}{first2}", "hello", "hehe"),
        ("{firstx}-{last2}", "hello", "{firstx}-lo"),
        (
            "{first+2}{first2}",
            "hello",
            "{first+2}he",
        ),  # Rust parse accepts '+', replaces {first2}
        (
            "{first2}-{first3}",
            "hello",
            "he-{first3}",
        ),  # only the first token's N is used
        ("fixed", "hello", "fixed"),
    ],
)
def test_mask(pattern: str, value: str, want: str) -> None:
    assert run([value, None], "mask", mask_pattern=pattern) == [want, None]


def test_non_string_columns_are_cast_to_string() -> None:
    df = pl.DataFrame({"i": [42, None], "b": [True, False], "x": [1.5, 2.0]})
    cols = [
        PiiColumn("i", "redact"),
        PiiColumn("b", "hash"),
        PiiColumn("x", "mask", mask_pattern="{last2}"),
    ]
    out = apply_pii_masking(df, cols, {})
    assert out["i"].to_list() == ["[REDACTED]", None]
    assert out["b"].to_list() == [sha("true"), sha("false")]
    assert out["x"].to_list() == [".5", ".0"]
    assert out.schema == pl.Schema({"i": pl.String, "b": pl.String, "x": pl.String})


def test_missing_column_is_skipped_and_key_not_resolved() -> None:
    df = pl.DataFrame({"other": ["value"]})
    out = apply_pii_masking(
        df, [PiiColumn("email", "hash", key="${UNSET_FLOE_VAR}")], {}
    )
    assert out.equals(df)


def test_schema_to_runtime_name_is_used() -> None:
    df = pl.DataFrame({"credit_card": ["secret"]})
    out = apply_pii_masking(
        df, [PiiColumn("Credit Card", "redact")], {"Credit Card": "credit_card"}
    )
    assert out["credit_card"].to_list() == ["[REDACTED]"]


def test_error_names_the_runtime_column(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FLOE_MISSING_PII_KEY_XYZ", raising=False)
    df = pl.DataFrame({"credit_card": ["a"]})
    col = PiiColumn("Credit Card", "hash", key="${FLOE_MISSING_PII_KEY_XYZ}")
    with pytest.raises(FloeRunError, match="pii column credit_card: "):
        apply_pii_masking(df, [col], {"Credit Card": "credit_card"})


def test_uncastable_column_is_run_error() -> None:
    df = pl.DataFrame({"f": [[1, 2]]})
    with pytest.raises(FloeRunError, match="^PII: failed to cast column f to string: "):
        apply_pii_masking(df, [PiiColumn("f", "redact")], {})
