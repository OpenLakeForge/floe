"""Strict YAML decoding, ported from `crates/floe-core/src/config/yaml_decode.rs` (yaml-rust2 0.11)."""

import json
import re
from collections.abc import Iterator
from pathlib import Path
from typing import cast

import yaml
from yaml.error import MarkedYAMLError
from yaml.nodes import CollectionNode, Node, ScalarNode, SequenceNode

from floe.errors import FloeConfigError, FloeError, FloeIoError

_I64 = range(-(2**63), 2**63)
_DIGITS = {10: r"[+-]?[0-9]+", 8: r"[+-]?[0-7]+", 16: r"[+-]?[0-9a-fA-F]+"}
_FLOAT = re.compile(r"[+-]?(?:[0-9]+\.?[0-9]*|\.[0-9]+)(?:[eE][+-]?[0-9]+)?")
_KEYWORDS = {"": None, "~": None, "null": None} | {
    w: w[0] in "tT" for w in ("true", "True", "TRUE", "false", "False", "FALSE")
}
_INF = re.compile(r"[+-]?\.(?:inf|Inf|INF)")


def _i64(digits: str, base: int) -> int | None:
    if not re.fullmatch(_DIGITS[base], digits):
        return None
    n = int(digits, base)
    return n if n in _I64 else None


def _plain(v: str) -> object:
    """`Yaml::from_str`: the only place a plain scalar becomes a non-string."""
    n = None
    if v.startswith("0x"):
        n = _i64(v[2:], 16)
    elif v.startswith("0o"):
        n = _i64(v[2:], 8)
    elif v.startswith("+"):
        n = _i64(v[1:], 10)
    if n is not None:
        return n
    if v in _KEYWORDS:
        return _KEYWORDS[v]
    n = _i64(v, 10)
    if n is not None:
        return n
    if _INF.fullmatch(v):
        return float(v.replace(".", ""))
    if v in (".nan", ".NaN", ".NAN"):
        return float("nan")
    return float(v) if _FLOAT.fullmatch(v) else v


def _where(text: str, index: int, line: int, column: int) -> str:
    return f"at byte {len(text[:index].encode())} line {line + 1} column {column + 1}"


def _debug(key: object, node: Node) -> str:
    """The key as Rust's `{:?}` of a `Yaml` prints it."""
    if key is None:
        return "Null"
    if isinstance(key, bool):
        return f"Boolean({str(key).lower()})"
    if isinstance(key, int):
        return f"Integer({key})"
    if isinstance(key, float):
        return f"Real({json.dumps(node.value)})"
    return f"String({json.dumps(key, ensure_ascii=False)})"


def _build(node: Node, text: str) -> object:
    if isinstance(node, ScalarNode):
        value = cast(str, node.value)
        # ponytail: an explicit tag (`!!str 5`) is ignored; yaml-rust2 honours it. Add when a config needs it.
        return _plain(value) if node.style is None else value
    if isinstance(node, SequenceNode):
        return [_build(item, text) for item in cast("list[Node]", node.value)]
    result: dict[object, object] = {}
    for key_node, value_node in cast("list[tuple[Node, Node]]", node.value):
        key = _build(key_node, text)
        if isinstance(key, (list, dict)):
            raise FloeError("unsupported complex mapping key")
        value = _build(value_node, text)
        if key in result:
            # yaml-rust2 reports where the value's last token starts: a flow collection ends on its bracket.
            # ponytail: wrong for a value with an anchor or tag, or a block scalar; fix if a user reports it
            if isinstance(value_node, CollectionNode):
                mark, back = value_node.end_mark, int(bool(value_node.flow_style))
            else:
                mark, back = value_node.start_mark, 0
            where = _where(text, mark.index - back, mark.line, mark.column - back)
            raise FloeError(
                f"{_debug(key, key_node)}: duplicated key in mapping {where}"
            )
        result[key] = value
    return result


def load_yaml(path: Path) -> list[object]:
    try:
        text = path.read_bytes().decode()
    except OSError as e:
        raise FloeIoError(f"{e.strerror} (os error {e.errno})") from e
    except UnicodeDecodeError as e:
        raise FloeIoError("stream did not contain valid UTF-8") from e
    try:
        # Composing keeps `<<` an ordinary key and aliases plain copies; no PyYAML constructor runs.
        docs = cast(
            "Iterator[Node]",
            yaml.compose_all(text, Loader=yaml.SafeLoader),  # pyright: ignore[reportUnknownMemberType] -- the stub leaves the stream type unknown
        )
        return [_build(doc, text) for doc in docs]
    except MarkedYAMLError as e:
        # ponytail: only some scanner messages match yaml-rust2 word for word; the rest keep PyYAML's wording
        context = f"{e.context}, " if e.context else ""
        problem = (e.problem or "").split(", but got")[0]
        mark = e.problem_mark
        where = _where(text, mark.index, mark.line, mark.column) if mark else ""
        raise FloeError(f"{context}{problem} {where}".strip()) from e
    except yaml.YAMLError as e:
        raise FloeError(str(e)) from e


def load_single_doc(path: Path) -> object:
    docs = load_yaml(path)
    if not docs:
        raise FloeConfigError("YAML is empty")
    if len(docs) > 1:
        raise FloeConfigError("YAML contains multiple documents; expected one")
    return docs[0]


def yaml_hash(value: object, ctx: str) -> dict[object, object]:
    if not isinstance(value, dict):
        raise FloeConfigError(f"expected map at {ctx}")
    return cast("dict[object, object]", value)


def yaml_string(value: object, ctx: str) -> str:
    if not isinstance(value, str):
        raise FloeConfigError(f"expected string at {ctx}")
    return value
