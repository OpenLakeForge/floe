"""Strict YAML decoding, ported from `crates/floe-core/src/config/yaml_decode.rs` (yaml-rust2 0.11)."""

import re
import unicodedata
from collections.abc import Iterator
from pathlib import Path
from typing import cast

import yaml
from yaml.error import MarkedYAMLError
from yaml.nodes import CollectionNode, Node, ScalarNode, SequenceNode

from floe.errors import FloeConfigError, FloeError, FloeIoError

_DIGITS = {10: r"[+-]?[0-9]+", 8: r"[+-]?[0-7]+", 16: r"[+-]?[0-9a-fA-F]+"}
_FLOAT = re.compile(r"[+-]?(?:[0-9]+\.?[0-9]*|\.[0-9]+)(?:[eE][+-]?[0-9]+)?")
_KEYWORDS = {"": None, "~": None, "null": None} | {
    w: w[0] in "tT" for w in ("true", "True", "TRUE", "false", "False", "FALSE")
}
_INF = re.compile(r"[+-]?\.(?:inf|Inf|INF)")


def _i64(digits: str, base: int) -> int | None:
    if not re.fullmatch(_DIGITS[base], digits):
        return None
    magnitude = digits.lstrip("+-").lstrip("0")
    if len(magnitude) > 25:  # past i64 in any base; keeps int() under its digit limit
        return None
    n = int(magnitude or "0", base) * (-1 if digits[0] == "-" else 1)
    return n if -(2**63) <= n < 2**63 else None


def _plain(v: str) -> object:
    """`Yaml::from_str`: the only place a plain scalar becomes a non-string."""
    for prefix, base in (("0x", 16), ("0o", 8), ("+", 10)):
        if v.startswith(prefix) and (n := _i64(v[len(prefix) :], base)) is not None:
            return n
    if v in _KEYWORDS:
        return _KEYWORDS[v]
    if (n := _i64(v, 10)) is not None:
        return n
    if _INF.fullmatch(v):
        return float(v.replace(".", ""))
    if v in (".nan", ".NaN", ".NAN"):
        return float("nan")
    return float(v) if _FLOAT.fullmatch(v) else v


_MARKS = ("Mn", "Me")
_FILLERS = "\u115f\u1160\u3164\uffa0"
_ESCAPES = {'"': '\\"', "\\": "\\\\", "\t": "\\t", "\n": "\\n", "\r": "\\r", "\0": "\\0"}  # fmt: skip


def _debug_char(c: str) -> str:
    # ponytail: Rust also hides other grapheme-extend/default-ignorable characters; this covers Mn, Me and the Hangul fillers
    hidden = not c.isprintable() or unicodedata.category(c) in _MARKS or c in _FILLERS
    return _ESCAPES.get(c) or (f"\\u{{{ord(c):x}}}" if hidden else c)


def rust_debug(s: str) -> str:
    """Rust's `{:?}` of a `str`."""
    return '"' + "".join(map(_debug_char, s)) + '"'


def _where(text: str, index: int, line: int, column: int) -> str:
    return f"at byte {len(text[:index].encode())} line {line + 1} column {column + 1}"


def _debug(key: object, node: Node) -> str:
    if key is None:
        return "Null"
    if isinstance(key, bool):
        return f"Boolean({str(key).lower()})"
    if isinstance(key, int):
        return f"Integer({key})"
    if isinstance(key, float):
        return f"Real({rust_debug(cast(str, node.value))})"
    return f"String({rust_debug(cast(str, key))})"


def _build(node: Node, text: str, active: tuple[int, ...] = ()) -> object:
    if id(node) in active:
        return None  # recursive alias (`&a [*a]`): yaml-rust2 loads BadValue, which no reader accepts either
    active = (*active, id(node))
    if isinstance(node, ScalarNode):
        value = cast(str, node.value)
        # ponytail: an explicit tag (`!!str 5`) is ignored; yaml-rust2 honours it. Add when a config needs it.
        return _plain(value) if node.style is None else value
    if isinstance(node, SequenceNode):
        return [_build(item, text, active) for item in cast("list[Node]", node.value)]
    result: dict[object, object] = {}
    seen: set[tuple[type, object]] = set()
    for key_node, value_node in cast("list[tuple[Node, Node]]", node.value):
        key = _build(key_node, text, active)
        if isinstance(key, (list, dict)):
            raise FloeError("unsupported complex mapping key")
        value = _build(value_node, text, active)
        # yaml-rust2 keeps `true`, `1`, `1.0`, `1.00` apart. ponytail: `result` still merges `true` and `1`
        identity = (type(key), key_node.value if isinstance(key, float) else key)
        if identity in seen:
            # position of the value's last token; ponytail: wrong for anchored/tagged values and block scalars
            collection = isinstance(value_node, CollectionNode)
            mark = value_node.end_mark if collection else value_node.start_mark
            back = int(collection and bool(value_node.flow_style))
            where = _where(text, mark.index - back, mark.line, mark.column - back)
            raise FloeError(
                f"{_debug(key, key_node)}: duplicated key in mapping {where}"
            )
        seen.add(identity)
        result[key] = value
    return result


def load_yaml(path: Path) -> list[object]:
    try:
        text = path.read_bytes().decode()
    except OSError as e:
        raise FloeIoError(f"{e.strerror} (os error {e.errno})") from e
    except UnicodeDecodeError as e:
        raise FloeIoError("stream did not contain valid UTF-8") from e
    except ValueError as e:  # an embedded NUL in the path
        raise FloeIoError("file name contained an unexpected NUL byte") from e
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
