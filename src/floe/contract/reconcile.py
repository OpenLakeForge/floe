"""Reimport: update only the importer's key paths in an existing entity file.

Owned values are spliced by YAML node position, so every other byte (engineer keys,
comments) stays as it was. Pure: reads nothing, writes nothing.
"""

import difflib
import json
import re
from collections.abc import Mapping
from typing import Any, cast

import yaml

from floe.add_entity import _render  # pyright: ignore[reportPrivateUsage] -- the one deterministic YAML emitter
from floe.config.yaml import parse_yaml
from floe.contract.publish import PlannedFile
from floe.contract.render import _OWNED, fingerprint, owned  # pyright: ignore[reportPrivateUsage] -- the ownership table lives in render

_HEADER = re.compile(r"# floe-contract: sha256:([0-9a-f]{64})")
_PARENTS = {p[:1] for p in _OWNED if len(p) > 1}  # owned children live under these


class _Unsplicable(Exception):
    pass


_Edit = tuple[int, int, str, int]  # start, end, replacement, depth of the key path


def _block_map(node: Any) -> bool:
    return (
        isinstance(node, yaml.MappingNode) and not node.flow_style and bool(node.value)
    )


def _end(text: str, node: Any) -> int:
    """End of the node's last content, without the comments after it."""
    if isinstance(node, yaml.CollectionNode) and not node.flow_style and node.value:
        last = node.value[-1]
        return _end(text, last[1] if isinstance(node, yaml.MappingNode) else last)
    end: int = node.end_mark.index
    if getattr(node, "style", None) in (
        "|",
        ">",
    ):  # a block scalar's mark is past its last newline
        while end > node.start_mark.index and text[end - 1] == "\n":
            end -= 1
    return end


def _eol(text: str, i: int) -> int:
    j = text.find("\n", i)
    return len(text) if j < 0 else j + 1


def _pair(key: str, value: Any, col: int) -> str:
    return _render({key: value}).rstrip("\n").replace("\n", "\n" + " " * col)


def _drop(text: str, k: Any, v: Any, depth: int) -> _Edit:
    """The edit that removes a whole `key: value` pair, lines included."""
    return (
        text.rfind("\n", 0, k.start_mark.index) + 1,
        _eol(text, _end(text, v)),
        "",
        depth,
    )


def _edit(
    text: str,
    node: Any,
    old: Mapping[str, Any],
    new: Mapping[str, Any],
    path: tuple[str, ...],
    edits: list[_Edit],
) -> None:
    pairs = {k.value: (k, v) for k, v in node.value}
    col = node.value[0][0].start_mark.column
    adds: list[str] = []
    for key in {**new, **old}:
        here = (*path, key)
        if key not in pairs:
            adds.append(" " * col + _pair(key, new[key], col) + "\n")
            continue
        k, v = pairs[key]
        start = k.start_mark.index
        if (key,) in _PARENTS and not path:
            if not _block_map(v):
                raise _Unsplicable(".".join(here))
            if not new.get(key) and {c.value for c, _ in v.value} <= set(
                old.get(key, {})
            ):
                edits.append(
                    _drop(text, k, v, len(path))
                )  # every child goes: drop the parent too
                continue
            _edit(text, v, old.get(key, {}), new.get(key, {}), here, edits)
        elif key in new:
            edits.append((start, _end(text, v), _pair(key, new[key], col), len(path)))
        else:  # no longer in the workbook: drop the whole line(s)
            edits.append(_drop(text, k, v, len(path)))
    if adds:
        pos = _eol(text, _end(text, node))
        lead = "" if text[:pos].endswith("\n") else "\n"
        edits.append((pos, pos, lead + "".join(adds), len(path)))


def _splice(text: str, old: Mapping[str, Any], new: Mapping[str, Any]) -> str:
    root = cast("Any", yaml.compose(text, Loader=yaml.SafeLoader))  # pyright: ignore[reportUnknownMemberType] -- the stub leaves the node type unknown
    if not _block_map(root):
        raise _Unsplicable("<document>")
    edits: list[_Edit] = [(0, text.index("\n") if "\n" in text else len(text), "", 0)]
    _edit(text, root, old, new, (), edits)
    header = f"# floe-contract: sha256:{fingerprint(new)}"
    edits[0] = (0, edits[0][1], header, 0)
    # at one offset a parent's new child must land before a new root key: apply the root first
    for start, end, repl, _ in sorted(
        edits, key=lambda e: (e[0], e[1], -e[3]), reverse=True
    ):
        text = text[:start] + repl + text[end:]
    return text


def _rest(entity: Any, path: tuple[str, ...] = ()) -> Any:
    """`entity` without the owned key paths (and the parents that leaves empty)."""
    if not isinstance(entity, dict):
        return entity
    items = cast("dict[str, Any]", entity).items()
    kept = {k: _rest(v, (*path, k)) for k, v in items if (*path, k) not in _OWNED}
    return {k: v for k, v in kept.items() if v not in (None, {})}


def _dump(d: Mapping[str, Any]) -> list[str]:
    return json.dumps(d, indent=2, sort_keys=True).splitlines()


def _diff(old: Mapping[str, Any], new: Mapping[str, Any]) -> str:
    diff = difflib.unified_diff(_dump(old), _dump(new), "file", "workbook", lineterm="")
    return "\n".join(diff)


def reconcile(
    path: str, existing_text: str, new_entity: Mapping[str, Any]
) -> PlannedFile:
    def blocked(reason: str) -> PlannedFile:
        return PlannedFile(path, existing_text, None, "blocked", reason)

    match = _HEADER.fullmatch(existing_text.split("\n", 1)[0])
    if not match:
        return blocked(
            "not created by floe contract import (no `# floe-contract:` header)"
        )
    try:
        doc = cast("dict[str, Any]", parse_yaml(existing_text)[0])
    except Exception as e:  # noqa: BLE001 -- any parse failure blocks the file, with its message
        return blocked(f"cannot parse the existing file: {e}")
    old, new = owned(doc), owned(new_entity)
    if fingerprint(old) != match.group(1):
        return blocked(
            "business-owned keys were edited by hand; change the workbook or revert them:\n"
            + _diff(old, new)
        )
    for key in ("name", "domain"):
        if old.get(key) != new.get(key):
            return blocked(
                f"{key} is {old.get(key)!r} in the file but {new.get(key)!r} in the workbook"
            )
    if fingerprint(old) == fingerprint(new):
        return PlannedFile(path, existing_text, existing_text, "unchanged")
    try:
        result = _splice(existing_text, old, new)
        again = cast("dict[str, Any]", parse_yaml(result)[0])
    except _Unsplicable as e:
        return blocked(f"owned key {e} is in flow style or has an unexpected shape")
    except Exception as e:  # noqa: BLE001 -- the splice must never raise; a bad one is blocked
        return blocked(f"self-check failed: the spliced file does not parse: {e}")
    if any(p in again and not again[p] for (p,) in _PARENTS):
        return blocked("self-check failed: the splice left an empty parent mapping")
    if fingerprint(owned(again)) != fingerprint(new) or _rest(again) != _rest(doc):
        return blocked("self-check failed: the splice changed more than the owned keys")
    return PlannedFile(path, existing_text, result, "updated")
