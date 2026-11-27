"""Classify the change between two business-owned entity projections (#1472)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, cast

Kind = Literal["breaking", "tightening", "loosening", "additive", "metadata"]
Entity = dict[str, object]
_LEVELS = {"warn": 0, "reject": 1, "abort": 2}
_LOWER_BOUNDS = ("min", "min_length")
_UPPER_BOUNDS = ("max", "max_length")


@dataclass(frozen=True)
class Change:
    kind: Kind
    path: str
    detail: str


def _d(value: object) -> dict[str, object]:
    return cast("dict[str, object]", value) if isinstance(value, dict) else {}


def _l(value: object) -> list[object]:
    return cast("list[object]", value) if isinstance(value, list) else []


def _keys(value: object) -> dict[tuple[str, ...], list[str]]:
    return {tuple(sorted(map(str, k))): list(map(str, k)) for k in map(_l, _l(value))}


def _rank(level: object, fallback: object) -> int:
    return _LEVELS.get(str(level), _LEVELS.get(str(fallback), 1))


def _tighter(kind_when_new_is_tighter: bool) -> Kind:
    return "tightening" if kind_when_new_is_tighter else "loosening"


def _bound(path: str, key: str, old: object, new: object, lower: bool) -> Change | None:
    if old is None or new is None:
        return Change(_tighter(old is None), path, f"{key} {old!r} -> {new!r}")
    try:
        a, b = (
            cast("float", old),
            cast("float", new),
        )  # untyped YAML scalars; TypeError handled below
        tighter = b > a if lower else b < a
    except TypeError:
        return Change("tightening", path, f"{key} {old!r} -> {new!r} (not comparable)")
    return Change(_tighter(tighter), path, f"{key} {old!r} -> {new!r}")


def _severity(
    path: str, old: object, new: object, old_base: object, new_base: object
) -> Change | None:
    if old is None and new is None:
        return None
    before, after = _rank(old, old_base), _rank(new, new_base)
    if before == after:
        return None
    return Change(_tighter(after > before), path, f"{old} -> {new}")


def _rules(
    prefix: str, old: object, new: object, old_base: object, new_base: object
) -> list[Change]:
    o, n = _d(old), _d(new)
    return [
        c
        for rule in sorted(o.keys() | n.keys())
        if (
            c := _severity(
                f"{prefix}.{rule}", o.get(rule), n.get(rule), old_base, new_base
            )
        )
    ]


def _column(
    name: str,
    old: dict[str, object],
    new: dict[str, object],
    bases: tuple[object, object],
) -> list[Change]:
    p = f"schema.columns[{name}]"
    out: list[Change] = []
    if old.get("type") != new.get("type"):
        out.append(
            Change("breaking", f"{p}.type", f"{old.get('type')} -> {new.get('type')}")
        )
    if old.get("nullable", True) != new.get("nullable", True):
        out.append(Change(_tighter(new.get("nullable", True) is False), f"{p}.nullable", f"{old.get('nullable')} -> {new.get('nullable')}"))  # fmt: skip
    for key in (*_LOWER_BOUNDS, *_UPPER_BOUNDS):
        if old.get(key) != new.get(key):
            out.append(cast("Change", _bound(f"{p}.{key}", key, old.get(key), new.get(key), key in _LOWER_BOUNDS)))  # fmt: skip
    if old.get("allowed_values") != new.get("allowed_values"):
        o, n = old.get("allowed_values"), new.get("allowed_values")
        wider = (
            o is not None
            and n is not None
            and set(map(str, _l(o))) < set(map(str, _l(n)))
        )
        out.append(
            Change(
                _tighter(not (n is None or wider)), f"{p}.allowed_values", f"{o} -> {n}"
            )
        )
    if old.get("pattern") != new.get("pattern"):
        o, n = old.get("pattern"), new.get("pattern")
        out.append(Change(_tighter(n is not None), f"{p}.pattern", f"{o!r} -> {n!r}"))
    out += _rules(
        f"{p}.rule_severity", old.get("rule_severity"), new.get("rule_severity"), *bases
    )
    return out


def classify(old: Entity | None, new: Entity | None) -> list[Change]:
    if old is None and new is None:
        return []
    if new is None:
        return [Change("breaking", "table", "table removed from workbook")]
    if old is None:
        return [Change("additive", "table", "new table")]
    out: list[Change] = []
    for key in ("owner", "description"):
        a, b = _d(old.get("metadata")).get(key), _d(new.get("metadata")).get(key)
        if a != b:
            out.append(Change("metadata", f"metadata.{key}", f"{a!r} -> {b!r}"))
    a, b = _d(old.get("source")).get("resource"), _d(new.get("source")).get("resource")
    if a != b:
        out.append(Change("breaking", "source.resource", f"{a!r} -> {b!r}"))
    po, pn = _d(old.get("policy")), _d(new.get("policy"))
    if c := _severity(
        "policy.severity", po.get("severity"), pn.get("severity"), None, None
    ):
        out.append(c)
    bases = (po.get("severity"), pn.get("severity"))
    out += _rules(
        "policy.rule_severity", po.get("rule_severity"), pn.get("rule_severity"), *bases
    )
    so, sn = _d(old.get("schema")), _d(new.get("schema"))
    if so.get("primary_key") != sn.get("primary_key"):
        out.append(Change("breaking", "schema.primary_key", f"{so.get('primary_key')} -> {sn.get('primary_key')}"))  # fmt: skip
    ko, kn = _keys(so.get("unique_keys")), _keys(sn.get("unique_keys"))
    for k in sorted(ko.keys() | kn.keys()):
        if (k in kn) != (k in ko):
            members = ",".join(kn.get(k) or ko[k])
            out.append(Change(_tighter(k in kn), f"schema.unique_keys[{members}]", "added" if k in kn else "removed"))  # fmt: skip
    co = {str(_d(c).get("name")): _d(c) for c in _l(so.get("columns"))}
    cn = {str(_d(c).get("name")): _d(c) for c in _l(sn.get("columns"))}
    for name in sorted(co.keys() | cn.keys()):
        if name not in cn:
            out.append(Change("breaking", f"schema.columns[{name}]", "column removed"))
        elif name not in co:
            nullable = cn[name].get("nullable", True) is not False
            out.append(Change("additive" if nullable else "tightening", f"schema.columns[{name}]", "new column"))  # fmt: skip
        else:
            out += _column(name, co[name], cn[name], bases)
    return sorted(out, key=lambda c: (c.path, c.kind))
