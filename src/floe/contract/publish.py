"""Preflight and publish a set of planned file writes.

Nothing is written unless every entry passes `preflight`. Files are staged next to
the target under a `.floe-tmp` name (never `.yml`, which a sibling-entity scan would
load) and moved into place whole. Atomicity across several files is not guaranteed
if the process crashes mid-publish, but no partial `.yml` is ever visible. Files are
never deleted.
"""

from __future__ import annotations

import os
import secrets
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal

from floe.errors import FloeIoError

Status = Literal["created", "updated", "unchanged", "blocked"]


@dataclass(frozen=True)
class PlannedFile:
    path: str  # relative to the publish root
    expected_old: str | None  # None: the file must not exist yet
    new_text: str | None
    status: Status
    reason: str = ""


def _resolve(root: str, path: str) -> str | None:
    base = os.path.realpath(root)
    full = os.path.realpath(os.path.join(base, path))
    return full if os.path.commonpath([base, full]) == base and full != base else None


def _read(full: str) -> str | None:
    try:
        with open(full, "rb") as f:
            return f.read().decode()
    except FileNotFoundError:
        return None


def _can_create_parent(parent: str) -> bool:
    while not os.path.exists(parent):
        parent = os.path.dirname(parent)
    return os.path.isdir(parent) and os.access(parent, os.W_OK | os.X_OK)


def preflight(root: str, plan: Sequence[PlannedFile]) -> list[str]:
    """Return every problem found; writes nothing."""
    problems: list[str] = []
    for p in plan:
        if p.status == "blocked":
            problems.append(f"{p.path}: blocked: {p.reason}")
            continue
        full = _resolve(root, p.path)
        if full is None:
            problems.append(f"{p.path}: path escapes {root}")
            continue
        try:
            current = _read(full)
        except (OSError, UnicodeDecodeError) as e:
            problems.append(f"{p.path}: cannot read current file: {e}")
            continue
        if current != p.expected_old:
            problems.append(f"{p.path}: changed since planning")
        elif p.status in ("created", "updated"):
            if p.new_text is None:
                problems.append(f"{p.path}: no new text to write")
            elif not _can_create_parent(os.path.dirname(full)):
                problems.append(f"{p.path}: parent directory is not writable")
    return problems


def _write(full: str, text: str, create: bool) -> None:
    head, name = os.path.split(full)
    tmp = os.path.join(head, f".{name}.{secrets.token_hex(6)}.floe-tmp")
    try:
        os.makedirs(head, exist_ok=True)
        with open(tmp, "xb") as f:
            f.write(text.encode())
        if create:
            os.link(tmp, full)  # fails if the file appeared meanwhile
        else:
            os.replace(tmp, full)
    except OSError as e:
        raise FloeIoError(f"failed to write {full}: {e}", code="io_write_error") from e
    finally:
        try:
            os.unlink(tmp)
        except FileNotFoundError:
            pass


def publish(root: str, plan: Sequence[PlannedFile]) -> None:
    """Write the `created` and `updated` entries; raises if preflight finds a problem."""
    if problems := preflight(root, plan):
        raise FloeIoError("; ".join(problems), code="io_write_error")
    for p in plan:
        if p.status in ("created", "updated") and p.new_text is not None:
            full = _resolve(root, p.path)
            if full is not None:
                _write(full, p.new_text, p.status == "created")


def render_status(plan: Sequence[PlannedFile]) -> str:
    counts = Counter(p.status for p in plan)
    lines = [
        ", ".join(
            f"{counts[s]} {s}" for s in ("created", "updated", "unchanged", "blocked")
        )
    ]
    for p in sorted(plan, key=lambda p: p.path):
        lines.append(f"{p.status:<9} {p.path}" + (f": {p.reason}" if p.reason else ""))
    return "\n".join(lines)
