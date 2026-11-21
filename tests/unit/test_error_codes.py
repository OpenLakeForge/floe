"""Typed error codes (#1260): the reference page lists them, and every raise site passes one."""

import ast
import re
from pathlib import Path
from typing import get_args

from floe.errors import ErrorCode

REPO = Path(__file__).resolve().parents[2]
SRC = REPO / "src" / "floe"
PREFIX = {
    "FloeConfigError": "config_",
    "FloeIoError": "io_",
    "FloeRunError": "run_",
    "FloeStorageError": "storage_",
}

# Files with a raise site that has no `code=` yet. Each of #1260's PRs 2-7 removes its files;
# the last one deletes this set and the `uncoded == ALLOWLIST` check.
ALLOWLIST = {
    "pii.py",
    "run/__init__.py",
    "run/entity.py",
    "run/file.py",
    "run/incremental.py",
    "storage/s3.py",
    "write/outputs.py",
}


def _sites(path: Path) -> list[tuple[int, str, ast.Call]]:
    """Every `Floe…Error(...)` call, and every `_fail(...)` call in config/validate.py (it builds a FloeConfigError)."""
    rel = path.relative_to(SRC).as_posix()
    out: list[tuple[int, str, ast.Call]] = []
    tree = ast.parse(path.read_text(encoding="utf-8"))
    # A helper that takes `code` forwards it; its callers are the sites (`_fail`, `_pii_error`).
    forwarded = {
        id(inner)
        for fn in ast.walk(tree)
        if isinstance(fn, ast.FunctionDef)
        and any(a.arg == "code" for a in fn.args.kwonlyargs)
        for inner in ast.walk(fn)
    }
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            name = node.func.id
            if id(node) in forwarded:
                continue
            if re.fullmatch(r"Floe\w*Error", name):
                out.append((node.lineno, name, node))
            elif name in ("_fail", "_pii_error") and rel == "config/validate.py":
                out.append((node.lineno, "FloeConfigError", node))
    return out


def _code(call: ast.Call) -> ast.expr | None:
    return next((k.value for k in call.keywords if k.arg == "code"), None)


def test_the_errors_page_lists_exactly_the_error_codes() -> None:
    page = (REPO / "docs" / "reference" / "errors.md").read_text(encoding="utf-8")
    assert re.findall(r"^\| `(\w+)` \|", page, re.MULTILINE) == list(
        get_args(ErrorCode)
    )


def test_every_raise_site_passes_a_code_whose_prefix_matches_its_class() -> None:
    codes = set(get_args(ErrorCode))
    uncoded: set[str] = set()
    for path in sorted(SRC.rglob("*.py")):
        rel = path.relative_to(SRC).as_posix()
        for line, cls, call in _sites(path):
            code = _code(call)
            if code is None:
                uncoded.add(rel)
                continue
            if (
                isinstance(code, ast.Attribute)
                and code.attr == "code"
                and isinstance(code.value, ast.Name)
            ):
                # A wrapper that re-raises a caught error keeps its code (`code=e.code`); the
                # prefix of a code that is not known statically is not checked.
                continue
            assert isinstance(code, ast.Constant), f"{rel}:{line} code is not a literal"
            assert code.value in codes, f"{rel}:{line} unknown code {code.value!r}"
            if cls in PREFIX:
                assert str(code.value).startswith(PREFIX[cls]), (
                    f"{rel}:{line} {cls} with {code.value}"
                )
    assert uncoded == ALLOWLIST, (
        f"missing code=: {sorted(uncoded - ALLOWLIST)}; migrated, remove from ALLOWLIST: {sorted(ALLOWLIST - uncoded)}"
    )
