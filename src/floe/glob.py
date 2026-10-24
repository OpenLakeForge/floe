"""Port of the Rust `glob` crate 0.3.4: `Pattern` matcher and the `glob` walker.

Behaviour (error text, ordering, `**` collapsing, no silently skipped directories)
follows the crate so a pattern selects the same files in Rust and Python.
"""

import os
from dataclasses import dataclass
from pathlib import PurePath

from floe.errors import FloeError

_SEPS = frozenset(s for s in (os.sep, os.altsep) if s)
_MATCH, _SUB_NO, _ALL_NO = 0, 1, 2  # Match / SubPattern / EntirePattern DoesntMatch
_DONE = -1  # the crate's usize::MAX index: already matched by fill_todo

_E_WILD = "wildcards are either regular `*` or recursive `**`"
_E_REC = "recursive wildcards must form a single path component"
_E_RANGE = "invalid range pattern"

_Spec = tuple[str, str | None]  # (char, None) or (start, end)
_Token = tuple[str, str, tuple[_Spec, ...]]  # (kind, char, specs); kind: c ? * ** in ex


class PatternError(FloeError):
    def __init__(self, pos: int, msg: str) -> None:
        super().__init__(f"Pattern syntax error near position {pos}: {msg}")
        self.pos = pos
        self.msg = msg


@dataclass(frozen=True)
class MatchOptions:
    """Defaults are the crate's `MatchOptions::new()`."""

    case_sensitive: bool = True
    require_literal_separator: bool = False
    require_literal_leading_dot: bool = False


_DEFAULT = MatchOptions()


def _specs(s: str) -> tuple[_Spec, ...]:
    out: list[_Spec] = []
    i = 0
    while i < len(s):
        if i + 3 <= len(s) and s[i + 1] == "-":
            out.append((s[i], s[i + 2]))
            i += 3
        else:
            out.append((s[i], None))
            i += 1
    return tuple(out)


def _chars_eq(a: str, b: str, case_sensitive: bool) -> bool:
    if os.name == "nt" and a in _SEPS and b in _SEPS:
        return True
    if not case_sensitive and a.isascii() and b.isascii():
        return a.lower() == b.lower()
    return a == b


def _in_specs(specs: tuple[_Spec, ...], c: str, o: MatchOptions) -> bool:
    for start, end in specs:
        if end is None:
            if _chars_eq(c, start, o.case_sensitive):
                return True
            continue
        if not o.case_sensitive and c.isascii() and start.isascii() and end.isascii():
            lo, hi = start.lower(), end.lower()
            # only fold case when both ends are letters, as the crate does
            if lo != lo.upper() and hi != hi.upper() and lo <= c.lower() <= hi:
                return True
        if start <= c <= end:
            return True
    return False


class Pattern:
    def __init__(self, pattern: str) -> None:
        s, n, i = pattern, len(pattern), 0
        tokens: list[_Token] = []
        self.is_recursive = self.has_metachars = False
        while i < n:
            c = s[i]
            if c == "?":
                self.has_metachars = True
                tokens.append(("?", "", ()))
                i += 1
            elif c == "*":
                self.has_metachars = True
                old = i
                while i < n and s[i] == "*":
                    i += 1
                count = i - old
                if count > 2:
                    raise PatternError(old + 2, _E_WILD)
                if count < 2:
                    tokens.append(("*", "", ()))
                    continue
                if i == 2 or s[i - count - 1] in _SEPS:
                    if i < n and s[i] in _SEPS:
                        i += 1
                    elif i != n:
                        raise PatternError(i, _E_REC)
                else:
                    raise PatternError(old - 1, _E_REC)
                # the crate's `tokens_len > 1` is kept: "**/**" in one string stays 2
                if not (len(tokens) > 1 and tokens[-1][0] == "**"):
                    self.is_recursive = True
                    tokens.append(("**", "", ()))
            elif c == "[":
                self.has_metachars = True
                if i + 4 <= n and s[i + 1] == "!":
                    k = s.find("]", i + 3)
                    if k >= 0:
                        tokens.append(("ex", "", _specs(s[i + 2 : k])))
                        i = k + 1
                        continue
                elif i + 3 <= n and s[i + 1] != "!":
                    k = s.find("]", i + 2)
                    if k >= 0:
                        tokens.append(("in", "", _specs(s[i + 1 : k])))
                        i = k + 1
                        continue
                raise PatternError(i, _E_RANGE)
            else:
                tokens.append(("c", c, ()))
                i += 1
        self.original = pattern
        self.tokens = tokens

    @staticmethod
    def escape(s: str) -> str:
        return "".join(f"[{c}]" if c in "?*[]" else c for c in s)

    def matches_with(self, path: str, options: MatchOptions = _DEFAULT) -> bool:
        return self._from(True, path, 0, 0, options) == _MATCH

    def _from(self, follows: bool, s: str, p: int, ti: int, o: MatchOptions) -> int:
        for t in range(ti, len(self.tokens)):
            kind, ch, specs = self.tokens[t]
            if kind in ("*", "**"):
                m = self._from(follows, s, p, t + 1, o)
                if m != _SUB_NO:
                    return m
                while p < len(s):
                    c = s[p]
                    p += 1
                    if follows and o.require_literal_leading_dot and c == ".":
                        return _SUB_NO
                    follows = c in _SEPS
                    if kind == "**" and not follows:
                        continue
                    if kind == "*" and o.require_literal_separator and follows:
                        return _SUB_NO
                    m = self._from(follows, s, p, t + 1, o)
                    if m != _SUB_NO:
                        return m
                continue
            if p >= len(s):
                return _ALL_NO
            c = s[p]
            p += 1
            is_sep = c in _SEPS
            if kind != "c" and (
                (o.require_literal_separator and is_sep)
                or (follows and o.require_literal_leading_dot and c == ".")
            ):
                return _SUB_NO
            if kind == "in":
                ok = _in_specs(specs, c, o)
            elif kind == "ex":
                ok = not _in_specs(specs, c, o)
            elif kind == "c":
                ok = _chars_eq(c, ch, o.case_sensitive)
            else:
                ok = True
            if not ok:
                return _SUB_NO
            follows = is_sep
        return _MATCH if p >= len(s) else _SUB_NO


_Entry = tuple[str, bool, int]  # (path, is_directory, pattern index)
_Todo = list[_Entry | FloeError]


def _glob_error(path: str, e: OSError) -> FloeError:
    msg = f"{e.strerror} (os error {e.errno})" if e.errno else str(e)
    return FloeError(f"attempting to read `{path}` resulted in an error: {msg}")


def _is_dir(e: "os.DirEntry[str]") -> bool:
    try:
        return e.is_dir()
    except OSError:
        return False


def _is_utf8(name: str) -> bool:
    return not any("\udc80" <= c <= "\udcff" for c in name)  # surrogate-escaped bytes


def _fill_todo(
    todo: _Todo, pats: list[Pattern], idx: int, path: str, is_dir: bool, o: MatchOptions
) -> None:
    def add(p: str) -> None:
        d = os.path.isdir(p)
        if idx + 1 == len(pats):
            todo.append((p, d, _DONE))
        else:
            _fill_todo(todo, pats, idx + 1, p, d, o)

    pat = pats[idx]
    if not pat.has_metachars:
        s = pat.original
        special = s in (".", "..")
        nxt = s if path == "." else os.path.join(path, s)
        if (special and is_dir) or (not special and os.path.lexists(nxt)):
            add(nxt)
    elif is_dir:
        try:
            with os.scandir(path) as it:
                kids = [(e.name, _is_dir(e)) for e in it]
        except OSError as e:
            todo.append(_glob_error(path, e))
            return
        if o.require_literal_leading_dot:
            kids = [k for k in kids if not k[0].startswith(".")]
        kids.sort(key=lambda k: os.fsencode(k[0]), reverse=True)
        todo.extend(
            (k[0] if path == "." else os.path.join(path, k[0]), k[1], idx) for k in kids
        )
        # `.` and `..` only match a pattern with an explicit leading dot
        if pat.tokens and pat.tokens[0] == ("c", ".", ()):
            for special in (".", ".."):
                if pat.matches_with(special, o):
                    add(os.path.join(path, special))


def glob(pattern: str, options: MatchOptions = _DEFAULT) -> list[str]:
    """Paths matching `pattern` in the crate's order.

    Raises `PatternError` on bad syntax and `FloeError` (the crate's `GlobError`
    text) on the first directory that cannot be read.
    """
    Pattern(pattern)  # whole-pattern syntax check first, as the crate does
    drive = os.path.splitdrive(pattern)[0]
    root_len = len(drive) + (1 if pattern[len(drive) : len(drive) + 1] in _SEPS else 0)
    body = pattern[root_len:]
    if os.altsep:
        body = body.replace(os.altsep, os.sep)
    parts = body.split(os.sep)
    if parts[-1] == "":
        parts.pop()
    pats = [Pattern(c) for c in parts]
    if root_len == len(pattern):
        pats.append(Pattern(""))
    require_dir = pattern[-1:] in _SEPS
    scope = pattern[:root_len] or "."
    todo: _Todo = []
    _fill_todo(todo, pats, 0, scope, os.path.isdir(scope), options)
    out: list[str] = []
    last = len(pats) - 1
    while todo:
        item = todo.pop()
        if isinstance(item, FloeError):
            raise item
        path, is_dir, idx = item
        if idx == _DONE:
            if not require_dir or is_dir:
                out.append(path)
            continue
        if pats[idx].is_recursive:
            nxt = idx
            while nxt + 1 < len(pats) and pats[nxt + 1].is_recursive:
                nxt += 1
            if is_dir:
                _fill_todo(todo, pats, nxt, path, is_dir, options)
                if nxt == last:
                    out.append(path)
                    continue
            elif nxt == last:
                continue
            idx = nxt + 1
        name = PurePath(path).name
        # the crate skips non-UTF-8 names
        if name in ("", "..") or not _is_utf8(name):
            continue
        if not pats[idx].matches_with(name, options):
            continue
        if idx == last:
            if not require_dir or is_dir:
                out.append(path)
        else:
            _fill_todo(todo, pats, idx + 1, path, is_dir, options)
    return out
