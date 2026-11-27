import os
from pathlib import Path

import pytest

from floe.contract.publish import PlannedFile, preflight, publish, render_status
from floe.errors import FloeIoError


def _plan(path: str, old: str | None, new: str | None, status: str, reason: str = ""):
    return PlannedFile(path, old, new, status, reason)  # type: ignore[arg-type]  # test passes the literal as str


def test_created_and_updated_are_written(tmp_path: Path) -> None:
    (tmp_path / "b.yml").write_text("old")
    plan = [
        _plan("sub/a.yml", None, "A", "created"),
        _plan("b.yml", "old", "B", "updated"),
    ]
    assert preflight(str(tmp_path), plan) == []
    publish(str(tmp_path), plan)
    assert (tmp_path / "sub/a.yml").read_text() == "A"
    assert (tmp_path / "b.yml").read_text() == "B"
    assert sorted(p.name for p in tmp_path.rglob("*") if p.is_file()) == [
        "a.yml",
        "b.yml",
    ]


def test_blocked_entry_writes_nothing(tmp_path: Path) -> None:
    plan = [
        _plan("a.yml", None, "A", "created"),
        _plan("b.yml", None, None, "blocked", "hand edited"),
    ]
    assert preflight(str(tmp_path), plan) == ["b.yml: blocked: hand edited"]
    with pytest.raises(FloeIoError):
        publish(str(tmp_path), plan)
    assert list(tmp_path.iterdir()) == []


def test_stale_expected_old_writes_nothing(tmp_path: Path) -> None:
    (tmp_path / "b.yml").write_text("changed")
    plan = [
        _plan("a.yml", None, "A", "created"),
        _plan("b.yml", "old", "B", "updated"),
    ]
    assert preflight(str(tmp_path), plan) == ["b.yml: changed since planning"]
    with pytest.raises(FloeIoError):
        publish(str(tmp_path), plan)
    assert not (tmp_path / "a.yml").exists()
    assert (tmp_path / "b.yml").read_text() == "changed"


def test_created_file_that_now_exists_is_stale(tmp_path: Path) -> None:
    (tmp_path / "a.yml").write_text("theirs")
    plan = [_plan("a.yml", None, "A", "created")]
    assert preflight(str(tmp_path), plan) == ["a.yml: changed since planning"]


def test_staged_name_is_not_yml(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "b.yml").write_text("old")
    seen: list[str] = []
    link, replace = os.link, os.replace

    def spy_link(src: str, dst: str) -> None:
        seen.append(os.path.basename(src))
        link(src, dst)

    def spy_replace(src: str, dst: str) -> None:
        seen.append(os.path.basename(src))
        replace(src, dst)

    monkeypatch.setattr(os, "link", spy_link)
    monkeypatch.setattr(os, "replace", spy_replace)
    publish(
        str(tmp_path),
        [_plan("a.yml", None, "A", "created"), _plan("b.yml", "old", "B", "updated")],
    )
    assert len(seen) == 2
    assert all(n.endswith(".floe-tmp") and not n.endswith(".yml") for n in seen)


def test_symlink_escape_blocked(tmp_path: Path) -> None:
    root, outside = tmp_path / "root", tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    (root / "link").symlink_to(outside, target_is_directory=True)
    plan = [
        _plan("link/a.yml", None, "A", "created"),
        _plan("../x.yml", None, "X", "created"),
    ]
    assert preflight(str(root), plan) == [
        f"link/a.yml: path escapes {root}",
        f"../x.yml: path escapes {root}",
    ]
    assert list(outside.iterdir()) == []


def test_unchanged_plan_touches_nothing(tmp_path: Path) -> None:
    f = tmp_path / "a.yml"
    f.write_text("same")
    os.utime(f, (1, 1))
    publish(str(tmp_path), [_plan("a.yml", "same", None, "unchanged")])
    assert f.stat().st_mtime == 1


def test_render_status_is_deterministic() -> None:
    plan = [
        _plan("b.yml", None, "B", "created"),
        _plan("a.yml", "x", None, "unchanged"),
        _plan("c.yml", None, None, "blocked", "no"),
    ]
    assert render_status(plan) == (
        "1 created, 0 updated, 1 unchanged, 1 blocked\n"
        "unchanged a.yml\n"
        "created   b.yml\n"
        "blocked   c.yml: no"
    )


def test_conflicting_destinations_write_nothing(tmp_path: Path) -> None:
    plan = [
        _plan("entities", None, "A", "created"),
        _plan("entities/a.yml", None, "B", "created"),
        _plan("b.yml", None, "C", "created"),
        _plan("./b.yml", None, "D", "created"),
    ]
    assert preflight(str(tmp_path), plan) == [
        "./b.yml: same destination as b.yml",
        "entities/a.yml: inside planned file entities",
    ]
    with pytest.raises(FloeIoError):
        publish(str(tmp_path), plan)
    assert list(tmp_path.iterdir()) == []


def test_update_keeps_file_mode(tmp_path: Path) -> None:
    f = tmp_path / "a.yml"
    f.write_text("old")
    f.chmod(0o640)
    old_umask = os.umask(0o077)
    try:
        publish(str(tmp_path), [_plan("a.yml", "old", "new", "updated")])
    finally:
        os.umask(old_umask)
    assert f.read_text() == "new"
    assert f.stat().st_mode & 0o777 == 0o640


def test_staged_update_is_created_private(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    f = tmp_path / "a.yml"
    f.write_text("old")
    f.chmod(0o600)
    created: list[tuple[str, int]] = []
    real_open = os.open

    def spy_open(path: str, flags: int, mode: int = 0o777) -> int:
        if path.endswith(".floe-tmp"):
            created.append((path, mode))
        return real_open(path, flags, mode)

    monkeypatch.setattr(os, "open", spy_open)
    old_umask = os.umask(0o022)
    try:
        publish(str(tmp_path), [_plan("a.yml", "old", "new", "updated")])
    finally:
        os.umask(old_umask)
    assert [m for _, m in created] == [0o600]
    assert f.stat().st_mode & 0o777 == 0o600


def test_unencodable_text_writes_nothing(tmp_path: Path) -> None:
    plan = [
        _plan("a.yml", None, "A", "created"),
        _plan("b.yml", None, "\ud800", "created"),
    ]
    assert preflight(str(tmp_path), plan) == ["b.yml: new text is not valid UTF-8"]
    with pytest.raises(FloeIoError):
        publish(str(tmp_path), plan)
    assert list(tmp_path.iterdir()) == []


def test_update_works_without_fchmod(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    f = tmp_path / "a.yml"
    f.write_text("old")
    f.chmod(0o640)
    monkeypatch.delattr(os, "fchmod", raising=False)
    publish(str(tmp_path), [_plan("a.yml", "old", "new", "updated")])
    assert f.read_text() == "new"
    assert f.stat().st_mode & 0o777 == 0o640
