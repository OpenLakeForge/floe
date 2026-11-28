"""`floe contract import`: plan, gate and publish a workbook's entity files."""

import os
import re
import shutil
import tempfile
from collections import defaultdict
from collections.abc import Mapping
from pathlib import Path
from typing import Any, cast

from floe.config.parse import parse_config
from floe.config.validate import validate_config
from floe.config.yaml import parse_yaml
from floe.contract.changes import Change, classify
from floe.contract.effective import contract_overrides
from floe.contract.publish import PlannedFile, preflight, publish, render_status
from floe.contract.reconcile import reconcile
from floe.contract.render import build_entity, owned, plan_first_import, render_entity
from floe.contract.workbook import invalid, parse_workbook
from floe.errors import FloeConfigError

_KINDS = ("breaking", "tightening", "loosening", "additive", "metadata")
# the two errors an untouched scaffold raises; any other message naming TODO_ (an entity called TODO_x) is real
_SCAFFOLD = re.compile(
    r"source\.ref=TODO_source is not a declared source|references unknown variable TODO_(accepted|rejected)_path"
)
_MARK = "# floe-contract:"


def _read(path: str) -> str | None:
    try:
        if not os.path.exists(path):
            return None
        with open(
            path, "rb"
        ) as f:  # byte-exact (CRLF kept), as publish's preflight reads it
            return f.read().decode("utf-8")
    except (OSError, UnicodeDecodeError) as e:
        raise FloeConfigError(
            f"cannot read {path}: {e}", code="config_invalid_value"
        ) from e


def _owned(text: str) -> dict[str, Any]:
    return owned(cast("Mapping[Any, Any]", parse_yaml(text)[0]))


_HEAVY = {".git", ".venv", "venv", "node_modules", "__pycache__"}


def _skip(directory: str, names: list[str]) -> list[str]:
    """Copy only YAML (and directories), not the known heavy directories."""
    return [
        n
        for n in names
        if n in _HEAVY
        or not (
            os.path.isdir(os.path.join(directory, n)) or n.endswith((".yml", ".yaml"))
        )
    ]


def _unstageable(tmp: str) -> str | None:
    """A staged path whose reads would leave the staged copy: an escaping link, or an absolute `entities`."""
    base = os.path.realpath(tmp)
    for here, dirs, files in os.walk(tmp):
        for n in dirs + files:
            path = os.path.join(here, n)
            if (
                os.path.islink(path)
                and os.path.commonpath([base, os.path.realpath(path)]) != base
            ):
                return os.path.relpath(path, tmp)
        if "_domain.yml" in files:
            doc = parse_yaml(Path(here, "_domain.yml").read_text(encoding="utf-8"))[0]
            if isinstance(doc, dict) and os.path.isabs(
                str(cast("dict[str, Any]", doc).get("entities") or "")
            ):
                return os.path.relpath(os.path.join(here, "_domain.yml"), tmp)
    return None


def _validate(
    config: str, output_dir: str, plan: list[PlannedFile], domain: str
) -> None:
    """Validate the project as it would be after the import, in a temporary copy of its YAML."""
    real = os.path.realpath(config)
    root = os.path.dirname(real)
    out = os.path.realpath(output_dir)
    if os.path.commonpath([root, out]) != root:
        raise FloeConfigError(
            f"--output-dir {output_dir} is outside the project of {config}",
            code="config_invalid_value",
        )
    doc = cast("dict[str, Any]", parse_yaml(Path(real).read_text(encoding="utf-8"))[0])
    include = cast("dict[str, Any]", doc.get("include") or {})
    patterns = cast("list[Any]", include.get("domains") or [])
    if any(isinstance(x, str) and os.path.isabs(x) for x in patterns):
        raise FloeConfigError(
            "-c needs relative include.domains patterns", code="config_invalid_value"
        )
    with tempfile.TemporaryDirectory() as tmp:
        # ponytail: copies YAML only, so it checks config, not data files; upgrade to a full copy if that ever matters
        shutil.copytree(root, tmp, dirs_exist_ok=True, symlinks=True, ignore=_skip)
        for p in plan:
            if p.new_text is not None and p.status in ("created", "updated"):
                dest = os.path.join(
                    tmp, os.path.relpath(os.path.join(out, p.path), root)
                )
                os.makedirs(os.path.dirname(dest), exist_ok=True)
                Path(dest).write_text(p.new_text, encoding="utf-8")
        if bad := _unstageable(tmp):
            raise FloeConfigError(
                f"-c cannot validate this layout: {bad}", code="config_invalid_value"
            )
        try:
            project = parse_config(Path(tmp) / os.path.basename(real))
            validate_config(project)
        except FloeConfigError as e:
            if not _SCAFFOLD.search(str(e)):
                raise FloeConfigError(
                    f"the project would not validate after the import: {e}", code=e.code
                ) from e
            print("Project not yet valid: complete _domain.yml, then run floe validate")
            return
    for entity in project.entities:
        if entity.domain == domain:
            for o in contract_overrides(entity):
                print(f"override: {entity.name}: {o.path} = {o.setting}: {o.effect}")


def run_import(
    book: str,
    output_dir: str,
    domain: str | None,
    dry_run: bool,
    check: bool,
    accept_breaking: bool,
    config: str | None = None,
) -> int:
    wb = parse_workbook(Path(book))
    if domain is not None and domain != wb.domain.name:
        raise FloeConfigError(
            f"--domain {domain} does not match the workbook's domain {wb.domain.name}",
            code="config_invalid_value",
        )
    errors: list[str] = []
    severity = wb.domain.default_severity
    if severity is None:  # fail closed: no silent default severity
        errors += [
            f"Guide.default_severity is blank and table {t.target_table} sets none: set warn, reject or abort"
            for t in wb.tables
            if t.default_severity is None
        ]
        if errors:
            raise invalid(errors)
    texts: dict[str, str] = {}
    entities: dict[str, dict[str, Any]] = {}
    for table in wb.tables:
        cols = [c for c in wb.columns if c.target_table == table.target_table]
        entity = build_entity(wb.domain.name, table, cols, severity or "", errors)
        entities[table.target_table] = entity
        texts[table.target_table] = render_entity(entity, table, cols)
    if errors:
        raise invalid(errors)
    plan = plan_first_import(output_dir, wb.domain.name, texts)
    changes: dict[str, list[Change]] = {}
    for name, entity in entities.items():
        existing = _read(os.path.join(output_dir, f"{name}.yml"))
        if existing is None:
            continue
        planned = reconcile(f"{name}.yml", existing, entity)
        plan.append(planned)
        if planned.status != "blocked":
            changes[name] = classify(_owned(existing), owned(entity))
    if os.path.isdir(output_dir):
        for file in sorted(os.listdir(output_dir)):
            stem = file[:-4]
            text = (
                _read(os.path.join(output_dir, file)) if file.endswith(".yml") else None
            )
            if text and stem not in entities and text.startswith(_MARK):
                changes[stem] = classify(_owned(text), None)  # never deleted
    for line in wb.warnings:
        print(f"warning: {line}")
    print(render_status(plan))
    by_kind: dict[str, list[str]] = defaultdict(list)
    for name in sorted(changes):
        for c in changes[name]:
            by_kind[c.kind].append(f"  {name}: {c.path}: {c.detail}")
    for kind in _KINDS:
        if by_kind[kind]:
            print(f"{kind}:\n" + "\n".join(by_kind[kind]))
    problems = preflight(output_dir, plan)
    if problems:
        print("\n".join(problems))
        return 1
    if by_kind["breaking"] and not accept_breaking:
        print("breaking changes need --accept-breaking; nothing was written")
        return 1
    if config is not None:
        _validate(config, output_dir, plan, wb.domain.name)
    pending = any(p.status in ("created", "updated") for p in plan)
    if check:
        return 1 if pending else 0
    if not dry_run:
        publish(output_dir, plan)
        if any(p.path == "_domain.yml" for p in plan):
            print(
                f"Next: complete {os.path.join(output_dir, '_domain.yml')}, add the domain to "
                "include.domains, then run floe validate"
            )
    return 0
