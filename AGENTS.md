# AGENTS.md — rules for anyone (human or AI) changing Floe

These rules are not suggestions. A PR that breaks one gets closed, not fixed up.
The goal: **the smallest correct change, one concern per PR, nothing speculative.**

## 1. Read the context you need — and only that

| Read | When |
|---|---|
| `context/project.md` | First time in the repo: what Floe is and is not |
| `context/architecture.md` | Before touching anything under `crates/` |
| `context/coding-standards.md` | Before writing Rust |
| `context/testing-strategy.md` | Before writing or moving tests |
| `context/orchestrators.md` | Before touching `orchestrators/` |
| `context/bump-release.md` | Version bump PRs only |
| `context/decisions/*.md` | Before changing errors, incremental state, merge, or Polars/Arrow usage |

## 2. Ponytail is mandatory

- **Claude Code:** install the plugin once (`/plugin marketplace add DietrichGebert/ponytail`,
  then `/plugin install ponytail@ponytail`) and work in `full` mode. Before opening a PR, run
  `/ponytail-review` on your diff and resolve every finding, or justify it in the PR body.
- **Other agents** apply the same ladder. Stop at the first rung that holds:
  1. Does this need to exist at all? If not, skip it and say so.
  2. Does it already exist in this codebase? Reuse it. Grep before you write.
  3. Does the Rust/Python stdlib do it?
  4. Does a dependency we already have do it? Never add one for a few lines.
  5. Can it be one line?
  6. Only then: the minimum code that works.
- Mark deliberate shortcuts that have a known ceiling with a `ponytail:` comment naming the
  ceiling and the upgrade path.
- The ladder shortens the solution, never the reading. Understand the flow before picking a rung.

## 3. Hard rules

**Scope**
- One issue → one branch → one PR. Never bundle issues (#427 fixed three at once; don't).
- Noticed an unrelated bug or smell? Open an issue. Do not fix it in this PR.
- `fix:` PRs add no new CLI flag, config field, manifest field or public API. That is a `feat:`
  and gets its own issue (#444 was a "fix" that added `--runtime`; don't).
- Version bumps, dependency bumps and schema resyncs each get their own PR.

**Size** (enforced by `.github/workflows/pr-size.yml`)
- Budget: **≤ 300 changed lines and ≤ 8 files** of non-test code under `crates/` and
  `orchestrators/`. Tests, fixtures, docs, CHANGELOG, lockfiles and JSON schemas don't count.
- Over budget? Split it into stacked PRs: a behaviour-neutral prep PR (move, extract, rename)
  first, then the behaviour change on top.
- Only a maintainer may apply the `large-pr` label. Agents never apply it.

**Diff hygiene**
- Read before you write: trace the real flow end to end, and grep every caller of a function
  before changing it. Fix the root cause in the shared function, not the symptom in one caller.
- No drive-by refactors, renames, reformatting, comment rewrites or "while I'm here" edits in
  code the change does not need.
- No new dependency without approval on the issue first.
- No new trait, generic, builder, config knob or module until three concrete uses exist.
- Never hand-edit a vendored copy. Edit the canonical file (`orchestrators/schemas/`, or the
  dagster copy of a shared module) and let `python scripts/check_orchestrator_drift.py` confirm
  the copies match.
- No `#[allow(...)]` or `# noqa` without an inline reason.

**Tests**
- Every bug fix ships one test that fails without the fix.
- Every new behaviour ships the test listed for its change type in `context/testing-strategy.md`.
- Don't write tests for code you didn't change.

**Git**
- Never commit to `main`. Never force-push a branch you did not create.
- Use conventional commit titles: `fix(scope):`, `feat(scope):`, `docs:`, `ci:`, `chore:`.

## 4. Stop and ask instead of guessing

Ask on the issue, or ask the user, before writing code when:
- the issue is ambiguous, or the fix could go in more than one place;
- the change touches the config YAML schema or the manifest format (both are user contracts);
- it needs a new dependency;
- it would go over the size budget and you can't see a clean split.

## 5. Checks before every push

These mirror `.github/workflows/ci.yml`. All must pass.

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --no-default-features --features delta,iceberg -- -D warnings
cargo test -p floe-core --test unit <module>::          # the modules you touched
cargo test -p floe-core --test integration -- --test-threads=1   # if floe-core/src changed
cargo test -p floe-core --doc                           # if floe-core changed
cargo test -p floe-cli --tests                          # if floe-cli changed
python scripts/check_orchestrator_drift.py              # if orchestrators/ changed
```

If `crates/floe-python` changed (inside a venv with `maturin`, `pytest`, `mypy`):

```bash
RUSTFLAGS="-C debuginfo=0" maturin develop --manifest-path crates/floe-python/Cargo.toml
pytest crates/floe-python/tests/
mypy crates/floe-python/tests/test_floe.py --ignore-missing-imports
```

## 6. PR body

Use `.github/PULL_REQUEST_TEMPLATE.md`. It must state:
- `Closes #N`: exactly one issue;
- why this is the minimum change;
- what you deliberately did not do;
- the `/ponytail-review` result (or "ladder applied" for non-Claude agents);
- the commands you ran.

Update `CHANGELOG.md` and `docs/` only when the change is user-visible.
