# Contributing to Floe

Thanks for helping improve Floe! Humans and AI agents follow the same rules: **read
[`AGENTS.md`](AGENTS.md) before your first change.** It covers scope (one issue per PR), the
size budget, the required checks, and what the PR body must contain.

## Quick start

1. Open or pick an issue. Every PR closes exactly one.
2. Fork the repo and create a branch from `main` (`git switch -c fix/short-name`).
3. Set up the environment: install [uv](https://docs.astral.sh/uv/), then `uv sync --all-groups`
   (Python >= 3.11). `uv run floe --help` runs the CLI from your checkout.
4. Make the smallest change that fixes the issue.
5. Run the checks in `AGENTS.md` §5 (ruff, pyright, pytest and the golden suite).
6. Open a PR using the template.

Floe is pure Python (`src/floe/`); there is no native code to build. The Rust implementation of
0.7 and earlier is archived at [OpenLakeForge/floe-rust](https://github.com/OpenLakeForge/floe-rust).

## Coding guidelines

See `context/coding-standards.md` and `context/testing-strategy.md`. In short:

- Avoid breaking changes without a migration note.
- Update `docs/` and `CHANGELOG.md` when behaviour or config changes in a user-visible way.
- No secrets in logs or fixtures.

## Reporting issues

Please use the issue templates (Feature request, Bug report). They capture required details and
success criteria.
