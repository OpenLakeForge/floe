# Release & CI/CD

This document describes the GitHub Actions workflows that publish Floe.

## Workflows

- `CI` (`.github/workflows/ci.yml`)
  - Runs on PRs, the merge queue and pushes to `main`.
  - Runs ruff, pyright, pytest, the conformance cases and the config schema tests.

- `Release` (`.github/workflows/release.yml`)
  - Runs on version tags `vX.Y.Z` that are on `main`.
  - Checks that the tag equals the `pyproject.toml` version.
  - Builds `floe-python` with `uv build` (one `py3-none-any` wheel and an sdist),
    smoke-tests the wheel and publishes both to PyPI.
  - Creates the GitHub Release with the wheel and the sdist attached.
  - Builds `Dockerfile.python` for amd64 and arm64 and pushes
    `ghcr.io/openlakeforge/floe:<version>` and `:latest`.

## Publishing credentials

- PyPI uses a trusted publisher bound to `release.yml` and the `pypi`
  environment; there is no token. Renaming the workflow file breaks publishing.
- GHCR uses the built-in `GITHUB_TOKEN`.

## Release flow

1. Bump `version` in `pyproject.toml` in a chore PR and merge it.
2. Tag the release on `main`:
   - `git tag vX.Y.Z`
   - `git push origin vX.Y.Z`

For dry runs, dispatch the workflow with `dry_run: true`
(`gh workflow run release.yml --ref <branch> -f dry_run=true`). It builds the
wheel and the image and publishes nothing.
