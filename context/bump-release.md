# Floe — How to bump and release

Three packages are released from this repo, each from its own tag:

| Package | Version lives in | Tag | Workflow | Publishes |
|---|---|---|---|---|
| `floe-python` (Floe) | `pyproject.toml` `[project] version` | `vX.Y.Z` | `.github/workflows/release.yml` | PyPI wheel + sdist, GitHub Release, `ghcr.io/openlakeforge/floe:<X.Y.Z>` and `:latest` |
| `dagster-floe` | `orchestrators/dagster-floe/pyproject.toml` | `dagster-floe-vX.Y.Z` | `.github/workflows/release-dagster-floe.yml` | PyPI |
| `airflow-floe` | `orchestrators/airflow-floe/pyproject.toml` | `airflow-floe-vX.Y.Z` | `.github/workflows/release-airflow-floe.yml` | PyPI |

Each release is two steps: a bump PR, then a tag pushed by a maintainer once it is merged.
Version bumps get their own PR (AGENTS.md §3).

---

## 1. Decide the new version

Floe follows semantic versioning, pre-1.0:

| Change | Version bump |
|---|---|
| Breaking config, manifest, report or API change | minor (`0.X.0`) |
| New feature, backward-compatible | minor (`0.X.0`) or patch (`0.x.Y`) |
| Bug fix, internal refactor, test, doc | patch (`0.x.Y`) |

The Floe major.minor is also a contract version (D21, D22):
- a config's `version:` is the minimum Floe version it targets; Floe accepts any version up to
  its own major.minor (`FLOE_VERSION` in `src/floe/config/validate.py`, derived from the package
  version);
- the manifest `spec_version` is the major.minor that generated it.

So a **minor** Floe bump changes the manifest `spec_version` the connectors must accept (step 4).

---

## 2. Floe (`floe-python`) bump PR

Branch `chore/bump-v<NEW_VERSION>` from `main`, then:

1. Set `version` in `pyproject.toml`.
2. Run `uv lock` so `uv.lock` records the new version of the `floe-python` project.
3. Move the `## v<NEW_VERSION> (unreleased)` section of `CHANGELOG.md` to `## v<NEW_VERSION>`, or
   write it (one bullet per user-visible change; source: `git log <previous-tag>..HEAD`, read the
   commit bodies).
4. Check that `docs/` covers every user-visible change (`docs/config.md`, `docs/cli.md`,
   `docs/sinks/<format>.md`, `docs/pii.md`, `docs/lineage.md`, `docs/profiles.md`,
   `docs/manifest.md`, `docs/support-matrix.md`).
5. Run the checks in `AGENTS.md` §5. The golden suite ignores `tool.version`, so it needs no
   re-record.

Title: `chore: release floe-python v<NEW_VERSION>`.

## 3. Floe release

After the bump PR merges, a maintainer tags the merge commit on `main`:

```bash
git tag v<NEW_VERSION> <merge-commit> && git push origin v<NEW_VERSION>
```

`release.yml` then:
- refuses a tag that is not on `main`, or whose version differs from `pyproject.toml`;
- builds with `uv build` and smoke-tests the `py3-none-any` wheel (`floe --help`);
- publishes to PyPI through trusted publishing (environment `pypi`) and creates the GitHub
  Release with the built files;
- builds `Dockerfile.python` for linux/amd64 and linux/arm64 and pushes
  `ghcr.io/openlakeforge/floe:<version>` and `:latest`.

A `workflow_dispatch` run from a branch (or with `dry_run`) builds the wheel and the image and
publishes nothing.

## 4. Connector bump PRs

`dagster-floe` and `airflow-floe` are versioned independently; bump one only when it has changes,
or when a Floe minor release changes what it must accept. Each gets its own PR
(`chore(dagster-floe): release X.Y.Z`):

1. Set `version` in `orchestrators/<connector>/pyproject.toml`.
2. For a new Floe minor, add the new `spec_version` to `SUPPORTED_SPEC_VERSIONS` in the
   connector's `manifest.py`, and update `dagster-floe`'s `floe` extra pin (`floe-python>=X.Y,<X.Y+1`)
   and its compatibility table in the README.
3. If the manifest schema changed, edit the canonical `orchestrators/schemas/floe.manifest.schema.json`,
   copy it into the connectors, and run `python scripts/check_orchestrator_drift.py`.
4. Write the connector's `CHANGELOG.md` section (`## dagster-floe vX.Y.Z`).
5. Run the connector's tests (`.github/workflows/dagster-floe.yml` / `airflow-floe.yml`).

After the merge, a maintainer pushes `dagster-floe-v<X.Y.Z>` or `airflow-floe-v<X.Y.Z>`; the
release workflow builds the package with `python -m build` and publishes it to PyPI.

---

## Notes

- Floe 0.7 and earlier were released from the Rust crates (crates.io, Homebrew, Scoop, binaries);
  that process is archived with the code at
  [OpenLakeForge/floe-rust](https://github.com/OpenLakeForge/floe-rust).
