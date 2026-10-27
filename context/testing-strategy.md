# Floe — Testing Strategy

## Layout

```
tests/
├── unit/          ← one module or function; grouped by package (config/, read/, checks/,
│                    run/, state/, storage/, iceberg/, ...) plus test_cli_*.py per command
│   └── fixtures/  ← small input files
└── integration/   ← services: Polaris REST catalog, S3-compatible store (SeaweedFS)
conformance/       ← golden suite: one case per feature, `check.py`
```

## Running tests

```bash
uv sync --all-groups
uv run pytest                                    # everything that needs no service
uv run pytest tests/unit/<file>.py::<test>       # one test
uv run python conformance/check.py               # golden suite
uv run python conformance/check.py --case NAME   # one golden case
```

Integration tests skip unless their service is configured: `-m polaris` needs
`FLOE_POLARIS_URI`, `-m s3` needs `FLOE_S3_ENDPOINT` (see `.github/workflows/python-integration.yml`).

## Principles

**No storage mocking.** Tests use real temporary directories and real file I/O; the S3 tests
use a real S3-compatible service. The incremental state tests exercise the actual CAS path.
Mocking the storage layer has masked production bugs before.

**The golden suite is the regression gate.** `conformance/` holds the golden cases: run reports,
accepted and rejected rows, `floe validate` outcomes, error messages, CLI flags and manifests.
Any difference from `expected/` fails CI. An intended behaviour change re-records the case
(`uv run python conformance/record.py --case NAME`) and the PR shows the diff. See
`conformance/README.md` for the case layout.

**Unit tests focus on logic.** Pure functions (casts, error columns, config validation errors,
CAS retry logic) are tested without a full pipeline run. CLI tests call the command and assert
its output and exit code.

## What to test when adding a feature

| Change type | Test required |
|---|---|
| New config field | Unit test: valid value accepted, invalid value produces the exact error message |
| Config validation change | Unit test asserting on the exact error message string (or a validate golden case under `conformance/config/`) |
| New check or reader | Unit test for the logic; a golden case for end-to-end rejection routing |
| Sink or write-mode change | A golden case for local output; an integration test (`-m polaris` / `-m s3`) for Iceberg |
| New storage operation | Test via the local client (same `StorageClient` protocol); S3 behaviour in `-m s3` |
| Incremental state change | Unit test for the CAS retry / claim / promote path |
| CLI flag or command | `tests/unit/test_cli_<command>.py` |
| Public API change | `tests/unit/test_public_api.py` |

## Assertion style

- Assert on the run report (`run.json`) for row counts and status, not on output bytes.
- For rejection tests, assert `rejected_total` and read the rejected CSV for the rows that matter.
- For config validation tests, assert the **exact error message string**: messages are part of
  the user-facing contract.
- Every bug fix ships one test that fails without the fix.
