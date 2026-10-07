# Floe — Testing Strategy

## Test location

All tests live in `crates/floe-core/tests/`. There are two categories:

```
tests/
├── unit/         ← focused tests for a single module or function
│   ├── config/   ← config parsing, validation, variable resolution
│   ├── run/      ← individual pipeline stages (checks, output routing)
│   ├── state/    ← incremental state CAS logic
│   └── ...
└── integration/  ← full end-to-end pipeline runs against real files
    ├── csv_run.rs
    ├── delta_run.rs
    ├── parquet_run.rs
    └── ...
```

## Principles

**No storage mocking.** Integration tests use real temporary directories and real file I/O. The incremental state tests exercise the actual CAS path. Mocking at the storage layer has historically masked production bugs (e.g., a mock passing while a real migration failed).

**Integration tests run the full pipeline.** They call `floe_core::run(config)` with a real config pointing at real fixture files. They assert on output file presence, row counts from the JSON report, and rejection file content.

**Unit tests focus on logic.** Pure functions (schema validation, error column construction, config validation errors, CAS retry logic) are tested at the unit level without spinning up a full pipeline run.

## Running tests

```bash
# All tests
cargo test -p floe-core

# Single test by name
cargo test -p floe-core -- unit::state::claim_expires_after_ttl

# Integration tests only
cargo test -p floe-core -- integration::

# Unit tests only
cargo test -p floe-core -- unit::
```

## Fixture files

Input fixture files live in `crates/floe-core/tests/fixtures/` (or inline in the test as temp files). A fixture set for a format typically includes:
- A valid file (all rows pass)
- An invalid file (some rows fail type checks or null constraints)
- A missing-columns file (triggers file-level rejection)

## What to test when adding a feature

| Change type | Test required |
|---|---|
| New config field | Unit test: valid value accepted, invalid value produces correct error message |
| New check type | Unit test for check logic; integration test for end-to-end rejection routing |
| New sink format | Integration test: full run writes accepted output, invalid rows go to rejected |
| New storage operation | Test via the local provider (same `StorageClient` trait) |
| Config validation change | Unit test asserting on the exact error message string |
| Incremental state change | Unit test for the CAS retry / claim / promote path |

## Assertion style

- Assert on the JSON run report (`run.json`) for row counts and status — not on file contents directly, as file formats vary.
- For rejection tests, assert that `rejected_total > 0` and optionally read the rejected CSV to verify specific rows.
- For config validation tests, assert the **exact error message string** — these are part of the user-facing contract.
