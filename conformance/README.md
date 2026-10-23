# Conformance suite

Golden cases recorded from the Rust v0.7.0 release binary. Parity with Rust 0.7 means zero diff
against `expected/` (see `context/decisions/python-core-for-olf.md`, "Conformance suite").

## Layout

```
conformance/<case>/
├── config.yml    run as `floe run -c config.yml` from a temp copy of the case
├── in/           input files
└── expected/
    ├── reports/  normalized run.summary.json and each entity run.json
    ├── accepted/ accepted rows, one Parquet file per accepted output dir
    └── rejected/ rejected rows (CSV)
```

A case is any directory holding a `config.yml` (it may be nested, e.g. `example/standalone`).

## Record

```bash
uv sync --group conformance
uv run python conformance/record.py [--case NAME]
```

Downloads the v0.7.0 binary with `gh`, verifies its `.sha256`, and caches it in
`~/.cache/floe-conformance/`. Recording twice gives an identical `expected/`.

## Check

```bash
uv run python conformance/check.py --impl rust|python [--case NAME]
```

Runs the implementation on every case and prints a diff for each difference; exits 1 if any.
`--impl python` runs `uv run floe run -c config.yml`.

## Normalization

- Masked when recording: run ids, timestamps, durations, absolute paths (`<ROOT>`, `run_<id>`).
- Compared by shape: UUID-based part file names (`part-<uuid>`), Iceberg `snapshot_id`.
- Recorded but ignored on compare (G5 ruling, #594): `total_bytes_written`, `avg_file_size_mb`.
- Key order in reports is kept: it is part of the contract.
