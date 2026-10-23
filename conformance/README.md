# Conformance suite

Golden cases recorded from the Rust v0.7.0 release binary. Parity with Rust 0.7 means zero diff
against `expected/` (see `context/decisions/python-core-for-olf.md`, "Conformance suite").

## Layout

```
conformance/<case>/
├── config.yml        run as `floe run -c config.yml` from a temp copy of the case
├── in/               input files
├── expected/
│   ├── reports/      normalized run.summary.json and each entity run.json
│   ├── accepted/     accepted rows, one Parquet file per accepted output dir
│   └── rejected/     rejected rows (CSV)
├── delta.yml         optional: intentional deltas of this case, e.g. `[D9]`
└── expected_python/  optional: only the `expected/` files that differ for Python
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

## Intentional deltas

Where Python must differ from Rust 0.7 on purpose (the D-register in #619), a case carries:

- `delta.yml`: one line listing the delta ids, e.g. `[D9]`;
- `expected_python/`: the files that differ, at the same relative path as in `expected/`.

`check.py --impl rust` always compares against `expected/`. `--impl python` compares each file against
`expected_python/<file>` when it exists, else `expected/<file>`, and prints the delta ids when such a case fails.
`record.py` only writes `expected/` (from Rust); `expected_python/` is derived from it and committed. The case
README says how, so a reviewer can redo it.

## Normalization

- Masked when recording: run ids, timestamps, durations, absolute paths (`<ROOT>`, `run_<id>`).
- Compared by shape: UUID-based part file names (`part-<uuid>`), Iceberg `snapshot_id`.
- Recorded but ignored on compare (G5 ruling, #594): `total_bytes_written`, `avg_file_size_mb`.
- Paths in `expected/` use `run_ID` and `part-UUID` (no `<`/`>`: Windows can't check them out); file contents keep `run_<id>` / `part-<uuid>`.
- Only `out/accepted` and `out/rejected` are captured; `record.py` fails on a case that produces neither.
- Parquet dates and timestamps are compared as Arrow-rendered text, so years outside 1..9999 (which Python's `date` cannot hold) work.
- Key order in reports is kept: it is part of the contract.
