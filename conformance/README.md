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

**Validate cases** are the cases under `config/`: they run `floe validate -c config.yml` (plus `--profile profile.yml`
when the case has one) and hold only `expected/validate.json`, `{"ok": true}` or `{"ok": false, "error": "<message>"}`.
See [config/README.md](config/README.md).

## Record

```bash
uv sync --group conformance
uv run python conformance/record.py [--case NAME]
```

Downloads the v0.7.0 binary with `gh`, verifies its `.sha256`, and caches it in
`~/.cache/floe-conformance/`. Recording twice gives an identical `expected/`.

## Check

```bash
uv run python conformance/check.py --impl rust|python [--case NAME | --cases-file PATH]
```

Runs the implementation on every case and prints a diff for each difference; exits 1 if any.
`--impl python` runs `uv run floe run -c config.yml` (`validate -c config.yml` for the cases under `config/`). `--cases-file` runs only the case names listed in the file
(one per line, `#` comments; an empty list runs nothing and passes).

## CI

The `conformance` job in `.github/workflows/ci.yml` runs on changes to `conformance/`, `src/floe/`, `pyproject.toml`
or `uv.lock`:

- `check.py --impl rust` must pass on every case (guards the recorded cases).
- `check.py --impl python --cases-file conformance/python_passing.txt` runs only the listed cases. Each port task adds
  the cases it makes pass; a listed case that fails breaks the build, so parity only ratchets up.

## Intentional deltas

Where Python must differ from Rust 0.7 on purpose (the D-register in #619), a case carries:

- `delta.yml`: one line listing the delta ids, e.g. `[D9]`;
- `expected_python/`: the files that differ, at the same relative path as in `expected/`.

`check.py --impl rust` always compares against `expected/`. `--impl python` compares each file against
`expected_python/<file>` when it exists, else `expected/<file>`, and prints the delta ids when such a case fails.
`record.py` only writes `expected/` (from Rust); `expected_python/` is derived from it and committed. The case
README says how (for `config/` cases, the delta table in `config/README.md`), so a reviewer can redo it.

D1 (dropped features) cases are not recorded: they hold only `expected_python/` with the Python error, `record.py`
skips them and `check.py --impl rust` prints `skip` for them.

## Normalization

- Masked when recording: run ids, timestamps, durations, absolute paths (`<ROOT>`, `run_<id>`).
- Compared by shape: UUID-based part file names (`part-<uuid>`), Iceberg `snapshot_id`.
- Recorded but ignored on compare (G5 ruling, #594): `total_bytes_written`, `avg_file_size_mb`.
- Ignored on compare: `tool.version` in `run.summary.json` (each implementation reports its own version). `expected/` keeps the recorded `0.7.0`.
- Paths in `expected/` use `run_ID` and `part-UUID` (no `<`/`>`: Windows can't check them out); file contents keep `run_<id>` / `part-<uuid>`.
- Only `out/accepted` and `out/rejected` are captured; `record.py` fails on a case that produces neither.
- Parquet dates and timestamps are compared as Arrow-rendered text, so years outside 1..9999 (which Python's `date` cannot hold) work.
- Key order in reports is kept: it is part of the contract.
