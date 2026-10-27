# Golden suite

Golden cases of the Python core: each case runs `floe` and must match its `expected/` with zero diff.
The cases were first recorded from the Rust v0.7.0 binary; #970 folded the intentional deltas into `expected/`, so it
now holds the Python outputs. The Rust comparison and its delta history are in [ARCHIVE.md](ARCHIVE.md) (moving to
OpenLakeForge/floe-rust).

## Layout

```
conformance/<case>/
├── config.yml        run as `floe run -c config.yml` from a temp copy of the case
├── in/               input files
├── expected/
│   ├── reports/      normalized run.summary.json and each entity run.json
│   ├── accepted/     accepted rows, one Parquet file per accepted output dir
│   └── rejected/     rejected rows (CSV)
├── runs              optional: number of runs (default 1), or one line of extra `run` args per run (`-` for none,
│                     e.g. `-` then `--full-refresh`); only the last run is captured, state carries over
└── manifest_run      optional marker: generate `manifest.json` from `config.yml`, then run `floe run --manifest manifest.json`
```

A case may also hold `expected/manifest.json`: the output of `floe manifest generate -c config.yml -o - --deterministic`,
run with `FLOE_RUNTIME=cli` after the case's own run. It is compared only when present (never with a `profile.yml`).

A case is any directory holding a `config.yml` (it may be nested, e.g. `example/standalone`).

**Validate cases** are the cases under `config/`: they run `floe validate -c config.yml` (plus `--profile profile.yml`
when the case has one) and hold only `expected/validate.json`, `{"ok": true}` or `{"ok": false, "error": "<message>"}`.
See [config/README.md](config/README.md).

## Check

```bash
uv sync --group conformance
uv run python conformance/check.py [--case NAME]
```

Runs `uv run floe run -c config.yml` (`validate -c config.yml` for the cases under `config/`) on every case, in
parallel, and prints a diff for each difference; exits 1 if any.

## Record

```bash
uv run python conformance/record.py --case NAME
```

Rewrites the case's `expected/` from the Python core. Re-record only for an intended behaviour change, and review the
diff in the PR.

## CI

The `golden` job in `.github/workflows/ci.yml` (check name `conformance`) runs `check.py` on changes to
`conformance/`, `src/floe/`, `pyproject.toml` or `uv.lock`.

## Normalization

- Masked when recording: run ids, timestamps, durations, absolute paths (`<ROOT>`, `run_<id>`), and the run id and
  source hash in archived file names (`__run-<id>__src-<hash>`).
- Compared by shape: UUID-based part file names (`part-<uuid>`), Iceberg `snapshot_id`.
- Recorded but ignored on compare (G5 ruling, #594): `total_bytes_written`, `avg_file_size_mb`.
- Ignored on compare: `tool.version` in `run.summary.json` (it changes every release). Rust-recorded files keep `0.7.0`.
- Paths in `expected/` use `run_ID` and `part-UUID` (no `<`/`>`: Windows can't check them out); file contents keep `run_<id>` / `part-<uuid>`.
- Only `out/accepted` and `out/rejected` are captured; `record.py` fails on a case that produces neither, unless its
  last run passes `--log-format json`: then stdout's event lines are captured too, in `expected/events.jsonl` (`ts_ms` masked).
- Parquet dates and timestamps are compared as Arrow-rendered text, so years outside 1..9999 (which Python's `date` cannot hold) work.
- A run's non-zero exit code is recorded in `expected/exit_code` (e.g. `2` for an aborted run) and compared; no file means 0. Codes other than 0 and 2 fail recording, except on a `--log-format json` run.
- `manifest.json`: `floe_version`, `spec_version`, `manifest_id` and `manifest_revision` are masked (the version string
  and the absolute paths feed them) and the temp dir holding the work dir is `<ROOT>` (so `../` paths mask too); every
  other byte is compared, key order included, except the `sources` index and project `metadata`, which
  `tests/unit/test_manifest.py` checks.
  Python's profile fields (`-p`) and remote `-o` are not covered yet (#635, storage clients).
- Key order in reports is kept: it is part of the contract.
