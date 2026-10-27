# Floe — Architecture

Floe 0.8 is one pure-Python package. The Rust implementation (0.7 and earlier) is archived at
[OpenLakeForge/floe-rust](https://github.com/OpenLakeForge/floe-rust); its decisions and the
port are in `context/decisions/python-core-for-olf.md`.

## Repository layout

```
floe/
├── pyproject.toml      ← dist `floe-python`, console script `floe`, Python >= 3.11
├── src/floe/           ← the library and the CLI
├── tests/              ← pytest: unit/ and integration/
├── conformance/        ← golden suite: one case per feature, `check.py` diffs against expected/
├── schemas/            ← YAML authoring schemas (editor tooling, not used at runtime)
├── example/            ← sample configs and projects
├── Dockerfile.python   ← the `ghcr.io/openlakeforge/floe` image
└── orchestrators/
    ├── dagster-floe/   ← Dagster integration (own package and release)
    ├── airflow-floe/   ← Airflow integration (own package and release, frozen)
    └── schemas/        ← canonical manifest schema, vendored into both connectors
```

## Modules in `src/floe/`

```
src/floe/
├── __init__.py      ← public API (`__all__`): validate, run, run_manifest, maintain, load_config,
│                      state helpers, observer hooks, RunOutcome, typed exceptions, event TypedDicts
├── _cli.py          ← argparse CLI: validate, run, manifest generate, add-entity, state, maintain
├── _cli_output.py   ← console and NDJSON output of `floe run`
├── config/          ← YAML decoding, includes, variables, profiles, pydantic models, validation
├── read/            ← one reader per format: csv (also tsv), json, parquet, orc, avro, xlsx, xml
├── checks/          ← casts, schema mismatch, column normalisation, not_null, unique
├── run/             ← run loop (`__init__`), one entity run (`entity.py`), observer events
├── write/           ← accepted Parquet parts and rejected CSV parts
├── iceberg/         ← Polaris REST catalog, overwrite/append in one commit, DuckDB merges,
│                      unique keys from table history, experimental compaction
├── storage/         ← StorageClient protocol, local and S3 clients, input resolution,
│                      archiving, `source.partitioning`
├── state.py         ← incremental file state (CAS claim / promote / release / renew / reset)
├── report/          ← entity `run.json` and `run.summary.json`, exit codes
├── lineage/         ← OpenLineage events and HTTP observer (failures never fail the run)
├── manifest.py      ← `floe manifest generate`
├── manifest_run.py  ← rebuild the config a run needs from a manifest
├── maintain.py      ← `floe maintain`: Iceberg snapshot expiry, orphan removal, compaction
├── add_entity.py    ← `floe add-entity` schema inference
├── pii.py           ← PII masking strategies
├── glob.py          ← glob matcher with the 0.7 semantics (decides which files are ingested)
└── errors.py        ← FloeError, FloeConfigError, FloeRunError, FloeStorageError, FloeIoError
```

The CLI and the Python API share the same functions: `_cli.py` parses arguments and calls
`config`, `run`, `manifest` and `maintain`, as `floe.run()` does.

## Libraries

| Library | Used for |
|---|---|
| Polars | Reading (CSV, JSON, Parquet, Avro), casting, checks, the accepted/rejected split, Parquet and CSV output |
| PyIceberg (+ `pyiceberg-core`) | Polaris REST catalog, table creation, schema evolution, overwrite/append commits, maintenance |
| DuckDB (pinned) | Iceberg `MERGE INTO`, unique checks against table history, compaction; spills to disk |
| pyarrow | Arrow exchange between the three, ORC reads |
| fastexcel | XLSX |
| boto3 | S3 storage client |
| pydantic, PyYAML | Config models and strict YAML decoding |

They exchange data through the Arrow C Data Interface, so there is no version lockstep between
them (see `context/decisions/polars-and-arrow.md`).

## Data flow per entity (`run/entity.py`)

```
1. Resolve inputs
   source.path → local glob or S3 prefix listing → drop `.floe/` and the archive (D23, D31)
   → incremental state: claim pending files, skip processed or claimed ones

2. File-level checks (per file)
   read the header/schema → schema mismatch policy
   → reject_file: copy the raw file to the rejected location, next file

3. Row-level checks (per file, in bounded chunks)
   cast to declared types → not_null → unique (within the run; with `append`, seeded from
   the accepted sink) → rows with errors get `__floe_row_index` + `__floe_errors`
   → severity decides: warn keeps them, reject routes them to rejected CSV, abort fails the entity

4. Write outputs (once per entity)
   PII masking on accepted and rejected rows
   accepted → Parquet parts, or one Iceberg catalog commit (overwrite/append via PyIceberg,
   merge_scd1 via DuckDB MERGE INTO)

5. After the commit
   archive inputs (D32) → promote state claims → write run.json
```

`floe run` runs entities in config order. A policy abort fails only its entity; any raised
`FloeError` aborts the run (D36). Reports are written under `<report.path>/run_<id>/`, or
`<report.path>/<domain>/run_<id>/` for a single-domain run (D37).

## Key contracts

- **Config YAML, run report and manifest** are user contracts, pinned by the golden suite
  (`conformance/`). Every accepted difference from 0.7 is a numbered delta (D1, D2, …) in the
  master issue #619.
- **One catalog commit per entity** for Iceberg writes, and an append retry guard: append
  snapshots carry `floe.input_hash`, so a retry finds its batch committed (D5, D35).
- **Incremental state** is CAS over `state.json` (`context/decisions/incremental-state-cas.md`).
  A claim has a one-hour TTL and is renewed by a background thread while the entity runs.
- **Errors** are typed exceptions (`errors.py`); messages are part of the contract and tests
  assert them exactly.
