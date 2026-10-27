# CLI Usage

## Commands

- `floe validate -c <config> [--entities <name[,name...]>]`
  - Validate YAML config and exit with non-zero code on errors.

- `floe run -c <config> [--run-id <id>] [--entities <name[,name...]>] [--log-format json|text]`
  - Execute ingestion using the config.
  - Run reports are written under `<report.path>/run_<run_id>/...` as defined in the config,
    or `<report.path>/<domain>/run_<run_id>/...` when every selected entity has one domain (D37).
  - `-c` accepts local paths or cloud URIs (`s3://`, `gs://`, `abfs://`).
  - `--log-format json` emits NDJSON events to stdout (useful for orchestrators).
  - `--dry-run` resolves entity inputs and output targets without running ingestion.
  - `--quiet` reduces console output to run totals.
  - `--verbose` expands run output details.

- `floe manifest generate -c <config> --output <path|-> [-p <profile>] [--entities <name[,name...]>]`
  - Validate config and generate a common orchestrator manifest JSON (`spec_version` = the Floe major.minor).
  - `-p` applies an environment profile (variables, storages, catalogs, lineage) as `floe run -p` does, and records its `profile_uri` and `profile_checksum`. Its `execution` block sets `execution.orchestration` and, for a `kubernetes_job` or `databricks_job` runner, the `default` runner definition.
  - `--output -` writes manifest JSON to stdout.

- `floe add-entity -c <config> --input <path|file://...> [--format csv|json|parquet] [--name <entity>] [--domain <domain>] [--source <ref> --resource <name>]`
  - Infer a schema from an input file and add a new entity to the config.
  - If `--domain` matches an included domain and `--output` is omitted, writes `<domain folder>/<name>.yml`; fields supplied by domain defaults are omitted from that file.
  - If `-c` points to a missing file, Floe creates a minimal config and adds the entity.
  - If `--format` is omitted, Floe infers it from the input file extension (`.csv`, `.json`, `.parquet`).
  - If `--name` is omitted, Floe infers it from the input filename stem.
  - `--source` and `--resource` write a reference to a declared `sources:` entry; `--input` remains the file used for schema inference.
  - Generates defaults for `sink`, `policy`, and `schema.mismatch`.
  - JSON inference is v0.3 bootstrap-focused: top-level keys only; nested objects/arrays are inferred as `string`.
  - `--dry-run` prints the YAML that would be written without creating the file.
  - `--output <new.yml>` writes the updated root config to a new file instead of overwriting `-c`; with an included domain, this keeps the existing inline entity behavior.

- `floe state inspect -c <config> --entity <name> [-p <profile>]`
  - Print the resolved incremental state URI for an entity and the current state JSON when present.
  - Shows processed-file and active-claim counts.
  - Helpful for confirming what Floe has already seen in `incremental_mode: file`.
  - See [docs/incremental.md](incremental.md) for state file and CAS details.

- `floe state reset -c <config> --entity <name> --yes [-p <profile>]`
  - Remove the local or remote state object for an entity.
  - Requires `--yes` because the next incremental run may reprocess previously tracked files.
  - Remote resets use the same conditional delete support as remote incremental state.
  - See [docs/incremental.md](incremental.md) for reset behavior and permissions.

### Dry-run behavior

- Dry-run resolves input files/objects using the same planning path as real runs.
- For cloud sources, dry-run lists matching objects but does not download them.
- Console preview prints resolved file count and a capped list (`50` entries by default).
- Use `--verbose` to print the full resolved file list.

## Examples

- Validate the sample config:
  - `floe validate -c example/config.yml --entities customer`

- Run with default paths from the config:
  - `floe run -c example/config.yml --entities customer`
- Run with JSON logs:
  - `floe run -c example/config.yml --entities customer --log-format json`
- Dry-run preview:
  - `floe run -c example/config.yml --entities customer --dry-run`
- Generate common manifest:
  - `floe manifest generate -c example/config.yml --output orchestrators/airflow-floe/example/manifest.airflow.json`
- Print common manifest to stdout:
  - `floe manifest generate -c example/config.yml --output -`
- Add an entity from a CSV file:
  - `floe add-entity -c example/config.yml --input ./in/customers.csv --format csv --name customers`
- Bootstrap a new config file and infer name/format:
  - `floe add-entity -c new-config.yml --input ./in/orders.csv`
- Inspect incremental state:
  - `floe state inspect -c example/config.yml --entity customer`
- Reset incremental state:
  - `floe state reset -c example/config.yml --entity customer --yes`
- Report output:
  - `example/report/run_<run_id>/run.summary.json`
  - `example/report/run_<run_id>/customer/run.json`

- Set a run id:
  - `floe run -c example/config.yml --entities customer --run-id dev-001`
