# Stability policy

What Floe 1.x promises, what it may add in a minor release, and what it does not promise.

Floe follows [semantic versioning](https://semver.org) from 1.0. Within 1.x, a config, manifest or
program that works on 1.N keeps working on every later 1.x release. Breaking changes wait for 2.0.

## Frozen in 1.x

None of these is removed, renamed or given a different meaning before 2.0:

- **Config YAML**: every key of the project, entity, sources, domain and profile documents
  (`schemas/*.schema.yaml`), its accepted values, its default and its meaning. Meaning includes
  which rows a check accepts or rejects and the type each declared column is written as.
- **Manifest**: the format `floe manifest generate` writes, read by `floe run --manifest`
  and the connectors (`orchestrators/schemas/floe.manifest.schema.json`).
- **Run reports**: the fields of `run.summary.json`, `<entity>/run.json` and `run.failure.json`,
  and the status and `skip_reason` values ([Run report](report.md)).
- **Events**: `--log-format json` lines (`schema: "floe.log.v1"`), their event names and fields,
  and the `log` event `code` values ([Logging](logging.md)). The same events go to
  `floe.set_observer` callbacks.
- **Rejected row envelope**: the `__floe_*` columns of rejected CSV, Parquet and Iceberg output,
  and the `rule` and `column` of each `__floe_errors` entry ([Rejected row envelope](rejected.md)).
- **Exit codes**: `0` success, success with warnings or rejected rows; `1` technical failure or
  invalid config; `2` aborted by policy. `2` also means a command-line usage error, before any run starts.
- **Error codes and exception classes**: every code in [Error codes](errors.md), and
  `FloeError` with its subclasses `FloeConfigError`, `FloeIoError`, `FloeRunError` and
  `FloeStorageError`. The code and class a given failure raises are part of this.
- **CLI**: the commands and flags listed in [CLI](cli.md).
- **Python API**: the names in `floe.__all__`, their parameters, and the fields of the objects
  they return (`RunOutcome`, `RootConfig`, `EntityConfig`, the event classes).

## Added in minor releases

A 1.x minor release may add:
- optional config keys and new values for existing keys (a new source format, sink or check);
- report fields, event types, event fields and `log` codes;
- `__floe_*` columns in the rejected output;
- error codes;
- CLI commands and flags, and names in `floe.__all__`.

So readers must ignore fields they do not know. A new optional key never changes what an
existing config does.

A patch release only fixes bugs. A fix that makes Floe do what this documentation says may
change output. For example, rows the contract rejects but an earlier release accepted are
rejected. Each such fix is listed in `CHANGELOG.md`.

## Not promised

These may change in any release:
- error and log message wording (match on `err.code`, not on text), the human end-of-run
  summary, `--log-format text` lines, and the `message` of a `__floe_errors` entry;
- anything not in `floe.__all__`, whatever its module name (`floe.run`, `floe.config`, ...);
- the layout of the incremental `state.json` and the table properties Floe writes. Floe 1.x
  still reads state and resumes retries left by any earlier 1.x (and 0.10) release;
- performance, memory use and the number or names of written part files;
- the supported Python versions (a version past its upstream end of life may be dropped in a
  minor release) and the versions of dependencies.

## `version` and `spec_version`

- A config's `version: "1.N"` is the oldest Floe it needs. Floe 1.M runs any config with
  `version` from `"0.1"` up to `"1.M"`, and refuses a newer one with `config_version` (a 1.3 config
  on Floe 1.2 fails at validate time, before any row is read). `floe add-entity` writes the
  running major.minor.
- A manifest's `spec_version` is the Floe major.minor that generated it. `floe run --manifest`
  runs any manifest with `spec_version` up to its own and refuses a newer one with `config_version`.
- In run reports, `spec_version` is the config's `version` (unchanged since 0.x). The Floe
  release that wrote a report is `tool.version`.

## Connectors

dagster-floe and airflow-floe have their own version numbers. A connector release lists the
manifest `spec_version`s it accepts, and refuses any other with a clear error instead of
misreading it. So a manifest from a new Floe minor needs a connector release that lists it. Each
Floe minor ships with one. The tables of Floe and connector versions that work together are in the
[dagster-floe](../../orchestrators/dagster-floe/README.md) and
[airflow-floe](../../orchestrators/airflow-floe/README.md) READMEs.

## Deprecation

A frozen item that has to go is first deprecated in a 1.x minor release:
1. `CHANGELOG.md` and its reference page say it is deprecated and what replaces it.
2. Using it logs a warning (`warn` level) and still works for the rest of 1.x.
3. 2.0 removes it. The removal fails with `config_not_supported`, naming the replacement.

The one exception is a security fix: an item that is a security hole may be removed in a
minor release, with a `CHANGELOG.md` entry.
