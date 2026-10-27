# Incremental File Ingestion

Floe's file incremental mode tracks source files that already passed through an
entity. Later runs skip those files, process only new files, and keep enough
state to avoid duplicate work when multiple runners overlap.

Use this when a source directory or object-storage prefix receives new files
over time, for example daily partner drops, API exports, or landing-zone batches.

## Enable File Incremental Mode

Set `incremental_mode: file` on the entity:

```yaml
storages:
  default: raw_s3
  definitions:
    - name: raw_s3
      type: s3
      bucket: raw-bucket
      region: eu-west-1
    - name: lake_s3
      type: s3
      bucket: lake-bucket
      region: eu-west-1

entities:
  - name: orders
    incremental_mode: file
    source:
      format: csv
      storage: raw_s3
      path: incoming/orders/
    sink:
      write_mode: append
      accepted:
        format: delta
        storage: lake_s3
        path: bronze/orders/
      rejected:
        format: parquet
        storage: lake_s3
        path: rejected/orders/
    policy:
      severity: reject
    schema:
      columns:
        - name: order_id
          type: string
          nullable: false
```

If `state.path` is omitted, Floe derives the state object under the source root:

```text
<source root>/.floe/state/<entity>/state.json
```

For the example above, the default state URI is:

```text
s3://raw-bucket/incoming/orders/.floe/state/orders/state.json
```

## Override The State Path

Use `state.path` when you want state stored separately from the input prefix:

```yaml
entities:
  - name: orders
    incremental_mode: file
    state:
      path: s3://raw-bucket/floe-state/orders/state.json
    source:
      format: csv
      storage: raw_s3
      path: incoming/orders/
```

Only explicit cloud URIs (`s3://`, `gs://`, `abfs://`, `abfss://`) create remote
state. Relative `state.path` values are local filesystem paths, even when the
source is remote.

Explicit remote `state.path` values are resolved through the entity's
`source.storage`. The URI must therefore match the same storage definition, such
as the same S3 bucket, GCS bucket, or ADLS account/container. A separate state
prefix in the same bucket is supported; a separate state bucket is not currently
supported by this override.

## Inspect State

Use `floe state inspect` to see the resolved state URI and current JSON:

```bash
floe state inspect -c config.yml --entity orders
```

This is useful before or after a run to confirm:

- where Floe stores incremental state
- how many files have already been processed
- whether another run currently has active claims

## Reset State

Use `floe state reset` to remove the state object:

```bash
floe state reset -c config.yml --entity orders --yes
```

The next run treats matching source files as new again. Remote resets use a
conditional delete so Floe fails if another process changes state while the
reset is running.

## State File Shape

File incremental state uses schema `floe.state.file-ingest.v2`:

```json
{
  "schema": "floe.state.file-ingest.v2",
  "entity": "orders",
  "updated_at": "2026-06-23T09:00:00Z",
  "files": {
    "s3://raw-bucket/incoming/orders/orders_001.csv": {
      "processed_at": "2026-06-23T09:00:00Z",
      "size": 18422,
      "mtime": "2026-06-23T08:45:00Z"
    }
  },
  "claims": {
    "s3://raw-bucket/incoming/orders/orders_002.csv": {
      "run_id": "2026-06-23T09-10-00Z",
      "acquired_at": "2026-06-23T09:10:00Z",
      "expires_at": "2026-06-23T10:10:00Z",
      "size": 20114,
      "mtime": "2026-06-23T09:05:00Z"
    }
  }
}
```

The `files` map is durable history: source URIs that finished successfully. The
`claims` map is temporary ownership: source URIs currently reserved by a run.

Existing `floe.state.file-ingest.v1` files are still readable. Floe upgrades
them to v2 the next time it writes state.

## Run Behavior

At the start of an entity run, Floe:

1. Loads the state file or creates an empty state.
2. Removes expired claims.
3. Skips files already present in `files`.
4. Skips files with active claims from another run and emits an incremental warning.
5. Claims remaining pending files before accepted sink output is written.

When the entity finishes successfully, Floe promotes this run's claims into the
`files` map with observed size and mtime. If the entity fails, Floe releases
this run's claims so the same files can be retried later. If a file aborts the
entity, the files before it are still written. With `append` and the merge
modes, Floe promotes those and releases only the aborted file and the files
after it: once the aborted file is fixed, a rerun writes it without writing the
earlier files again. With `overwrite`, Floe releases every claim, because the
rerun rebuilds the output from all the files. Either way the claims are settled
before the next entity starts, which runs as usual (D36).

If a file URI is already in `files` but its size or mtime changed, Floe emits an
`incremental_file_changed` warning and still skips it. The URI is the identity;
Floe does not automatically reprocess changed files.

## Retries Of An Append

An Iceberg `append` stamps each snapshot with `floe.input_hash`, a fingerprint of the
entity and its input files (URI, size, mtime). Before reading any file, a run checks it:

- `file` mode: when the run takes over a dead run's expired claims, it looks for the
  fingerprint of its pending files in the table's snapshots since that claim. On a match,
  the files are reported `skipped` with `skip_reason: already_committed` and promoted.
- Other modes (`archive`, `none`, `row`): the same check against the whole input set, over
  the whole retained history, so a rerun of exactly the same files is skipped too. Only exactly
  the same set matches: if new files arrived since the run that died, its rows are appended
  again. Retry before new files land, or use `file` mode.

Snapshot expiry must keep at least 7 days and 10 snapshots. If the snapshots since a dead
run's claim were expired, Floe can't tell whether that run committed: the run fails and
asks for `floe state reset`.

## CAS And Concurrent Runs

Remote state uses compare-and-swap (CAS) writes so two runners cannot silently
overwrite each other's state:

- S3 and ADLS use object ETags through conditional writes.
- GCS uses object generation numbers through conditional writes.
- Local state uses a local file version check with a local file lock.

Every state mutation is written only if the version read by the runner is still
current. If another runner wins the race, Floe reloads state, backs off, and
retries. After repeated conflicts, the run fails with a clear incremental state
conflict error.

Claims expire after one hour. A background heartbeat renews active claims during
long-running processes so normal long runs keep ownership. If a process crashes,
its claims eventually expire and another run can pick those files up.

## Permissions

Remote incremental state needs permission to:

- read the state object
- create or update it with conditional preconditions
- delete it conditionally for `state reset`

The source still needs its normal list/read permissions. The state object itself
does not require listing except where the selected cloud provider or IAM policy
requires it for object access.

## Related References

- Full config reference: [config.md](config.md)
- CLI reference: [cli.md](cli.md)
- Pipeline details: [how-it-works.md](how-it-works.md)
