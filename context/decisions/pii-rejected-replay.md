# Decision — Replaying the rejected rows of a PII entity

## Status

Accepted (design for #1255, approved by the maintainer on the decision comment of #1255, refs checked against `main`
at `6526c789`). Docs only. Linked from #264 (replay). The sibling code changes are #1362 (report fields) and #1363
(lift the `pii` + `sink.archive` refusal).

## Context

Rejected frames are masked before they are written: `output()` at `src/floe/run/file.py:223-227` (CSV/Parquet rejects
`:416-422`; Iceberg rejects `src/floe/run/entity.py:439`). `drop`/`nullify` cannot be undone, and `hash`/`redact`/`mask`
cannot be reversed, so the rejected output of a PII entity cannot be replayed. The question: what is the replay input?

Current behaviour that matters:

- Raw copies are skipped or refused under PII: an unreadable file gets no raw copy (`file.py:331-335`), mismatch
  `reject_file` is refused (`validate.py:736-750`), and `severity: abort` is refused (`validate.py:721-727`).
- `sink.archive` is refused under PII (`validate.py:728-734`, golden `conformance/config/val1_pii_with_archive`), so a
  PII entity cannot use `incremental_mode: archive` and its source files are never moved. Note: the setting is
  `sink.archive`, not `source.archive`.
- The archive runs after the sink commit (`run/entity.py:504-511`), so the archived URI is not known when rejected
  rows are written. It is reported once per file as `files[].output.archived_path` (`report/entity.py:72,113`).
- `__floe_row_index` is the record index plus the chunk offset (`checks/rows.py:126`, `run/file.py:376`).
  `__floe_run_id` and `__floe_source_uri` were added to Parquet and Iceberg rejects only (`file.py:299`); #1384 adds
  them to CSV rejects too, so all three formats carry the four envelope columns.
- There is no per-file content fingerprint in the report. `InputFile.source_size` / `source_mtime`
  (`src/floe/storage/inputs.py:35-40`) reach only the state and `input_hash`.

## Decision

| Question | Decision |
|---|---|
| Where is the original source recorded? | Once per file, in the run report. A rejected row's `(__floe_run_id, __floe_source_uri)` joins `files[].input_file` in that run's report. `files[].output.archived_path` gives the moved location, and is null when the file was not moved. No archive URI per row. One additive report field pair, `files[].input_size` / `input_mtime` (#1362), lets a replay prove the bytes behind the URI are still the bytes that were validated. |
| Should `floe validate` warn on pii + rejected + no archive? | No warning and no new requirement. `validate` has no warning channel (`ponytail:` notes at `validate.py:111,701,818`), and the condition would fire on every PII entity. The source file left in place is a valid replay input. The contract is documented instead. |
| What does a replay do for masked columns? | It re-reads the original file (`archived_path`, else `__floe_source_uri`) with the same reader and `source.options`, checks size and mtime, and selects rows by `__floe_row_index`. It never uses masked values. |
| Opt-in unmasked quarantine? | No, not in 1.x and not behind a flag. The authorized original file is the only unmasked replay input. |
| Lift the `pii` + `sink.archive` refusal | Yes (#1363). Archiving is a move within the source storage (`storage/archive.py:62-79`, same storage enforced at `validate.py:512-517`), not an extra copy. The archive is the "immutable original" that #264 asks for. This changes the config contract (AGENTS.md §4); the maintainer approved it on #1255. |

### Contract

- The rejected output of a PII entity is diagnostic, not replayable. It holds only masked values, with no opt-in.
- The replay input is the original file: `files[].output.archived_path` when set, otherwise `__floe_source_uri`
  (= `files[].input_file`). The user's storage ACLs and retention own access to it (#264).
- Rows are selected by `__floe_row_index` and read with the run's `source.options`. The replay refuses the file when
  its size or mtime differs from the report's `input_size` / `input_mtime`.
- `__floe_source_uri` keeps the pre-archive URI. It is the identity shared by the report, the incremental state and
  the documented dedupe key `(__floe_source_uri, __floe_row_index)` (`docs/reference/config.md:541`). It is not
  repurposed.
- The rule "raw files are never written to `sink.rejected`" stays.
- Users who need replay keep their source files, through the archive or retention, for the repair window.
- Hand-off to #1256: pin `__floe_row_index` as "the record index for the file's bytes and `source.options`". CSV
  rejects identify the file only through `output.rejected_path`. Whether to add `__floe_source_uri` there is #1256's
  call, and this decision does not need it.

Not doing: no new config knob, no per-row archive URI, no replay code, no unmasked quarantine, no change to `__floe_*`
columns.

### Is `__floe_row_index` stable per format?

It is the 0-based index in the reader's record stream, not a line number. Chunk size and row-group layout do not
change it.

| Format | Index counts | Depends on |
|---|---|---|
| CSV/TSV | Data records as Polars parses them, after the header if `header` is true. A quoted multi-line field is one record. There is no skip-rows option. | `separator`, `encoding`, `header`, `null_values`, Polars CSV record rules |
| Parquet | Global row order across row groups | file bytes only |
| Avro / ORC | Record order | file bytes only |
| JSON | Records in `json_records` order | `json_mode` |
| XML | `row_tag` elements in document order | `row_tag`, `namespace` |
| XLSX | Rows from `data_row` on (one chunk, offset 0) | `sheet`, `header_row`, `data_row` |

So a replay must use the same bytes and the same `source.options`: a pinned config (#264's "pinned contract") plus a
size and mtime check.

## Open risks

- **Polars CSV record rules** (blank lines, quoting edge cases) could change between versions and move indexes for
  the same bytes. Mitigation: #1256 pins this with a golden case, and replay pins or compares the Floe version
  recorded in the report.
- **mtime is weak.** An S3 copy can keep the size and the second-resolution mtime. It is the same guard Floe already
  trusts for `input_hash` and `already_ingested_changed`, and a checksum would cost a second full read. The upgrade
  path is an ETag or checksum field later (additive).
- **The source can vanish.** Without an archive, or with a lifecycle rule on the archive, the original can be deleted
  before the repair. Floe does not own retention (#264), and the docs say so plainly.
- **Lifting the archive refusal** changes the config contract. Some users may rely on the refusal as a guard against
  unmasked data in an archive prefix with wider ACLs: the archive shares the source's storage, not necessarily its
  prefix ACLs. The maintainer approved the change, and the docs state the ACL point.
- `incremental_mode: none` without an archive re-reads the same files every run, so the same rejected rows repeat.
  Consumers already dedupe on `(__floe_source_uri, __floe_row_index)`. Unchanged.

## Sub-issues

1. #1361 this record, `docs/guides/pii.md` and `docs/reference/report.md`. Its PR closes #1255.
2. #1362 `feat(report): record each input file's size and mtime`. Must land before 1.0.
3. #1363 `feat(config): allow pii with sink.archive`. Config contract change, approved on #1255.
