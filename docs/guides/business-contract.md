# Business contract workbook

A business analyst keeps one Excel workbook per domain (`Guide`, `Tables`, `Columns`; see `example/business-contract/`). `floe contract import` turns it into normal Floe entity files, one per table, holding only the business-owned keys. The data engineer owns everything else.

It needs the `xlsx` extra: `pip install 'floe-python[xlsx]'`.

## Workflow

1. **Import.** No `config.yml` is needed:

   ```bash
   floe contract import sales.xlsx --output-dir domains/sales
   ```

   This writes `domains/sales/<table>.yml` for each table, and `_domain.yml` if it does not exist. The scaffold fails closed (`TODO_…` placeholders), so nothing runs until it is completed.
2. **Complete `_domain.yml` once.** Set the domain's source and sink defaults, and add the domain to `include.domains` in your `config.yml`.
3. **Validate and run:** `floe validate -c config.yml`, then `floe run -c config.yml`.
4. **Change the workbook and import again.** Only the owned keys change; your own keys and comments stay byte-identical. Add `--dry-run` to preview, `--check` in CI to fail when the YAML is out of sync, and `-c config.yml` to validate the project as it would be after the import.

## What the importer owns

`name`, `domain`, `metadata.owner`, `metadata.description`, `source.resource`, `policy.severity`, `policy.rule_severity`, `schema.columns`, `schema.primary_key`, `schema.unique_keys`. Every entity file starts with a `# floe-contract: sha256:…` line over those keys. If you edit an owned key by hand, or the file was not generated, reimport is **blocked** with a diff: change the workbook or revert the edit.

## Changes

Each reimport classifies the change per table as `breaking` (column removed or renamed, type, primary key or `source_dataset` change, table removed from the workbook), `tightening`, `loosening`, `additive` or `metadata`. A breaking change needs `--accept-breaking`. No file is ever deleted.

## Difference from `floe add-entity`

`floe add-entity --input sample.xlsx` infers a schema from sample data and writes into your `config.yml`. `floe contract import` takes the schema and checks from what the business wrote down, and never reads data.

See the [CLI reference](../reference/cli.md).
