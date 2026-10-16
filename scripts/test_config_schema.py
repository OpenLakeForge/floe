import unittest
import re
from copy import deepcopy
from pathlib import Path

import yaml
from jsonschema import Draft202012Validator


ROOT = Path(__file__).resolve().parents[1]
SCHEMA = yaml.safe_load((ROOT / "schemas/project.schema.yaml").read_text())
ROOT_VALIDATOR = Draft202012Validator(SCHEMA)


def complete_entity():
    return {
        "name": "orders",
        "source": {"format": "csv", "path": "in.csv"},
        "sink": {"accepted": {"format": "parquet", "path": "out"}},
        "policy": {"severity": "warn"},
        "schema": {"columns": [{"name": "id", "type": "string"}]},
    }


def validator_for(branch):
    name = {"sources_file": "sources", "domain_file": "domains", "entity_file": "entity"}[branch]
    return Draft202012Validator(yaml.safe_load((ROOT / f"schemas/{name}.schema.yaml").read_text()))


class ConfigSchemaTest(unittest.TestCase):
    def test_independent_document_schemas(self):
        self.assertFalse((ROOT / "schemas/config.schema.yaml").exists())
        self.assertFalse((ROOT / "schemas/0.3").exists())
        for name in ("project", "sources", "domains", "entity"):
            schema = yaml.safe_load((ROOT / f"schemas/{name}.schema.yaml").read_text())
            Draft202012Validator.check_schema(schema)
            def check_refs(node):
                if isinstance(node, dict):
                    if "$ref" in node:
                        self.assertTrue(node["$ref"].startswith("#/$defs/"), node["$ref"])
                        self.assertIn(node["$ref"].split("/")[-1], schema["$defs"])
                    for value in node.values():
                        check_refs(value)
                elif isinstance(node, list):
                    for value in node:
                        check_refs(value)
            check_refs(schema)
        self.assert_invalid(ROOT_VALIDATOR, {"sources": []})
        self.assert_invalid(validator_for("sources_file"), {"name": "crm"})
        self.assert_invalid(validator_for("domain_file"), {"defaults": {}})
        self.assert_invalid(validator_for("entity_file"), {"name": "orders"})

    def test_rust_model_field_coverage(self):
        source = (ROOT / "crates/floe-core/src/config/types.rs").read_text()
        structs = {
            name: set(re.findall(r"pub (\w+):", body))
            for name, body in re.findall(r"pub struct (\w+) \{(.*?)^\}", source, re.M | re.S)
        }
        bindings = {
            "RootConfig": None, "ProjectMetadata": "project_metadata",
            "LineageConfig": "lineage", "EnvConfig": "env",
            "DomainConfig": "domain", "EntityConfig": "entity_base",
            "PiiConfig": "pii", "PiiColumnConfig": "pii_column",
            "EntityMetadata": "entity_metadata", "EntityStateConfig": "entity_state",
            "SourceConfig": "source_fragment", "SourceOptions": "source_options",
            "SinkConfig": "sink", "SinkTarget": "sink_accepted_fragment",
            "SinkOptions": "sink_accepted_fragment", "MergeOptionsConfig": "merge",
            "MergeScd2OptionsConfig": "merge_scd2", "IcebergPartitionFieldConfig": "partition_field",
            "IcebergSinkTargetConfig": "iceberg", "DeltaSinkTargetConfig": "delta",
            "DuckDbSinkTargetConfig": "duckdb", "StoragesConfig": "storages",
            "StorageDefinition": "storage_definition", "CatalogsConfig": "catalogs",
            "CatalogDefinition": "catalog_definition", "ReportConfig": "report",
            "ArchiveTarget": "archive_target", "PolicyConfig": "policy_fragment",
            "SchemaConfig": "schema", "NormalizeColumnsConfig": "normalize_columns",
            "SchemaEvolutionConfig": "schema_evolution", "SchemaMismatchConfig": "schema_mismatch",
            "ColumnConfig": "column",
        }
        self.assertEqual(set(structs), set(bindings))
        internal = {"RootConfig": {"config_files"}, "DomainConfig": {"resolved_incoming_dir"}, "EntityConfig": {"id"}, "SinkTarget": {"write_mode"}}
        aliases = {"SourceConfig": {"source_ref": "ref"}, "StorageDefinition": {"fs_type": "type"}, "ColumnConfig": {"column_type": "type"}, "CatalogDefinition": {"type_config": "type"}}
        wire_extras = {
            "RootConfig": {"sources", "include", "filesystems"},
            "DomainConfig": {"defaults"}, "SourceConfig": {"filesystem"},
            "SinkTarget": {"filesystem"},
            "CatalogDefinition": {"region", "database", "create_database_if_missing", "allow_takeover", "uri", "credential", "warehouse", "oauth2_server_uri", "scope", "host", "catalog", "schema", "token", "create_schema_if_missing"},
        }
        for model, definition in bindings.items():
            with self.subTest(model=model):
                expected = structs[model] - internal.get(model, set())
                expected = {aliases.get(model, {}).get(field, field) for field in expected}
                block = SCHEMA if definition is None else SCHEMA["$defs"][definition]
                if model == "SinkOptions":
                    block = block["properties"]["options"]
                self.assertEqual(expected | wire_extras.get(model, set()), set(block["properties"]))
                self.assertFalse(internal.get(model, set()) & set(block["properties"]))

    def test_numeric_versions(self):
        fragment = {"name": "orders", "schema": {"columns": []}}
        for version in ("0.01", "00.2", "+0.+02"):
            with self.subTest(version=version):
                self.assert_invalid(ROOT_VALIDATOR, {"version": version, "entities": [fragment]})
        for version in ("abc", "0.0", "0.3.1", "-1.0"):
            with self.subTest(version=version):
                self.assert_invalid(ROOT_VALIDATOR, {"version": version, "entities": [fragment]})
        for version in ("0.03", "01.0", "+1.+0"):
            with self.subTest(version=version):
                self.assert_valid(ROOT_VALIDATOR, {"version": version, "entities": [complete_entity()]})

    def test_inline_entities_are_not_included_fragments(self):
        fragment = {"name": "orders", "schema": {"columns": []}}
        self.assert_invalid(ROOT_VALIDATOR, {"version": "0.3", "entities": [fragment]})
        self.assert_invalid(ROOT_VALIDATOR, {"version": "0.3", "entities": []})
        entity = complete_entity()
        defaults = {key: entity[key] for key in ("source", "sink", "policy")}
        self.assert_valid(ROOT_VALIDATOR, {
            "version": "0.3",
            "domains": [{"name": "sales", "defaults": defaults}],
            "entities": [{**fragment, "domain": "sales"}],
        })

    def test_evolution_version_and_local_sink(self):
        entity = complete_entity()
        entity["schema"]["schema_evolution"] = {"mode": "strict"}
        self.assert_invalid(ROOT_VALIDATOR, {"version": "0.1", "entities": [entity]})
        self.assert_valid(ROOT_VALIDATOR, {"version": "0.2", "entities": [entity]})
        entity["schema"]["schema_evolution"]["mode"] = "add_columns"
        self.assert_invalid(ROOT_VALIDATOR, {"version": "0.2", "entities": [entity]})
        entity["sink"]["accepted"]["format"] = "delta"
        self.assert_valid(ROOT_VALIDATOR, {"version": "0.2", "entities": [entity]})

    def test_inherited_pii(self):
        pii = {"columns": [{"name": "id", "strategy": "hash"}]}
        self.assert_valid(validator_for("domain_file"), {"name": "sales", "defaults": {"pii": pii}})
        self.assert_valid(validator_for("entity_file"), {**complete_entity(), "pii": pii})
        pii["columns"][0].update(strategy="mask", mask_pattern="****{last4}")
        self.assert_valid(validator_for("domain_file"), {"name": "sales", "defaults": {"pii": pii}})

    def test_existing_rust_model_sections(self):
        entity = complete_entity()
        entity["source"]["filesystem"] = "local"
        entity["sink"]["accepted"] = {
            "format": "duckdb",
            "duckdb": {"table": "orders", "connection": "md:lake"},
        }
        document = {
            "version": "0.3", "entities": [entity],
            "storages": {"default": "local", "definitions": [{"name": "local", "type": "local"}]},
            "catalogs": {"definitions": [{"name": "rest", "type": "rest", "uri": "https://catalog.example"}]},
            "lineage": {"url": "https://lineage.example", "namespace": "floe"},
        }
        self.assert_valid(ROOT_VALIDATOR, document)
        entity["source"]["storage"] = "local"
        self.assert_invalid(ROOT_VALIDATOR, document)

    def test_reviewed_runtime_rejections(self):
        base = {"version": "0.3", "entities": [complete_entity()]}
        mutations = [
            lambda doc: doc["entities"][0]["sink"].update(archive={"filesystem": "local"}),
            lambda doc: doc["entities"][0]["sink"]["accepted"].update(format="iceberg", partition_spec=[{"column": "id", "transform": "bucket[16]"}]),
            lambda doc: doc["entities"][0]["sink"].update(rejected={"format": "parquet", "path": "out"}),
            lambda doc: doc.update(storages={"definitions": [{"name": "local", "type": "local"}]}),
            lambda doc: doc.update(catalogs={"definitions": []}),
            lambda doc: doc.update(lineage={"url": "https://lineage.example", "namespace": "floe", "max_failures": 0}),
            lambda doc: doc["entities"][0]["sink"]["accepted"].update(format="duckdb"),
            lambda doc: doc["entities"][0]["sink"]["accepted"].update(iceberg={}),
            lambda doc: doc["entities"][0]["sink"]["accepted"].update(delta={}),
            lambda doc: doc["entities"][0]["sink"]["accepted"].update(merge={}),
        ]
        for index, mutate in enumerate(mutations):
            document = deepcopy(base)
            mutate(document)
            with self.subTest(case=index):
                self.assert_invalid(ROOT_VALIDATOR, document)

    def test_partition_by_requires_delta(self):
        entity = complete_entity()
        entity["sink"]["accepted"]["partition_by"] = ["id"]
        self.assert_invalid(ROOT_VALIDATOR, {"version": "0.3", "entities": [entity]})
        entity["sink"]["accepted"]["format"] = "delta"
        self.assert_valid(ROOT_VALIDATOR, {"version": "0.3", "entities": [entity]})

    def test_mask_tokens_follow_runtime_syntax(self):
        entity = complete_entity()
        column = {"name": "id", "strategy": "mask", "mask_pattern": "****"}
        entity["pii"] = {"columns": [column]}
        document = {"version": "0.3", "entities": [entity]}
        for pattern in ("****", "{first0}", "{last-1}", "{firstx}{first3}"):
            column["mask_pattern"] = pattern
            with self.subTest(pattern=pattern):
                self.assert_invalid(ROOT_VALIDATOR, document)
        for pattern in ("****{last4}", "{first03}***", "{first+3}", "{firstx}{last2}"):
            column["mask_pattern"] = pattern
            with self.subTest(pattern=pattern):
                self.assert_valid(ROOT_VALIDATOR, document)
                self.assert_valid(validator_for("entity_file"), entity)
                self.assert_valid(validator_for("domain_file"), {"name": "sales", "defaults": {"pii": entity["pii"]}})

    def test_motherduck_has_no_filesystem_storage(self):
        entity = complete_entity()
        target = {"format": "duckdb", "duckdb": {"table": "orders", "connection": "md:lake"}}
        entity["sink"]["accepted"] = target
        document = {"version": "0.3", "entities": [entity]}
        self.assert_valid(ROOT_VALIDATOR, document)
        for alias in ("storage", "filesystem"):
            target[alias] = "local"
            self.assert_invalid(ROOT_VALIDATOR, document)
            target["path"] = "ignored.db"
            self.assert_invalid(ROOT_VALIDATOR, document)
            target.pop("path")
            target.pop(alias)

    def test_pii_excludes_raw_output_modes(self):
        mutations = [
            lambda entity: entity["policy"].update(severity="abort"),
            lambda entity: entity["sink"].update(archive={"path": "archive"}),
            lambda entity: entity["schema"].update(mismatch={"missing_columns": "reject_file"}),
            lambda entity: entity["schema"].update(mismatch={"extra_columns": "reject_file"}),
        ]
        for index, mutate in enumerate(mutations):
            entity = complete_entity()
            mutate(entity)
            document = {"version": "0.3", "entities": [entity]}
            with self.subTest(case=index):
                self.assert_valid(ROOT_VALIDATOR, document)
                entity["pii"] = {"columns": [{"name": "id", "strategy": "hash"}]}
                self.assert_invalid(ROOT_VALIDATOR, document)
                self.assert_invalid(validator_for("entity_file"), entity)

    def test_drop_pii_sink_compatibility(self):
        entity = complete_entity()
        column = {"name": "id", "strategy": "drop"}
        entity["pii"] = {"columns": [column]}
        document = {"version": "0.3", "entities": [entity]}
        self.assert_valid(ROOT_VALIDATOR, document)
        for sink in ("delta", "iceberg"):
            entity["sink"]["accepted"]["format"] = sink
            column["strategy"] = "drop"
            with self.subTest(sink=sink):
                self.assert_invalid(ROOT_VALIDATOR, document)
                self.assert_invalid(validator_for("entity_file"), entity)
                column["strategy"] = "hash"
                self.assert_valid(ROOT_VALIDATOR, document)

    def test_default_pii_safety_values_can_be_overridden(self):
        entity = complete_entity()
        defaults = {"pii": {"columns": [{"name": "id", "strategy": "hash"}]}, "policy": {"severity": "abort"}}
        self.assert_valid(validator_for("domain_file"), {"name": "sales", "defaults": defaults})
        entity["domain"] = "sales"
        self.assert_valid(ROOT_VALIDATOR, {"version": "0.3", "domains": [{"name": "sales", "defaults": defaults}], "entities": [entity]})

    def test_qualified_entity_ids(self):
        for field in ("name", "domain"):
            for value in ("", "sales/orders", "sales.orders", "sales\\orders"):
                entity = complete_entity()
                entity[field] = value
                with self.subTest(field=field, value=value):
                    self.assert_invalid(ROOT_VALIDATOR, {"version": "0.3", "entities": [entity]})
                    self.assert_invalid(validator_for("entity_file"), entity)
        entity = complete_entity()
        entity["name"] = "legacy/orders"
        self.assert_valid(ROOT_VALIDATOR, {"version": "0.2", "entities": [entity]})

    def test_fixed_columns_require_width(self):
        entity = complete_entity()
        entity["source"]["format"] = "fixed"
        document = {"version": "0.3", "entities": [entity]}
        self.assert_invalid(ROOT_VALIDATOR, document)
        self.assert_invalid(validator_for("entity_file"), entity)
        entity["schema"]["columns"][0]["width"] = 12
        self.assert_valid(ROOT_VALIDATOR, document)
        self.assert_valid(validator_for("entity_file"), entity)

    def test_complete_xml_source_requires_row_tag(self):
        entity = complete_entity()
        entity["source"]["format"] = "xml"
        document = {"version": "0.3", "entities": [entity]}
        self.assert_invalid(ROOT_VALIDATOR, document)
        entity["source"]["options"] = {"row_tag": " "}
        self.assert_invalid(ROOT_VALIDATOR, document)
        entity["source"]["options"]["row_tag"] = "order"
        self.assert_valid(ROOT_VALIDATOR, document)
        entity["source"].pop("options")
        defaults = {"source": {"options": {"row_tag": "order"}}}
        entity["domain"] = "sales"
        document["domains"] = [{"name": "sales", "defaults": defaults}]
        self.assert_valid(ROOT_VALIDATOR, document)
        self.assert_valid(validator_for("entity_file"), entity)

    def test_free_form_metadata(self):
        entity = complete_entity()
        entity["metadata"] = {"owner": "team", "cost_center": "finops", "custom": {"a": 1}}
        document = {"version": "0.3", "metadata": {"project": "demo", "cost_center": "finops"}, "entities": [entity]}
        self.assert_valid(ROOT_VALIDATOR, document)
        self.assert_valid(validator_for("entity_file"), entity)
        self.assert_valid(validator_for("domain_file"), {"name": "sales", "defaults": {"metadata": entity["metadata"]}})

    def test_empty_catalog_name_matches_runtime(self):
        document = {"version": "0.3", "entities": [complete_entity()], "catalogs": {"definitions": [{"name": "", "type": "rest", "uri": "https://catalog.example"}]}}
        self.assert_valid(ROOT_VALIDATOR, document)

    def test_xlsx_sheet_names_are_not_blank(self):
        entity = complete_entity()
        entity["source"].update(format="xlsx", options={"sheet": " "})
        document = {"version": "0.3", "entities": [entity]}
        self.assert_invalid(ROOT_VALIDATOR, document)
        self.assert_invalid(validator_for("entity_file"), entity)
        entity["source"]["options"]["sheet"] = "Sheet1"
        self.assert_valid(ROOT_VALIDATOR, document)
        entity["source"]["format"] = "csv"
        entity["source"]["options"]["sheet"] = " "
        self.assert_valid(ROOT_VALIDATOR, document)

    def test_state_paths_are_not_blank(self):
        entity = complete_entity()
        entity["state"] = {"path": " "}
        document = {"version": "0.3", "entities": [entity]}
        self.assert_invalid(ROOT_VALIDATOR, document)
        self.assert_invalid(validator_for("entity_file"), entity)
        entity["state"]["path"] = "./state.json"
        self.assert_valid(ROOT_VALIDATOR, document)

    def test_json_selector_column_limit(self):
        entity = complete_entity()
        entity["source"]["format"] = "json"
        entity["schema"]["columns"] = [{"name": f"c{i}", "type": "string"} for i in range(1024)]
        document = {"version": "0.3", "entities": [entity]}
        self.assert_valid(ROOT_VALIDATOR, document)
        entity["schema"]["columns"].append({"name": "overflow", "type": "string"})
        self.assert_invalid(ROOT_VALIDATOR, document)
        self.assert_invalid(validator_for("entity_file"), entity)
        entity["source"]["format"] = "csv"
        self.assert_valid(ROOT_VALIDATOR, document)

    def test_archive_conflict_is_checked_on_entity_fragments(self):
        entity = {
            "name": "orders", "domain": "sales", "incremental_mode": "file",
            "sink": {"archive": {"path": "archive"}}, "schema": {"columns": []},
        }
        document = {
            "version": "0.3",
            "domains": [{"name": "sales", "defaults": {
                "source": {"format": "csv", "path": "in.csv"},
                "sink": {"accepted": {"format": "parquet", "path": "out"}},
                "policy": {"severity": "warn"},
            }}],
            "entities": [entity],
        }
        self.assert_invalid(ROOT_VALIDATOR, document)
        self.assert_invalid(validator_for("entity_file"), entity)
        entity["incremental_mode"] = "archive"
        self.assert_valid(ROOT_VALIDATOR, document)

    def test_scd2_system_columns_are_not_blank(self):
        entity = complete_entity()
        entity["sink"].update(write_mode="merge_scd2")
        entity["sink"]["accepted"].update(format="delta", merge={"scd2": {"current_flag_column": " "}})
        entity["schema"]["primary_key"] = ["id"]
        document = {"version": "0.3", "entities": [entity]}
        self.assert_invalid(ROOT_VALIDATOR, document)
        self.assert_invalid(validator_for("entity_file"), entity)
        entity["sink"]["accepted"]["merge"]["scd2"]["current_flag_column"] = "is_current"
        self.assert_valid(ROOT_VALIDATOR, document)
        for field in ("valid_from_column", "valid_to_column"):
            entity["sink"]["accepted"]["merge"]["scd2"] = {field: "\t"}
            with self.subTest(field=field):
                self.assert_invalid(ROOT_VALIDATOR, document)

    def test_column_type_spellings_match_runtime_normalization(self):
        entity = complete_entity()
        document = {"version": "0.3", "entities": [entity]}
        for type_name in ("INT_64", "Date-Time", "B_O_O_L", "UINT_32", "FLOAT_64", "+int64"):
            entity["schema"]["columns"][0]["type"] = type_name
            # Rust removes underscores and hyphens, but does not trim or remove plus signs.
            expected_valid = type_name != "+int64"
            with self.subTest(type=type_name):
                (self.assert_valid if expected_valid else self.assert_invalid)(ROOT_VALIDATOR, document)
                (self.assert_valid if expected_valid else self.assert_invalid)(validator_for("entity_file"), entity)
        entity["schema"]["columns"][0]["type"] = "int128"
        self.assert_invalid(ROOT_VALIDATOR, document)

    def test_standalone_document(self):
        path = ROOT / "example/standalone/config.yml"
        self.assert_valid(ROOT_VALIDATOR, yaml.safe_load(path.read_text()))

    def test_source_reference_boundaries(self):
        for source in ({"ref": "crm"}, {"resource": "orders"}):
            self.assert_invalid(validator_for("domain_file"), {"name": "sales", "defaults": {"source": source}})
        entity = {"name": "orders", "schema": {"columns": []}}
        self.assert_invalid(validator_for("entity_file"), {**entity, "source": {"format": "csv", "path": "in.csv", "resource": "orders"}})
        self.assert_valid(validator_for("entity_file"), {**entity, "source": {"ref": "crm", "resource": "orders"}})

    def test_inherited_schema_evolution(self):
        self.assert_valid(validator_for("domain_file"), {"name": "sales", "defaults": {"schema": {"schema_evolution": {"mode": "add_columns"}}}})
        self.assert_invalid(validator_for("domain_file"), {"name": "sales", "defaults": {"schema": {"schema_evolution": {"mode": "unknown"}}}})
        self.assert_invalid(validator_for("domain_file"), {"name": "sales", "defaults": {"schema": {"schema_evolution": {"on_incompatible": "reject"}}}})
        self.assert_valid(validator_for("entity_file"), {"name": "orders", "schema": {"columns": [], "schema_evolution": {"mode": "add_columns", "on_incompatible": "fail"}}})

    def test_ci_covers_fixture_changes(self):
        workflow = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())
        filters = yaml.safe_load(workflow["jobs"]["changes"]["steps"][1]["with"]["filters"])
        self.assertIn("example/project/**", filters["config_schema"])
        self.assertIn("crates/floe-core/src/config/types.rs", filters["config_schema"])

    def assert_valid(self, validator, document):
        self.assertTrue(validator.is_valid(document), list(validator.iter_errors(document)))

    def assert_invalid(self, validator, document):
        self.assertFalse(validator.is_valid(document), document)

    def test_schema_and_project_fragments(self):
        Draft202012Validator.check_schema(SCHEMA)
        self.assert_valid(
            ROOT_VALIDATOR,
            {
                "version": "0.3",
                "include": {"sources": "sources.yml", "domains": ["domains/*/_domain.yml"]},
            },
        )
        self.assert_invalid(
            ROOT_VALIDATOR,
            {"version": "0.3", "include": {"sources": "sources.yml"}},
        )
        self.assert_invalid(
            ROOT_VALIDATOR,
            {
                "version": "0.3",
                "sources": [{"name": "crm", "format": "csv"}],
                "include": {"sources": "sources.yml"},
            },
        )
        self.assert_invalid(ROOT_VALIDATOR, {"version": "1.0"})
        self.assert_valid(
            ROOT_VALIDATOR,
            {"version": "1.0", "entities": [complete_entity()]},
        )

    def test_source_domain_and_entity_fragments(self):
        self.assert_valid(
            validator_for("sources_file"),
            {"sources": [{"name": "crm", "format": "csv"}]},
        )
        self.assert_invalid(validator_for("sources_file"), {"sources": [{"format": "csv"}]})
        self.assert_valid(
            validator_for("domain_file"),
            {
                "name": "sales",
                "defaults": {
                    "source": {"cast_mode": "coerce"},
                    "sink": {"accepted": {"path": "out.parquet"}},
                },
            },
        )
        self.assert_invalid(validator_for("domain_file"), {"incoming_dir": "in"})
        self.assert_valid(
            validator_for("entity_file"),
            {"name": "orders", "source": {"path": "in.csv"}, "schema": {"columns": []}},
        )
        self.assert_invalid(validator_for("entity_file"), {"name": "orders"})
        self.assert_invalid(validator_for("entity_file"), {"schema": {"columns": []}})

    def test_legacy_entities_remain_complete(self):
        complete = {
            "name": "orders",
            "source": {"format": "csv", "path": "in.csv"},
            "sink": {"accepted": {"format": "parquet", "path": "out.parquet"}},
            "policy": {"severity": "warn"},
            "schema": {"columns": []},
        }
        self.assert_valid(ROOT_VALIDATOR, {"version": "0.2", "entities": [complete]})
        self.assert_invalid(
            ROOT_VALIDATOR,
            {"version": "0.2", "entities": [{"name": "orders", "schema": {"columns": []}}]},
        )
        with_source_ref = {**complete, "source": {"ref": "orders", "path": "in.csv"}}
        self.assert_invalid(ROOT_VALIDATOR, {"version": "0.1", "entities": [with_source_ref]})
        self.assert_invalid(
            ROOT_VALIDATOR,
            {"version": "0.1", "entities": [complete], "domains": [{"name": "sales"}]},
        )

    def test_example_project_documents(self):
        self.assert_valid(ROOT_VALIDATOR, yaml.safe_load((ROOT / "example/project/config.yml").read_text()))
        self.assert_valid(
            validator_for("sources_file"),
            yaml.safe_load((ROOT / "example/project/sources.yml").read_text()),
        )
        for path in (ROOT / "example/project/domains").rglob("*.yml"):
            branch = "domain_file" if path.name == "_domain.yml" else "entity_file"
            self.assert_valid(validator_for(branch), yaml.safe_load(path.read_text()))


if __name__ == "__main__":
    unittest.main()
