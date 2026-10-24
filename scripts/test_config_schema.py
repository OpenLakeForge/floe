import unittest
import re
from copy import deepcopy
from pathlib import Path

import yaml
from jsonschema import Draft202012Validator, validators


ROOT = Path(__file__).resolve().parents[1]
SCHEMA = yaml.safe_load((ROOT / "schemas/project.schema.yaml").read_text())


def is_yaml_integer(_checker, instance):
    return type(instance) is int


YamlDraft202012Validator = validators.extend(
    Draft202012Validator,
    type_checker=Draft202012Validator.TYPE_CHECKER.redefine("integer", is_yaml_integer),
)
ROOT_VALIDATOR = YamlDraft202012Validator(SCHEMA)


def complete_entity():
    return {
        "name": "orders",
        "source": {"format": "csv", "path": "in.csv"},
        "sink": {"accepted": {"format": "parquet", "path": "out"}},
        "policy": {"severity": "warn"},
        "schema": {"columns": [{"name": "id", "type": "string"}]},
    }


def profile_validator():
    return YamlDraft202012Validator(yaml.safe_load((ROOT / "schemas/profile.schema.yaml").read_text()))


def validator_for(branch):
    name = {"sources_file": "sources", "domain_file": "domains", "entity_file": "entity"}[branch]
    return YamlDraft202012Validator(yaml.safe_load((ROOT / f"schemas/{name}.schema.yaml").read_text()))


class ConfigSchemaTest(unittest.TestCase):
    def test_independent_document_schemas(self):
        self.assertFalse((ROOT / "schemas/config.schema.yaml").exists())
        self.assertFalse((ROOT / "schemas/0.3").exists())
        schemas = {}
        for name in ("project", "sources", "domains", "entity"):
            schema = yaml.safe_load((ROOT / f"schemas/{name}.schema.yaml").read_text())
            schemas[name] = schema
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
        for name in ("sources", "domains", "entity"):
            self.assertEqual(
                schemas["project"]["$defs"]["source_options"],
                schemas[name]["$defs"]["source_options"],
                f"{name} source option schema drifted from the project contract",
            )
        self.assertEqual(
            schemas["project"]["$defs"]["source_definition"],
            schemas["sources"]["$defs"]["source_definition"],
        )
        for name in ("entity", "domains"):
            self.assertEqual(
                schemas["project"]["$defs"]["sink_accepted_fragment"],
                schemas[name]["$defs"]["sink_accepted_fragment"],
                f"{name} sink fragment schema drifted from the project contract",
            )

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
            "IcebergPartitionFieldConfig": {"num_buckets"},  # D12: Python-only, Rust 0.7 has no bucket transform
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
        self.assert_invalid(ROOT_VALIDATOR, {"version": "+0.+0", "entities": [complete_entity()]})
        max_u64 = "18446744073709551615"
        self.assert_valid(ROOT_VALIDATOR, {
            "version": f"000{max_u64}.{max_u64}", "entities": [complete_entity()],
        })
        for version in ("18446744073709551616.0", "0.18446744073709551616", "0.1\n"):
            with self.subTest(version=version):
                self.assert_invalid(ROOT_VALIDATOR, {"version": version, "entities": [complete_entity()]})

    def test_inline_entities_are_not_included_fragments(self):
        fragment = {"name": "orders", "schema": {"columns": []}}
        self.assert_invalid(ROOT_VALIDATOR, {"version": "0.3", "entities": [fragment]})
        self.assert_invalid(ROOT_VALIDATOR, {"version": "0.3", "entities": []})
        self.assert_invalid(ROOT_VALIDATOR, {"version": "0.3", "entities": [{**fragment, "domain": None}]})
        entity = complete_entity()
        defaults = {key: entity[key] for key in ("source", "sink", "policy")}
        self.assert_valid(ROOT_VALIDATOR, {
            "version": "0.3",
            "domains": [{"name": "sales", "defaults": defaults}],
            "entities": [{**fragment, "domain": "sales"}],
        })

    def test_source_option_constraints_follow_the_source_format(self):
        sources_validator = validator_for("sources_file")
        document = {"sources": [{"name": "shared", "format": "csv", "options": {
            "separator": ",", "encoding": "LOSSY_UTF_8", "json_mode": "ignored",
            "header_row": 0, "data_row": 0,
        }}]}
        self.assert_valid(sources_validator, document)
        document["sources"][0]["options"]["separator"] = "||"
        self.assert_invalid(sources_validator, document)
        document["sources"][0]["options"]["separator"] = "é"
        self.assert_invalid(sources_validator, document)
        document["sources"][0]["options"]["separator"] = ""
        self.assert_invalid(sources_validator, document)
        document["sources"][0]["options"]["separator"] = ",\n"
        self.assert_invalid(sources_validator, document)
        document["sources"][0].update(format="json", options={"separator": "||", "encoding": "ignored"})
        self.assert_valid(sources_validator, document)
        document["sources"][0]["options"]["json_mode"] = "object"
        self.assert_invalid(sources_validator, document)
        document["sources"][0].update(format="xlsx", options={"sheet": " \t", "header_row": 1, "data_row": 2})
        self.assert_invalid(sources_validator, document)
        document["sources"][0]["options"].update(sheet="Sheet1", header_row=0)
        self.assert_invalid(sources_validator, document)
        document["sources"][0]["options"] = {"sheet": "Sheet1", "header_row": 1, "data_row": 2}
        self.assert_valid(sources_validator, document)
        document["sources"][0]["options"]["data_row"] = 1
        self.assert_invalid(sources_validator, document)
        document["sources"][0]["options"]["data_row"] = 2

        document["sources"][0]["options"] = {
            "header": None, "separator": None, "encoding": None, "json_mode": None,
            "sheet": None, "header_row": None, "data_row": None,
        }
        document["sources"][0]["format"] = "csv"
        self.assert_valid(sources_validator, document)

        entity = complete_entity()
        entity["source"]["options"] = {"separator": ",\n"}
        self.assert_invalid(ROOT_VALIDATOR, {"version": "0.3", "entities": [entity]})
        self.assert_invalid(validator_for("entity_file"), entity)
        self.assert_invalid(validator_for("domain_file"), {
            "name": "sales", "defaults": {"source": {
                "format": "csv", "path": "in.csv", "options": {"separator": ",\n"},
            }},
        })

    def test_csv_encoding_rejects_terminal_newline_in_every_schema_copy(self):
        invalid_encoding = "utf8\n"
        entity = complete_entity()
        entity["source"]["options"] = {"encoding": invalid_encoding}
        self.assert_invalid(ROOT_VALIDATOR, {"version": "0.3", "entities": [entity]})

        legacy_entity = complete_entity()
        legacy_entity["source"]["options"] = {"encoding": invalid_encoding}
        self.assert_invalid(ROOT_VALIDATOR, {"version": "0.2", "entities": [legacy_entity]})

        self.assert_invalid(ROOT_VALIDATOR, {
            "version": "0.3", "entities": [complete_entity()],
            "sources": [{"name": "shared", "format": "csv", "options": {"encoding": invalid_encoding}}],
        })
        self.assert_invalid(validator_for("entity_file"), {
            "name": "orders", "domain": "sales", "schema": {"columns": []},
            "source": {"format": "csv", "path": "in.csv", "options": {"encoding": invalid_encoding}},
        })
        self.assert_invalid(validator_for("domain_file"), {
            "name": "sales", "defaults": {"source": {
                "format": "csv", "path": "in.csv", "options": {"encoding": invalid_encoding},
            }},
        })
        self.assert_invalid(validator_for("sources_file"), {"sources": [{
            "name": "shared", "format": "csv", "options": {"encoding": invalid_encoding},
        }]})

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
        column["mask_pattern"] = None
        self.assert_invalid(ROOT_VALIDATOR, document)
        self.assert_invalid(validator_for("entity_file"), entity)
        self.assert_invalid(validator_for("domain_file"), {"name": "sales", "defaults": {"pii": entity["pii"]}})
        for pattern in ("****", "{first0}", "{last-1}", "{firstx}{first3}"):
            column["mask_pattern"] = pattern
            with self.subTest(pattern=pattern):
                self.assert_invalid(ROOT_VALIDATOR, document)
        for pattern in (
            "****{last4}", "{first03}***", "{first+3}", "{firstx}{last2}",
            "{first+00018446744073709551615}", "{last18446744073709551615}",
        ):
            column["mask_pattern"] = pattern
            with self.subTest(pattern=pattern):
                self.assert_valid(ROOT_VALIDATOR, document)
                self.assert_valid(validator_for("entity_file"), entity)
                self.assert_valid(validator_for("domain_file"), {"name": "sales", "defaults": {"pii": entity["pii"]}})
        for pattern in ("{first18446744073709551616}", "{last18446744073709551616}"):
            column["mask_pattern"] = pattern
            with self.subTest(pattern=pattern):
                self.assert_invalid(ROOT_VALIDATOR, document)
                self.assert_invalid(validator_for("entity_file"), entity)
                self.assert_invalid(validator_for("domain_file"), {"name": "sales", "defaults": {"pii": entity["pii"]}})

    def test_unformatted_sink_fragments_cannot_combine_catalog_blocks(self):
        combinations = (
            {"delta": {}, "iceberg": {}},
            {"delta": {}, "duckdb": {"table": "orders"}},
            {"iceberg": {}, "duckdb": {"table": "orders"}},
        )
        for schema_name in ("entity", "domain", "project"):
            for blocks in combinations:
                if schema_name == "entity":
                    valid = validator_for("entity_file").is_valid({
                        "name": "orders", "domain": "sales", "schema": {"columns": []},
                        "sink": {"accepted": blocks},
                    })
                elif schema_name == "domain":
                    valid = validator_for("domain_file").is_valid({
                        "name": "sales", "defaults": {"sink": {"accepted": blocks}},
                    })
                else:
                    valid = ROOT_VALIDATOR.is_valid({
                        "version": "0.3",
                        "domains": [{"name": "sales", "defaults": {
                            "source": {"format": "csv", "path": "in.csv"},
                            "sink": {"accepted": blocks}, "policy": {"severity": "warn"},
                        }}],
                        "entities": [{"name": "orders", "domain": "sales", "schema": {"columns": []}}],
                    })
                with self.subTest(schema=schema_name, blocks=blocks):
                    self.assertFalse(valid, blocks)

    def test_motherduck_has_no_filesystem_storage(self):
        entity = complete_entity()
        target = {"format": "duckdb", "duckdb": {"table": "orders", "connection": "md:lake"}}
        entity["sink"]["accepted"] = target
        document = {"version": "0.3", "entities": [entity]}
        self.assert_valid(ROOT_VALIDATOR, document)
        for alias in ("storage", "filesystem"):
            target[alias] = None
            self.assert_valid(ROOT_VALIDATOR, document)
            self.assert_valid(validator_for("entity_file"), entity)
            self.assert_valid(validator_for("domain_file"), {
                "name": "sales", "defaults": {"sink": {"accepted": target}},
            })
            target.pop(alias)
        target["duckdb"]["connection"] = None
        self.assert_invalid(ROOT_VALIDATOR, document)
        target["duckdb"]["connection"] = "md:lake"
        for alias in ("storage", "filesystem"):
            target[alias] = "local"
            self.assert_invalid(ROOT_VALIDATOR, document)
            target["path"] = "ignored.db"
            self.assert_invalid(ROOT_VALIDATOR, document)
            target.pop("path")
            target.pop(alias)
        target["duckdb"]["connection"] = None
        self.assert_invalid(ROOT_VALIDATOR, document)

    def test_motherduck_storage_conflicts_in_fragments(self):
        for alias in ("storage", "filesystem"):
            target = {
                "format": "duckdb", alias: "local",
                "duckdb": {"table": "orders", "connection": "md:lake"},
            }
            entity = {"name": "orders", "domain": "sales", "schema": {"columns": []},
                      "sink": {"accepted": target}}
            with self.subTest(alias=alias):
                self.assert_invalid(validator_for("entity_file"), entity)
                self.assert_invalid(validator_for("domain_file"), {
                    "name": "sales", "defaults": {"sink": {"accepted": target}},
                })
                self.assert_invalid(ROOT_VALIDATOR, {
                    "version": "0.3", "domains": [{"name": "sales"}], "entities": [entity],
                })
                complete = complete_entity()
                complete.update(domain="sales")
                complete["sink"]["accepted"] = target
                self.assert_invalid(ROOT_VALIDATOR, {
                    "version": "0.3", "domains": [{"name": "sales"}], "entities": [complete],
                })

    def test_partition_formats_are_checked_in_sink_fragments(self):
        entity = {"name": "orders", "domain": "sales", "schema": {"columns": []}, "sink": {
            "accepted": {"format": "parquet", "partition_by": ["id"]},
        }}
        self.assert_invalid(validator_for("entity_file"), entity)
        self.assert_invalid(validator_for("domain_file"), {"name": "sales", "defaults": {
            "sink": {"accepted": {"format": "parquet", "partition_by": ["id"]}},
        }})
        self.assert_invalid(ROOT_VALIDATOR, {
            "version": "0.3", "domains": [{"name": "sales"}], "entities": [entity],
        })
        entity["sink"]["accepted"]["format"] = "delta"
        self.assert_valid(validator_for("entity_file"), entity)

        entity["sink"]["accepted"] = {"format": "delta", "partition_spec": [{
            "column": "id", "transform": "identity",
        }]}
        self.assert_invalid(validator_for("entity_file"), entity)
        self.assert_invalid(validator_for("domain_file"), {"name": "sales", "defaults": {
            "sink": {"accepted": {"format": "delta", "partition_spec": [{
                "column": "id", "transform": "identity",
            }]}},
        }})
        self.assert_invalid(ROOT_VALIDATOR, {
            "version": "0.3", "domains": [{"name": "sales"}], "entities": [entity],
        })
        entity["sink"]["accepted"]["format"] = "iceberg"
        self.assert_valid(validator_for("entity_file"), entity)

        for field, format_name, values in (
            ("partition_by", "delta", ["id"]),
            ("partition_spec", "iceberg", [{"column": "id", "transform": "identity"}]),
        ):
            defaults = {"source": {"format": "csv", "path": "in.csv"}, "policy": {"severity": "warn"},
                        "sink": {"accepted": {"path": "out", field: values}}}
            fragment = {"name": "orders", "domain": "sales", "sink": {
                "accepted": {"format": format_name},
            }, "schema": {"columns": [{"name": "id", "type": "string"}]}}
            self.assert_valid(ROOT_VALIDATOR, {
                "version": "0.3", "domains": [{"name": "sales", "defaults": defaults}],
                "entities": [fragment],
            })

    def test_format_option_blocks_follow_local_sink_fragment_format(self):
        formats = ("parquet", "delta", "iceberg", "duckdb")
        option_blocks = {"delta": {}, "iceberg": {}, "duckdb": {"table": "orders"}}

        for schema_name in ("entity", "domain", "project"):
            for option_format, options in option_blocks.items():
                def validate(target):
                    if schema_name == "entity":
                        return validator_for("entity_file").is_valid({
                            "name": "orders", "domain": "sales", "schema": {"columns": []},
                            "sink": {"accepted": target},
                        })
                    defaults = {"sink": {"accepted": target}}
                    if schema_name == "domain":
                        return validator_for("domain_file").is_valid({"name": "sales", "defaults": defaults})
                    defaults.update(
                        source={"format": "csv", "path": "in.csv"},
                        policy={"severity": "warn"},
                    )
                    return ROOT_VALIDATOR.is_valid({
                        "version": "0.3", "domains": [{"name": "sales", "defaults": defaults}],
                        "entities": [{"name": "orders", "domain": "sales", "schema": {"columns": []}}],
                    })

                matching = {"format": option_format, option_format: options}
                with self.subTest(schema=schema_name, format=option_format):
                    self.assertTrue(validate(matching), matching)
                    for format_name in formats:
                        if format_name != option_format:
                            mismatched = {**matching, "format": format_name}
                            with self.subTest(schema=schema_name, option=option_format, format=format_name):
                                self.assertFalse(validate(mismatched), mismatched)

                # A fragment may supply one option block while inheriting its format.
                self.assertTrue(validate({option_format: options}), (schema_name, option_format))

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

    def test_included_domain_names_match_qualified_entity_ids(self):
        for name in ("", "sales/orders", "sales.orders", "sales\\orders"):
            with self.subTest(name=name):
                self.assert_invalid(validator_for("domain_file"), {"name": name})
                self.assert_invalid(ROOT_VALIDATOR, {
                    "version": "0.3", "domains": [{"name": name}], "entities": [complete_entity()],
                })
        self.assert_valid(validator_for("domain_file"), {"name": "sales west"})
        self.assert_valid(ROOT_VALIDATOR, {
            "version": "0.2", "domains": [{"name": "sales/orders", "incoming_dir": "incoming"}],
            "entities": [complete_entity()],
        })

    def test_fixed_columns_require_width(self):
        entity = complete_entity()
        entity["source"]["format"] = "fixed"
        document = {"version": "0.3", "entities": [entity]}
        self.assert_invalid(ROOT_VALIDATOR, document)
        self.assert_invalid(validator_for("entity_file"), entity)
        entity["schema"]["columns"][0]["width"] = None
        self.assert_invalid(ROOT_VALIDATOR, document)
        self.assert_invalid(validator_for("entity_file"), entity)
        entity["schema"]["columns"][0]["width"] = 12
        self.assert_valid(ROOT_VALIDATOR, document)
        self.assert_valid(validator_for("entity_file"), entity)

    def test_format_specific_numeric_limits_match_runtime(self):
        entity = complete_entity()
        entity["schema"]["columns"][0]["width"] = 0
        entity["source"]["options"] = {"header_row": 0, "data_row": 0}
        document = {"version": "0.3", "entities": [entity]}
        self.assert_valid(ROOT_VALIDATOR, document)
        self.assert_valid(validator_for("entity_file"), entity)

        entity["source"].update(format="fixed", options={})
        self.assert_invalid(ROOT_VALIDATOR, document)
        self.assert_invalid(validator_for("entity_file"), entity)
        entity["schema"]["columns"][0]["width"] = 1
        entity["source"].update(format="xlsx", options={"header_row": 0, "data_row": 2})
        self.assert_invalid(ROOT_VALIDATOR, document)
        entity["source"]["options"] = {"header_row": 1, "data_row": 2}
        self.assert_valid(ROOT_VALIDATOR, document)
        entity["source"]["options"] = {"header_row": 1, "data_row": 1}
        self.assert_invalid(ROOT_VALIDATOR, document)
        self.assert_invalid(validator_for("entity_file"), entity)
        fragment = {"name": "orders", "domain": "sales", "schema": {"columns": []}, "source": {
            "format": "xlsx", "path": "in.xlsx", "options": {"header_row": 1, "data_row": 1},
        }}
        self.assert_invalid(validator_for("entity_file"), fragment)
        self.assert_invalid(validator_for("domain_file"), {"name": "sales", "defaults": {
            "source": {"format": "xlsx", "path": "in.xlsx", "options": {"header_row": 1, "data_row": 1}},
        }})
        entity["source"]["options"] = {"header_row": 1, "data_row": 2}
        entity["source"]["options"] = {"header_row": 0, "data_row": 1}
        self.assert_invalid(ROOT_VALIDATOR, document)
        entity["source"]["options"] = {"header_row": 1, "data_row": 0}
        self.assert_invalid(ROOT_VALIDATOR, document)

    def test_yaml_float_scalars_are_not_integer_fields(self):
        cases = []
        for field, value in (("header_row", 1.0), ("data_row", 2.0)):
            document = {"version": "0.3", "entities": [complete_entity()]}
            document["entities"][0]["source"]["options"] = {field: value}
            cases.append((field, document, ROOT_VALIDATOR, ("entities", 0, "source", "options", field)))

        document = {"version": "0.3", "entities": [complete_entity()]}
        document["entities"][0]["schema"]["columns"][0]["width"] = 1.0
        cases.append(("width", document, ROOT_VALIDATOR, ("entities", 0, "schema", "columns", 0, "width")))

        for field in ("row_group_size", "max_size_per_file"):
            document = {"version": "0.3", "entities": [complete_entity()]}
            document["entities"][0]["sink"]["accepted"]["options"] = {field: 1.0}
            cases.append((field, document, ROOT_VALIDATOR, ("entities", 0, "sink", "accepted", "options", field)))

        for field in ("timeout_secs", "max_failures"):
            document = {"version": "0.3", "entities": [complete_entity()], "lineage": {
                "url": "https://lineage.example", "namespace": "floe", field: 1.0,
            }}
            cases.append((field, document, ROOT_VALIDATOR, ("lineage", field)))

        sources = {"sources": [{"name": "shared", "format": "xlsx", "options": {"header_row": 1.0}}]}
        cases.append(("sources.header_row", sources, validator_for("sources_file"), ("sources", 0, "options", "header_row")))

        for name, document, validator, path in cases:
            parsed = yaml.safe_load(yaml.safe_dump(document))
            value = parsed
            for part in path:
                value = value[part]
            with self.subTest(field=name):
                self.assertIs(type(value), float)
                self.assertTrue(Draft202012Validator(validator.schema).is_valid(parsed))
                self.assert_invalid(validator, parsed)

    def test_key_members_are_not_blank_and_storage_defaults_are_required(self):
        entity = complete_entity()
        entity["schema"]["primary_key"] = [" "]
        document = {"version": "0.3", "entities": [entity]}
        self.assert_invalid(ROOT_VALIDATOR, document)
        entity["schema"].pop("primary_key")
        entity["schema"]["unique_keys"] = [["\t"]]
        self.assert_invalid(ROOT_VALIDATOR, document)

        config = {"version": "0.3", "entities": [complete_entity()], "storages": {
            "default": None, "definitions": [{"name": "local", "type": "local"}],
        }}
        self.assert_invalid(ROOT_VALIDATOR, config)
        config["storages"]["default"] = "local"
        config["storages"]["definitions"][0].update(bucket=None, region=None)
        self.assert_valid(ROOT_VALIDATOR, config)
        config["storages"]["definitions"][0].update(type="s3", bucket=None, region="us-east-1")
        self.assert_invalid(ROOT_VALIDATOR, config)

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
        entity["source"] = {"format": "xml", "path": "orders.xml", "options": {"row_tag": None}}
        entity["domain"] = "sales"
        document["domains"] = [{"name": "sales", "defaults": {
            "source": {"format": "xml", "path": "orders.xml", "options": {"row_tag": "order"}},
        }}]
        self.assert_invalid(ROOT_VALIDATOR, document)

    def test_xml_source_options_cannot_be_blank_in_fragments(self):
        entity = {"name": "orders", "domain": "sales", "schema": {"columns": []},
                  "source": {"format": "xml", "options": {"namespace": " "}}}
        self.assert_invalid(validator_for("entity_file"), entity)
        entity["source"]["options"] = {"value_tag": "\t"}
        self.assert_invalid(validator_for("entity_file"), entity)
        entity["source"]["options"] = {"row_tag": " "}
        self.assert_invalid(validator_for("entity_file"), entity)
        entity["source"]["options"] = {"row_tag": None}
        self.assert_invalid(validator_for("entity_file"), entity)
        entity["source"]["options"] = {}
        self.assert_valid(validator_for("entity_file"), entity)

        source = {"sources": [{"name": "xml", "format": "xml", "options": {"row_tag": " "}}]}
        self.assert_invalid(validator_for("sources_file"), source)
        source["sources"][0]["options"] = {}
        self.assert_valid(validator_for("sources_file"), source)
        self.assert_invalid(validator_for("domain_file"), {
            "name": "sales", "defaults": {"source": {"format": "xml", "options": {"namespace": " "}}},
        })
        self.assert_invalid(validator_for("domain_file"), {
            "name": "sales", "defaults": {"source": {"format": "xml", "options": {"row_tag": None}}},
        })
        self.assert_invalid(validator_for("sources_file"), {
            "sources": [{"name": "xml", "format": "xml", "options": {"row_tag": None}}],
        })

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

    def test_json_and_xml_selectors_are_not_blank(self):
        # Rust: the selector is `source`, else `name`; blank is rejected for json/xml only.
        blank = {
            "source empty": {"name": "id", "type": "string", "source": ""},
            "source blank": {"name": "id", "type": "string", "source": " "},
            "name empty": {"name": "", "type": "string"},
            "null source, name empty": {"name": "", "type": "string", "source": None},
        }
        fine = {"name empty, source set": {"name": "", "type": "string", "source": "a"}}

        def document(fmt, column):
            entity = complete_entity()
            entity["source"]["format"] = fmt
            if fmt == "xml":
                entity["source"]["options"] = {"row_tag": "row"}
            entity["schema"]["columns"] = [column]
            return entity, {"version": "0.3", "entities": [entity]}

        for fmt in ("json", "xml"):
            for label, column in blank.items():
                with self.subTest(fmt=fmt, case=label):
                    entity, project = document(fmt, column)
                    self.assert_invalid(ROOT_VALIDATOR, project)
                    self.assert_invalid(validator_for("entity_file"), entity)
                    self.assert_valid(ROOT_VALIDATOR, document("csv", column)[1])
            for label, column in fine.items():
                with self.subTest(fmt=fmt, case=label):
                    entity, project = document(fmt, column)
                    self.assert_valid(ROOT_VALIDATOR, project)
                    self.assert_valid(validator_for("entity_file"), entity)

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

    def test_null_archive_path_uses_runtime_default(self):
        entity = complete_entity()
        entity["sink"]["archive"] = {"path": None}
        entity["incremental_mode"] = "archive"
        self.assert_valid(ROOT_VALIDATOR, {"version": "0.3", "entities": [entity]})
        self.assert_valid(validator_for("entity_file"), entity)
        entity["sink"]["archive"] = {}
        self.assert_valid(validator_for("entity_file"), entity)

    def test_null_partition_by_is_absent(self):
        entity = complete_entity()
        entity["sink"]["accepted"].update(format="parquet", partition_by=None)
        document = {"version": "0.3", "entities": [entity]}
        self.assert_valid(ROOT_VALIDATOR, document)
        self.assert_valid(validator_for("entity_file"), entity)
        entity["sink"]["accepted"].update(format="delta", partition_by=None)
        self.assert_valid(ROOT_VALIDATOR, document)

    def test_null_write_mode_uses_the_default_for_parquet(self):
        entity = complete_entity()
        entity["sink"].update(write_mode=None)
        document = {"version": "0.3", "entities": [entity]}
        self.assert_valid(ROOT_VALIDATOR, document)
        self.assert_valid(validator_for("entity_file"), entity)

    def test_fragments_reject_write_modes_unsupported_by_local_sink_format(self):
        entity = {
            "name": "orders", "domain": "sales",
            "sink": {"write_mode": "merge_scd1", "accepted": {"format": "parquet", "path": "out"}},
            "schema": {"columns": [{"name": "id", "type": "string"}], "primary_key": ["id"]},
        }
        self.assert_invalid(validator_for("entity_file"), entity)
        entity["sink"].update(write_mode=None)
        self.assert_valid(validator_for("entity_file"), entity)
        self.assert_invalid(validator_for("domain_file"), {
            "name": "sales", "defaults": {"sink": {
                "write_mode": "merge_scd2", "accepted": {"format": "iceberg", "path": "out"},
            }},
        })
        self.assert_valid(validator_for("domain_file"), {
            "name": "sales", "defaults": {"sink": {
                "write_mode": "merge_scd1", "accepted": {"path": "out"},
            }},
        })
        self.assert_valid(validator_for("entity_file"), {
            "name": "orders", "domain": "sales", "sink": {
                "write_mode": "APPEND", "accepted": {"format": "parquet"},
            }, "schema": {"columns": []},
        })
        self.assert_valid(validator_for("domain_file"), {
            "name": "sales", "defaults": {"sink": {
                "write_mode": "APPEND", "accepted": {"format": "parquet"},
            }},
        })
        defaults = {"sink": {"write_mode": "merge_scd1", "accepted": {"path": "out"}}}
        entity = complete_entity()
        entity.update(domain="sales")
        entity["sink"]["accepted"].update(format="delta", path="out")
        entity["schema"]["primary_key"] = ["id"]
        document = {"version": "0.3", "domains": [{"name": "sales", "defaults": defaults}], "entities": [entity]}
        self.assert_valid(ROOT_VALIDATOR, document)

    def test_locally_formatted_sink_fragments_reject_null_paths(self):
        entity = {"name": "orders", "domain": "sales", "sink": {
            "accepted": {"format": "delta", "path": None},
        }, "schema": {"columns": []}}
        self.assert_invalid(validator_for("entity_file"), entity)
        complete = complete_entity()
        complete["sink"]["accepted"].update(format="delta", path=None)
        self.assert_invalid(ROOT_VALIDATOR, {"version": "0.3", "entities": [complete]})
        self.assert_invalid(validator_for("domain_file"), {
            "name": "sales", "defaults": {"sink": {"accepted": {"format": "iceberg", "path": None}}},
        })
        entity["sink"]["accepted"]["path"] = "out"
        self.assert_valid(validator_for("entity_file"), entity)

    def test_unsigned_yaml_integer_fields_reject_negative_values(self):
        entity = complete_entity()
        document = {"version": "0.3", "entities": [entity]}
        for field in ("header_row", "data_row"):
            entity["source"]["options"] = {field: -1}
            with self.subTest(field=field):
                self.assert_invalid(ROOT_VALIDATOR, document)
                self.assert_invalid(validator_for("entity_file"), entity)
        entity["source"].pop("options")
        entity["schema"]["columns"][0]["width"] = -1
        self.assert_invalid(ROOT_VALIDATOR, document)
        self.assert_invalid(validator_for("entity_file"), entity)
        entity["schema"]["columns"][0].pop("width")
        entity["sink"]["accepted"]["format"] = "delta"
        for field in ("row_group_size", "max_size_per_file"):
            entity["sink"]["accepted"]["options"] = {field: -1}
            with self.subTest(field=field):
                self.assert_invalid(ROOT_VALIDATOR, document)
                self.assert_invalid(validator_for("entity_file"), entity)
                self.assert_invalid(validator_for("domain_file"), {"name": "sales", "defaults": {
                    "sink": {"accepted": {"format": "delta", "options": {field: -1}}},
                }})
        self.assert_invalid(validator_for("sources_file"), {"sources": [{
            "name": "shared", "format": "csv", "options": {"header_row": -1},
        }]})
        self.assert_invalid(validator_for("domain_file"), {"name": "sales", "defaults": {
            "source": {"options": {"data_row": -1}},
            "sink": {"accepted": {"format": "parquet", "options": {"row_group_size": -1}}},
        }})

    def test_fragments_reject_merge_options_with_non_merge_write_modes(self):
        entity = {
            "name": "orders", "domain": "sales",
            "sink": {"write_mode": "overwrite", "accepted": {
                "format": "delta", "path": "out", "merge": {},
            }},
            "schema": {"columns": [{"name": "id", "type": "string"}]},
        }
        self.assert_invalid(validator_for("entity_file"), entity)
        entity["sink"]["write_mode"] = None
        self.assert_invalid(validator_for("entity_file"), entity)
        entity["sink"].update(write_mode="merge_scd1")
        entity["sink"]["accepted"]["merge"] = {"scd2": {}}
        self.assert_invalid(validator_for("entity_file"), entity)
        self.assert_invalid(validator_for("domain_file"), {
            "name": "sales", "defaults": {"sink": {
                "write_mode": "overwrite", "accepted": {"format": "delta", "path": "out", "merge": {}},
            }},
        })

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
        for type_name in ("INT_64", "Date-Time", "B_O_O_L", "UINT_32", "FLOAT_64", "_INT_64-", "+int64"):
            entity["schema"]["columns"][0]["type"] = type_name
            # Rust removes underscores and hyphens, but does not trim or remove plus signs.
            expected_valid = type_name != "+int64"
            with self.subTest(type=type_name):
                (self.assert_valid if expected_valid else self.assert_invalid)(ROOT_VALIDATOR, document)
                (self.assert_valid if expected_valid else self.assert_invalid)(validator_for("entity_file"), entity)
        entity["schema"]["columns"][0]["type"] = "int128"
        self.assert_invalid(ROOT_VALIDATOR, document)

    def test_normalize_strategy_spellings_match_runtime(self):
        entity = complete_entity()
        entity["schema"]["normalize_columns"] = {"enabled": True, "strategy": "-SNAKE-CASE_"}
        document = {"version": "0.3", "entities": [entity]}
        self.assert_valid(ROOT_VALIDATOR, document)
        self.assert_valid(validator_for("entity_file"), entity)
        entity["schema"]["normalize_columns"]["strategy"] = "snake case"
        self.assert_invalid(ROOT_VALIDATOR, document)
        self.assert_invalid(validator_for("entity_file"), entity)
        self.assert_valid(validator_for("domain_file"), {
            "name": "sales", "defaults": {"schema": {"normalize_columns": {"strategy": "CAMEL-CASE"}}},
        })

    def test_runtime_normalized_enums_match_the_schema(self):
        entity = complete_entity()
        entity["incremental_mode"] = " FILE "
        entity["sink"]["write_mode"] = " MERGE_SCD1 "
        entity["sink"]["accepted"].update(format="delta", merge={"ignore_columns": []})
        entity["schema"].update(primary_key=["id"], schema_evolution={
            "mode": " ADD_COLUMNS ", "on_incompatible": " FAIL ",
        })
        document = {"version": "0.3", "entities": [entity]}
        self.assert_valid(ROOT_VALIDATOR, document)
        self.assert_valid(validator_for("entity_file"), entity)
        entity["incremental_mode"] = "not-file"
        self.assert_invalid(ROOT_VALIDATOR, document)

        entity = complete_entity()
        entity["pii"] = {"columns": [{"name": "id", "strategy": " HASH "}]}
        entity["source"]["options"] = {"encoding": "LOSSY_UTF_8"}
        document = {"version": "0.3", "entities": [entity]}
        self.assert_valid(ROOT_VALIDATOR, document)
        entity["source"]["options"]["encoding"] = "latin1"
        self.assert_invalid(ROOT_VALIDATOR, document)

    def test_optional_null_values_match_yaml_parser(self):
        entity = complete_entity()
        entity.update(domain=None, incremental_mode=None, state={"path": None})
        entity["source"].update(storage=None, filesystem="local", options={"header": None, "encoding": None})
        entity["sink"].update(write_mode="merge_scd2")
        entity["sink"]["accepted"].update(
            format="delta", storage=None, filesystem=None,
            options={"compression": None, "row_group_size": None, "max_size_per_file": None},
            merge={"ignore_columns": None, "compare_columns": None, "scd2": {
                "current_flag_column": None, "valid_from_column": None, "valid_to_column": None,
            }},
            partition_by=None,
        )
        entity["schema"].update(primary_key=["id"], unique_keys=None, normalize_columns={
            "enabled": None, "strategy": None,
        }, mismatch={"missing_columns": None, "extra_columns": None}, schema_evolution={
            "mode": None, "on_incompatible": None,
        })
        document = {"version": "0.3", "metadata": {"project": None, "tags": None},
                    "report": {"path": None, "formatter": None, "storage": None},
                    "lineage": {"url": "https://lineage.example", "namespace": "floe",
                                "endpoint": None, "timeout_secs": None},
                    "entities": [entity]}
        self.assert_valid(ROOT_VALIDATOR, document)
        self.assert_valid(validator_for("entity_file"), entity)
        entity["sink"]["accepted"]["merge"]["scd2"]["current_flag_column"] = " "
        self.assert_invalid(ROOT_VALIDATOR, document)

    def test_partition_names_and_transforms_match_runtime(self):
        entity = complete_entity()
        entity["sink"]["accepted"].update(format="iceberg", partition_spec=[{"column": "id", "transform": " YEAR "}])
        document = {"version": "0.3", "entities": [entity]}
        self.assert_valid(ROOT_VALIDATOR, document)
        self.assert_valid(validator_for("entity_file"), entity)
        entity["sink"]["accepted"]["partition_spec"][0]["transform"] = "bucket[16]"
        self.assert_invalid(ROOT_VALIDATOR, document)
        self.assert_invalid(validator_for("entity_file"), entity)
        for field, valid in (
            ({"column": "id", "transform": " Bucket ", "num_buckets": 16}, True),
            ({"column": "id", "transform": "bucket"}, False),
            ({"column": "id", "transform": "bucket", "num_buckets": 0}, False),
            ({"column": "id", "transform": "bucket", "num_buckets": 1.5}, False),
            ({"column": "id", "transform": "bucket", "num_buckets": "16"}, False),
            ({"column": "id", "transform": "day", "num_buckets": 16}, False),
            ({"column": "id", "num_buckets": 16}, False),
        ):
            with self.subTest(field=field):
                entity["sink"]["accepted"]["partition_spec"][0] = field
                check = self.assert_valid if valid else self.assert_invalid
                check(ROOT_VALIDATOR, document)
                check(validator_for("entity_file"), entity)
        entity["sink"]["accepted"]["partition_spec"][0] = {"column": " ", "transform": "year"}
        self.assert_invalid(ROOT_VALIDATOR, document)
        self.assert_invalid(validator_for("entity_file"), entity)
        entity["sink"]["accepted"].pop("partition_spec")
        entity["sink"]["accepted"].update(format="delta", partition_by=[" "])
        self.assert_invalid(ROOT_VALIDATOR, document)
        self.assert_invalid(validator_for("entity_file"), entity)

    def test_duplicate_list_entries_match_runtime(self):
        # Rust rejects repeated entries at validate time; uniqueItems only catches exact repeats
        # (Rust also trims, lowercases partition_spec transforms and defaults them to identity).
        merge = {"write_mode": "merge_scd1"}
        spec = {"column": "id", "transform": "day"}
        cases = {
            "S8": ("schema", {"primary_key": ["id", "id"]}, None),
            "S9": ("schema", {"unique_keys": [["id", "id"]]}, None),
            "S10": ("sink", {"format": "delta", "partition_by": ["id", "id"]}, None),
            "S11": ("sink", {"format": "delta", "merge": {"ignore_columns": ["amt", "amt"]}}, merge),
            "S12": ("sink", {"format": "delta", "merge": {"compare_columns": ["amt", "amt"]}}, merge),
            "S13": ("sink", {"format": "iceberg", "partition_spec": [spec, spec]}, None),
        }
        for case, (kind, fields, sink_extra) in cases.items():
            with self.subTest(case):
                entity = complete_entity()
                entity["schema"]["primary_key"] = ["id"]
                entity["schema"]["columns"].append({"name": "amt", "type": "number"})
                if kind == "schema":
                    entity["schema"].update(fields)
                else:
                    entity["sink"].update(sink_extra or {})
                    entity["sink"]["accepted"].update(fields)
                self.assert_invalid(ROOT_VALIDATOR, {"version": "0.3", "entities": [entity]})
                self.assert_invalid(validator_for("entity_file"), entity)
                if case != "S8":  # domain defaults cannot declare primary_key
                    defaults = {"schema": fields} if kind == "schema" else {"sink": {**(sink_extra or {}), "accepted": fields}}
                    self.assert_invalid(validator_for("domain_file"), {"name": "sales", "defaults": defaults})
                    self.assert_invalid(
                        ROOT_VALIDATOR,
                        {"version": "0.3", "domains": [{"name": "sales", "defaults": defaults}]},
                    )
        # duplicate groups are only a warning in Rust
        entity = complete_entity()
        entity["schema"]["unique_keys"] = [["id"], ["id"]]
        self.assert_valid(ROOT_VALIDATOR, {"version": "0.3", "entities": [entity]})
        self.assert_valid(validator_for("entity_file"), entity)

    def test_merge_column_entries_are_not_blank(self):
        entity = complete_entity()
        entity["sink"].update(write_mode="merge_scd1")
        entity["sink"]["accepted"].update(format="delta", merge={"ignore_columns": [" "]})
        entity["schema"]["primary_key"] = ["id"]
        document = {"version": "0.3", "entities": [entity]}
        self.assert_invalid(ROOT_VALIDATOR, document)
        self.assert_invalid(validator_for("entity_file"), entity)
        entity["sink"]["accepted"]["merge"] = {"compare_columns": ["\t"]}
        self.assert_invalid(ROOT_VALIDATOR, document)
        self.assert_invalid(validator_for("entity_file"), entity)

    def test_merge_mode_fragments_require_local_primary_key(self):
        entity = {
            "name": "orders", "domain": "sales",
            "sink": {"write_mode": "merge_scd1"},
            "schema": {"columns": [{"name": "id", "type": "string"}]},
        }
        self.assert_invalid(validator_for("entity_file"), entity)
        document = {"version": "0.3", "domains": [{"name": "sales", "defaults": {
            "source": {"format": "csv", "path": "in.csv"},
            "sink": {"accepted": {"format": "delta", "path": "out"}},
            "policy": {"severity": "warn"},
        }}], "entities": [entity]}
        self.assert_invalid(ROOT_VALIDATOR, document)
        entity["schema"]["primary_key"] = ["id"]
        self.assert_valid(validator_for("entity_file"), entity)
        self.assert_valid(ROOT_VALIDATOR, document)
        entity["schema"]["primary_key"] = None
        self.assert_invalid(validator_for("entity_file"), entity)
        self.assert_invalid(ROOT_VALIDATOR, document)
        entity["schema"]["primary_key"] = ["id"]
        self.assert_invalid(validator_for("domain_file"), {
            "name": "sales", "defaults": {"schema": {"primary_key": ["id"]}},
        })

    def test_empty_inline_entities_are_allowed_with_domain_includes(self):
        self.assert_valid(ROOT_VALIDATOR, {
            "version": "0.3", "entities": [], "include": {"domains": ["domains/*/_domain.yml"]},
        })
        self.assert_valid(ROOT_VALIDATOR, {
            "version": "0.3", "entities": [complete_entity()], "include": {"domains": []},
        })
        self.assert_invalid(ROOT_VALIDATOR, {
            "version": "0.3", "entities": [], "include": {"domains": []},
        })
        self.assert_invalid(ROOT_VALIDATOR, {"version": "0.3", "entities": []})

    def test_optional_lineage_values_allow_yaml_null(self):
        document = {"version": "0.3", "entities": [complete_entity()], "lineage": {
            "url": "https://lineage.example", "namespace": "floe", "endpoint": None, "api_key": None,
            "dataset_namespace": None, "producer": None, "job_name": None, "timeout_secs": None,
            "max_failures": None,
        }}
        self.assert_valid(ROOT_VALIDATOR, document)
        for field in ("url", "namespace"):
            document["lineage"][field] = None
            with self.subTest(field=field):
                self.assert_invalid(ROOT_VALIDATOR, document)
            document["lineage"][field] = "floe" if field == "namespace" else "https://lineage.example"

    def test_sink_options_are_constrained_only_for_parquet(self):
        entity = complete_entity()
        entity["sink"]["accepted"]["options"] = {
            "compression": "brotli", "row_group_size": 0, "max_size_per_file": 0,
        }
        document = {"version": "0.3", "entities": [entity]}
        entity["sink"]["accepted"]["format"] = "delta"
        self.assert_valid(ROOT_VALIDATOR, document)
        self.assert_valid(validator_for("entity_file"), entity)
        entity["sink"]["accepted"]["format"] = "parquet"
        self.assert_invalid(ROOT_VALIDATOR, document)
        self.assert_invalid(validator_for("entity_file"), entity)
        entity["sink"]["accepted"]["options"] = {
            "compression": None, "row_group_size": None, "max_size_per_file": None,
        }
        self.assert_valid(ROOT_VALIDATOR, document)
        self.assert_valid(validator_for("entity_file"), entity)
        for options in ({"compression": "brotli"}, {"row_group_size": 0}, {"max_size_per_file": 0}):
            entity["sink"]["accepted"]["options"] = options
            with self.subTest(options=options):
                self.assert_invalid(ROOT_VALIDATOR, document)

    def test_unsigned_yaml_integer_limits_match_parser(self):
        entity = complete_entity()
        document = {"version": "0.3", "entities": [entity]}
        entity["schema"]["columns"][0]["width"] = 9223372036854775807
        entity["source"]["options"] = {"header_row": 9223372036854775807, "data_row": 1}
        self.assert_valid(ROOT_VALIDATOR, document)
        entity["schema"]["columns"][0]["width"] += 1
        self.assert_invalid(ROOT_VALIDATOR, document)
        entity["schema"]["columns"][0].pop("width")
        entity["source"]["options"]["header_row"] = 9223372036854775808
        self.assert_invalid(ROOT_VALIDATOR, document)
        entity["source"]["options"] = {"header_row": 1, "data_row": 9223372036854775808}
        self.assert_invalid(ROOT_VALIDATOR, document)
        self.assert_valid(validator_for("domain_file"), {"name": "sales", "defaults": {
            "source": {"options": {"header_row": 9223372036854775807}},
        }})

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
        self.assertIn("crates/floe-core/src/config/parse.rs", filters["config_schema"])
        self.assertIn("crates/floe-core/src/config/validate.rs", filters["config_schema"])

    def test_include_paths_must_not_be_blank(self):
        for include in ({"sources": ""}, {"sources": "   "}, {"domains": [""]}, {"domains": ["  "]}):
            with self.subTest(include=include):
                self.assert_invalid(ROOT_VALIDATOR, {
                    "version": "0.3", "entities": [complete_entity()], "include": include,
                })

    def test_env_vars_require_yaml_string_keys(self):
        yaml_vars = yaml.safe_load("1: value\n")
        self.assertIs(type(next(iter(yaml_vars))), int)
        self.assert_invalid(ROOT_VALIDATOR, {
            "version": "0.3", "entities": [complete_entity()], "env": {"vars": yaml_vars},
        })
        self.assert_valid(ROOT_VALIDATOR, {
            "version": "0.3", "entities": [complete_entity()], "env": {"vars": {"API_TOKEN": "value"}},
        })

    def test_profile_rejects_what_rust_rejects(self):
        validator = profile_validator()

        def profile(**sections):
            return {"apiVersion": "floe/v1", "kind": "EnvironmentProfile", "metadata": {"name": "dev"}, **sections}

        def runner(**fields):
            return profile(execution={"runner": fields})

        k8s = {"type": "kubernetes_job", "image": "i", "namespace": "n"}
        databricks = {
            "type": "databricks_job",
            "workspace_url": "u",
            "existing_cluster_id": "c",
            "config_uri": "f",
            "python_file_uri": "p",
            "auth": {"service_principal_oauth_ref": "r"},
        }
        too_big = 9223372036854775808
        invalid = {
            "S27 missing image": runner(type="kubernetes_job", namespace="n"),
            "S27 missing namespace": runner(type="kubernetes_job", image="i"),
            "S27 blank image": runner(**{**k8s, "image": " "}),
            "S27 blank namespace": runner(**{**k8s, "namespace": " "}),
            "S28 only type": runner(type="databricks_job"),
            "S28 blank workspace_url": runner(**{**databricks, "workspace_url": " "}),
            "S28 blank existing_cluster_id": runner(**{**databricks, "existing_cluster_id": " "}),
            "S28 blank config_uri": runner(**{**databricks, "config_uri": " "}),
            "S28 blank python_file_uri": runner(**{**databricks, "python_file_uri": " "}),
            "S28 blank oauth ref": runner(**{**databricks, "auth": {"service_principal_oauth_ref": " "}}),
            "S28 missing auth": runner(**{k: v for k, v in databricks.items() if k != "auth"}),
            "S29": {**profile(), "metadata": {"name": " "}},
            "S30": profile(catalogs={"definitions": [{"name": " ", "type": "rest", "uri": "u"}]}),
            "S31 unclosed": profile(variables={"A": "${X"}),
            "S31 empty": profile(variables={"A": "${}"}),
            "S31 blank": profile(variables={"A": "a ${ } b"}),
            "S31 unclosed after valid": profile(variables={"A": "${B}/${X"}),
            "S32 max_concurrent_entities": profile(
                execution={"runner": {"type": "local"}, "orchestration": {"max_concurrent_entities": too_big}}
            ),
            **{
                f"S32 {field}": runner(type="local", **{field: too_big})
                for field in ("timeout_seconds", "ttl_seconds_after_finished", "poll_interval_seconds")
            },
            "S32 memory_mb": runner(type="local", resources={"memory_mb": too_big}),
        }
        for name, document in invalid.items():
            with self.subTest(name):
                self.assert_invalid(validator, document)

        valid = {
            "kubernetes_job": runner(**k8s),
            "databricks_job": runner(**databricks),
            "cross-reference": profile(variables={"B": "x", "A": "${B}/y", "C": "$${B} $ {"}),
            "i64 maximum": runner(type="local", timeout_seconds=9223372036854775807, resources={"memory_mb": 9223372036854775807}),
            "image on local runner": runner(type="local", image="x"),
        }
        for name, document in valid.items():
            with self.subTest(name):
                self.assert_valid(validator, document)
        for path in (ROOT / "example/profiles").glob("*.yml"):
            with self.subTest(path.name):
                self.assert_valid(validator, yaml.safe_load(path.read_text()))

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

    def test_profile_accepts_what_rust_accepts(self):
        schema = yaml.safe_load((ROOT / "schemas/profile.schema.yaml").read_text())
        Draft202012Validator.check_schema(schema)
        validator = YamlDraft202012Validator(schema)

        def profile(**extra):
            return {"apiVersion": "floe/v1", "kind": "EnvironmentProfile", "metadata": {"name": "n"}, **extra}

        def runner(**fields):
            return profile(execution={"runner": {"type": "local", **fields}})

        # Each case is accepted by `floe validate --profile` in Rust 0.7.0 (#584 S18-S26).
        cases = {
            "S18": profile(storages={"default": "loc", "definitions": [{"name": "loc", "type": "local"}]}),
            "S19": profile(lineage={"url": "http://lineage.example", "namespace": "floe"}),
            "S20": runner(env_parameters={"A": "b"}),
            "S21": runner(job_name="daily"),
            "S22 timeout_seconds": runner(timeout_seconds=0),
            "S22 poll_interval_seconds": runner(poll_interval_seconds=0),
            "S23": runner(resources={"memory_mb": 0}),
            "S24": runner(secrets=[{"name": "", "secret_name": "", "key": ""}]),
            "S25 image": runner(image=""),
            "S25 namespace": runner(namespace=""),
            "S26": profile(catalogs={"default": None, "definitions": [{"name": "r", "type": "rest", "uri": "u"}]}),
        }
        for name, document in cases.items():
            with self.subTest(id=name):
                self.assert_valid(validator, document)
        self.assert_invalid(validator, runner(env_parameters={"A": 1}))
        self.assert_invalid(validator, runner(job_name=1))
        self.assert_invalid(validator, runner(timeout_seconds=-1))
        self.assert_invalid(validator, runner(resources={"memory_mb": -1}))
        for name in ("storages", "lineage"):
            self.assertEqual(SCHEMA["properties"][name], schema["properties"][name])
            self.assertEqual(
                SCHEMA["$defs"][name], schema["$defs"][name],
                f"profile {name} drifted from the project contract",
            )
        self.assertEqual(SCHEMA["$defs"]["storage_definition"], schema["$defs"]["storage_definition"])
        for path in sorted((ROOT / "example/profiles").glob("*.yml")):
            self.assert_valid(validator, yaml.safe_load(path.read_text()))

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
