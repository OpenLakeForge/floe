import unittest
from pathlib import Path

import yaml
from jsonschema import Draft202012Validator


ROOT = Path(__file__).resolve().parents[1]
SCHEMA = yaml.safe_load((ROOT / "schemas/config.schema.yaml").read_text())
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
    return Draft202012Validator(
        {"$schema": SCHEMA["$schema"], "$ref": f"#/$defs/{branch}", "$defs": SCHEMA["$defs"]}
    )


class ConfigSchemaTest(unittest.TestCase):
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

    def test_existing_rust_model_sections(self):
        entity = complete_entity()
        entity["source"]["filesystem"] = "local"
        entity["sink"]["accepted"] = {
            "format": "duckdb",
            "duckdb": {"table": "orders", "connection": "md:lake"},
        }
        document = {
            "version": "0.3", "entities": [entity],
            "storages": {"definitions": [{"name": "local", "type": "local"}]},
            "catalogs": {"definitions": []},
            "lineage": {"url": "https://lineage.example", "namespace": "floe"},
        }
        self.assert_valid(ROOT_VALIDATOR, document)
        entity["source"]["storage"] = "local"
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
