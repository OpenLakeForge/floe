import json
from pathlib import Path
import subprocess
from unittest.mock import patch

import pytest

from dagster import AssetKey, AssetOut, Definitions, multi_asset

from floe_dagster.assets import (
    _count_files_with_rejections,
    _dominant_rejection_reason,
    build_floe_asset_defs,
    load_floe_assets,
)
from floe_dagster.definitions import build_definitions, build_definitions_from_manifest_paths
from floe_dagster.manifest import ManifestRunnerDefinition
from floe_dagster.runner import LocalRunner, RunResult, Runner


class _NoopRunner(Runner):
    def run_floe_entity(
        self,
        config_uri: str,
        run_id: str | None,
        entity: str,
        log_format: str = "json",
        execution=None,
        runner_definition=None,
        manifest_uri: str | None = None,
        dagster_job_name: str | None = None,
    ) -> RunResult:
        del config_uri, run_id, entity, log_format, execution, runner_definition, manifest_uri, dagster_job_name
        return RunResult(stdout="", stderr="", exit_code=0)


FIXTURE = Path(__file__).parent / "fixtures" / "manifest.json"


# ── load-time: manifest parsing ───────────────────────────────────────────────

def test_no_runner_manifest_loads_and_defaults_to_local() -> None:
    """Entity with no explicit runner → falls back to manifest default (local_process)."""
    defs = load_floe_assets(
        manifest_path=str(FIXTURE),
        runner=_NoopRunner(),
        entities=["employees"],  # employees.runner == null → default "local" (local_process)
    )
    assert defs is not None


def test_local_runner_manifest_loads_successfully() -> None:
    """Entity explicitly assigned the local_process runner loads without error."""
    defs = load_floe_assets(
        manifest_path=str(FIXTURE),
        runner=_NoopRunner(),
        entities=["employees"],
    )
    assert defs is not None


def test_kubernetes_runner_manifest_loads_successfully() -> None:
    """Entity with a non-local runner should load — error is deferred to execution time."""
    defs = load_floe_assets(
        manifest_path=str(FIXTURE),
        runner=_NoopRunner(),
        entities=["orders"],  # orders.runner == "k8s" (kubernetes_job)
    )
    assert defs is not None


# ── execution-time: LocalRunner routing ───────────────────────────────────────

def test_local_runner_accepts_local_process_definition() -> None:
    """LocalRunner.run_floe_entity proceeds when runner_definition.runner_type is local_process."""
    runner = LocalRunner(floe_bin="floe")
    definition = ManifestRunnerDefinition(
        runner_type="local_process",
        image=None,
        namespace=None,
        service_account=None,
        env=None,
        command=None,
        args=None,
        timeout_seconds=None,
        ttl_seconds_after_finished=None,
        poll_interval_seconds=None,
        secrets=None,
    )
    fake_result = subprocess.CompletedProcess(
        args=[], returncode=0, stdout="", stderr=""
    )
    with patch("subprocess.run", return_value=fake_result):
        result = runner.run_floe_entity(
            config_uri="local:///tmp/config.yml",
            run_id=None,
            entity="employees",
            runner_definition=definition,
        )
    assert result.exit_code == 0


def test_local_runner_raises_not_implemented_for_unknown_runner() -> None:
    """LocalRunner raises NotImplementedError for non-local_process runner types."""
    runner = LocalRunner(floe_bin="floe")
    definition = ManifestRunnerDefinition(
        runner_type="docker",
        image=None,
        namespace=None,
        service_account=None,
        env=None,
        command=None,
        args=None,
        timeout_seconds=None,
        ttl_seconds_after_finished=None,
        poll_interval_seconds=None,
        secrets=None,
    )
    with pytest.raises(NotImplementedError) as exc_info:
        runner.run_floe_entity(
            config_uri="local:///tmp/config.yml",
            run_id=None,
            entity="orders",
            runner_definition=definition,
        )
    assert "docker" in str(exc_info.value).lower()


# ── multi-asset structure ──────────────────────────────────────────────────────

def _get_asset_def(manifest_path, entity_name):
    asset_defs, _source_assets, entities = build_floe_asset_defs(
        manifest_path=str(manifest_path),
        runner=_NoopRunner(),
        entities=[entity_name],
    )
    return asset_defs[0], entities[0]


def test_multi_asset_accepted_key_matches_entity_asset_key() -> None:
    """Accepted output key == entity.asset_key (backward compatibility)."""
    asset_def, entity = _get_asset_def(FIXTURE, "employees")
    assert AssetKey(entity.asset_key) in asset_def.keys


def test_multi_asset_rejected_key_present_when_policy_is_reject() -> None:
    """employees has policy_severity='reject' → rejected output exposed."""
    asset_def, entity = _get_asset_def(FIXTURE, "employees")
    rejected_key = AssetKey([*entity.asset_key[:-1], "rejected", entity.asset_key[-1]])
    assert rejected_key in asset_def.keys


def test_rejected_key_is_a_segment_between_domain_and_table() -> None:
    """[sales, orders] → [sales, rejected, orders]."""
    from floe_dagster.assets import _rejected_key

    _, entity = _get_asset_def(FIXTURE, "orders")
    assert entity.asset_key == ["sales", "orders"]
    assert _rejected_key(entity) == ["sales", "rejected", "orders"]


def test_table_named_like_the_old_rejected_suffix_does_not_collide(tmp_path) -> None:
    """A real `employees_rejected` table next to `employees` (which has a rejected sink)."""
    import copy
    import json

    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
    employees = payload["entities"][0]
    sibling = copy.deepcopy(employees)
    sibling["name"] = "employees_rejected"
    sibling["asset_key"] = ["hr", "employees_rejected"]
    payload["entities"].append(sibling)
    manifest_path = tmp_path / "manifest.sibling.json"
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")

    defs = build_definitions(manifest_path=str(manifest_path), runner=_NoopRunner(), with_job=False)
    defs.resolve_implicit_global_asset_job_def()  # raises on a duplicate asset key
    keys = {key for asset in defs.assets if hasattr(asset, "keys") for key in asset.keys}
    assert AssetKey(["hr", "employees_rejected"]) in keys
    assert AssetKey(["hr", "rejected", "employees"]) in keys
    assert AssetKey(["hr", "rejected", "employees_rejected"]) in keys


def test_multi_asset_outputs_carry_entity_and_output_metadata() -> None:
    asset_def, entity = _get_asset_def(FIXTURE, "employees")
    by_output = {spec.metadata["floe/output"]: spec for spec in asset_def.specs}
    assert set(by_output) == {"accepted", "rejected"}
    assert all(spec.metadata["floe/entity"] == entity.name for spec in by_output.values())
    assert by_output["accepted"].key == AssetKey(entity.asset_key)
    assert by_output["rejected"].key == AssetKey([*entity.asset_key[:-1], "rejected", entity.asset_key[-1]])


def test_multi_asset_no_rejected_key_when_policy_is_warn() -> None:
    """orders has policy_severity='warn' → no rejected output (rows discarded silently)."""
    asset_def, entity = _get_asset_def(FIXTURE, "orders")
    rejected_key = AssetKey([*entity.asset_key[:-1], "rejected", entity.asset_key[-1]])
    assert rejected_key not in asset_def.keys


def test_multi_asset_rejected_key_present_when_policy_is_abort(tmp_path) -> None:
    """policy_severity='abort' → rejected output exposed (same as reject)."""
    import json

    fixture = Path(__file__).parent / "fixtures" / "manifest.json"
    payload = json.loads(fixture.read_text(encoding="utf-8"))
    # Patch employees to use abort severity
    payload["entities"][0]["policy_severity"] = "abort"
    manifest_path = tmp_path / "manifest.abort.json"
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")

    asset_def, entity = _get_asset_def(manifest_path, "employees")
    rejected_key = AssetKey([*entity.asset_key[:-1], "rejected", entity.asset_key[-1]])
    assert rejected_key in asset_def.keys


def test_multi_asset_source_dep_declared() -> None:
    """The _source dep key is declared as an upstream dependency."""
    asset_def, entity = _get_asset_def(FIXTURE, "employees")
    source_key = AssetKey(entity.asset_key[:-1] + [entity.asset_key[-1] + "_source"])
    dep_keys = set(asset_def.keys_by_input_name.values())
    assert source_key in dep_keys


# ── SourceAsset registration ───────────────────────────────────────────────────

def test_build_floe_asset_defs_returns_source_assets() -> None:
    """build_floe_asset_defs returns a non-empty list of SourceAsset objects."""
    _asset_defs, source_assets, _entities = build_floe_asset_defs(
        manifest_path=str(FIXTURE),
        runner=_NoopRunner(),
        entities=["employees"],
    )
    assert len(source_assets) == 1
    assert source_assets[0].key.path[-1].endswith("_source")


def test_source_asset_key_matches_entity_source_dep() -> None:
    """The SourceAsset key matches the dep declared in the multi-asset."""
    _asset_defs, source_assets, entities = build_floe_asset_defs(
        manifest_path=str(FIXTURE),
        runner=_NoopRunner(),
        entities=["employees"],
    )
    entity = entities[0]
    expected_key = AssetKey(entity.asset_key[:-1] + [entity.asset_key[-1] + "_source"])
    assert source_assets[0].key == expected_key


def test_source_asset_has_format_metadata() -> None:
    """SourceAsset metadata includes the source format."""
    _asset_defs, source_assets, entities = build_floe_asset_defs(
        manifest_path=str(FIXTURE),
        runner=_NoopRunner(),
        entities=["employees"],
    )
    entity = entities[0]
    metadata = source_assets[0].metadata
    assert "format" in metadata
    assert metadata["format"].text == entity.source_format


def test_source_asset_has_uri_metadata_when_source_uri_set() -> None:
    """SourceAsset metadata includes dagster/uri when entity.source_uri is set."""
    _asset_defs, source_assets, entities = build_floe_asset_defs(
        manifest_path=str(FIXTURE),
        runner=_NoopRunner(),
        entities=["employees"],
    )
    entity = entities[0]
    if entity.source_uri:
        assert "dagster/uri" in source_assets[0].metadata


def test_source_asset_group_matches_entity_group() -> None:
    """SourceAsset group_name matches entity.group_name."""
    _asset_defs, source_assets, entities = build_floe_asset_defs(
        manifest_path=str(FIXTURE),
        runner=_NoopRunner(),
        entities=["employees"],
    )
    assert source_assets[0].group_name == entities[0].group_name


def _crm_manifest(tmp_path, project: str, uri: str | None) -> Path:
    """The fixture with every entity reading the named source `crm`, as project `project`."""
    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
    payload["manifest_name"] = project
    payload["manifest_id"] = f"mfv1-{project}"
    for entity in payload["entities"]:
        entity["source"]["ref"] = "crm"
        entity["asset_key"] = [project, *entity["asset_key"]]
    payload["sources"] = [
        {"name": "crm", "format": "csv", "uri": uri, "entities": ["employees", "orders"]}
    ]
    manifest_path = tmp_path / f"{project}.manifest.json"
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")
    return manifest_path


def test_entities_sharing_a_named_source_depend_on_one_source_asset(tmp_path) -> None:
    """D20: entities with the same source.ref share the asset of that `sources` entry."""
    manifest_path = _crm_manifest(tmp_path, "sales", "s3://b/crm")

    asset_defs, source_assets, _entities = build_floe_asset_defs(
        manifest_path=str(manifest_path), runner=_NoopRunner()
    )
    shared = AssetKey(["sources", "sales", "crm"])
    assert [sa.key for sa in source_assets] == [shared]
    assert source_assets[0].metadata["dagster/uri"].url == "s3://b/crm"
    for asset_def in asset_defs:
        assert shared in set(asset_def.keys_by_input_name.values())


def test_source_key_fn_gives_entities_of_one_named_source_their_own_upstream(tmp_path) -> None:
    """The key from `source_key_fn` replaces the shared named-source key, and stays an op input."""
    manifest_path = _crm_manifest(tmp_path, "sales", "s3://b/crm")

    asset_defs, source_assets, _entities = build_floe_asset_defs(
        manifest_path=str(manifest_path),
        runner=_NoopRunner(),
        source_key_fn=lambda entity: AssetKey(["crm", entity.name]),
    )
    inputs = {a.op.name.split("__")[-1]: set(a.keys_by_input_name.values()) for a in asset_defs}
    assert inputs == {
        "employees": {AssetKey(["crm", "employees"])},
        "orders": {AssetKey(["crm", "orders"])},
    }
    assert source_assets == []  # the caller owns the keys it chose


def test_source_key_fn_returning_none_keeps_the_default_key(tmp_path) -> None:
    manifest_path = _crm_manifest(tmp_path, "sales", "s3://b/crm")

    asset_defs, source_assets, _entities = build_floe_asset_defs(
        manifest_path=str(manifest_path),
        runner=_NoopRunner(),
        source_key_fn=lambda entity: AssetKey(["crm", "orders"]) if entity.name == "orders" else None,
    )
    inputs = {a.op.name.split("__")[-1]: set(a.keys_by_input_name.values()) for a in asset_defs}
    shared = AssetKey(["sources", "sales", "crm"])
    assert inputs == {"employees": {shared}, "orders": {AssetKey(["crm", "orders"])}}
    assert [sa.key for sa in source_assets] == [shared]


def test_source_key_fn_key_equal_to_a_default_key_is_not_generated_for_other_entities(tmp_path) -> None:
    """A key the callback returned is the caller's, even when another entity's default key is the same."""
    manifest_path = _crm_manifest(tmp_path, "sales", "s3://b/crm")
    shared = AssetKey(["sources", "sales", "crm"])
    own = multi_asset(name="crm_source", outs={"out": AssetOut(key=shared)})(lambda: None)

    asset_defs, source_assets, _entities = build_floe_asset_defs(
        manifest_path=str(manifest_path),
        runner=_NoopRunner(),
        source_key_fn=lambda entity: shared if entity.name == "orders" else None,
    )
    assert source_assets == []
    Definitions(assets=[*asset_defs, *source_assets, own])  # no duplicate asset key


def test_build_definitions_orders_each_entity_after_its_own_upstream(tmp_path) -> None:
    """`build_definitions` threads `source_key_fn`; the job runs each entity after its own upstream asset."""
    manifest_path = _crm_manifest(tmp_path, "sales", "s3://b/crm")
    upstream = [
        multi_asset(name=f"crm_{n}", outs={"out": AssetOut(key=AssetKey(["crm", n]))})(lambda: None)
        for n in ("employees", "orders")
    ]
    defs = Definitions.merge(
        build_definitions(
            manifest_path=str(manifest_path),
            runner=_NoopRunner(),
            with_job=False,
            source_key_fn=lambda entity: AssetKey(["crm", entity.name]),
        ),
        Definitions(assets=upstream),
    )
    job = defs.resolve_implicit_global_asset_job_def()
    for op, name in (("sales__hr__employees", "employees"), ("sales__sales__orders", "orders")):
        parents = {out.node_name for out in job.dependency_structure.all_upstream_outputs_from_node(op)}
        assert parents == {f"crm_{name}"}


def test_named_source_without_a_shared_location_has_no_uri_metadata(tmp_path) -> None:
    """D20: `sources[].uri` is null when the consumers share no location."""
    manifest_path = _crm_manifest(tmp_path, "sales", None)
    _asset_defs, source_assets, _entities = build_floe_asset_defs(
        manifest_path=str(manifest_path), runner=_NoopRunner()
    )
    assert "dagster/uri" not in source_assets[0].metadata


def test_two_projects_naming_the_same_source_get_two_source_assets(tmp_path) -> None:
    """D20: the shared source key is scoped by project, so two manifests can both name `crm`."""
    paths = [
        str(_crm_manifest(tmp_path, "sales", "s3://b/sales/crm")),
        str(_crm_manifest(tmp_path, "hr", "s3://b/hr/crm")),
    ]
    defs = build_definitions_from_manifest_paths(manifest_paths=paths, runner=_NoopRunner())
    graph = defs.get_repository_def().asset_graph
    for project in ("sales", "hr"):
        assert AssetKey(["sources", project, "crm"]) in graph.get_all_asset_keys()


def test_register_source_assets_false_returns_empty_source_list() -> None:
    """register_source_assets=False skips SourceAsset creation (opt-out for upstream pipelines)."""
    _asset_defs, source_assets, _entities = build_floe_asset_defs(
        manifest_path=str(FIXTURE),
        runner=_NoopRunner(),
        entities=["employees"],
        register_source_assets=False,
    )
    assert source_assets == []


def test_load_floe_assets_register_source_assets_false() -> None:
    """load_floe_assets passes register_source_assets=False through to build_floe_asset_defs."""
    defs = load_floe_assets(
        manifest_path=str(FIXTURE),
        runner=_NoopRunner(),
        entities=["employees"],
        register_source_assets=False,
    )
    assert defs is not None


# ── rejected metadata helpers ─────────────────────────────────────────────────

def test_count_files_with_rejections_empty_report() -> None:
    assert _count_files_with_rejections({}) == 0


def test_count_files_with_rejections_no_rejections() -> None:
    report = {"files": [{"rejected_count": 0}, {"rejected_count": 0}]}
    assert _count_files_with_rejections(report) == 0


def test_count_files_with_rejections_partial() -> None:
    report = {"files": [{"rejected_count": 3}, {"rejected_count": 0}, {"rejected_count": 1}]}
    assert _count_files_with_rejections(report) == 2


def test_dominant_rejection_reason_empty_report() -> None:
    assert _dominant_rejection_reason({}) is None


def test_dominant_rejection_reason_no_violations() -> None:
    report = {
        "files": [{"validation": {"rules": [{"rule": "not_null", "violations": 0}]}}]
    }
    assert _dominant_rejection_reason(report) is None


def test_dominant_rejection_reason_single_rule() -> None:
    report = {
        "files": [{"validation": {"rules": [{"rule": "cast_error", "violations": 5}]}}]
    }
    assert _dominant_rejection_reason(report) == "cast_error"


def test_dominant_rejection_reason_picks_highest_count() -> None:
    report = {
        "files": [
            {"validation": {"rules": [
                {"rule": "not_null", "violations": 2},
                {"rule": "cast_error", "violations": 7},
            ]}},
            {"validation": {"rules": [
                {"rule": "not_null", "violations": 3},
            ]}},
        ]
    }
    assert _dominant_rejection_reason(report) == "cast_error"


# ── defensive: malformed report payloads ─────────────────────────────────────

def test_count_files_with_rejections_files_is_null() -> None:
    assert _count_files_with_rejections({"files": None}) == 0


def test_count_files_with_rejections_files_is_not_list() -> None:
    assert _count_files_with_rejections({"files": "bad"}) == 0


def test_count_files_with_rejections_non_dict_element() -> None:
    report = {"files": [None, {"rejected_count": 2}, "oops"]}
    assert _count_files_with_rejections(report) == 1


def test_dominant_rejection_reason_files_is_null() -> None:
    assert _dominant_rejection_reason({"files": None}) is None


def test_dominant_rejection_reason_files_is_not_list() -> None:
    assert _dominant_rejection_reason({"files": 42}) is None


def test_dominant_rejection_reason_non_dict_file_element() -> None:
    report = {"files": [None, {"validation": {"rules": [{"rule": "cast_error", "violations": 3}]}}]}
    assert _dominant_rejection_reason(report) == "cast_error"


def test_dominant_rejection_reason_validation_is_null() -> None:
    report = {"files": [{"validation": None}]}
    assert _dominant_rejection_reason(report) is None


def test_dominant_rejection_reason_rules_is_null() -> None:
    report = {"files": [{"validation": {"rules": None}}]}
    assert _dominant_rejection_reason(report) is None


def test_dominant_rejection_reason_non_dict_rule_element() -> None:
    report = {"files": [{"validation": {"rules": [None, {"rule": "not_null", "violations": 1}]}}]}
    assert _dominant_rejection_reason(report) == "not_null"


# ── schema-drift / type-coercion edge cases ───────────────────────────────────

def test_count_files_with_rejections_string_count() -> None:
    """rejected_count as string (schema drift) is coerced to int."""
    report = {"files": [{"rejected_count": "2"}, {"rejected_count": "0"}]}
    assert _count_files_with_rejections(report) == 1


def test_dominant_rejection_reason_list_rule_name() -> None:
    """Non-string rule name is skipped without raising TypeError."""
    report = {"files": [{"validation": {"rules": [{"rule": ["cast_error"], "violations": 1}]}}]}
    assert _dominant_rejection_reason(report) is None


def test_build_floe_asset_defs_raises_on_intra_manifest_source_key_collision(tmp_path) -> None:
    """build_floe_asset_defs raises ValueError when a generated source key matches an entity key."""
    import json

    fixture = Path(__file__).parent / "fixtures" / "manifest.json"
    payload = json.loads(fixture.read_text(encoding="utf-8"))
    employees = payload["entities"][0]
    payload["entities"].append({
        **employees,
        "name": "employees_source",
        "asset_key": employees["asset_key"][:-1] + [employees["asset_key"][-1] + "_source"],
    })
    manifest_path = tmp_path / "manifest.collision.json"
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="duplicate asset key"):
        build_floe_asset_defs(manifest_path=str(manifest_path), runner=_NoopRunner())


# ── domains filter and op names ────────────────────────────────────────────────

def _two_domain_manifest(tmp_path) -> Path:
    """sales.accounts and finance.accounts share an entity name across domains."""
    import json

    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
    employees = payload["entities"][0]
    payload["entities"] = [
        {**employees, "name": f"{domain}.accounts", "domain": domain,
         "group_name": domain, "asset_key": [domain, "accounts"]}
        for domain in ("sales", "finance")
    ]
    manifest_path = tmp_path / "manifest.domains.json"
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")
    return manifest_path


def test_same_entity_name_in_two_domains_loads(tmp_path) -> None:
    manifest_path = str(_two_domain_manifest(tmp_path))
    # Building the job is where Dagster rejects duplicate op names.
    load_floe_assets(manifest_path=manifest_path, runner=_NoopRunner()).resolve_implicit_global_asset_job_def()
    asset_defs, _source_assets, _entities = build_floe_asset_defs(
        manifest_path=manifest_path, runner=_NoopRunner()
    )
    assert sorted(a.node_def.name for a in asset_defs) == ["finance__accounts", "sales__accounts"]


def test_domains_filter_keeps_only_requested_domain(tmp_path) -> None:
    asset_defs, _source_assets, entities = build_floe_asset_defs(
        manifest_path=str(_two_domain_manifest(tmp_path)), runner=_NoopRunner(), domains=["sales"]
    )
    assert [e.name for e in entities] == ["sales.accounts"]
    assert AssetKey(["sales", "accounts"]) in asset_defs[0].keys


def test_domains_filter_unknown_domain_raises(tmp_path) -> None:
    with pytest.raises(ValueError, match="unknown domain"):
        load_floe_assets(
            manifest_path=str(_two_domain_manifest(tmp_path)), runner=_NoopRunner(), domains=["hr"]
        )


def test_op_name_unchanged_without_domain(tmp_path) -> None:
    import json

    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
    payload["entities"][0].update(domain=None, group_name="default", asset_key=["default", "employees"])
    manifest_path = tmp_path / "manifest.nodomain.json"
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")
    asset_def, _entity = _get_asset_def(manifest_path, "employees")
    assert asset_def.node_def.name == "employees"


def test_qualified_op_name_colliding_with_domainless_name_raises(tmp_path) -> None:
    import json

    payload = json.loads(_two_domain_manifest(tmp_path).read_text(encoding="utf-8"))
    payload["entities"][1].update(
        name="sales__accounts", domain=None, group_name="default", asset_key=["default", "sales__accounts"]
    )
    manifest_path = tmp_path / "manifest.opcollision.json"
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="'sales.accounts' and 'sales__accounts'"):
        build_floe_asset_defs(manifest_path=str(manifest_path), runner=_NoopRunner())


# ── floe/table concurrency tag (#924) ──────────────────────────────────────────

def _iceberg_manifest(tmp_path, entities, catalogs=None):
    data = json.loads(FIXTURE.read_text())
    template = data["entities"][0]
    data["catalogs"] = catalogs
    data["entities"] = []
    for name, domain, iceberg in entities:
        entity = json.loads(json.dumps(template))
        entity.update(name=name, domain=domain, asset_key=[domain or "default", name.split(".")[-1]])
        entity["sinks"]["accepted"].update(format="iceberg", iceberg=iceberg)
        data["entities"].append(entity)
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(data))
    return path


def _table_tags(path):
    asset_defs, _sources, _entities = build_floe_asset_defs(manifest_path=str(path), runner=_NoopRunner())
    return {a.op.name.split("__")[-1]: a.op.tags.get("floe/table") for a in asset_defs}


def test_entities_on_one_table_share_a_table_tag_and_other_tables_do_not(tmp_path) -> None:
    target = {"catalog": "rest", "namespace": "sales", "table": "orders"}
    path = _iceberg_manifest(
        tmp_path,
        [
            ("orders_eu", "sales", target),
            ("orders_us", "sales", target),
            ("returns", "sales", {**target, "table": "returns"}),
        ],
    )
    tags = _table_tags(path)
    assert tags["orders_eu"] == tags["orders_us"] == "rest.sales.orders"
    assert tags["returns"] == "rest.sales.returns"


def test_table_tag_resolves_defaults_like_a_run(tmp_path) -> None:
    path = _iceberg_manifest(
        tmp_path,
        [("Orders", "sales", {}), ("explicit", None, {"catalog": "other"})],
        catalogs={"default": "rest"},
    )
    tags = _table_tags(path)
    assert tags["Orders"] == "rest.sales.orders"
    assert tags["explicit"] == "other.default.explicit"


def test_table_tag_default_table_is_the_unqualified_entity_name(tmp_path) -> None:
    path = _iceberg_manifest(
        tmp_path,
        [("sales.orders", "sales", {}), ("explicit", "sales", {"table": "orders"})],
        catalogs={"default": "rest"},
    )
    assert set(_table_tags(path).values()) == {"rest.sales.orders"}


def test_glue_table_tag_uses_the_database_the_run_writes(tmp_path) -> None:
    glue = {"name": "glue", "type_config": {"type": "glue", "region": "eu-west-1", "database": "sales_silver"}}
    path = _iceberg_manifest(
        tmp_path,
        [("a", "sales", {"namespace": "sales", "table": "orders"}), ("b", "other", {"namespace": "other", "table": "orders"})],
        catalogs={"default": "glue", "definitions": [glue]},
    )
    assert set(_table_tags(path).values()) == {"glue.sales_silver.orders"}


def test_non_iceberg_entity_has_no_table_tag() -> None:
    asset_def, _entity = _get_asset_def(FIXTURE, "employees")
    assert "floe/table" not in asset_def.op.tags
