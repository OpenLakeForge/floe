"""Ported from `crates/floe-python/tests/test_floe.py`: the public `floe` API (`_floe.pyi`)."""

import shutil
from pathlib import Path

import pytest

import floe
from floe.state import EntityState, FileState, dump_state
from floe.storage.s3 import S3Client

FIXTURES = Path(__file__).parent / "fixtures" / "public_api"
VALID_CONFIG = "config.yml"
INVALID_CONFIG = "invalid_config.yml"
PROFILE_CONFIG = "profile.yml"


def _vars(tmp_path: Path, suffix: str = "") -> dict[str, str]:
    """As the Rust suite; the config's `env.vars` take precedence, as in Rust 0.7."""
    return {
        "out_root": str(tmp_path / f"out{suffix}"),
        "report_root": str(tmp_path / f"report{suffix}"),
        "incoming_root": str(tmp_path / "in"),
    }


@pytest.fixture(autouse=True)
def fixture_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Runs write next to the config, so each test gets its own copy (the Rust suite wrote into its fixtures)."""
    shutil.copytree(FIXTURES, tmp_path, dirs_exist_ok=True)
    monkeypatch.chdir(tmp_path)
    floe.clear_observer()


def test_import() -> None:
    for name in (
        "validate", "run", "load_config", "extract_config_env_vars",
        "inspect_entity_state", "reset_entity_state", "set_observer", "clear_observer",
    ):  # fmt: skip
        assert callable(getattr(floe, name))
    assert issubclass(floe.FloeError, Exception)
    assert issubclass(floe.FloeConfigError, floe.FloeError)
    assert issubclass(floe.FloeRunError, floe.FloeError)
    assert issubclass(floe.FloeStorageError, floe.FloeError)
    assert issubclass(floe.FloeIoError, floe.FloeError)
    assert not hasattr(floe, "config_targets_duckdb")  # D1


def test_validate_valid_config() -> None:
    floe.validate(VALID_CONFIG)


def test_validate_with_entity_filter() -> None:
    floe.validate(VALID_CONFIG, entities=["customer"])


def test_validate_unknown_entity_raises() -> None:
    with pytest.raises(floe.FloeConfigError, match="entities not found: nope"):
        floe.validate(VALID_CONFIG, entities=["nope"])


def test_validate_invalid_config_raises() -> None:
    with pytest.raises(floe.FloeConfigError):
        floe.validate(INVALID_CONFIG)


def test_validate_missing_file_raises() -> None:
    with pytest.raises(floe.FloeError):
        floe.validate("/nonexistent/path/config.yml")


def test_load_config_returns_root_config() -> None:
    cfg = floe.load_config(VALID_CONFIG)
    assert isinstance(cfg, floe.RootConfig)
    assert cfg.version == "0.1"
    assert "customer" in cfg.entity_names


def test_load_config_entities() -> None:
    customer = next(e for e in floe.load_config(VALID_CONFIG).entities if e.name == "customer")  # fmt: skip
    assert isinstance(customer, floe.EntityConfig)
    assert customer.source_format == "csv"
    assert customer.source_path == "./in/customer"
    assert customer.source_storage is None
    assert customer.accepted_format == "parquet"
    assert customer.accepted_path == "./out/accepted/customer/"
    assert customer.incremental_mode == "none"
    assert repr(customer) == 'EntityConfig(name="customer", source_format="csv", accepted_format="parquet")'  # fmt: skip


def test_root_config_repr() -> None:
    assert (
        repr(floe.load_config(VALID_CONFIG)) == 'RootConfig(version="0.1", entities=1)'
    )


def test_run_returns_outcome(tmp_path: Path) -> None:
    outcome = floe.run(VALID_CONFIG, profile_vars=_vars(tmp_path))
    assert isinstance(outcome, floe.RunOutcome)
    assert outcome.run_id
    assert not outcome.dry_run
    assert outcome.dry_run_previews is None
    assert outcome.report_base_path == str(tmp_path / "report")
    assert outcome.summary["results"]["accepted_total"] == 5
    assert (tmp_path / "out" / "accepted" / "customer").is_dir()


def test_run_entity_reports(tmp_path: Path) -> None:
    outcome = floe.run(
        VALID_CONFIG, entities=["customer"], profile_vars=_vars(tmp_path)
    )
    [report] = outcome.entity_reports
    assert "results" in report
    assert report["entity"]["name"] == "customer"


def test_dry_run(tmp_path: Path) -> None:
    outcome = floe.run(VALID_CONFIG, dry_run=True, profile_vars=_vars(tmp_path))
    assert outcome.dry_run is True
    assert outcome.dry_run_previews is not None
    [preview] = outcome.dry_run_previews
    assert preview["name"] == "customer"
    assert preview["input_format"] == "csv"
    assert preview["rejected_format"] == "csv"
    assert preview["archive_path"] == ""
    assert preview["report_file"] == f"{tmp_path}/report/run_{outcome.run_id}/customer/run.json"  # fmt: skip
    assert [Path(f).name for f in preview["scanned_files"]] == ["customers_valid.csv"]
    assert outcome.summary["report"]["report_file"] == "disabled (dry-run)"
    assert not (tmp_path / "out").exists()


def test_run_outcome_to_dict(tmp_path: Path) -> None:
    outcome = floe.run(VALID_CONFIG, dry_run=True, profile_vars=_vars(tmp_path))
    d = outcome.to_dict()
    assert list(d) == ["run_id", "report_base_path", "dry_run", "summary", "entity_reports", "dry_run_previews"]  # fmt: skip
    assert d["run_id"] == outcome.run_id
    assert d["entity_reports"] == []


def test_observer_receives_events(tmp_path: Path) -> None:
    events: list[floe.RunEvent] = []
    assert floe.set_observer(events.append) is True
    floe.run(VALID_CONFIG, profile_vars=_vars(tmp_path))
    event_types = {e["event"] for e in events}
    assert {"run_started", "entity_finished", "run_finished"} <= event_types


def test_observer_callback_is_swappable(tmp_path: Path) -> None:
    first: list[str] = []
    second: list[str] = []
    floe.set_observer(lambda e: first.append(e["event"]))
    floe.run(VALID_CONFIG, profile_vars=_vars(tmp_path))
    assert "run_finished" in first
    floe.set_observer(lambda e: second.append(e["event"]))
    floe.run(VALID_CONFIG, profile_vars=_vars(tmp_path, "2"))
    assert "run_finished" in second
    assert first.count("run_finished") == 1


def test_cleared_observer_receives_nothing(tmp_path: Path) -> None:
    events: list[floe.RunEvent] = []
    floe.set_observer(events.append)
    floe.clear_observer()
    floe.run(VALID_CONFIG, profile_vars=_vars(tmp_path))
    assert events == []


def test_set_observer_rejects_non_callable() -> None:
    with pytest.raises(TypeError, match="observer callback must be callable"):
        floe.set_observer(42)  # pyright: ignore[reportArgumentType]  # the runtime check under test


def test_extract_config_env_vars() -> None:
    assert floe.extract_config_env_vars(VALID_CONFIG) == {
        "report_root": "./report",
        "out_root": "./out",
        "incoming_root": "./in",
    }


def test_inspect_entity_state_no_state() -> None:
    result = floe.inspect_entity_state(VALID_CONFIG, "customer")
    assert result["entity_name"] == "customer"
    assert result["incremental_mode"] == "none"
    assert result["path_uri"].endswith("/in/customer/.floe/state/customer/state.json")
    assert result["path_local"] == result["path_uri"].removeprefix("local://")
    assert result["state"] is None


def _incremental_config(tmp_path: Path) -> tuple[str, Path]:
    config = tmp_path / "floe.yml"
    config.write_text(
        f"""version: "0.1"
entities:
  - name: "sales"
    incremental_mode: "file"
    source: {{format: "csv", path: "{tmp_path / "incoming"}"}}
    sink: {{accepted: {{format: "parquet", path: "{tmp_path / "out"}"}}}}
    policy: {{severity: "warn"}}
    schema: {{columns: [{{name: "id", type: "string"}}]}}
""",
        encoding="utf-8",
    )
    (tmp_path / "incoming").mkdir()
    return str(config), tmp_path / "incoming/.floe/state/sales/state.json"


def test_inspect_and_reset_entity_state(tmp_path: Path) -> None:
    config, state_file = _incremental_config(tmp_path)
    state = EntityState("sales", "2026-04-22T09:00:00Z", {"local:///tmp/incoming/sales.csv": FileState("2026-04-22T08:59:00Z", 42, "2026-04-22T08:00:00Z")}, {})  # fmt: skip
    state_file.parent.mkdir(parents=True)
    state_file.write_bytes(dump_state(state))
    result = floe.inspect_entity_state(config, "sales")
    assert result["incremental_mode"] == "file"
    assert result["path_local"] == str(state_file)
    assert result["state"]["schema"] == "floe.state.file-ingest.v2"
    assert result["state"]["files"]["local:///tmp/incoming/sales.csv"]["size"] == 42
    assert floe.reset_entity_state(config, "sales") is True
    assert not state_file.exists()
    assert floe.reset_entity_state(config, "sales") is False


def test_1100_state_calls_do_not_reuse_a_client_built_for_an_earlier_endpoint(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:  # fmt: skip
    config = tmp_path / "floe.yml"
    config.write_text(
        """version: "0.1"
storages:
  default: lake
  definitions:
    - {name: lake, type: s3, bucket: b, region: us-east-1}
entities:
  - name: "sales"
    incremental_mode: "file"
    source: {format: "csv", path: "in"}
    sink: {accepted: {format: "parquet", path: "out"}}
    policy: {severity: "warn"}
    schema: {columns: [{name: "id", type: "string"}]}
""",
        encoding="utf-8",
    )
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "k")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "s")
    used: list[str] = []

    def read(client: S3Client, *_: object) -> None:
        used.append(client.endpoint)

    monkeypatch.setattr(floe, "read_state", read)
    for endpoint in ("http://one:9000", "http://two:9000"):
        monkeypatch.setenv("AWS_ENDPOINT_URL", endpoint)
        floe.inspect_entity_state(str(config), "sales")
    assert used == ["http://one:9000", "http://two:9000"]


def test_version() -> None:
    assert isinstance(floe.__version__, str)
    assert floe.__version__ != "unknown"


def test_validate_with_profile_path() -> None:
    floe.validate(VALID_CONFIG, profile_path=PROFILE_CONFIG)


def test_validate_invalid_profile_path_raises() -> None:
    with pytest.raises(floe.FloeConfigError, match='^failed to load profile "/nonexistent/profile.yml": '):  # fmt: skip
        floe.validate(VALID_CONFIG, profile_path="/nonexistent/profile.yml")


def test_run_with_profile_path(tmp_path: Path) -> None:
    outcome = floe.run(
        VALID_CONFIG, profile_path=PROFILE_CONFIG, profile_vars=_vars(tmp_path)
    )
    assert isinstance(outcome, floe.RunOutcome)
    assert repr(outcome) == f'RunOutcome(run_id="{outcome.run_id}", entities=1, dry_run=false)'  # fmt: skip


def test_repr_html(tmp_path: Path) -> None:
    outcome = floe.run(VALID_CONFIG, profile_vars=_vars(tmp_path))
    html = outcome._repr_html_()  # pyright: ignore[reportPrivateUsage]  # the notebook hook under test
    assert "<table" in html
    assert "RunOutcome" in html
    assert outcome.run_id in html


def test_run_resolves_profile_vars_that_reference_each_other(tmp_path: Path) -> None:
    """Rust `run_with_runtime` resolves `${VAR}` references in profile vars before parsing (#847 review)."""
    text = (tmp_path / VALID_CONFIG).read_text()
    config = text.replace('    incoming_root: "./in"\n', "")
    (tmp_path / "chained.yml").write_text(config)
    variables = {"ROOT": str(tmp_path), "incoming_root": "${ROOT}/in"}
    outcome = floe.run("chained.yml", profile_vars=variables)
    assert outcome.summary["run"]["status"] != "failed"
