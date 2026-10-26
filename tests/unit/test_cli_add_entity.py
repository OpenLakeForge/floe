"""`floe add-entity` (Rust: `crates/floe-cli/tests/add_entity.rs`, `crates/floe-core/tests/unit/config/add_entity.rs`).

Expected YAML was recorded from the Rust 0.7 binary.
"""

import datetime as dt
import decimal
import os
import stat
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Self

import polars as pl
import pytest

from floe._cli import main
from floe.config.parse import parse_config
from floe.config.types import ColumnConfig, EntityConfig

Floe = tuple[int, str, str]
Run = Callable[..., Floe]


@pytest.fixture
def floe(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> Run:
    def call(*args: str | Path) -> Floe:
        monkeypatch.setattr(sys, "argv", ["floe", *map(str, args)])
        code = main()
        out, err = capsys.readouterr()
        return code, out, err

    return call


def add(floe: Run, config: Path, input: Path | str, *args: str) -> Floe:
    return floe("add-entity", "-c", config, "--input", input, *args)


def entities(config: Path) -> list[EntityConfig]:
    return parse_config(config).entities


def column(entity: EntityConfig, name: str) -> ColumnConfig:
    return next(c for c in entity.schema_.columns if c.name == name)


def included_sales_project(tmp: Path) -> tuple[Path, Path]:
    config = tmp / "floe.yml"
    domain_dir = tmp / "silver/sales"
    domain_dir.mkdir(parents=True)
    config.write_text('version: "0.3"\ninclude:\n  domains: ["silver/*/_domain.yml"]\n')
    (domain_dir / "_domain.yml").write_text(
        "name: sales\nincoming_dir: ./in\ndefaults:\n  sink:\n"  # D7: Rust also has `name` here
        "    accepted:\n      format: parquet\n      path: ./accepted\n  policy:\n    severity: warn\n"
        "  schema:\n    mismatch:\n      missing_columns: reject_file\n      extra_columns: ignore\n"
    )
    return config, domain_dir


BASE_CONFIG = 'version: "0.2"\ndomains:\n  - name: "sales"\n    incoming_dir: "./in/sales"\nentities: []\n'
SOURCES_CONFIG = 'version: "0.3"\nsources:\n  - name: crm\n    format: csv\n    path: ./bronze/{{resource}}.csv\nentities: []\n'


# crates/floe-cli/tests/add_entity.rs


def test_add_entity_writes_config_that_validates(floe: Run, tmp_path: Path) -> None:
    config, input = tmp_path / "config.yml", tmp_path / "orders.csv"
    input.write_text("id,total\n1,10.0\n2,20.0\n")
    code, out, _ = add(floe, config, input)
    assert code == 0
    assert "Entity added: orders" in out
    assert "format=csv" in out
    code, out, _ = floe("validate", "-c", config, "--entities", "orders")
    assert code == 0
    assert "Config valid:" in out


def test_add_entity_dry_run_does_not_write_config_file(
    floe: Run, tmp_path: Path
) -> None:
    config, input = tmp_path / "new-config.yml", tmp_path / "employees.csv"
    input.write_text("id,name\n1,Alice\n")
    code, out, _ = add(floe, config, input, "--dry-run")
    assert code == 0
    assert "# add-entity dry run" in out
    assert "name: employees" in out
    assert "format: csv" in out
    assert not config.exists()


def test_add_entity_can_reference_a_declared_source(floe: Run, tmp_path: Path) -> None:
    config, input = tmp_path / "config.yml", tmp_path / "orders.csv"
    config.write_text(SOURCES_CONFIG)
    input.write_text("id,total\n1,10.0\n")
    code, out, _ = add(
        floe, config, input, "--source", "crm", "--resource", "crm_orders"
    )
    assert code == 0
    assert "Entity added: orders" in out
    entity = config.read_text().split("entities:")[1]
    source = entity.split("source:", 1)[1].split("sink:", 1)[0]
    assert "ref: crm" in source
    assert "resource: crm_orders" in source
    assert "format:" not in source
    assert "path:" not in source
    assert floe("validate", "-c", config)[0] == 0


def test_add_entity_unknown_source_lists_declared_sources(
    floe: Run, tmp_path: Path
) -> None:
    config, input = tmp_path / "config.yml", tmp_path / "orders.csv"
    config.write_text(SOURCES_CONFIG)
    input.write_text("id\n1\n")
    code, _, err = add(
        floe, config, input, "--source", "missing", "--resource", "orders"
    )
    assert code == 1
    assert "source.ref=missing" in err
    assert "available: crm" in err


# crates/floe-core/tests/unit/config/add_entity.rs


def test_add_entity_writes_included_domain_sibling_and_omits_defaults(
    floe: Run, tmp_path: Path
) -> None:
    config, domain_dir = included_sales_project(tmp_path)
    input = tmp_path / "orders.csv"
    input.write_text("id,total\n1,10\n")
    original = config.read_text()
    code, out, _ = add(floe, config, input, "--domain", "sales")
    assert code == 0
    entity_path = domain_dir / "orders.yml"
    assert f"Config written: {entity_path}\n" in out
    assert config.read_text() == original
    assert entity_path.read_text() == (
        "name: orders\nsource:\n  format: csv\n"
        f"  path: {input}\n"
        '  options:\n    separator: ","\nsink:\n  rejected:\n    format: csv\n'
        "    path: out/rejected/orders/\nschema:\n  columns:\n"
        "    - name: id\n      source: id\n      type: number\n      nullable: false\n      unique: false\n"
        "    - name: total\n      source: total\n      type: number\n      nullable: false\n      unique: false\n"
    )
    assert floe("validate", "-c", config)[0] == 0
    [entity] = entities(config)
    assert (entity.name, entity.domain) == ("orders", "sales")
    assert entity.sink.accepted.path == "./accepted"
    assert entity.policy.severity == "warn"
    assert len(entity.schema_.columns) == 2


@pytest.mark.skipif(os.name != "posix", reason="unix permissions")
def test_add_entity_sibling_uses_normal_file_permissions(
    floe: Run, tmp_path: Path
) -> None:
    config, domain_dir = included_sales_project(tmp_path)
    input, control = tmp_path / "orders.csv", tmp_path / "normal.yml"
    input.write_text("id\n1\n")
    control.write_text("")
    assert add(floe, config, input, "--domain", "sales")[0] == 0
    mode = stat.S_IMODE((domain_dir / "orders.yml").stat().st_mode)
    assert mode == stat.S_IMODE(control.stat().st_mode)


def test_add_entity_validates_the_rendered_sibling_yaml(
    floe: Run, tmp_path: Path
) -> None:
    # `0o123` is written unquoted (as Rust does) and reloads as an integer
    config, domain_dir = included_sales_project(tmp_path)
    input = tmp_path / "orders.csv"
    input.write_text("0o123\n1\n")
    code, _, err = add(floe, config, input, "--domain", "sales")
    assert code == 1
    assert err.startswith(
        f"Error: generated entity file {domain_dir / 'orders.yml'} failed validation: "
    )
    assert not (domain_dir / "orders.yml").exists()


def test_add_entity_removes_a_staging_file_that_failed_to_write(
    floe: Run, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config, domain_dir = included_sales_project(tmp_path)
    input = tmp_path / "orders.csv"
    input.write_text("id\n1\n")
    real_fdopen = os.fdopen

    class FullDisk:
        def __init__(self, fd: int, mode: str) -> None:
            self.file = real_fdopen(fd, mode)

        def __enter__(self) -> Self:
            return self

        def __exit__(self, *_: object) -> None:
            self.file.close()

        def write(self, data: bytes) -> int:
            self.file.write(data[:10])
            self.file.flush()
            raise OSError(28, "No space left on device")

    monkeypatch.setattr("floe.add_entity.os.fdopen", FullDisk)
    code, _, err = add(floe, config, input, "--domain", "sales")
    assert (code, err) == (
        1,
        "Error: failed to stage entity file: No space left on device (os error 28)\n",
    )
    assert sorted(p.name for p in domain_dir.iterdir()) == ["_domain.yml"]


def test_add_entity_refuses_to_overwrite_included_domain_sibling(
    floe: Run, tmp_path: Path
) -> None:
    config, domain_dir = included_sales_project(tmp_path)
    input, entity_path = tmp_path / "orders.csv", domain_dir / "orders.yml"
    input.write_text("id\n1\n")
    entity_path.write_text("keep this file\n")
    code, _, err = add(floe, config, input, "--domain", "sales")
    assert (code, err) == (
        1,
        f"Error: refusing to overwrite existing entity file at {entity_path}\n",
    )
    assert entity_path.read_text() == "keep this file\n"


def test_add_entity_rejects_path_separator_in_included_domain_name(
    floe: Run, tmp_path: Path
) -> None:
    config, domain_dir = included_sales_project(tmp_path)
    input = tmp_path / "orders.csv"
    input.write_text("id\n1\n")
    code, _, err = add(floe, config, input, "--name", "../outside", "--domain", "sales")
    assert code == 1
    assert err == (
        f"Error: entity name cannot contain a path separator for an included domain at {domain_dir}\n"
    )
    assert not (tmp_path / "silver/outside.yml").exists()


def test_add_entity_dry_run_in_included_domain_renders_sibling_without_writing(
    floe: Run, tmp_path: Path
) -> None:
    config, domain_dir = included_sales_project(tmp_path)
    input = tmp_path / "orders.csv"
    input.write_text("id\n1\n")
    code, out, _ = add(floe, config, input, "--domain", "sales", "--dry-run")
    assert code == 0
    assert "name: orders" in out
    assert "accepted:" not in out
    assert not (domain_dir / "orders.yml").exists()


@pytest.mark.skipif(os.name != "posix" or os.geteuid() == 0, reason="unix permissions")
def test_add_entity_dry_run_does_not_write_to_read_only_domain_directory(
    floe: Run, tmp_path: Path
) -> None:
    config, domain_dir = included_sales_project(tmp_path)
    input = tmp_path / "orders.csv"
    input.write_text("id\n1\n")
    domain_dir.chmod(0o555)
    try:
        code, out, _ = add(floe, config, input, "--domain", "sales", "--dry-run")
    finally:
        domain_dir.chmod(0o755)
    assert code == 0
    assert "# add-entity dry run" in out
    assert not (domain_dir / "orders.yml").exists()


def test_add_entity_matches_domain_files_with_nonstandard_names(
    floe: Run, tmp_path: Path
) -> None:
    config, domain_dir = included_sales_project(tmp_path)
    (domain_dir / "_domain.yml").rename(domain_dir / "domain.yml")
    config.write_text('version: "0.3"\ninclude:\n  domains: ["silver/*/domain.yml"]\n')
    input = tmp_path / "orders.csv"
    input.write_text("id\n1\n")
    code, out, _ = add(floe, config, input, "--domain", "sales")
    assert code == 0
    assert f"Config written: {domain_dir / 'orders.yml'}\n" in out
    assert (domain_dir / "orders.yml").exists()


def test_add_entity_output_keeps_writing_a_root_config_copy_for_included_domains(
    floe: Run, tmp_path: Path
) -> None:
    config, domain_dir = included_sales_project(tmp_path)
    input, output = tmp_path / "orders.csv", tmp_path / "copy.yml"
    input.write_text("id\n1\n")
    assert (
        add(floe, config, input, "--domain", "sales", "--output", str(output))[0] == 0
    )
    assert not (domain_dir / "orders.yml").exists()
    assert [e.name for e in entities(output)] == ["orders"]
    assert entities(config) == []


def test_add_entity_infers_csv_and_appends_defaults(floe: Run, tmp_path: Path) -> None:
    config, input = tmp_path / "config.yml", tmp_path / "customers.csv"
    config.write_text(BASE_CONFIG)
    input.write_text("id,name,active\n1,Alice,true\n2,Bob,false\n3,,true\n")
    code, out, _ = add(
        floe,
        config,
        input,
        "--format",
        "csv",
        "--name",
        "customers",
        "--domain",
        "sales",
    )
    assert (code, out) == (
        0,
        f"Entity added: customers (format=csv, columns=3)\nConfig written: {config}\n",
    )
    assert floe("validate", "-c", config)[0] == 0
    [entity] = entities(config)
    assert (entity.name, entity.domain) == ("customers", "sales")
    assert (entity.source.format, entity.source.path) == ("csv", str(input))
    assert entity.source.options.separator == ","
    assert (entity.sink.accepted.format, entity.sink.accepted.path) == (
        "parquet",
        "out/accepted/customers/",
    )
    assert entity.sink.rejected is not None
    assert entity.sink.rejected.path == "out/rejected/customers/"
    assert entity.policy.severity == "reject"
    mismatch = entity.schema_.mismatch
    assert mismatch is not None
    assert (mismatch.missing_columns, mismatch.extra_columns) == (
        "reject_file",
        "ignore",
    )
    assert (column(entity, "id").source, column(entity, "id").column_type) == (
        "id",
        "number",
    )
    assert column(entity, "active").column_type == "boolean"
    assert column(entity, "name").nullable is True


def test_add_entity_requires_source_and_resource_together(
    floe: Run, tmp_path: Path
) -> None:
    code, _, err = add(
        floe,
        tmp_path / "config.yml",
        tmp_path / "orders.csv",
        "--source",
        "crm",
        "--dry-run",
    )
    assert (code, err) == (
        1,
        "Error: --source and --resource must be provided together\n",
    )


def test_add_entity_normalizes_file_uri_before_persisting_source_path(
    floe: Run, tmp_path: Path
) -> None:
    config, input = tmp_path / "config.yml", tmp_path / "customers.csv"
    config.write_text(BASE_CONFIG)
    input.write_text("id,name\n1,Alice\n")
    assert (
        add(floe, config, input.as_uri(), "--format", "csv", "--name", "customers")[0]
        == 0
    )
    assert entities(config)[0].source.path == str(input)


def test_add_entity_bootstraps_missing_config_file(floe: Run, tmp_path: Path) -> None:
    config, input = tmp_path / "new.yml", tmp_path / "orders.csv"
    input.write_text("id,total\n1,10.1\n")
    code, out, _ = add(floe, config, input)
    assert (code, out.splitlines()[0]) == (
        0,
        "Entity added: orders (format=csv, columns=2)",
    )
    assert config.read_text().startswith(
        'version: "0.8"\n'
    )  # D21: Rust 0.7 writes "0.2"
    assert floe("validate", "-c", config)[0] == 0


def test_add_entity_infers_name_from_file_uri_stem_by_default(
    floe: Run, tmp_path: Path
) -> None:
    config, input = tmp_path / "config.yml", tmp_path / "order_events.json"
    config.write_text('version: "0.2"\nentities: []\n')
    input.write_text('[{"id":1}]')
    code, out, _ = add(floe, config, input.as_uri())
    assert (code, out.splitlines()[0]) == (
        0,
        "Entity added: order_events (format=json, columns=1)",
    )
    assert entities(config)[0].name == "order_events"


def test_add_entity_infers_format_from_extension_csv_json_and_parquet(
    floe: Run, tmp_path: Path
) -> None:
    (tmp_path / "orders.csv").write_text("id\n1\n")
    (tmp_path / "events.json").write_text('[{"id":1}]')
    pl.DataFrame({"employee_id": [1, 2], "active": [True, False]}).write_parquet(
        tmp_path / "employees.parquet"
    )
    for name, fmt in (
        ("orders.csv", "csv"),
        ("events.json", "json"),
        ("employees.parquet", "parquet"),
    ):
        config = tmp_path / f"{fmt}.yml"
        assert add(floe, config, tmp_path / name)[0] == 0
        assert entities(config)[0].source.format == fmt


def test_add_entity_explicit_name_and_format_override_inferred_values(
    floe: Run, tmp_path: Path
) -> None:
    config, input = tmp_path / "config.yml", tmp_path / "strange_name.data"
    input.write_text("id,name\n1,Alice\n")
    code, out, _ = add(
        floe, config, input, "--format", "csv", "--name", "customers_override"
    )
    assert (code, out.splitlines()[0]) == (
        0,
        "Entity added: customers_override (format=csv, columns=2)",
    )
    [entity] = entities(config)
    assert (entity.name, entity.source.format) == ("customers_override", "csv")


def test_add_entity_unknown_extension_requires_explicit_format(
    floe: Run, tmp_path: Path
) -> None:
    config, input = tmp_path / "config.yml", tmp_path / "payload.unknown"
    input.write_text("some,data\n")
    code, _, err = add(floe, config, input)
    assert (code, err) == (
        1,
        (
            f"Error: could not infer add-entity format from input {input} "
            "(supported extensions: .csv, .json, .parquet); use --format\n"
        ),
    )


def test_add_entity_rejects_multi_document_config(floe: Run, tmp_path: Path) -> None:
    config, input = tmp_path / "config.yml", tmp_path / "customers.csv"
    config.write_text(BASE_CONFIG + '---\nversion: "0.2"\nentities: []\n')
    input.write_text("id,name\n1,Alice\n")
    original = config.read_text()
    code, _, err = add(floe, config, input, "--format", "csv", "--name", "customers")
    assert (code, err) == (1, "Error: YAML contains multiple documents; expected one\n")
    assert config.read_text() == original


def test_add_entity_v03_allows_same_name_in_another_domain(
    floe: Run, tmp_path: Path
) -> None:
    config, input = tmp_path / "config.yml", tmp_path / "accounts.csv"
    config.write_text(
        'version: "0.3"\ndomains:\n  - name: "sales"\n    incoming_dir: "./in/sales"\n'
        '  - name: "finance"\n    incoming_dir: "./in/finance"\nentities: []\n'
    )
    input.write_text("id\n1\n")
    for domain in ("sales", "finance"):
        assert (
            add(floe, config, input, "--name", "accounts", "--domain", domain)[0] == 0
        )
    code, _, err = add(floe, config, input, "--name", "accounts", "--domain", "sales")
    assert (code, err) == (1, "Error: entity already exists: accounts\n")
    assert [e.id for e in entities(config)] == ["sales.accounts", "finance.accounts"]


# Byte-for-byte YAML, recorded from the Rust 0.7 binary


RICH_CONFIG = """# comment dropped
version: "0.3"
metadata:
  project: "demo: x"
  owner: '-team'
  tags: []
  extra: {}
  note: "line1\\nline2"
  yes_str: "yes"
  n: 0x10
  empty: ""
  tilde: ~
  uni: café
domains:
  - name: sales
    incoming_dir: ./in/sales
entities:
  - name: old
    domain: sales
    source: {format: csv, path: "./in/old.csv", options: {separator: ",", null_values: ["", "NA"]}}
    sink:
      accepted: {format: parquet, path: ./out/a}
    policy: {severity: warn}
    schema:
      columns:
        - {name: id, type: int64, nullable: false}
"""

RICH_EXPECTED = """# add-entity dry run (no file written)
# entity: my_orders_2024
version: "0.3"
metadata:
  project: "demo: x"
  owner: "-team"
  tags: []
  extra: {}
  note: "line1\\nline2"
  yes_str: "yes"
  "n": 16
  empty: ""
  tilde: ~
  uni: café
domains:
  - name: sales
    incoming_dir: "./in/sales"
entities:
  - name: old
    domain: sales
    source:
      format: csv
      path: "./in/old.csv"
      options:
        separator: ","
        null_values:
          - ""
          - NA
    sink:
      accepted:
        format: parquet
        path: "./out/a"
    policy:
      severity: warn
    schema:
      columns:
        - name: id
          type: int64
          nullable: false
  - name: my_orders_2024
    domain: sales
    source:
      format: csv
      path: My Orders-2024.csv
      options:
        separator: ","
    sink:
      accepted:
        format: parquet
        path: out/accepted/my_orders_2024/
      rejected:
        format: csv
        path: out/rejected/my_orders_2024/
    policy:
      severity: reject
    schema:
      mismatch:
        missing_columns: reject_file
        extra_columns: ignore
      columns:
        - name: Id
          source: Id
          type: number
          nullable: false
          unique: false
        - name: Price
          source: Price
          type: number
          nullable: false
          unique: false

"""


def test_add_entity_re_emits_the_config_as_rust_does(
    floe: Run, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    Path("cfg.yml").write_text(RICH_CONFIG)
    Path("My Orders-2024.csv").write_text("Id,Price\n1,2.5\n")
    code, out, _ = add(
        floe, Path("cfg.yml"), "My Orders-2024.csv", "--domain", "sales", "--dry-run"
    )
    assert (code, out) == (0, RICH_EXPECTED)


def test_add_entity_sniffs_the_csv_separator(
    floe: Run, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    Path("cust.csv").write_text(
        "id;name;active;amt;d\n1;Alice;true;1.5;2024-01-01\n2;;false;2;2024-01-02\n"
    )
    assert add(floe, Path("cfg.yml"), "cust.csv")[0] == 0
    columns = "".join(
        f"        - name: {n}\n          source: {n}\n          type: {t}\n          nullable: {u}\n          unique: false\n"
        for n, t, u in (
            ("id", "number", "false"),
            ("name", "string", "true"),
            ("active", "boolean", "false"),
            ("amt", "number", "false"),
            ("d", "string", "false"),
        )
    )
    assert Path("cfg.yml").read_text() == (
        'version: "0.8"\nentities:\n  - name: cust\n    source:\n      format: csv\n      path: cust.csv\n'
        "      options:\n        separator: ;\n    sink:\n      accepted:\n        format: parquet\n"
        "        path: out/accepted/cust/\n      rejected:\n        format: csv\n        path: out/rejected/cust/\n"
        "    policy:\n      severity: reject\n    schema:\n      mismatch:\n        missing_columns: reject_file\n"
        "        extra_columns: ignore\n      columns:\n" + columns
    )


def test_add_entity_maps_parquet_types_as_rust_does(
    floe: Run, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    pl.DataFrame(
        {
            "i8": pl.Series([1], dtype=pl.Int8),
            "u32": pl.Series([1], dtype=pl.UInt32),
            "f": [1.5],
            "dec": pl.Series([decimal.Decimal("1.25")], dtype=pl.Decimal(10, 2)),
            "b": [True],
            "s": ["x"],
            "d": [dt.date(2024, 1, 1)],
            "ts": [dt.datetime(2024, 1, 1)],
            "l": [[1]],
            "st": [{"a": 1}],
            "dur": [dt.timedelta(1)],
            "cat": pl.Series(["a"], dtype=pl.Categorical),
            "nul": pl.Series([None], dtype=pl.Null),
        }
    ).write_parquet("emp.parquet")
    code, out, _ = add(floe, Path("new.yml"), "emp.parquet", "--dry-run")
    types = {
        "i8": "number",
        "u32": "number",
        "f": "number",
        "dec": "number",
        "b": "boolean",
        "s": "string",
        "d": "date",
        "ts": "datetime",
    }
    types |= dict.fromkeys(("l", "st", "dur", "cat", "nul"), "string")
    columns = "".join(
        f"        - name: {n}\n          source: {n}\n          type: {t}\n          nullable: true\n          unique: false\n"
        for n, t in types.items()
    )
    assert code == 0
    assert out == (
        '# add-entity dry run (no file written)\n# entity: emp\nversion: "0.8"\nentities:\n  - name: emp\n'
        "    source:\n      format: parquet\n      path: emp.parquet\n    sink:\n      accepted:\n"
        "        format: parquet\n        path: out/accepted/emp/\n      rejected:\n        format: csv\n"
        "        path: out/rejected/emp/\n    policy:\n      severity: reject\n    schema:\n      mismatch:\n"
        "        missing_columns: reject_file\n        extra_columns: ignore\n      columns:\n"
        + columns
        + "\n"
    )


def test_add_entity_writes_parquet_time_as_string(floe: Run, tmp_path: Path) -> None:
    # D27b: Rust writes `time`, which 0.8 rejects (D10)
    config, input = tmp_path / "config.yml", tmp_path / "events.parquet"
    pl.DataFrame({"at": [dt.time(1, 2)]}).write_parquet(input)
    assert add(floe, config, input)[0] == 0
    assert column(entities(config)[0], "at").column_type == "string"
    assert floe("validate", "-c", config)[0] == 0


# JSON (D27a: Polars infers; same output as Rust on clean data)


def test_add_entity_infers_json_top_level_keys_only(floe: Run, tmp_path: Path) -> None:
    config, input = tmp_path / "config.yml", tmp_path / "orders.json"
    config.write_text(BASE_CONFIG)
    input.write_text(
        '[\n  {"id": 1, "meta": {"source": "api"}, "tags": ["new"], "created_at": "2026-01-01T10:00:00Z"},\n'
        '  {"id": 2, "meta": null, "tags": ["vip"], "created_at": "2026-01-02T10:00:00Z"}\n]'
    )
    code, out, _ = add(floe, config, input, "--format", "json")
    assert code == 0
    assert out.splitlines()[0] == "Entity added: orders (format=json, columns=4)"
    assert "Note: nested JSON values inferred as string for top-level key(s): meta, tags" in out  # fmt: skip
    assert floe("validate", "-c", config)[0] == 0
    [entity] = entities(config)
    assert (entity.name, entity.source.format) == ("orders", "json")
    assert entity.source.options.json_mode == "array"
    created_at = column(entity, "created_at")
    assert (created_at.source, created_at.column_type) == ("created_at", "datetime")
    meta = column(entity, "meta")
    assert (meta.source, meta.column_type, meta.nullable) == ("meta", "string", True)
    assert column(entity, "tags").column_type == "string"


def json_columns(*cols: tuple[str, str, bool]) -> str:
    return "".join(
        f"        - name: {n}\n          source: {n}\n          type: {t}\n"
        f"          nullable: {str(u).lower()}\n          unique: false\n"
        for n, t, u in cols
    )


def json_entity(name: str, mode: str, notes: str, columns: str) -> str:
    return (
        f"# add-entity dry run (no file written)\n# entity: {name}\n{notes}"
        f'version: "0.8"\nentities:\n  - name: {name}\n    source:\n      format: json\n'
        f"      path: {name}.json\n      options:\n        json_mode: {mode}\n    sink:\n"
        f"      accepted:\n        format: parquet\n        path: out/accepted/{name}/\n"
        f"      rejected:\n        format: csv\n        path: out/rejected/{name}/\n"
        "    policy:\n      severity: reject\n    schema:\n      mismatch:\n"
        "        missing_columns: reject_file\n        extra_columns: ignore\n      columns:\n"
        f"{columns}\n"
    )


def test_add_entity_json_array_matches_rust(
    floe: Run, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    Path("orders.json").write_text(
        '[\n  {"id": 1, "meta": {"source": "api"}, "tags": ["new"], "created_at": "2026-01-01T10:00:00Z",'
        ' "day": "2026-01-01", "ok": true, "amt": 1.5, "mixed": 1, "note": null},\n'
        '  {"id": 2, "meta": null, "tags": ["vip"], "created_at": "2026-01-02T10:00:00Z",'
        ' "day": "2026-01-02", "ok": false, "amt": 2, "mixed": "x"}\n]\n'
    )
    code, out, _ = add(floe, Path("c.yml"), "orders.json", "--dry-run")
    assert code == 0
    assert out == json_entity(
        "orders",
        "array",
        "# note: json inference sampled 2 row(s) in array mode\n"
        "# note: nested JSON values inferred as string for top-level key(s): meta, tags\n",
        json_columns(
            ("amt", "number", False),
            ("created_at", "datetime", False),
            ("day", "date", False),
            ("id", "number", False),
            ("meta", "string", True),
            ("mixed", "string", False),
            ("note", "string", True),
            ("ok", "boolean", False),
            ("tags", "string", False),
        ),
    )


def test_add_entity_json_ndjson_mixed_kinds_become_string(
    floe: Run, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    Path("events.json").write_text(
        '{"b": 1, "a": true, "when": "2026-01-01T10:00:00Z"}\n\n'
        '{"b": true, "a": false, "c": [1], "when": "2026-01-02"}\n'
    )
    code, out, _ = add(floe, Path("c.yml"), "events.json", "--dry-run")
    assert code == 0
    assert out == json_entity(
        "events",
        "ndjson",
        "# note: json inference sampled 2 row(s) in ndjson mode\n"
        "# note: nested JSON values inferred as string for top-level key(s): c\n",
        json_columns(
            ("a", "boolean", False),
            ("b", "string", False),  # int then bool: Polars' ndjson reader errors
            ("c", "string", True),
            ("when", "string", False),  # D27a: Rust `datetime` (date + datetime mix)
        ),
    )


@pytest.mark.parametrize(
    ("text", "error"),
    [
        ("[]", "no json objects found in {path}"),
        ("[1]", "expected top-level json objects inside array"),
        ('{"a":1}\n[1]', "expected top-level json object at line 2"),
        ("   ", "json input {path} is empty"),
    ],
)
def test_add_entity_json_errors_match_rust(
    floe: Run, tmp_path: Path, text: str, error: str
) -> None:
    input = tmp_path / "e.json"
    input.write_text(text)
    code, _, err = add(floe, tmp_path / "c.yml", input)
    assert (code, err) == (1, f"Error: {error.format(path=input)}\n")
