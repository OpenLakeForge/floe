"""`run_entity`: statuses, outputs and file-level rejections for a local CSV entity."""

import json
from datetime import timedelta
from functools import partial
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from floe.checks.unique import UniqueTracker
from floe.config.parse import parse_config
from floe.errors import FloeConfigError, FloeRunError
from floe.read import json as json_module
from floe.read.csv import read_csv
from floe.read.xml import read_xml
from floe.run import file as file_module
from floe.run import sources as sources_module
from floe.run.entity import run_entity

CONFIG = """\
version: "0.3"
domains:
  - name: d
entities:
  - name: orders
    domain: d
    source:
      format: csv
      path: ./in
      options: {{header: true, separator: ","}}
    sink:
      accepted: {{format: parquet, path: ./out/accepted}}
      rejected: {{format: csv, path: ./out/rejected}}
    policy: {{severity: {severity}}}
    schema:
      {mismatch}
      columns:
        - {{name: id, type: int64, nullable: false}}
        - {{name: name, type: string}}
report: {{path: ./report}}
"""


def run(
    tmp_path: Path, severity: str, rows: str, mismatch: str = ""
) -> tuple[dict[str, Any], bool]:
    (tmp_path / "in").mkdir()
    (tmp_path / "in" / "orders.csv").write_text(rows)
    (tmp_path / "config.yml").write_text(
        CONFIG.format(severity=severity, mismatch=mismatch)
    )
    entity = parse_config(tmp_path / "config.yml").entities[0]
    result = run_entity(entity, tmp_path, "0.3")
    return result.report, result.aborted


def accepted(tmp_path: Path) -> pl.DataFrame:
    return pl.read_parquet(tmp_path / "out" / "accepted")


def test_clean_file_is_all_accepted(tmp_path: Path) -> None:
    report, aborted = run(tmp_path, "reject", "id,name\n1,a\n2,b\n")
    assert not aborted
    assert report["results"]["accepted_total"] == 2
    assert accepted(tmp_path)["id"].to_list() == [1, 2]
    assert report["files"][0]["status"] == "success"


def test_reject_splits_rows_and_numbers_them_by_file_position(tmp_path: Path) -> None:
    report, aborted = run(tmp_path, "reject", "id,name\n1,a\nx,b\n3,c\n,d\n")
    assert not aborted
    assert accepted(tmp_path)["id"].to_list() == [1, 3]
    (part,) = (tmp_path / "out" / "rejected").glob("*.csv")
    rejected = pl.read_csv(part)
    assert rejected["__floe_row_index"].to_list() == [1, 3]
    assert report["files"][0]["status"] == "rejected"
    assert report["files"][0]["output"]["rejected_path"] == str(part)


def test_warn_keeps_every_row_and_writes_an_error_report(tmp_path: Path) -> None:
    report, _ = run(tmp_path, "warn", "id,name\n1,a\nx,b\n")
    assert accepted(tmp_path)["id"].to_list() == [1, None]
    f = report["files"][0]
    assert f["status"] == "success" and f["validation"]["warnings"] == 2
    errors = Path(f["output"]["errors_path"])
    assert errors.name == "orders_reject_errors.json"
    assert errors.read_text().startswith('[{"row_index":1,"errors":[')


def test_abort_copies_the_raw_file_and_writes_the_accepted_schema(
    tmp_path: Path,
) -> None:
    report, aborted = run(tmp_path, "abort", "id,name\n1,a\nx,b\n")
    assert aborted
    assert report["files"][0]["status"] == "aborted"
    assert (
        tmp_path / "out" / "rejected" / "orders.csv"
    ).read_text() == "id,name\n1,a\nx,b\n"
    frame = accepted(tmp_path)
    assert frame.height == 0 and frame.columns == ["id", "name"]


def test_abort_with_a_cast_error_leaves_not_null_out_of_the_errors(
    tmp_path: Path,
) -> None:
    report, _ = run(tmp_path, "abort", "id,name\nx,a\n,b\n")
    rules = [r["rule"] for r in report["files"][0]["validation"]["rules"]]
    assert rules == ["cast_error"]


def test_mismatch_reject_file_skips_the_rows(tmp_path: Path) -> None:
    report, _ = run(
        tmp_path, "reject", "id\n1\n", "mismatch: {missing_columns: reject_file}"
    )
    f = report["files"][0]
    assert f["status"] == "rejected" and f["row_count"] == 0
    assert f["mismatch"]["error"]["rule"] == "schema_mismatch"
    assert (tmp_path / "out" / "rejected" / "orders.csv").exists()
    assert accepted(tmp_path).height == 0


def test_unreadable_file_is_rejected_with_the_reader_rule(tmp_path: Path) -> None:
    (tmp_path / "in").mkdir()
    (tmp_path / "in" / "orders.csv").write_bytes(b"")
    (tmp_path / "config.yml").write_text(CONFIG.format(severity="reject", mismatch=""))
    entity = parse_config(tmp_path / "config.yml").entities[0]
    f = run_entity(entity, tmp_path, "0.3").report["files"][0]
    assert f["status"] == "rejected"
    assert f["mismatch"]["error"]["rule"] == "csv_read_error"
    assert f["validation"]["errors"] == 1


def test_coerce_mode_runs_a_multi_row_file(tmp_path: Path) -> None:
    (tmp_path / "in").mkdir()
    (tmp_path / "in" / "orders.csv").write_text("id,name\n1,a\n2,b\n")
    config = CONFIG.format(severity="reject", mismatch="").replace(
        "    source:\n", "    source:\n      cast_mode: coerce\n"
    )
    (tmp_path / "config.yml").write_text(config)
    entity = parse_config(tmp_path / "config.yml").entities[0]
    run_entity(entity, tmp_path, "0.3")
    assert accepted(tmp_path)["id"].to_list() == [1, 2]


def test_tsv_source_uses_the_csv_reader(tmp_path: Path) -> None:
    (tmp_path / "in").mkdir()
    (tmp_path / "in" / "orders.tsv").write_text("id\tname\n1\ta\n")
    config = CONFIG.format(severity="reject", mismatch="")
    config = config.replace(
        "format: csv\n      path: ./in", "format: tsv\n      path: ./in"
    )
    config = config.replace(
        'options: {header: true, separator: ","}', "options: {header: true}"
    )
    (tmp_path / "config.yml").write_text(config)
    entity = parse_config(tmp_path / "config.yml").entities[0]
    run_entity(entity, tmp_path, "0.3")
    assert accepted(tmp_path).to_dicts() == [{"id": 1, "name": "a"}]


def test_append_runs_reject_keys_already_in_the_sink(tmp_path: Path) -> None:
    config = CONFIG.format(severity="reject", mismatch="").replace(
        "    sink:\n", "    sink:\n      write_mode: append\n"
    )
    config = config.replace("type: int64, nullable: false", "type: int64, unique: true")
    (tmp_path / "in").mkdir()
    (tmp_path / "config.yml").write_text(config)
    entity = parse_config(tmp_path / "config.yml").entities[0]
    (tmp_path / "in" / "orders.csv").write_text("id,name\n1,a\n")
    run_entity(entity, tmp_path, "0.3")
    (tmp_path / "in" / "orders.csv").write_text("id,name\n1,b\n2,c\n")
    report = run_entity(entity, tmp_path, "0.3").report
    assert sorted(accepted(tmp_path)["id"].to_list()) == [1, 2]
    (unique,) = report["unique_constraints"]
    assert unique["target_duplicates_count"] == 1
    assert "batch_duplicates_count" not in unique


def test_append_seeds_only_the_history_keys_a_chunk_holds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """#1057: the tracker held every key of the sink's history."""
    config = CONFIG.format(severity="reject", mismatch="").replace(
        "    sink:\n", "    sink:\n      write_mode: append\n"
    )
    config = config.replace("type: int64, nullable: false", "type: int64, unique: true")
    (tmp_path / "in").mkdir()
    (tmp_path / "config.yml").write_text(config)
    entity = parse_config(tmp_path / "config.yml").entities[0]
    for ids in (range(1, 51), range(51, 101)):  # two parts
        (tmp_path / "in" / "orders.csv").write_text("id,name\n" + "".join(f"{i},n\n" for i in ids))  # fmt: skip
        run_entity(entity, tmp_path, "0.3")
    seeded: list[list[int]] = []
    seed = UniqueTracker.seed

    def spy(tracker: UniqueTracker, df: pl.DataFrame) -> None:
        seeded.append(df["id"].to_list())
        seed(tracker, df)

    monkeypatch.setattr(UniqueTracker, "seed", spy)
    monkeypatch.setattr(file_module, "read_csv", partial(read_csv, chunk_rows=1))
    (tmp_path / "in" / "orders.csv").write_text("id,name\n7,a\n200,b\n")
    report = run_entity(entity, tmp_path, "0.3").report
    assert sorted(i for ids in seeded for i in ids) == [
        7
    ]  # never the 100 ids of the history
    assert sorted(accepted(tmp_path)["id"].to_list()) == [*range(1, 101), 200]
    (unique,) = report["unique_constraints"]
    assert unique["target_duplicates_count"] == 1


def test_append_checks_history_for_a_unique_column_named_like_a_pattern(tmp_path: Path) -> None:  # fmt: skip
    """#1057 review: `pl.col("*")` expanded every cached column; the name is selected literally."""
    config = CONFIG.format(severity="reject", mismatch="").replace("    sink:\n", "    sink:\n      write_mode: append\n")  # fmt: skip
    config = config.replace("{name: id, type: int64, nullable: false}", '{name: "*", source: id, type: int64, unique: true}')  # fmt: skip
    (tmp_path / "in").mkdir()
    (tmp_path / "config.yml").write_text(config)
    entity = parse_config(tmp_path / "config.yml").entities[0]
    (tmp_path / "in" / "orders.csv").write_text("id,name\n1,a\n2,b\n")
    run_entity(entity, tmp_path, "0.3")
    (tmp_path / "in" / "orders.csv").write_text("id,name\n2,c\n3,d\n")
    (unique,) = run_entity(entity, tmp_path, "0.3").report["unique_constraints"]
    assert unique["target_duplicates_count"] == 1


def test_append_fails_when_a_sink_part_lacks_a_unique_key_column(tmp_path: Path) -> None:  # fmt: skip
    config = CONFIG.format(severity="reject", mismatch="").replace("    sink:\n", "    sink:\n      write_mode: append\n")  # fmt: skip
    (tmp_path / "in").mkdir()
    (tmp_path / "in" / "orders.csv").write_text("id,name\n1,a\n")
    (tmp_path / "config.yml").write_text(config.replace("type: int64, nullable: false", "type: int64, unique: true"))  # fmt: skip
    entity = parse_config(tmp_path / "config.yml").entities[0]
    run_entity(entity, tmp_path, "0.3")
    (part,) = (tmp_path / "out" / "accepted").glob("part-*.parquet")
    pl.read_parquet(part).drop("id").write_parquet(part)
    (tmp_path / "in" / "orders.csv").write_text("id,name\n2,b\n")
    with pytest.raises(FloeRunError, match=r"^entity.name=orders cannot seed unique keys from .*part-.*\.parquet: "):  # fmt: skip
        run_entity(entity, tmp_path, "0.3")


def test_an_unreadable_file_leaves_no_unique_keys_behind(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(file_module, "read_xml", partial(read_xml, chunk_rows=1))
    config = CONFIG.format(severity="reject", mismatch="")
    config = config.replace(
        "format: csv\n      path: ./in", "format: xml\n      path: ./in"
    )
    config = config.replace(
        'options: {header: true, separator: ","}', "options: {row_tag: item}"
    )
    config = config.replace("type: int64, nullable: false", "type: int64, unique: true")
    (tmp_path / "in").mkdir()
    (tmp_path / "config.yml").write_text(config)
    # the parser sees the rows of the first 64 KiB block before it meets the damage after them
    rows = "".join(
        f"<item><id>{i}</id><pad>{'x' * 200}</pad></item>" for i in range(1, 400)
    )
    (tmp_path / "in" / "a.xml").write_text(f"<r>{rows}<item><id>")
    (tmp_path / "in" / "b.xml").write_text("<r><item><id>1</id></item></r>")
    entity = parse_config(tmp_path / "config.yml").entities[0]
    files = run_entity(entity, tmp_path, "0.3").report["files"]
    assert [f["status"] for f in files] == ["rejected", "success"]
    assert accepted(tmp_path)["id"].to_list() == [1]


def test_primary_key_names_are_trimmed_before_they_become_required(
    tmp_path: Path,
) -> None:
    config = CONFIG.format(severity="reject", mismatch="primary_key: [' id ']")
    config = config.replace("type: int64, nullable: false", "type: int64")
    (tmp_path / "in").mkdir()
    (tmp_path / "in" / "orders.csv").write_text("id,name\n,a\n")
    (tmp_path / "config.yml").write_text(config)
    entity = parse_config(tmp_path / "config.yml").entities[0]
    report = run_entity(entity, tmp_path, "0.3").report
    assert report["files"][0]["status"] == "rejected"
    assert accepted(tmp_path).height == 0


def test_unique_samples_use_the_schema_column_names(tmp_path: Path) -> None:
    config = CONFIG.format(severity="reject", mismatch="").replace(
        "{name: id, type: int64, nullable: false}",
        "{name: id, source: external_id, type: int64, unique: true}",
    )
    (tmp_path / "in").mkdir()
    (tmp_path / "in" / "orders.csv").write_text("external_id,name\n1,a\n1,b\n")
    (tmp_path / "config.yml").write_text(config)
    entity = parse_config(tmp_path / "config.yml").entities[0]
    (unique,) = run_entity(entity, tmp_path, "0.3").report["unique_constraints"]
    assert unique["columns"] == ["id"]
    assert unique["samples"] == [{"values": {"id": "1"}, "count": 1}]


def test_a_prefixed_xml_selector_keeps_its_value(tmp_path: Path) -> None:
    config = CONFIG.format(severity="reject", mismatch="")
    config = config.replace(
        "format: csv\n      path: ./in", "format: xml\n      path: ./in"
    )
    config = config.replace(
        'options: {header: true, separator: ","}',
        "options: {row_tag: row, namespace: 'urn:a'}",
    )
    config = config.replace(
        "{name: id, type: int64, nullable: false}",
        "{name: id, source: 'a:id', type: int64}",
    )
    (tmp_path / "in").mkdir()
    (tmp_path / "config.yml").write_text(config)
    xml = '<rows xmlns:a="urn:a"><a:row><a:id>1</a:id></a:row></rows>'
    (tmp_path / "in" / "a.xml").write_text(xml)
    entity = parse_config(tmp_path / "config.yml").entities[0]
    run_entity(entity, tmp_path, "0.3")
    assert accepted(tmp_path)["id"].to_list() == [1]


def test_a_json_source_runs_end_to_end(tmp_path: Path) -> None:
    """#998: `floe validate` accepts `source.format: json`, so the run must read it too."""
    config = CONFIG.format(severity="reject", mismatch="")
    config = config.replace(
        "format: csv\n      path: ./in", "format: json\n      path: ./in"
    )
    config = config.replace('options: {header: true, separator: ","}', "options: {}")
    (tmp_path / "in").mkdir()
    (tmp_path / "config.yml").write_text(config)
    (tmp_path / "in" / "a.json").write_text('[{"id": 1}, {"id": 2}]')
    entity = parse_config(tmp_path / "config.yml").entities[0]
    files = run_entity(entity, tmp_path, "0.3").report["files"]
    assert [f["status"] for f in files] == ["success"]
    assert accepted(tmp_path)["id"].to_list() == [1, 2]


def test_a_json_source_maps_nested_selectors_to_the_schema_names(
    tmp_path: Path,
) -> None:
    """#998: every row is read and each selector's values land in its schema column, as Rust 0.7."""
    config = CONFIG.format(
        severity="reject",
        mismatch="normalize_columns: {enabled: true, strategy: snake_case}",
    )
    config = config.replace(
        "format: csv\n      path: ./in", "format: json\n      path: ./in"
    )
    config = config.replace('options: {header: true, separator: ","}', "options: {}")
    config = config.replace(
        "{name: id, type: int64, nullable: false}",
        "{name: order_id, source: order.id, type: int64, nullable: false}",
    )
    config = config.replace(
        "{name: name, type: string}",
        "{name: first_sku, source: 'items[0].sku', type: string}",
    )
    (tmp_path / "in").mkdir()
    (tmp_path / "config.yml").write_text(config)
    rows = '[{"order": {"id": 1}, "items": [{"sku": "a"}]}, {"order": {"id": 2}, "items": []}, {"order": {"id": 3}}]'
    (tmp_path / "in" / "a.json").write_text(rows)
    entity = parse_config(tmp_path / "config.yml").entities[0]
    (file,) = run_entity(entity, tmp_path, "0.3").report["files"]
    assert (file["status"], file["row_count"], file["accepted_count"]) == (
        "success",
        3,
        3,
    )
    assert file["mismatch"]["extra_columns"] == ["items", "order"]
    assert accepted(tmp_path).to_dicts() == [
        {"order_id": 1, "first_sku": "a"},
        {"order_id": 2, "first_sku": None},
        {"order_id": 3, "first_sku": None},
    ]


@pytest.mark.parametrize(
    ("body", "rule"),
    [
        ('[{"order": {"id": 1}', "json_parse_error"),
        ('[{"order": {"id": {"x": 1}}}]', "json_selector_non_scalar"),
    ],
)
def test_an_unreadable_json_file_carries_the_rust_rule(
    tmp_path: Path, body: str, rule: str
) -> None:
    """#998: Rust 0.7's `JsonReadError.rule`, not a generic `json_read_error`."""
    config = CONFIG.format(severity="reject", mismatch="")
    config = config.replace(
        "format: csv\n      path: ./in", "format: json\n      path: ./in"
    )
    config = config.replace('options: {header: true, separator: ","}', "options: {}")
    config = config.replace("{name: id,", "{name: id, source: order.id,")
    (tmp_path / "in").mkdir()
    (tmp_path / "config.yml").write_text(config)
    (tmp_path / "in" / "a.json").write_text(body)
    entity = parse_config(tmp_path / "config.yml").entities[0]
    (file,) = run_entity(entity, tmp_path, "0.3").report["files"]
    assert (file["status"], file["mismatch"]["error"]["rule"]) == ("rejected", rule)


def test_append_retry_overwrites_the_rejected_part(tmp_path: Path) -> None:
    """S6 (#919): the same rejecting input run twice leaves one rejected file with the same rows."""
    config = CONFIG.format(severity="reject", mismatch="").replace(
        "    sink:\n", "    sink:\n      write_mode: append\n"
    )
    (tmp_path / "in").mkdir()
    (tmp_path / "config.yml").write_text(config)
    (tmp_path / "in" / "orders.csv").write_text("id,name\n1,a\nx,b\n")
    entity = parse_config(tmp_path / "config.yml").entities[0]
    run_entity(entity, tmp_path, "0.3")
    run_entity(entity, tmp_path, "0.3")
    (part,) = (tmp_path / "out" / "rejected").glob("*.csv")
    assert pl.read_csv(part)["__floe_row_index"].to_list() == [1]


def _json_run(
    tmp_path: Path,
    body: str,
    options: str = "{}",
    id_column: str = "{name: id, type: int64, nullable: false}",
    severity: str = "reject",
    mismatch: str = "",
) -> tuple[list[str], pl.DataFrame | None]:
    """Run one JSON file through `run_entity`; the file statuses and the accepted rows."""
    config = CONFIG.format(severity=severity, mismatch=mismatch)
    config = config.replace(
        "format: csv\n      path: ./in", "format: json\n      path: ./in"
    )
    config = config.replace(
        'options: {header: true, separator: ","}', f"options: {options}"
    )
    config = config.replace("{name: id, type: int64, nullable: false}", id_column)
    (tmp_path / "in").mkdir()
    (tmp_path / "config.yml").write_text(config)
    (tmp_path / "in" / "a.json").write_text(body, encoding="utf-8")
    entity = parse_config(tmp_path / "config.yml").entities[0]
    report = run_entity(entity, tmp_path, "0.3").report
    has_rows = any((tmp_path / "out" / "accepted").glob("*.parquet"))
    return [f["status"] for f in report["files"]], accepted(
        tmp_path
    ) if has_rows else None


def test_ndjson_probe_keeps_a_unicode_line_separator_inside_a_string(
    tmp_path: Path,
) -> None:
    """Codex P1 on #1004: the probe must split records where the reader does (LF), not on U+2028."""
    body = '{"id": 1, "name": "a\u2028b"}\n'  # the file's only record
    statuses, rows = _json_run(tmp_path, body, "{json_mode: ndjson}")
    assert statuses == ["success"]
    assert rows is not None and rows["name"].to_list() == ["a\u2028b"]


def test_ndjson_probe_trims_unicode_whitespace_like_the_reader(tmp_path: Path) -> None:
    """Codex P1 on #1004: the reader strips Unicode whitespace around a record; the probe must too."""
    body = '\u3000{"id": 1, "name": "a"}\u00a0\n'  # the file's only record
    statuses, rows = _json_run(tmp_path, body, "{json_mode: ndjson}")
    assert statuses == ["success"]
    assert rows is not None and rows["id"].to_list() == [1]


def test_a_json_key_with_an_at_sign_stays_a_declared_column(tmp_path: Path) -> None:
    """Codex P1 on #1004: `@` and `/` mark nested XML paths, not JSON keys."""
    statuses, rows = _json_run(
        tmp_path,
        '[{"@id": 1}, {"@id": 2}, {"@id": 3}]',
        id_column="{name: id, source: '@id', type: int64}",
        severity="warn",
    )
    assert statuses == ["success"]
    assert rows is not None and rows["id"].to_list() == [1, 2, 3]


def test_an_unpaired_surrogate_key_rejects_the_file_with_a_report(
    tmp_path: Path,
) -> None:
    """Codex P1 on #1004: the probe must reject it like the reader, not crash the report writer."""
    (tmp_path / "in").mkdir()
    config = CONFIG.format(severity="reject", mismatch="")
    config = config.replace(
        "format: csv\n      path: ./in", "format: json\n      path: ./in"
    )
    config = config.replace('options: {header: true, separator: ","}', "options: {}")
    (tmp_path / "config.yml").write_text(config)
    (tmp_path / "in" / "a.json").write_text('[{"id": 1, "\\ud800": 2}]')
    entity = parse_config(tmp_path / "config.yml").entities[0]
    report = run_entity(entity, tmp_path, "0.3").report
    assert [f["status"] for f in report["files"]] == ["rejected"]
    json.dumps(report, ensure_ascii=False).encode(
        "utf-8"
    )  # the report writer's encoding


def test_d40_the_json_mismatch_check_sees_every_record(tmp_path: Path) -> None:
    """D40: a key that only a later record carries is still an extra column (Rust probed the first)."""
    statuses, rows = _json_run(
        tmp_path,
        '[{"id": 1, "name": "a"}, {"id": 2, "name": "b", "extra": 1}]',
        mismatch="mismatch: {extra_columns: reject_file}",
    )
    assert statuses == ["rejected"]
    assert rows is None or rows.height == 0


def test_d41_an_empty_json_array_is_zero_rows_not_a_parse_error(tmp_path: Path) -> None:
    """D41: `[]` is a valid empty extract (Rust 0.7 rejected it as json_parse_error)."""
    statuses, rows = _json_run(tmp_path, "[]")
    assert statuses == ["success"]
    assert rows is None or rows.height == 0


def test_d41_an_ndjson_file_without_records_is_zero_rows(tmp_path: Path) -> None:
    statuses, _ = _json_run(tmp_path, "\n  \n", "{json_mode: ndjson}")
    assert statuses == ["success"]


def test_d41_records_without_keys_still_face_the_missing_columns_policy(
    tmp_path: Path,
) -> None:
    """Codex P0 on #1004: only a file with no records is exempt; `[{}, {}]` is missing every column."""
    statuses, rows = _json_run(
        tmp_path,
        "[{}, {}]",
        id_column="{name: id, type: int64}",
        mismatch="mismatch: {missing_columns: reject_file}",
    )
    assert statuses == ["rejected"]
    assert rows is None or rows.height == 0


def test_clean_small_csv_stays_within_the_polars_plan_budget(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """#1104: every eager DataFrame op is a one-node query plan, so each costs about a millisecond."""
    plans: list[str] = []
    for name in ("collect", "_collect_eager"):
        original = vars(pl.LazyFrame)[name]

        def counted(*args: Any, _original: Any = original, **kwargs: Any) -> Any:
            plans.append("plan")
            return _original(*args, **kwargs)

        monkeypatch.setattr(pl.LazyFrame, name, counted)
    rows = "".join(f"{i},n{i}\n" for i in range(200))
    report, _ = run(tmp_path, "reject", "id,name\n" + rows)
    assert report["files"][0]["status"] == "success"
    assert len(plans) <= 6, len(plans)


@pytest.mark.parametrize("clash", ["__floe_errors", "_e_cast_id", "__floe_row_index"])
def test_declared_column_named_like_an_internal_one_is_accepted(
    tmp_path: Path, clash: str
) -> None:
    (tmp_path / "in").mkdir()
    (tmp_path / "in" / "orders.csv").write_text(f"id,{clash}\n1,hello\n")
    config = CONFIG.format(severity="reject", mismatch="").replace(
        "        - {name: name, type: string}",
        f"        - {{name: {clash}, type: string}}",
    )
    (tmp_path / "config.yml").write_text(config)
    entity = parse_config(tmp_path / "config.yml").entities[0]
    report = run_entity(entity, tmp_path, "0.3").report
    assert report["files"][0]["status"] == "success"
    assert accepted(tmp_path)[clash].to_list() == ["hello"]


@pytest.mark.parametrize(("cap", "passes"), [(1000, 1), (30, 1), (29, 2)])
def test_json_file_is_parsed_once_up_to_the_single_pass_cap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cap: int, passes: int
) -> None:
    """#1115: the key probe and the read share one parse; a file over the byte cap parses twice."""
    monkeypatch.setattr(file_module, "JSON_SINGLE_PASS_BYTES", cap)
    parsed: list[str] = []
    real = json_module.json_loads

    def counting(text: str) -> Any:
        parsed.append(text)
        return real(text)

    monkeypatch.setattr(json_module, "json_loads", counting)
    body = '{"id": 1}\n{"id": 2}\n{"id": 3}\n'
    statuses, rows = _json_run(tmp_path, body, "{json_mode: ndjson}")
    assert statuses == ["success"]
    assert rows is not None and rows["id"].to_list() == [1, 2, 3]
    assert len(parsed) == 3 * passes


def test_json_file_removed_after_listing_is_a_failed_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """#1115: the size comes from the listing, so a vanished file still fails as a read error."""
    real = sources_module.resolve_inputs

    def listed_then_removed(*args: Any, **kwargs: Any) -> Any:
        inputs = real(*args, **kwargs)
        (tmp_path / "in" / "a.json").unlink()
        return inputs

    monkeypatch.setattr(sources_module, "resolve_inputs", listed_then_removed)
    (tmp_path / "in").mkdir()
    (tmp_path / "in" / "a.json").write_text('{"id": 1}')
    config = CONFIG.format(severity="reject", mismatch="")
    config = config.replace("      rejected: {format: csv, path: ./out/rejected}\n", "")
    config = config.replace(
        "format: csv\n      path: ./in", "format: json\n      path: ./in"
    )
    (tmp_path / "config.yml").write_text(config)
    entity = parse_config(tmp_path / "config.yml").entities[0]
    report = run_entity(entity, tmp_path, "0.3").report
    assert [f["status"] for f in report["files"]] == ["rejected"]
    assert "failed to read json at" in json.dumps(report["files"][0])


@pytest.mark.parametrize("fmt", ["json", "csv"])
def test_file_removed_after_listing_with_a_rejected_sink_is_a_failed_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fmt: str
) -> None:
    """#1121: the raw copy of a vanished file is skipped, so the read error is reported and the run goes on."""
    real = sources_module.resolve_inputs

    def listed_then_removed(*args: Any, **kwargs: Any) -> Any:
        inputs = real(*args, **kwargs)
        (tmp_path / "in" / f"a.{fmt}").unlink()
        return inputs

    monkeypatch.setattr(sources_module, "resolve_inputs", listed_then_removed)
    (tmp_path / "in").mkdir()
    body = '{"id": 1}' if fmt == "json" else "id\n1\n"
    (tmp_path / "in" / f"a.{fmt}").write_text(body)
    config = CONFIG.format(severity="reject", mismatch="")
    config = config.replace(
        "format: csv\n      path: ./in", f"format: {fmt}\n      path: ./in"
    )
    (tmp_path / "config.yml").write_text(config)
    entity = parse_config(tmp_path / "config.yml").entities[0]
    report = run_entity(entity, tmp_path, "0.3").report
    assert [f["status"] for f in report["files"]] == ["rejected"]
    assert "No such file" in json.dumps(report["files"][0])
    assert report["files"][0]["output"]["rejected_path"] is None
    assert not (tmp_path / "out" / "rejected").exists()


@pytest.mark.parametrize("chunk_rows", [2, 4, 1000])
@pytest.mark.parametrize("mode", ["ndjson", "array"])
def test_json_late_key_and_late_errors_are_the_same_in_every_chunking(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, chunk_rows: int, mode: str
) -> None:
    """D40 and the error order hold whether the file is one chunk (4 rows = exactly full) or several."""
    monkeypatch.setattr(file_module, "JSON_CHUNK_ROWS", chunk_rows)
    records = ['{"id": 1}', '{"id": 2}', '{"id": 3}', '{"id": 4, "late": 1}']
    options = f"{{json_mode: {mode}}}"

    def body(*lines: str) -> str:
        return "[" + ",".join(lines) + "]" if mode == "array" else "\n".join(lines)

    def run_in(name: str, text: str, **kwargs: str) -> list[str]:
        (tmp_path / name).mkdir()
        return _json_run(tmp_path / name, text, options, **kwargs)[0]

    reject = "mismatch: {extra_columns: reject_file}"
    statuses = run_in("a", body(*records), mismatch=reject)
    assert statuses == ["rejected"]
    # a non-scalar selector value in an early record: the mismatch verdict still comes first
    bad = ['{"id": {"x": 1}}', *records[1:]]
    statuses = run_in("b", body(*bad), mismatch=reject)
    assert statuses == ["rejected"]
    statuses = run_in("c", body(*bad))
    assert statuses == ["rejected"], statuses
    # a parse error in a later record fails the file whatever the chunking
    if mode == "ndjson":
        statuses = run_in("d", body(*records[:3], "nope"))
        assert statuses == ["rejected"], statuses


PII_CONFIG = CONFIG.replace(
    "    schema:\n",
    "    pii: {{columns: [{{name: name, strategy: {strategy}}}]}}\n    schema:\n",
).replace("{{name: id, type: int64, nullable: false}}", "{{name: id, type: int64, unique: true}}")  # fmt: skip


def _rejected_csv(tmp_path: Path) -> pl.DataFrame:
    (part,) = (tmp_path / "out" / "rejected").glob("*.csv")
    return pl.read_csv(part, infer_schema=False)


def test_rejected_rows_hold_the_raw_value_of_a_failed_cast(tmp_path: Path) -> None:
    """#1152: `not_a_number` stays in the rejected row; the accepted rows are typed."""
    config = CONFIG.format(severity="reject", mismatch="").replace(
        "{name: name, type: string}", "{name: amount, type: 'decimal(10,2)'}"
    )
    (tmp_path / "in").mkdir()
    (tmp_path / "in" / "orders.csv").write_text("id,amount\n1,1.50\n2,not_a_number\n")
    (tmp_path / "config.yml").write_text(config)
    entity = parse_config(tmp_path / "config.yml").entities[0]
    run_entity(entity, tmp_path, "0.3")
    assert accepted(tmp_path)["id"].to_list() == [1]
    assert accepted(tmp_path).schema["amount"] != pl.String
    rejected = _rejected_csv(tmp_path)
    assert rejected["amount"].to_list() == ["not_a_number"]
    assert rejected["__floe_row_index"].to_list() == ["1"]


@pytest.mark.parametrize(
    ("value", "kind"), [(b"alice", "string"), (timedelta(microseconds=7), "int64")]
)
def test_a_source_column_csv_cannot_hold_keeps_its_typed_value_in_the_rejected_csv(
    tmp_path: Path, value: object, kind: str
) -> None:
    """#1152 (Codex P1s): a Parquet binary or duration column cannot be written to CSV, so the rejected row keeps its typed value."""
    config = (
        CONFIG.format(severity="reject", mismatch="")
        .replace(
            'format: csv\n      path: ./in\n      options: {header: true, separator: ","}',
            "format: parquet\n      path: ./in",
        )
        .replace("{name: name, type: string}", f"{{name: name, type: {kind}}}")
    )
    (tmp_path / "in").mkdir()
    pl.DataFrame({"id": ["bad", "1"], "name": [value, value]}).write_parquet(
        tmp_path / "in" / "orders.parquet"
    )
    (tmp_path / "config.yml").write_text(config)
    entity = parse_config(tmp_path / "config.yml").entities[0]
    report = run_entity(entity, tmp_path, "0.3").report
    assert report["results"]["rejected_total"] == 1
    rejected = _rejected_csv(tmp_path)
    assert rejected["id"].to_list() == ["bad"]
    assert (
        rejected["name"].to_list()
        == accepted(tmp_path)["name"].cast(pl.String).to_list()
    )


def test_rejected_row_indices_follow_the_file_across_chunks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """#1152: null, cast and unique failures land on the same whole-file rows in any chunking."""
    config = CONFIG.format(severity="reject", mismatch="").replace(
        "{name: id, type: int64, nullable: false}",
        "{name: id, type: int64, nullable: false, unique: true}",
    )
    (tmp_path / "in").mkdir()
    (tmp_path / "config.yml").write_text(config)
    # row 1 null id, row 2 cast error, row 4 is a duplicate of row 0 (chunks of 2 rows hold rows 0-1, 2-3, 4-5)
    (tmp_path / "in" / "orders.csv").write_text(
        "id,name\n1,a\n,b\nx,c\n3,d\n1,e\n5,f\n"
    )
    entity = parse_config(tmp_path / "config.yml").entities[0]
    monkeypatch.setattr(file_module, "read_csv", partial(read_csv, chunk_rows=2))
    report = run_entity(entity, tmp_path, "0.3").report
    rejected = _rejected_csv(tmp_path)
    assert rejected["__floe_row_index"].to_list() == ["1", "2", "4"]
    assert rejected["name"].to_list() == ["b", "c", "e"]
    assert rejected["id"].to_list() == [None, "x", "1"]
    assert report["results"]["rejected_total"] == 3


@pytest.mark.parametrize("strategy", ["redact", "hash", "drop"])
def test_rejected_raw_values_of_a_pii_column_are_never_plaintext(
    tmp_path: Path, strategy: str
) -> None:
    """#1152: PII masking runs on the raw rejected frame before the write."""
    config = PII_CONFIG.format(severity="reject", mismatch="", strategy=strategy)
    (tmp_path / "in").mkdir()
    (tmp_path / "in" / "orders.csv").write_text("id,name\n1,alice\nx,bob\n")
    (tmp_path / "config.yml").write_text(config)
    entity = parse_config(tmp_path / "config.yml").entities[0]
    run_entity(entity, tmp_path, "0.3")
    (part,) = (tmp_path / "out" / "rejected").glob("*.csv")
    text = part.read_text()
    assert "bob" not in text
    assert "x" in _rejected_csv(tmp_path)["id"].to_list()
    assert ("name" in text.splitlines()[0]) == (strategy != "drop")


PQ_CONFIG = CONFIG.replace("rejected: {{format: csv", "rejected: {{format: parquet")


def _rejected_parquet(tmp_path: Path) -> pl.DataFrame:
    parts = sorted((tmp_path / "out" / "rejected" / "rows").glob("part-*.parquet"))
    return pl.concat([pl.read_parquet(p) for p in parts])


def _run_pq(tmp_path: Path, text: str, config: str = PQ_CONFIG, **fmt: str) -> Any:
    (tmp_path / "in").mkdir(exist_ok=True)
    (tmp_path / "in" / "orders.csv").write_text(text)
    (tmp_path / "config.yml").write_text(config.format(**{"severity": "reject", "mismatch": "", **fmt}))  # fmt: skip
    return run_entity(parse_config(tmp_path / "config.yml").entities[0], tmp_path, "0.3").report  # fmt: skip


def test_1154_rejected_parquet_holds_raw_values_and_metadata(tmp_path: Path) -> None:
    config = (
        PQ_CONFIG.replace("{{name: name, type: string}}", "{{name: amount, type: 'decimal(10,2)'}}")
        .replace("{{name: id, type: int64, nullable: false}}", "{{name: id, type: int64, nullable: false, unique: true}}")
        .replace("        - {{name: amount", "        - {{name: day, type: date}}\n        - {{name: amount")
    )  # fmt: skip
    report = _run_pq(tmp_path, "id,day,amount\n1,2024-01-01,1.50\n2,2024-13-45,abc\n1,2024-01-02,2.00\n,2024-01-03,3.00\n", config)  # fmt: skip
    rejected = _rejected_parquet(tmp_path)
    assert rejected.columns == ["id", "day", "amount", "__floe_row_index", "__floe_errors", "__floe_run_id", "__floe_source_uri"]  # fmt: skip
    assert rejected.schema["__floe_row_index"] == pl.UInt64
    assert rejected["id"].to_list() == ["2", "1", None]
    assert rejected["day"].to_list() == ["2024-13-45", "2024-01-02", "2024-01-03"]
    assert rejected["amount"].to_list() == ["abc", "2.00", "3.00"]
    assert rejected["__floe_row_index"].to_list() == [1, 2, 3]
    assert "cast_error" in rejected["__floe_errors"][0]
    assert "unique" in rejected["__floe_errors"][1] and "not_null" in rejected["__floe_errors"][2]  # fmt: skip
    assert rejected["__floe_source_uri"].unique().to_list() == [f"local://{tmp_path / 'in' / 'orders.csv'}"]  # fmt: skip
    assert len(set(rejected["__floe_run_id"])) == 1
    assert report["files"][0]["output"]["rejected_path"] == str(tmp_path / "out" / "rejected" / "rows")  # fmt: skip


def test_1154_rejected_parquet_spills_chunks_and_keeps_whole_file_indices(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    spilled: list[str] = []
    real = pl.DataFrame.write_ipc

    def spy(self: pl.DataFrame, file: Any, *a: Any, **k: Any) -> Any:
        spilled.append(Path(file).name)
        return real(self, file, *a, **k)  # pyright: ignore[reportUnknownVariableType]  # spy passes through the untyped args

    monkeypatch.setattr(pl.DataFrame, "write_ipc", spy)
    monkeypatch.setattr(file_module, "read_csv", partial(read_csv, chunk_rows=2))
    (tmp_path / "in").mkdir()
    (tmp_path / "in" / "a.csv").write_text("id,name\nx,a\n1,b\ny,c\n2,d\nz,e\n")
    (tmp_path / "in" / "b.csv").write_text("id,name\n1,a\nq,b\n")
    (tmp_path / "config.yml").write_text(PQ_CONFIG.format(severity="reject", mismatch=""))  # fmt: skip
    run_entity(parse_config(tmp_path / "config.yml").entities[0], tmp_path, "0.3")
    rejected = _rejected_parquet(tmp_path)
    got = {(Path(u).name, i) for u, i in zip(rejected["__floe_source_uri"], rejected["__floe_row_index"])}  # fmt: skip
    assert got == {("a.csv", 0), ("a.csv", 2), ("a.csv", 4), ("b.csv", 1)}
    assert {"rejected-0.ipc", "rejected-1.ipc", "rejected-2.ipc"} <= set(spilled)


def test_1154_rejected_parquet_keeps_native_binary(tmp_path: Path) -> None:
    config = PQ_CONFIG.replace("format: csv\n      path: ./in\n      options: {{header: true, separator: \",\"}}", "format: parquet\n      path: ./in")  # fmt: skip
    (tmp_path / "in").mkdir()
    pl.DataFrame({"id": ["bad", "1"], "name": [b"alice", b"bob"]}).write_parquet(tmp_path / "in" / "orders.parquet")  # fmt: skip
    (tmp_path / "config.yml").write_text(config.format(severity="reject", mismatch=""))
    run_entity(parse_config(tmp_path / "config.yml").entities[0], tmp_path, "0.3")
    rejected = _rejected_parquet(tmp_path)
    assert rejected["id"].to_list() == ["bad"]
    assert rejected.schema["name"] == pl.Binary
    assert rejected["name"].to_list() == [b"alice"]


def test_1154_append_retry_rewrites_the_same_parts(tmp_path: Path) -> None:
    config = PQ_CONFIG.replace("sink:\n", "sink:\n      write_mode: append\n", 1)
    _run_pq(tmp_path, "id,name\nx,a\n1,b\n", config)
    first = sorted(p.name for p in (tmp_path / "out" / "rejected" / "rows").iterdir())
    run_entity(parse_config(tmp_path / "config.yml").entities[0], tmp_path, "0.3")
    assert (
        sorted(p.name for p in (tmp_path / "out" / "rejected" / "rows").iterdir())
        == first
    )
    assert len(first) == 1 and first[0].startswith("part-")
    assert _rejected_parquet(tmp_path).height == 1


@pytest.mark.parametrize("strategy", ["redact", "hash", "drop"])
def test_1154_rejected_parquet_pii_is_never_plaintext(
    tmp_path: Path, strategy: str
) -> None:
    config = PII_CONFIG.replace(
        "rejected: {{format: csv", "rejected: {{format: parquet"
    )
    _run_pq(tmp_path, "id,name\n1,alice\nx,bob\n", config, strategy=strategy)
    (part,) = (tmp_path / "out" / "rejected" / "rows").glob("*.parquet")
    assert b"bob" not in part.read_bytes()
    assert ("name" in pl.read_parquet(part).columns) == (strategy != "drop")


@pytest.mark.parametrize("accepted", ["./out", "./out/rows"])
def test_1166_an_accepted_path_on_the_rejected_directories_is_refused(tmp_path: Path, accepted: str) -> None:  # fmt: skip
    """Codex P0s on #1166: accepted parts in `out/` would overwrite the raw file copies there, in `out/rows/` the row parts."""
    config = PQ_CONFIG.replace("path: ./out/rejected", "path: ./out").replace("path: ./out/accepted", f"path: {accepted}")  # fmt: skip
    (tmp_path / "in").mkdir()
    (tmp_path / "in" / "orders.csv").write_text("id\n1\nbad\n")
    (tmp_path / "config.yml").write_text(config.format(severity="reject", mismatch="").replace("        - {name: name, type: string}\n", ""))  # fmt: skip
    entity = parse_config(tmp_path / "config.yml").entities[0]
    with pytest.raises(FloeConfigError, match="sink.accepted.path must differ from sink.rejected.path and its rows directory"):  # fmt: skip
        run_entity(entity, tmp_path, "0.3")
    assert not (tmp_path / "out").exists()


def test_1166_raw_copies_and_row_parts_do_not_overwrite_each_other(tmp_path: Path) -> None:  # fmt: skip
    """Codex round 3: a raw `part-00000.parquet` copy and a later file's `rows/part-00000.parquet` coexist under overwrite."""
    config = PQ_CONFIG.replace("format: csv\n      path: ./in\n      options: {{header: true, separator: \",\"}}", "format: parquet\n      path: ./in").replace("      {mismatch}\n", "      mismatch: {{missing_columns: reject_file}}\n")  # fmt: skip
    (tmp_path / "in").mkdir()
    pl.DataFrame({"id": [1]}).write_parquet(tmp_path / "in" / "part-00000.parquet")
    pl.DataFrame({"id": ["x"], "name": ["n"]}).write_parquet(tmp_path / "in" / "z.parquet")  # fmt: skip
    (tmp_path / "config.yml").write_text(config.format(severity="reject"))
    entity = parse_config(tmp_path / "config.yml").entities[0]
    files = {Path(f["input_file"]).name: f["output"]["rejected_path"] for f in run_entity(entity, tmp_path, "0.3").report["files"]}  # fmt: skip
    rejected = tmp_path / "out" / "rejected"
    assert pl.read_parquet(rejected / "part-00000.parquet")["id"].to_list() == [1]
    assert pl.read_parquet(rejected / "rows" / "part-00000.parquet")["id"].to_list() == ["x"]  # fmt: skip
    assert files == {"part-00000.parquet": str(rejected / "part-00000.parquet"), "z.parquet": str(rejected / "rows")}  # fmt: skip


def _csv_rejected_parquet_inputs(tmp_path: Path, accepted: str, rejected: str) -> Any:
    config = PQ_CONFIG.replace("rejected: {{format: parquet", "rejected: {{format: csv").replace("path: ./out/rejected", f"path: {rejected}").replace("path: ./out/accepted", f"path: {accepted}").replace("format: csv\n      path: ./in\n      options: {{header: true, separator: \",\"}}", "format: parquet\n      path: ./in").replace("      {mismatch}\n", "      mismatch: {{missing_columns: reject_file}}\n")  # fmt: skip
    (tmp_path / "in").mkdir()
    pl.DataFrame({"id": [1]}).write_parquet(tmp_path / "in" / "part-00000.parquet")
    pl.DataFrame({"id": [2], "name": ["n"]}).write_parquet(tmp_path / "in" / "z.parquet")  # fmt: skip
    (tmp_path / "config.yml").write_text(config.format(severity="reject"))
    return parse_config(tmp_path / "config.yml").entities[0]


def test_1168_a_csv_rejected_sink_refuses_a_shared_accepted_path(tmp_path: Path) -> None:  # fmt: skip
    """Codex on #1166: a raw `part-00000.parquet` copy in a shared directory is overwritten by the accepted Parquet writer."""
    entity = _csv_rejected_parquet_inputs(tmp_path, "./out", "./out")
    with pytest.raises(FloeConfigError, match="sink.accepted.path must differ from sink.rejected.path"):  # fmt: skip
        run_entity(entity, tmp_path, "0.3")
    assert not (tmp_path / "out").exists()


def test_1168_a_csv_rejected_sink_at_another_path_still_runs(tmp_path: Path) -> None:  # fmt: skip
    entity = _csv_rejected_parquet_inputs(tmp_path, "./out/accepted", "./out/rejected")  # fmt: skip
    files = {Path(f["input_file"]).name for f in run_entity(entity, tmp_path, "0.3").report["files"]}  # fmt: skip
    assert files == {"part-00000.parquet", "z.parquet"}
    assert (tmp_path / "out" / "rejected" / "part-00000.parquet").exists()


def test_1166_a_parquet_rejected_sink_refuses_source_files_under_it(
    tmp_path: Path,
) -> None:
    """Codex P0 on #1166: an overwrite of the rejected Parquet parts would delete an input `part-*.parquet` there."""
    config = PQ_CONFIG.replace("format: csv\n      path: ./in\n      options: {{header: true, separator: \",\"}}", "format: parquet\n      path: ./in").replace("path: ./in", "path: ./in/rows", 1).replace("path: ./out/rejected", "path: ./in")  # fmt: skip
    (tmp_path / "in" / "rows").mkdir(parents=True)
    source = tmp_path / "in" / "rows" / "part-00000.parquet"
    pl.DataFrame({"id": ["1", "bad"], "name": ["a", "b"]}).write_parquet(source)
    (tmp_path / "config.yml").write_text(config.format(severity="reject", mismatch=""))
    entity = parse_config(tmp_path / "config.yml").entities[0]
    with pytest.raises(FloeConfigError, match="lies under sink.rejected.path"):
        run_entity(entity, tmp_path, "0.3")
    assert pl.read_parquet(source)["id"].to_list() == ["1", "bad"]
