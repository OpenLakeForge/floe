"""Spike S3 (#599): a minimal Python/Polars validation path that mirrors Rust `floe run` 0.7.0.

Throwaway spike code. Supports only what the parity fixtures and the benchmark use:
CSV input (file or directory), `policy.severity: reject`, default mismatch policies
(fill_nulls / ignore), cast + not_null + unique (per file and across files, composite keys),
PII `hash`, Parquet accepted output, CSV rejected output, and the run report.
Anything else raises NotImplementedError rather than guessing.

Usage: python floe_py.py run -c config.yml
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
import yaml

import pii_hash

TOOL_VERSION = "0.7.0"  # the Rust release this path is diffed against
DEFAULT_MAX_SIZE_PER_FILE = 256 * 1024 * 1024
SMALL_FILE_THRESHOLD = DEFAULT_MAX_SIZE_PER_FILE // 2
UNIQUE_SAMPLE_LIMIT = 5
STRING_TYPES = {"string", "str", "text"}
DTYPES = {
    "boolean": pl.Boolean, "bool": pl.Boolean,
    "int8": pl.Int8, "int16": pl.Int16, "int32": pl.Int32,
    "int64": pl.Int64, "int": pl.Int64, "integer": pl.Int64, "long": pl.Int64,
    "uint8": pl.UInt8, "uint16": pl.UInt16, "uint32": pl.UInt32, "uint64": pl.UInt64,
    "float32": pl.Float32,
    "float64": pl.Float64, "float": pl.Float64, "double": pl.Float64, "number": pl.Float64, "decimal": pl.Float64,
    "date": pl.Date,
    "datetime": pl.Datetime("ms"), "timestamp": pl.Datetime("ms"),
}


def norm_type(t: str) -> str:
    return t.lower().replace("-", "").replace("_", "")


def json_escape(s: str) -> str:
    return s.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t")


def row_error(rule: str, column: str, message: str) -> str:
    return f'{{"rule":"{json_escape(rule)}","column":"{json_escape(column)}","message":"{json_escape(message)}"}}'


# --- casts -------------------------------------------------------------------------------------
# Rust 0.7.0 reads every column as a string, then casts with Polars 0.52 `CastOptions::NonStrict`.
# Polars 2.0 (Python) removed string -> date/datetime casts, so those two are re-expressed here
# to match the 0.52 kernels (polars-compute 0.52 `cast/binview_to.rs`, `cast/temporal.rs`):
#   date:     chrono `NaiveDate::from_str` (leading/trailing spaces and a sign allowed);
#   datetime: chrono partial parse of "%Y-%m-%dT%H:%M:%S%.f%:z" where any parsed prefix with at
#             least hour and minute is accepted, the offset is ignored, and second 60 rolls over.
# ponytail: regex emulation of chrono, verified on fixtures/casts only. Years outside 0..9999
# and other exotic inputs are untested; a real port needs a conformance table of chrono cases.
DATE_RE = r"^\s*([+-]?\d{1,4})-\s*(\d{1,2})-\s*(\d{1,2})\s*$"
DATETIME_RE = r"^\s*([+-]?\d{1,4})-(\d{1,2})-(\d{1,2})T(\d{1,2}):(\d{1,2})(?::(\d{1,2})(?:\.(\d{1,9}))?)?"


def pad_ymd(groups: pl.Expr) -> pl.Expr:
    year = groups.struct.field("1").str.strip_chars_start("+").str.zfill(4)
    return pl.format("{}-{}-{}", year, groups.struct.field("2").str.zfill(2), groups.struct.field("3").str.zfill(2))


def cast_expr(name: str, ctype: str) -> pl.Expr:
    t = norm_type(ctype)
    c = pl.col(name)
    if t == "boolean" or t == "bool":
        v = c.str.strip_chars().str.to_lowercase()
        return pl.when(v.is_in(["true", "1"])).then(True).when(v.is_in(["false", "0"])).then(False).otherwise(None).alias(name)
    if t == "date":
        return pad_ymd(c.str.extract_groups(DATE_RE)).str.to_date("%Y-%m-%d", strict=False).alias(name)
    if t in ("datetime", "timestamp"):
        g = c.str.extract_groups(DATETIME_RE)
        f = lambda i: g.struct.field(str(i))  # noqa: E731 - local shorthand for the regex groups
        base = pl.format("{} {}:{}:00", pad_ymd(g), f(4).str.zfill(2), f(5).str.zfill(2)).str.to_datetime(
            "%Y-%m-%d %H:%M:%S", time_unit="ms", strict=False
        )
        sec = f(6).cast(pl.Int64, strict=False).fill_null(0)
        millis = f(7).str.slice(0, 3).str.pad_end(3, "0").cast(pl.Int64, strict=False).fill_null(0)
        ok = base.is_not_null() & (sec <= 60)
        return pl.when(ok).then(base + pl.duration(seconds=sec, milliseconds=millis)).alias(name)
    if t in DTYPES:
        return c.cast(DTYPES[t], strict=False).alias(name)
    raise NotImplementedError(f"column type {ctype}")


# --- config ------------------------------------------------------------------------------------
def load_config(path: Path) -> dict:
    cfg = yaml.safe_load(path.read_text())
    for entity in cfg["entities"]:
        sev = entity.get("policy", {}).get("severity")
        if sev != "reject":
            raise NotImplementedError(f"policy.severity={sev}")
        if entity["source"].get("format") != "csv":
            raise NotImplementedError("only csv sources")
        if entity["schema"].get("mismatch") or entity["schema"].get("normalize_columns"):
            raise NotImplementedError("schema.mismatch / normalize_columns")
        if entity["sink"].get("write_mode", "overwrite") != "overwrite":
            raise NotImplementedError("only write_mode overwrite")
    return cfg


def unique_constraints(schema: dict) -> list[list[str]]:
    # check::resolve_schema_unique_keys: unique_keys replaces per-column `unique`, primary_key is added.
    keys = schema.get("unique_keys")
    if keys is None:
        keys = [[c["name"]] for c in schema["columns"] if c.get("unique") is True]
    out: list[list[str]] = []
    for key in keys + ([schema["primary_key"]] if schema.get("primary_key") else []):
        key = [k.strip() for k in key]
        if key and key not in out:
            out.append(key)
    return out


def required_columns(schema: dict) -> list[str]:
    cols = [c["name"] for c in schema["columns"] if c.get("nullable") is False]
    for k in schema.get("primary_key") or []:
        if k not in cols:
            cols.append(k)
    return cols


def source_options(opts: dict) -> dict:
    return {
        "encoding": opts.get("encoding", "UTF8"),
        "header": opts.get("header", True),
        "null_values": opts.get("null_values", [""]),
        "recursive": opts.get("recursive", False),
        "separator": opts.get("separator", ";"),
    }


def resolve_inputs(path: Path) -> tuple[str, list[Path]]:
    if path.is_dir():
        return "directory", sorted(p for p in path.glob("*.csv") if p.is_file())
    return "file", [path]


def uri(p: Path) -> str:
    return f"local://{p}"


# --- unique ------------------------------------------------------------------------------------
class UniqueState:
    """Cross-file state for one constraint: keys seen so far and duplicate counts per key."""

    def __init__(self, columns: list[str]):
        self.columns = columns
        self.seen: pl.DataFrame | None = None
        self.dup_counts: list[pl.DataFrame] = []
        self.duplicates = 0

    def duplicate_mask(self, typed: pl.DataFrame) -> pl.Series:
        keys = typed.select(self.columns)
        valid = pl.all_horizontal(pl.col(self.columns).is_not_null())
        flags = keys.select(
            valid.alias("valid"),
            (valid & ~pl.struct(self.columns).is_first_distinct()).alias("dup"),
        )
        dup = flags["dup"]
        if self.seen is not None:
            in_seen = keys.join(self.seen.with_columns(pl.lit(True).alias("__seen")), on=self.columns, how="left", maintain_order="left")["__seen"]
            dup = dup | (flags["valid"] & in_seen.fill_null(False))
        first = keys.filter(flags["valid"] & ~dup)
        self.seen = first if self.seen is None else pl.concat([self.seen, first])
        dups = keys.filter(dup)
        self.duplicates += dups.height
        if dups.height:
            self.dup_counts.append(dups.group_by(self.columns).len("count"))
        return dup

    def report(self) -> dict:
        samples = []
        if self.dup_counts:
            counts = pl.concat(self.dup_counts).group_by(self.columns).agg(pl.col("count").sum())
            # Rust orders ties by the Debug string of its CompositeKey enum.
            schema = counts.schema
            debug = pl.format("CompositeKey([{}])", pl.concat_str([debug_expr(c, schema[c]) for c in self.columns], separator=", "))
            top = counts.with_columns(debug.alias("__debug")).sort(["count", "__debug"], descending=[True, False]).head(UNIQUE_SAMPLE_LIMIT)
            for row in top.iter_rows(named=True):
                values = {c: value_string(row[c]) for c in sorted(self.columns)}
                samples.append({"values": values, "count": row["count"]})
        dupes = self.duplicates
        # Rust skips zero counts and empty samples when serializing (report/mod.rs UniqueConstraintReport).
        out: dict = {"columns": self.columns, "duplicates_count": dupes}
        if dupes:
            out["batch_duplicates_count"] = dupes
        out |= {
            "affected_rows_count": dupes,
            "action": "reject_rows" if dupes else "none",
            "status_effect": "rows_rejected" if dupes else "none",
        }
        if samples:
            out["samples"] = samples
        return out


def debug_expr(name: str, dtype: pl.DataType) -> pl.Expr:
    c = pl.col(name)
    if dtype == pl.String:
        # ponytail: escapes only \\ and \" like Rust's str Debug for printable text; control
        # characters would need Rust's \u{..} escapes.
        return pl.format('String("{}")', c.str.replace_all("\\", "\\\\", literal=True).str.replace_all('"', '\\"', literal=True))
    if dtype == pl.Boolean:
        return pl.format("Bool({})", c.cast(pl.String))
    if dtype.is_signed_integer():
        return pl.format("I64({})", c.cast(pl.String))
    if dtype.is_unsigned_integer():
        return pl.format("U64({})", c.cast(pl.String))
    raise NotImplementedError(f"unique key of type {dtype}")


def value_string(v) -> str:
    if isinstance(v, bool):
        return "true" if v else "false"
    return str(v)


# --- validation --------------------------------------------------------------------------------
def validate_file(typed: pl.DataFrame, raw: pl.DataFrame, columns: list[dict], required: list[str], uniques: list[UniqueState]):
    """Returns (accept mask, __floe_errors column, rule summaries) for one file.

    Error order per row matches Rust: not_null (required columns), cast_error (declared
    non-string columns), then unique (constraint order). Unique state carries across files.
    """
    checks: list[tuple[str, str, pl.Series]] = []  # (rule, column label, error mask)
    masks = typed.select(
        [pl.col(c).is_null().alias(f"nn:{c}") for c in required]
        + [
            (raw[c["name"]].is_not_null() & pl.col(c["name"]).is_null()).alias(f"cast:{c['name']}")
            for c in columns
            if norm_type(c["type"]) not in STRING_TYPES
        ]
    )
    for c in required:
        checks.append(("not_null", c, masks[f"nn:{c}"]))
    for c in columns:
        if norm_type(c["type"]) not in STRING_TYPES:
            checks.append(("cast_error", c["name"], masks[f"cast:{c['name']}"]))
    for u in uniques:
        label = u.columns[0] if len(u.columns) == 1 else f"[{','.join(u.columns)}]"
        checks.append(("unique", label, u.duplicate_mask(typed)))

    messages = {"not_null": "required value missing", "cast_error": "invalid value for target type"}
    flags = pl.DataFrame([m.rename(f"e{i}") for i, (_, _, m) in enumerate(checks)])
    ok = ~pl.any_horizontal(pl.all()) if checks else pl.lit(True)
    fragments = []
    for i, (rule, label, _) in enumerate(checks):
        msg = messages.get(rule) or ("duplicate value" if not label.startswith("[") else "duplicate key")
        fragments.append(pl.when(pl.col(f"e{i}")).then(pl.lit(row_error(rule, label, msg))))
    errors = pl.format("[{}]", pl.concat_str(fragments, separator=",", ignore_nulls=True)) if fragments else pl.lit(None, pl.String)
    out = flags.select(ok.alias("ok"), pl.when(~ok).then(errors).alias("__floe_errors")) if checks else None
    accept = out["ok"] if out is not None else pl.Series("ok", [True] * typed.height)
    errors_col = out["__floe_errors"] if out is not None else pl.Series("__floe_errors", [None] * typed.height, dtype=pl.String)

    # report::build::summarize_validation_exprs: rules in fixed order, columns sorted by name.
    types = {c["name"]: c["type"] for c in columns}
    counts = flags.select(pl.all().sum()).row(0) if checks else ()
    rules = []
    for rule in ("not_null", "cast_error", "unique"):
        cols: dict[str, int] = {}
        for (r, label, _), n in zip(checks, counts):
            if r == rule and n:
                cols[label] = cols.get(label, 0) + n
        if cols:
            rules.append({
                "rule": rule,
                "severity": "reject",
                "violations": sum(cols.values()),
                "columns": [
                    {"column": k, "violations": v, **({"target_type": types[k]} if rule == "cast_error" else {})}
                    for k, v in sorted(cols.items())
                ],
            })
    return accept, errors_col, rules


# --- writes ------------------------------------------------------------------------------------
def write_accepted(df: pl.DataFrame, target: Path) -> dict:
    # Mirrors ParquetSinkFormat::write: split into parts of max_size_per_file / avg row size.
    # ponytail: one flush per entity. Rust flushes its accepted buffer every 256 MB across files;
    # identical for single-file inputs and small fixtures, not for large multi-file entities.
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True)
    rows = df.height
    parts: list[str] = []
    sizes: list[int] = []
    if rows:
        avg_row = max(1, -(-df.estimated_size() // rows))
        max_rows = max(1, DEFAULT_MAX_SIZE_PER_FILE // avg_row)
        chunks = [df.slice(o, max_rows) for o in range(0, rows, max_rows)]
    else:
        chunks = [df]
    for chunk in chunks:
        name = f"part-{len(parts):05d}.parquet"
        chunk.write_parquet(target / name)
        parts.append(name)
        sizes.append((target / name).stat().st_size)
    total = sum(sizes)
    return {
        "files_written": len(parts),
        "parts_written": len(parts),
        "part_files": parts[:50],
        "total_bytes_written": total,
        "avg_file_size_mb": (total / len(parts)) / (1024 * 1024),
        "small_files_count": sum(1 for s in sizes if s < SMALL_FILE_THRESHOLD),
    }


def apply_pii(df: pl.DataFrame, pii: dict | None) -> pl.DataFrame:
    if not pii:
        return df
    for col in pii["columns"]:
        if col["strategy"] != "hash":
            raise NotImplementedError(f"pii strategy {col['strategy']}")
        if col["name"] not in df.columns:
            continue
        key = col.get("key")
        if key and "${" in key:
            key = os.environ[key[2:-1]]
        values = df[col["name"]].cast(pl.String)
        df = df.with_columns(pii_hash.hash_column(values, key))
    return df


# --- run ---------------------------------------------------------------------------------------
def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def run_entity(entity: dict, base: Path) -> dict:
    schema = entity["schema"]
    columns = schema["columns"]
    declared = [c["name"] for c in columns]
    required = required_columns(schema)
    uniques = [UniqueState(k) for k in unique_constraints(schema)]
    src = entity["source"]
    opts = source_options(src.get("options") or {})
    source_path = (base / src["path"]).resolve()
    mode, files = resolve_inputs(source_path)
    accepted_target = (base / entity["sink"]["accepted"]["path"]).resolve()
    rejected_target = (base / entity["sink"]["rejected"]["path"]).resolve()
    if rejected_target.exists():
        shutil.rmtree(rejected_target)

    accepted_frames: list[pl.DataFrame] = []
    file_reports = []
    totals = {"files_total": 0, "files_skipped": 0, "rows_total": 0, "accepted_total": 0, "rejected_total": 0, "warnings_total": 0, "errors_total": 0}
    for path in files:
        raw = pl.read_csv(
            path,
            has_header=opts["header"],
            separator=opts["separator"],
            null_values=opts["null_values"],
            infer_schema=False,
        )
        header = raw.columns
        missing = sorted(set(declared) - set(header))
        extra = sorted(set(header) - set(declared))
        raw = raw.select([pl.col(c) if c in header else pl.lit(None, pl.String).alias(c) for c in declared])
        typed = raw.select([pl.col(c["name"]) if norm_type(c["type"]) in STRING_TYPES else cast_expr(c["name"], c["type"]) for c in columns])

        ok, errors, rules = validate_file(typed, raw, columns, required, uniques)
        del raw
        rows = typed.height
        accept_count = int(ok.sum())
        reject_count = rows - accept_count
        violations = sum(r["violations"] for r in rules)

        accepted_frames.append(apply_pii(typed.filter(ok), entity.get("pii")))
        rejected_path = None
        if reject_count:
            rejected = typed.with_row_index("__floe_row_index").filter(~ok)
            rejected = rejected.select(declared + [pl.col("__floe_row_index").cast(pl.UInt64)]).with_columns(errors.filter(~ok))
            rejected = apply_pii(rejected, entity.get("pii"))
            rejected_target.mkdir(parents=True, exist_ok=True)
            # Rust: first rejected write of the run uses the overwrite allocator, later ones append (uuid parts).
            name = "part-00000.csv" if not any(rejected_target.iterdir()) else f"part-{uuid.uuid4()}.csv"
            rejected.write_csv(rejected_target / name)
            rejected_path = str(rejected_target / name)

        action = "filled_nulls" if missing else ("ignored_extras" if extra else "none")
        file_reports.append({
            "input_file": uri(path),
            "status": "rejected" if reject_count else "success",
            "row_count": rows,
            "accepted_count": accept_count,
            "rejected_count": reject_count,
            "mismatch": {
                "declared_columns_count": len(declared),
                "input_columns_count": len(header),
                "missing_columns": missing[:50],
                "extra_columns": extra[:50],
                "mismatch_action": action,
            },
            "output": {"accepted_path": uri(accepted_target), "rejected_path": rejected_path, "errors_path": None, "archived_path": None},
            "validation": {"errors": violations if reject_count else 0, "warnings": 0, "rules": rules},
        })
        totals["files_total"] += 1
        totals["rows_total"] += rows
        totals["accepted_total"] += accept_count
        totals["rejected_total"] += reject_count
        totals["errors_total"] += violations if reject_count else 0

    accepted_df = pl.concat(accepted_frames) if accepted_frames else pl.DataFrame()
    write_out = write_accepted(accepted_df, accepted_target)
    report = {
        "spec_version": "0.3",
        "entity": {"name": f"{entity['domain']}.{entity['name']}"},
        "source": {
            "format": "csv",
            "path": uri(source_path),
            "options": opts,
            "cast_mode": src.get("cast_mode", "strict"),
            "read_plan": "raw_and_typed",
            "resolved_inputs": {"mode": mode, "file_count": len(files), "files": [uri(p) for p in files]},
        },
        "sink": {
            "accepted": {"format": "parquet", "path": uri(accepted_target)},
            "rejected": {"format": "csv", "path": uri(rejected_target)},
            "archive": {"enabled": False},
        },
        "policy": {"severity": "reject"},
        "accepted_output": {
            "path": uri(accepted_target),
            "table_root_uri": uri(accepted_target),
            "write_mode": "overwrite",
            "accepted_rows": accepted_df.height,
            **write_out,
        },
        "schema_evolution": {"enabled": False, "mode": "strict", "applied": False, "incompatible_changes_detected": False},
    }
    if uniques:
        report["unique_constraints"] = [u.report() for u in uniques]
    report["results"] = totals
    report["files"] = file_reports
    return report


def run(config_path: Path) -> int:
    started = now_iso()
    t0 = time.monotonic()
    config_path = config_path.resolve()
    base = config_path.parent
    cfg = load_config(config_path)
    run_id = started.replace(":", "-")
    report_base = (base / cfg["report"]["path"]).resolve()
    run_dir = report_base / f"run_{run_id}"
    entity_summaries = []
    overall = {"files_total": 0, "files_skipped": 0, "rows_total": 0, "accepted_total": 0, "rejected_total": 0, "warnings_total": 0, "errors_total": 0}
    for entity in cfg["entities"]:
        report = run_entity(entity, base)
        status = "rejected" if any(f["status"] == "rejected" for f in report["files"]) else "success"
        out = run_dir / entity["domain"] / entity["name"] / "run.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(report, indent=2, ensure_ascii=False))
        for k in overall:
            overall[k] += report["results"][k]
        entity_summaries.append({"name": report["entity"]["name"], "status": status, "results": report["results"], "report_file": str(out)})
    status = "rejected" if any(e["status"] == "rejected" for e in entity_summaries) else "success"
    summary = {
        "spec_version": "0.3",
        "tool": {"name": "floe", "version": TOOL_VERSION},
        "run": {
            "run_id": run_id,
            "started_at": started,
            "finished_at": now_iso(),
            "duration_ms": int((time.monotonic() - t0) * 1000),
            "status": status,
            "exit_code": 0,
        },
        "config": {"path": str(config_path), "version": str(cfg.get("version"))},
        "report": {"path": str(report_base), "report_file": str(run_dir / "run.summary.json")},
        "results": overall,
        "entities": entity_summaries,
    }
    (run_dir / "run.summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False))
    print(f"Totals: rows={overall['rows_total']} accepted={overall['accepted_total']} rejected={overall['rejected_total']}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("run")
    p.add_argument("-c", "--config", type=Path, required=True)
    args = parser.parse_args()
    return run(args.config)


if __name__ == "__main__":
    sys.exit(main())
