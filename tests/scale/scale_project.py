"""#558: a generated Floe project of N small entities, so the scale suite commits a script and not hundreds of YAML files.

The layout is the split project of `example/project/`: a root `config.yml`, `sources.yml` with three named sources,
and one `domains/<domain>/_domain.yml` (defaults) plus one `<entity>.yml` per entity. The output is a pure function of
the arguments: the same call twice writes byte-identical files.

    python tests/scale/scale_project.py OUT_DIR [ENTITIES]
"""

import json
import sys
from pathlib import Path

import polars as pl

DOMAINS = ("sales", "finance", "hr", "ops", "marketing")
FORMATS = ("csv", "json", "parquet")
FILES = 2  # input files per entity
ROWS = 10  # rows per input file
REJECTS = (1, 2)  # rows with a null id in each file: an entity rejects 3 of its 20 rows
REJECTED = sum(REJECTS)
ACCEPTED = FILES * ROWS - REJECTED

ROOT = """\
version: "0.9"
metadata: {{project: scale-{n}}}
report: {{path: ./report}}
{extra}include:
  sources: sources.yml
  domains: ["domains/*/_domain.yml"]
"""
SOURCES = """\
sources:
  - {name: csv_drop, format: csv, path: "./in/csv/{{resource}}", options: {header: true, separator: ","}}
  - {name: json_drop, format: json, path: "./in/json/{{resource}}"}
  - {name: parquet_drop, format: parquet, path: "./in/parquet/{{resource}}"}
"""
DOMAIN = """\
name: {name}
defaults:
  incremental_mode: file
  sink:
    write_mode: append
    accepted: {accepted}
    rejected: {{format: csv, path: "./out/rejected/{{{{domain.name}}}}/{{{{entity.name}}}}"}}
  policy: {{severity: reject}}
"""
PARQUET = '{format: parquet, path: "./out/accepted/{{domain.name}}/{{entity.name}}"}'
ICEBERG = (
    '{format: iceberg, path: "tables/{{domain.name}}/{{entity.name}}", storage: lake,'
    " iceberg: {catalog: polaris}}"
)
ENTITY = """\
name: {name}
source: {{ref: {fmt}_drop}}
schema:
  columns:
    - {{name: id, type: int64, nullable: false}}
    - {{name: name, type: string}}
    - {{name: amount, type: float64}}
"""


def write_input(path: Path, fmt: str, ids: list[int | None]) -> None:
    frame = pl.DataFrame(
        {"id": ids, "name": [f"n{i}" for i in range(len(ids))], "amount": [i / 4 for i in range(len(ids))]},
        schema={"id": pl.Int64, "name": pl.String, "amount": pl.Float64},
    )  # fmt: skip
    if fmt == "csv":
        frame.write_csv(path)
    elif fmt == "json":
        path.write_text(json.dumps(frame.to_dicts()))
    else:
        frame.write_parquet(path)


def generate(
    out: Path,
    entities: int = 200,
    *,
    domains: int = len(DOMAINS),
    tag: str = "",
    iceberg: str = "",
) -> list[str]:
    """Write the project under `out` and return its entity ids (`domain.name`) in config order: sorted.

    `tag` prefixes the domain names; `iceberg` is extra root YAML (storages and catalogs) that switches the
    domain defaults to an Iceberg accepted sink.
    """
    names = [
        f"{tag}{DOMAINS[d % len(DOMAINS)]}{d // len(DOMAINS) or ''}"
        for d in range(domains)
    ]
    (out / "domains").mkdir(parents=True)
    (out / "config.yml").write_text(ROOT.format(n=entities, extra=iceberg))
    (out / "sources.yml").write_text(SOURCES)
    for name in names:
        (out / "domains" / name).mkdir()
        (out / "domains" / name / "_domain.yml").write_text(DOMAIN.format(name=name, accepted=ICEBERG if iceberg else PARQUET))  # fmt: skip
    ids: list[str] = []
    for k in range(entities):
        domain, fmt = names[k % domains], FORMATS[k % len(FORMATS)]
        name = f"{fmt}_{k:03d}"
        (out / "domains" / domain / f"{name}.yml").write_text(
            ENTITY.format(name=name, fmt=fmt)
        )
        source = out / "in" / fmt / name
        source.mkdir(parents=True)
        for f in range(FILES):
            rows: list[int | None] = list(
                range(k * 100 + f * ROWS, k * 100 + (f + 1) * ROWS)
            )
            for at in range(REJECTS[f]):
                rows[at * 5 + 3] = None
            write_input(source / f"part-{f}.{fmt}", fmt, rows)
        ids.append(f"{domain}.{name}")
    return sorted(
        ids
    )  # domain files load in name order, then each domain's entity files


if __name__ == "__main__":
    generate(Path(sys.argv[1]), int(sys.argv[2]) if len(sys.argv) > 2 else 200)
