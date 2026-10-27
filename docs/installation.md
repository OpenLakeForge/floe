# Installation

Floe is a pure-Python package (`floe-python` on PyPI) that installs the `floe` command and the
`floe` Python library. It is also published as a Docker image.

## pip (all platforms)

Requires Python 3.11 or later. Install into its own virtual environment: Floe pins `duckdb` and
needs recent `polars`, `pyarrow` and `pyiceberg` releases, which can conflict with other tools
in a shared environment.

```bash
python -m venv .venv
. .venv/bin/activate
pip install floe-python
floe --version
```

With [uv](https://docs.astral.sh/uv/):

```bash
uv tool install floe-python   # the `floe` command only
uv add floe-python            # as a library dependency of your project
```

The wheel is `py3-none-any`: there is no native extension to build.

## Docker (GHCR)

`ghcr.io/openlakeforge/floe` is a multi-arch image (linux/amd64 and linux/arm64) built from
`Dockerfile.python`: `python:3.11-slim`, a locked virtual environment, a non-root user, and
`floe` as the entrypoint. Tags are `<version>` and `latest`.

```bash
docker pull ghcr.io/openlakeforge/floe:latest
docker run --rm -v "$PWD:/work" ghcr.io/openlakeforge/floe:latest run -c /work/config.yml
```

The DuckDB `avro`, `iceberg`, `httpfs` and `aws` extensions are preinstalled for the pinned
`duckdb`, so Iceberg merges work without network access to the DuckDB extension repository.

Provide cloud credentials through environment variables or the runtime identity; never bake
them into the image.

To build the image locally:

```bash
docker build -f Dockerfile.python -t floe .
```

## From a checkout

For development (see [`CONTRIBUTING.md`](../CONTRIBUTING.md)):

```bash
git clone https://github.com/OpenLakeForge/floe
cd floe
uv sync --all-groups
uv run floe --version
```

## Earlier releases

Floe 0.7 and earlier were a Rust binary distributed through Homebrew, Scoop, crates.io and a
`floe-duckdb` companion. Floe 0.8 publishes none of these. The Rust code is archived at
[OpenLakeForge/floe-rust](https://github.com/OpenLakeForge/floe-rust).
