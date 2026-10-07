# Installation

Floe ships as a single CLI binary (`floe`). Choose the method that matches your OS.

## macOS / Linux — Homebrew (recommended)

If you don't have Homebrew yet, install it from [brew.sh](https://brew.sh).

```bash
brew tap malon64/floe
brew install floe
floe --version
```

## Windows — Scoop (recommended)

If you don't have Scoop yet, install it from [scoop.sh](https://scoop.sh).

```powershell
scoop bucket add floe https://github.com/malon64/scoop-floe
scoop install floe
floe --version
```

## Prebuilt binary (all platforms)

Download the archive for your platform from [GitHub Releases](https://github.com/OpenLakeForge/floe/releases),
extract it, and place the `floe` (or `floe.exe`) binary somewhere on your `PATH`.

| Platform | Archive |
|---|---|
| macOS arm64 (Apple Silicon) | `floe-vX.Y.Z-aarch64-apple-darwin.tar.gz` |
| macOS x86_64 | `floe-vX.Y.Z-x86_64-apple-darwin.tar.gz` |
| Linux x86_64 | `floe-vX.Y.Z-x86_64-unknown-linux-gnu.tar.gz` |
| Linux arm64 | `floe-vX.Y.Z-aarch64-unknown-linux-gnu.tar.gz` |
| Windows x86_64 | `floe-vX.Y.Z-x86_64-pc-windows-msvc.zip` |

## Cargo (from source)

Prereqs:
- Rust toolchain (stable) — install from [rustup.rs](https://rustup.rs)
- A C/C++ compiler available in `PATH`

```bash
cargo install floe-cli
floe --version
```

To build from a local checkout instead:

```bash
cargo build --release
./target/release/floe --version
```

## Docker (GHCR)

Floe is published as a multi-arch Docker image (linux/amd64 and linux/arm64).

```bash
docker pull ghcr.io/openlakeforge/floe:latest
```

Run (mount the current directory to `/work`):

```bash
docker run --rm -v "$PWD:/work" ghcr.io/openlakeforge/floe:latest run -c /work/example/config.yml
```

Cloud credentials should be provided via environment variables or runtime identity — not baked into the image.

## DuckDB support (companion distribution)

The default `floe` CLI, Docker image, and PyPI wheel are **lean**: they include the
Parquet, Delta, and Iceberg sinks but **not** the DuckDB sink. DuckDB bundles a large
native (C++) build that does not cross-compile in the release pipeline and exceeds
PyPI's per-file size limit, so it ships as a separate **companion** that the lean
`floe` transparently delegates to. There are three ways to get it:

### 1. Docker image (recommended)

```bash
docker pull ghcr.io/openlakeforge/floe-duckdb:latest
docker run --rm -v "$PWD:/work" ghcr.io/openlakeforge/floe-duckdb:latest run -c /work/config.yml
```

This image is multi-arch (linux/amd64 + linux/arm64) and runs DuckDB sinks directly.

### 2. Companion CLI binary (`floe-duckdb`)

Prebuilt `floe-duckdb` binaries are available for macOS and Linux via
GitHub Releases and Homebrew. `floe-duckdb` is a **complete, standalone CLI**
(same commands, same config format as `floe`) with DuckDB compiled in.

**Homebrew (macOS + Linux):**

```bash
brew tap malon64/floe   # skip if already tapped for the lean `floe`
brew install malon64/floe/floe-duckdb
```

> **Windows:** The prebuilt `floe-duckdb` binary is not yet available on Windows
> (the bundled C++ DuckDB build fails with MSVC). Windows users can use the Docker
> image (`docker run ghcr.io/openlakeforge/floe-duckdb`) or build from source:
> `cargo build -p floe-cli --release --features duckdb`.

**Prebuilt binary (macOS + Linux):**

Download from [GitHub Releases](https://github.com/OpenLakeForge/floe/releases):

| Platform | Archive |
|---|---|
| macOS arm64 (Apple Silicon) | `floe-duckdb-vX.Y.Z-aarch64-apple-darwin.tar.gz` |
| macOS x86_64 | `floe-duckdb-vX.Y.Z-x86_64-apple-darwin.tar.gz` |
| Linux x86_64 | `floe-duckdb-vX.Y.Z-x86_64-unknown-linux-gnu.tar.gz` |
| Linux arm64 | `floe-duckdb-vX.Y.Z-aarch64-unknown-linux-gnu.tar.gz` |

**Usage:** either call `floe-duckdb` directly (`floe-duckdb run config.yml`), or place
it alongside the lean `floe` binary (same directory) or anywhere on your `PATH` — `floe`
auto-detects a DuckDB sink in your config and re-executes `floe-duckdb` with the same
arguments, with no user action required.

> For security, the companion is resolved **only** from `PATH` or the directory of the
> running `floe` executable — never the current working directory.

**Build from source** (if you need a custom feature set):

```bash
cargo build -p floe-cli --release --features duckdb
cp target/release/floe target/release/floe-duckdb   # place on PATH next to floe
```

### 3. Python — off-PyPI `floe-duckdb` wheel

Install the lean `floe` package from PyPI, then add the DuckDB companion wheel from the
off-PyPI index (the companion is too large for PyPI):

```bash
pip install floe-python
pip install floe-duckdb --index-url https://openlakeforge.github.io/floe/simple/
```

> Use `--index-url` (not `--extra-index-url`) for the companion: `floe-duckdb` is
> intentionally **not** on PyPI, and `--extra-index-url` would keep PyPI in play,
> letting a squatted or higher-version `floe-duckdb` there shadow the real
> off-PyPI wheel (dependency confusion). The companion's `floe-python` dependency
> is already satisfied by the previous step, so consulting only the off-PyPI index
> for the companion install resolves cleanly.

The companion installs a `floe._floe_duckdb` extension into the same `floe` package.
`floe.run(...)` then transparently delegates a DuckDB-sink run to it; without the
companion installed, such a run raises a `FloeConfigError` with install instructions.

> DuckLake and remote-DuckDB (Quack) sinks are not yet available — they require a
> DuckDB / Arrow upgrade that is currently blocked upstream.

## Troubleshooting

- **`brew` not found**: install Homebrew from [brew.sh](https://brew.sh), then retry.
- **`scoop` not found**: install Scoop from [scoop.sh](https://scoop.sh), then retry.
- **Cargo compilation fails**: ensure you have a recent stable Rust toolchain (`rustup update stable`) and a system C compiler installed.
