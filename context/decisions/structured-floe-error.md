# Decision — Structured `FloeError` enum (archived)

Archived: this Rust-era decision (the `thiserror` `FloeError` enum in `floe-core`) is kept with the Rust code at [OpenLakeForge/floe-rust](https://github.com/OpenLakeForge/floe-rust). The Python core raises the typed exceptions in `src/floe/errors.py` (`FloeError` and its `FloeConfigError`, `FloeRunError`, `FloeStorageError`, `FloeIoError` subclasses); see `context/coding-standards.md`.
