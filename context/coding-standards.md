# Floe — Coding Standards

Applies to `src/floe/`, `tests/` and `conformance/` (Python >= 3.11, managed with `uv`).
Context: `context/architecture.md` and `context/decisions/python-core-for-olf.md`.

## Tooling

- **Types:** `pyright` runs in strict mode. No `Any` where a real type exists, no untyped defs,
  no `# type: ignore` or `# pyright: ignore` without an inline reason.
- **Lint/format:** `ruff check` and `ruff format --check` must pass. No `# noqa` without an
  inline reason.
- The canonical commands live in `AGENTS.md` §5 and mirror `.github/workflows/ci.yml`.

## Error handling

- Raise the typed exception that matches the failure (`src/floe/errors.py`): `FloeConfigError`
  for config and validation, `FloeRunError` for run failures, `FloeStorageError` for storage,
  `FloeIoError` for reads; all derive from `FloeError`.
- Error messages are a user contract: tests assert them exactly, and the golden suite pins the
  ones a run or `floe validate` prints. Change one only on purpose, with the test.
- Chain the cause (`raise FloeRunError(...) from err`).
- Do not add error handling for scenarios that cannot happen; trust internal invariants.

## Config

- Config models are pydantic, hand-written in `src/floe/config/`, with their validation and
  error messages. Never generate them from `schemas/`, and never validate configs at runtime
  against the JSON Schema. `schemas/` is editor tooling; `scripts/test_config_schema.py` keeps
  it in step with the parser.

## Data paths

- No per-row Python loops on data. Use Polars or DuckDB expressions.
- Keep memory bounded by the chunk or the batch, not the table: read in chunks, and let DuckDB
  do anything that scans the target table (merges, unique checks against history).

## Comments

- Write **no comments by default**. Well-named identifiers are the documentation.
- Only add a comment when the WHY is non-obvious: a hidden constraint, a subtle invariant, a
  workaround for a specific external bug, or behaviour that would surprise a future reader.
- Never write what the code does.
- A numbered delta (`D26`) or issue reference is fine where it explains why behaviour differs
  from 0.7; the reasoning of the change itself belongs in the commit message.
- Mark a deliberate shortcut with a `ponytail:` comment naming the ceiling and the upgrade path.

## Abstractions

- Do not introduce an abstraction (class hierarchy, protocol, registry, config knob) until there
  are at least three concrete instances of the same pattern.
- Three similar lines are better than a premature abstraction.
- Prefer the native capability of a dependency we already have (Polars, pyarrow, PyIceberg,
  DuckDB, the stdlib) over porting or re-implementing logic.

## Dependencies

- Runtime dependencies are listed in `pyproject.toml`; adding one needs approval on the issue
  first.
- `duckdb` stays pinned to the version the Iceberg gates ran on until a newer one passes the
  same checks.
- Lock changes (`uv.lock`) go in their own PR unless the change needs them.

## Testing

See `context/testing-strategy.md`.
