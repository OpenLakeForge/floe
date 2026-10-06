# Floe — Coding Standards

## Error handling

- All fallible public and internal functions return `FloeResult<T>`.
- Construct errors with the `FloeError` helpers: `FloeError::config(...)`, `FloeError::run(...)`, `FloeError::storage(...)` / `storage_at(path, ...)`, `FloeError::io(...)`. See `context/decisions/structured-floe-error.md`.
- No `unwrap()` or `expect()` in production paths. Use `?` or explicit `map_err`.
- Do not add error handling for scenarios that cannot happen — trust internal invariants.

## Comments

- Write **no comments by default**. Well-named identifiers are the documentation.
- Only add a comment when the WHY is non-obvious: a hidden constraint, a subtle invariant, a workaround for a specific external bug, or behaviour that would surprise a future reader.
- Never write what the code does (the code already says that).
- Never reference the current task, PR, or issue number in source comments — those belong in the commit message.

## Function signatures

- When a function needs more than 4-5 parameters, introduce a context struct instead of adding arguments.
- Pattern: `FooContext<'a>` or `FooRequest<'a>` holding all inputs as named fields. The function destructures it at the top.
- This is how `AcceptedWriteRequest`, `AcceptedOutputContext`, `SeedContext`, and `RunReportContext` are structured.

## Traits and dispatch

- Prefer static dispatch (`impl Trait` / generics) for hot paths.
- Use `dyn Trait` only when the set of implementations is not known at compile time or you need to store heterogeneous types (e.g., `Box<dyn StorageClient>`, `&dyn SinkFormat`).
- Avoid match-on-string dispatch. Use a registry (see `SINK_FORMATS`) so adding an implementation does not require touching dispatch code.

## Abstractions

- Do not introduce an abstraction until there are at least three concrete instances of the same pattern.
- Three similar lines is better than a premature abstraction.
- Do not design for hypothetical future requirements.

## Traits and new formats/backends

When adding a new sink format:
1. Create `crates/floe-core/src/io/write/<name>.rs`
2. Implement `SinkFormat` trait
3. Add `&<Name>SinkFormat` to `SINK_FORMATS` in `sink_format.rs`

No other files need changing — validation, dispatch, and seeding are automatic.

When adding a new storage backend:
1. Create `crates/floe-core/src/io/storage/providers/<name>.rs`
2. Implement `StorageClient` trait
3. Register the client in `CloudClient::client_for_context` match

## CI requirements — required before every push and PR

The canonical commands live in `AGENTS.md` §5 and mirror `.github/workflows/ci.yml`. Run them before every push.

No `#[allow(clippy::...)]` suppressions except where the lint is demonstrably wrong for the specific case — document why inline when you do suppress.

**Code style notes:**
- Prefer `impl Into<String>` for string-accepting constructor parameters.
- Use `BTreeMap` instead of `HashMap` when key order matters for deterministic output (e.g., serialised state files).

## Testing

- See `context/testing-strategy.md` for full test strategy.
- Tests live in `crates/floe-core/tests/` — never inline `#[test]` in `src/` except for pure unit logic with no I/O.
- Do not mock storage or the filesystem in integration tests. Use real temporary directories and real file I/O.

## Dependencies

- Prefer standard library solutions over new dependencies for simple problems.
- When adding a dependency, pin it to a compatible minor version range in `Cargo.toml`.
- Avoid async runtimes in `floe-core` — the storage providers own their own `tokio::Runtime` instance and call `block_on` internally. The core library is sync from the caller's perspective.
