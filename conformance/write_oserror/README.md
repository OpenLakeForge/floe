# write_oserror

The accepted sink path's parent (`out/accepted`) is a regular file, so writing the output fails with
`Not a directory (os error 20)`. Run with `--log-format json`: Rust 0.7 logs the error, emits
`entity_finished` (failed) and `run_finished`, prints `Error: ...` and exits 1 (#878).
