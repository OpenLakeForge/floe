# manifest_run

`floe run --manifest` (#662): two domains (v0.3 qualified ids), one incremental entity run twice, so the
second run skips the processed file through the `state_path` the manifest records. Each implementation
generates its own manifest from `config.yml`, then runs from it. Recorded from Rust 0.7.
