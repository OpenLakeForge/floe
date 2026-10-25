# unresolved_inputs

Two entities; the second's source glob (`in/missing_*.csv`) matches nothing. Run with `--log-format json`, so
`expected/events.jsonl` holds the event lines. Rust 0.7 resolves every entity's inputs before the first
`entity_started`: it logs the error and stops, with no entity events and nothing written for `first` (#864).

The same case with a leading `./` in the glob is `unresolved_inputs_dot` (#879).
