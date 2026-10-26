# unresolved_inputs_dot

Same as `unresolved_inputs`, but the second entity's glob has a leading `./` (`./in/missing_*.csv`). Rust 0.7 keeps
the path as written in the "no input files matched" message: `base_path=<ROOT>/./in` (#879).
