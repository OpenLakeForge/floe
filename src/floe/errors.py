from typing import Literal

# The prefix names the exception class: config_ FloeConfigError, io_ FloeIoError, run_ FloeRunError,
# storage_ FloeStorageError. Frozen at 1.0, additive after (docs/reference/errors.md).
ErrorCode = Literal[
    "config_invalid_yaml",
    "config_unknown_key",
    "config_missing_key",
    "config_invalid_value",
    "config_not_supported",
    "config_unknown_reference",
    "config_duplicate",
    "config_conflict",
    "config_version",
    "io_read_error",
    "io_write_error",
    "io_not_found",
    "io_schema_mismatch",
    "io_invalid_option",
    "storage_not_found",
    "storage_access_denied",
    "storage_conflict",
    "storage_error",
    "run_state_conflict",
    "run_commit_failed",
    "run_schema_evolution",
    "run_catalog_error",
    "run_merge_key",
    "run_aborted",
    "run_not_supported",
    "run_invalid_input",
    "run_invalid_setup",
]


class FloeError(Exception):
    """`code` is a stable snake_case tag for machines; `rule` names the check or reader rule that failed."""

    def __init__(
        self,
        *args: object,
        code: ErrorCode | None = None,
        rule: str | None = None,
    ) -> None:
        super().__init__(*args)
        self.code, self.rule = code, rule


class FloeConfigError(FloeError): ...


class FloeRunError(FloeError): ...


class FloeStorageError(FloeError): ...


class FloeIoError(FloeError): ...


NOT_SUPPORTED = (
    "is not supported (see CHANGELOG)"  # config inputs Floe used to accept and ignore
)
