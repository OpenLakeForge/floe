class FloeError(Exception):
    """`code` is a stable snake_case tag for machines; `rule` names the check or reader rule that failed."""

    def __init__(
        self, *args: object, code: str | None = None, rule: str | None = None
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
