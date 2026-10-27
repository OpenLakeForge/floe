"""Observer events: the shapes of `crates/floe-python/python/floe/_floe.pyi` (Rust: `run/events.rs`)."""

import time
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from typing import Any, Literal, TypedDict

Observer = Callable[[dict[str, Any]], None]


@dataclass(frozen=True)
class Emitter:
    """Stamps `run_id` and `ts_ms`; with parallel entities the observer is called from worker threads."""

    observer: Observer | None = None
    run_id: str = ""

    def __call__(self, event: str, **fields: Any) -> None:
        if self.observer:
            with suppress(
                Exception
            ):  # an observer must not abort ingestion (Rust ignores callback errors)
                self.observer(
                    {"event": event, "run_id": self.run_id, **fields}
                    | {"ts_ms": time.time_ns() // 1_000_000}
                )


NO_EVENTS = Emitter()


# The `RunEvent` dicts an observer receives, as typed in `_floe.pyi`.
RunStartedEvent = TypedDict("RunStartedEvent", {"event": Literal["run_started"], "run_id": str, "config": str, "report_base": str | None, "ts_ms": int})  # fmt: skip
EntityStartedEvent = TypedDict("EntityStartedEvent", {"event": Literal["entity_started"], "run_id": str, "name": str, "ts_ms": int})  # fmt: skip
FileStartedEvent = TypedDict("FileStartedEvent", {"event": Literal["file_started"], "run_id": str, "entity": str, "input": str, "ts_ms": int})  # fmt: skip
FileFinishedEvent = TypedDict("FileFinishedEvent", {"event": Literal["file_finished"], "run_id": str, "entity": str, "input": str, "status": str, "rows": int, "accepted": int, "rejected": int, "elapsed_ms": int, "ts_ms": int})  # fmt: skip
SchemaEvolutionAppliedEvent = TypedDict("SchemaEvolutionAppliedEvent", {"event": Literal["schema_evolution_applied"], "run_id": str, "entity": str, "mode": str, "added_columns": list[str], "ts_ms": int})  # fmt: skip
EntityFinishedEvent = TypedDict("EntityFinishedEvent", {"event": Literal["entity_finished"], "run_id": str, "name": str, "status": str, "files": int, "rows": int, "accepted": int, "rejected": int, "warnings": int, "errors": int, "ts_ms": int})  # fmt: skip
RunFinishedEvent = TypedDict("RunFinishedEvent", {"event": Literal["run_finished"], "run_id": str, "status": str, "exit_code": int, "files": int, "rows": int, "accepted": int, "rejected": int, "warnings": int, "errors": int, "summary_uri": str | None, "ts_ms": int})  # fmt: skip
LogEvent = TypedDict("LogEvent", {"event": Literal["log"], "run_id": str, "log_level": str, "code": str | None, "message": str, "entity": str | None, "input": str | None, "ts_ms": int})  # fmt: skip
RunEvent = RunStartedEvent | EntityStartedEvent | FileStartedEvent | FileFinishedEvent | SchemaEvolutionAppliedEvent | EntityFinishedEvent | RunFinishedEvent | LogEvent  # fmt: skip
