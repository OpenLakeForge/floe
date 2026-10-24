"""Observer events: the shapes of `crates/floe-python/python/floe/_floe.pyi` (Rust: `run/events.rs`)."""

import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

Observer = Callable[[dict[str, Any]], None]


@dataclass(frozen=True)
class Emitter:
    """Stamps `run_id` and `ts_ms`; with parallel entities the observer is called from worker threads."""

    observer: Observer | None = None
    run_id: str = ""

    def __call__(self, event: str, **fields: Any) -> None:
        if self.observer:
            self.observer(
                {"event": event, "run_id": self.run_id, **fields}
                | {"ts_ms": time.time_ns() // 1_000_000}
            )


NO_EVENTS = Emitter()
