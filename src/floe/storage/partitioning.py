"""D28: `source.partitioning`, the date and batch of each input from its directory, and the load order."""

import re
from datetime import date, datetime, timedelta
from typing import NamedTuple

from floe.config.types import SourcePartitioning
from floe.errors import FloeRunError
from floe.state import EntityState
from floe.storage.inputs import InputFile, ResolvedInputs

_TOKEN = re.compile(r"\{(yyyy|MM|dd|HH|batch)\}")  # validated: no other token
_GROUP = {"yyyy": r"\d{4}", "MM": r"\d{2}", "dd": r"\d{2}", "HH": r"\d{2}", "batch": r"[^/]+"}  # fmt: skip
_FRACTION = re.compile(r"(?:\.(\d+))?Z$")
_ARCHIVED = re.compile(r"__run-(?!.*__run-).*__src-[0-9a-f]{16}(?=(?:\.[^.]*)?$)")  # D29 suffix  # fmt: skip


class Partition(NamedTuple):
    when: date | datetime  # a datetime when the pattern has `{HH}`
    batch: str | None


def source_root(uri: str) -> str:
    """The directory partition paths are relative to: the source URI, cut before its first glob."""
    cut = next((i for i, c in enumerate(uri) if c in "*?["), None)
    return (uri if cut is None else uri[: uri.rfind("/", 0, cut)]).rstrip("/")


def partition_of(p: SourcePartitioning, entity: str, root: str, uri: str) -> Partition:  # fmt: skip
    """Match the input's directory, relative to `root`, against the whole pattern."""
    parts = _TOKEN.split(p.pattern.strip("/"))
    regex = "".join(re.escape(s) if i % 2 == 0 else f"(?P<{s}>{_GROUP[s]})" for i, s in enumerate(parts))  # fmt: skip
    folder = uri.removeprefix(f"{root}/").rpartition("/")[0] if uri.startswith(f"{root}/") else None  # fmt: skip
    match = None if folder is None else re.fullmatch(regex, folder)
    if match is None:
        raise FloeRunError(f"entity.name={entity} input {uri} does not match source.partitioning.pattern={p.pattern}")  # fmt: skip
    hour = match.groupdict().get("HH")
    try:
        when = datetime(int(match["yyyy"]), int(match["MM"]), int(match["dd"]), int(hour or 0))  # fmt: skip
    except (
        ValueError
    ) as err:  # the date as written: Python's own wording varies by version
        written = f"{match['yyyy']}-{match['MM']}-{match['dd']}" + (f"T{hour}" if hour is not None else "")  # fmt: skip
        raise FloeRunError(f"entity.name={entity} input {uri} has an invalid source.partitioning date: {written}") from err  # fmt: skip
    return Partition(when if hour is not None else when.date(), match.groupdict().get("batch"))  # fmt: skip


def _mtime(file: InputFile) -> str:
    """RFC 3339 with nine fraction digits, so the strings sort in time order."""
    return _FRACTION.sub(lambda m: f".{(m[1] or '').ljust(9, '0')}Z", file.source_mtime or "")  # fmt: skip


def in_load_order(p: SourcePartitioning, entity: str, root: str, inputs: ResolvedInputs, archived: bool = False, start: date | None = None) -> ResolvedInputs:  # fmt: skip
    """By date, then batch (earliest modification time, then name), then file name; hash batches don't sort by name.

    D29: `archived` inputs sort by their original name, so a full refresh replays the files in their first order.
    D30: a file dated before `start` is dropped, whatever the listing returned.
    """
    of = {f.source_uri: partition_of(p, entity, root, f.source_uri) for f in inputs.files}  # fmt: skip
    if start is not None:
        inputs = ResolvedInputs([f for f in inputs.files if _day(of[f.source_uri].when) >= start], [], inputs.mode)  # fmt: skip
    first: dict[Partition, str] = {}
    for f in inputs.files:
        first[of[f.source_uri]] = min(first.get(of[f.source_uri], "~"), _mtime(f))

    def key(f: InputFile) -> tuple[str, str, str, str]:
        part = of[f.source_uri]
        name = _ARCHIVED.sub("", f.source_name) if archived else f.source_name
        return (part.when.isoformat(), first[part], part.batch or "", name)

    files = sorted(inputs.files, key=key)
    return ResolvedInputs(files, [f.source_uri for f in files], inputs.mode)


def _day(when: date | datetime) -> date:
    return when.date() if isinstance(when, datetime) else when


def _dated(p: SourcePartitioning, entity: str, root: str, uri: str) -> date | None:
    """The partition date of a state entry; None for one outside the pattern (another source path)."""
    try:
        return _day(partition_of(p, entity, root, uri).when)
    except FloeRunError:
        return None


def window(p: SourcePartitioning, entity: str, root: str, state: EntityState | None) -> date | None:  # fmt: skip
    """D30: the first date a file-mode run lists, the watermark minus `lookback_days`.

    An outstanding claim holds it back to the claim's date, so a crashed run's files stay listed until it is retried.
    """
    if state is None or state.watermark is None:
        return None
    start = date.fromisoformat(state.watermark) - timedelta(days=p.lookback_days)
    return min([start, *(d for u in state.claims if (d := _dated(p, entity, root, u)))])  # fmt: skip


def listing_start(p: SourcePartitioning, start: date) -> str | None:
    """D30: the pattern up to `{dd}` for the day before `start`, which `start` and every later day sort after; None when the date does not lead the pattern.

    The day before, not `start`: SeaweedFS treats `StartAfter` as a directory marker and skips that whole directory.
    """
    head, start = p.pattern.strip("/").partition("{dd}")[0], start - timedelta(days=1)
    if "{batch}" in head or "{HH}" in head or not 0 <= head.find("{yyyy}") < head.find("{MM}"):  # fmt: skip
        return None
    return head.replace("{yyyy}", f"{start.year:04d}").replace("{MM}", f"{start.month:02d}") + f"{start.day:02d}"  # fmt: skip


def advance(p: SourcePartitioning, entity: str, root: str, state: EntityState, promoted: list[str]) -> None:  # fmt: skip
    """D30, on promote: the watermark moves to the latest date promoted, never back (#949 will prune `files`).

    Without one (a first run, or after a failed full refresh), every file in the state counts.
    """
    known = promoted if state.watermark else [*promoted, *state.files]
    days = [d.isoformat() for u in known if (d := _dated(p, entity, root, u))]
    state.watermark = max([state.watermark or "", *days]) or None
