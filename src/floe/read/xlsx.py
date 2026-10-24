"""XLSX reader: every cell as a raw string (Rust: `io/read/xlsx.rs`), via `fastexcel` (calamine)."""

from pathlib import Path

import fastexcel
import polars as pl

from floe.config.types import SourceOptions
from floe.errors import FloeIoError


def _fail(rule: str, message: str) -> FloeIoError:
    return FloeIoError(f"{rule}: {message}")


def _header(cells: tuple[str | None, ...]) -> list[str]:
    """Trimmed names; blank cells become `column_N`; repeats become `<name>_dup_<count>` (as Rust)."""
    counts: dict[str, int] = {}
    used: set[str] = set()
    names: list[str] = []
    for index, cell in enumerate(cells):
        name = (cell or "").strip() or f"column_{index + 1}"
        count = counts.get(name, 0) + 1
        candidate = name if count == 1 else f"{name}_dup_{count}"
        while candidate in used:
            count += 1
            candidate = f"{name}_dup_{count}"
        counts[name] = count
        used.add(candidate)
        names.append(candidate)
    return names


def read_xlsx(path: Path, options: SourceOptions) -> pl.DataFrame:
    """Read the whole sheet; rows and columns count from the sheet's used range, as in Rust.

    The data rows start at `data_row` (default `header_row + 1`). Numbers, booleans and dates are
    rendered by calamine (see the tests for the one rendering that differs from Rust: dates).
    """
    # ponytail: calamine loads the whole sheet in memory, so there is one frame, not chunks.
    header_row = 1 if options.header_row is None else options.header_row
    data_row = header_row + 1 if options.data_row is None else options.data_row
    try:
        book = fastexcel.read_excel(path)
    except fastexcel.FastExcelError as err:
        raise _fail("xlsx_read_error", f"failed to open xlsx at {path}: {err}") from err
    names = book.sheet_names
    if not names:
        raise _fail("xlsx_read_error", f"no sheets found in {path}")
    sheet = names[0] if options.sheet is None else options.sheet
    if sheet not in names:
        raise _fail(
            "xlsx_sheet_error",
            f"sheet {sheet} not found in {path} (available: {', '.join(names)})",
        )
    try:
        # the eager=True overload of fastexcel's stubs returns an untyped pyarrow table
        loaded = book.load_sheet(  # pyright: ignore[reportUnknownMemberType]
            sheet, header_row=None, dtypes="string"
        )
        raw = loaded.to_polars()
    except fastexcel.FastExcelError as err:
        raise _fail(
            "xlsx_read_error", f"failed to read sheet {sheet} in {path}: {err}"
        ) from err
    if raw.width == 0:
        raise _fail("xlsx_read_error", f"sheet is empty in {path}")
    header_idx = max(header_row - 1, 0)
    if header_idx >= raw.height:
        raise _fail(
            "xlsx_read_error",
            f"header_row={header_row} is outside sheet bounds in {path}",
        )
    names = _header(raw.row(header_idx))
    return raw.slice(max(data_row - 1, 0)).rename(dict(zip(raw.columns, names)))
