"""Build a real XLSX with the stdlib: inline-string cells, plus `Num` and `Day` cells."""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from xml.sax.saxutils import escape
from zipfile import ZipFile


@dataclass(frozen=True)
class Num:
    """A numeric cell, stored as its literal text (`"0.1"`)."""

    text: str


@dataclass(frozen=True)
class Day:
    """A date cell: a serial number with a date number format."""

    value: date


Cell = str | Num | Day | None
Rows = Sequence[Sequence[Cell]]

_STYLES = (
    '<?xml version="1.0" encoding="UTF-8"?>'
    '<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
    '<cellXfs count="2"><xf numFmtId="0"/><xf numFmtId="14" applyNumberFormat="1"/></cellXfs>'
    "</styleSheet>"
)


def _ref(row: int, col: int) -> str:
    name = ""
    col += 1
    while col:
        col, rem = divmod(col - 1, 26)
        name = chr(65 + rem) + name
    return f"{name}{row + 1}"


def _cell(row: int, col: int, cell: Cell) -> str:
    ref = _ref(row, col)
    if cell is None:
        return ""
    if isinstance(cell, Num):
        return f'<c r="{ref}"><v>{cell.text}</v></c>'
    if isinstance(cell, Day):
        serial = (cell.value - date(1899, 12, 30)).days
        return f'<c r="{ref}" s="1"><v>{serial}</v></c>'
    return f'<c r="{ref}" t="inlineStr"><is><t xml:space="preserve">{escape(cell)}</t></is></c>'


def _sheet(rows: Rows) -> str:
    body = "".join(
        f'<row r="{r + 1}">{"".join(_cell(r, c, v) for c, v in enumerate(row))}</row>'
        for r, row in enumerate(rows)
    )
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        f"<sheetData>{body}</sheetData></worksheet>"
    )


def write_book(path: Path, sheets: Mapping[str, Rows]) -> Path:
    """Sheets are written in dict order; a row is a list of cells, `None` for blank."""
    names = list(sheets)
    ns = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
    pkg = "http://schemas.openxmlformats.org/package/2006/relationships"
    with ZipFile(path, "w") as z:
        z.writestr(
            "[Content_Types].xml",
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
            '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
            '<Default Extension="xml" ContentType="application/xml"/>'
            '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
            '<Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>'
            + "".join(
                f'<Override PartName="/xl/worksheets/sheet{i}.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
                for i in range(1, len(names) + 1)
            )
            + "</Types>",
        )
        z.writestr(
            "_rels/.rels",
            f'<?xml version="1.0" encoding="UTF-8"?><Relationships xmlns="{pkg}">'
            f'<Relationship Id="rId1" Type="{ns}/officeDocument" Target="xl/workbook.xml"/></Relationships>',
        )
        z.writestr(
            "xl/workbook.xml",
            f'<?xml version="1.0" encoding="UTF-8"?><workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="{ns}"><sheets>'
            + "".join(
                f'<sheet name="{escape(n)}" sheetId="{i}" r:id="rId{i}"/>'
                for i, n in enumerate(names, 1)
            )
            + "</sheets></workbook>",
        )
        z.writestr(
            "xl/_rels/workbook.xml.rels",
            f'<?xml version="1.0" encoding="UTF-8"?><Relationships xmlns="{pkg}">'
            + "".join(
                f'<Relationship Id="rId{i}" Type="{ns}/worksheet" Target="worksheets/sheet{i}.xml"/>'
                for i in range(1, len(names) + 1)
            )
            + f'<Relationship Id="rId{len(names) + 1}" Type="{ns}/styles" Target="styles.xml"/></Relationships>',
        )
        z.writestr("xl/styles.xml", _STYLES)
        for i, n in enumerate(names, 1):
            z.writestr(f"xl/worksheets/sheet{i}.xml", _sheet(sheets[n]))
    return path
