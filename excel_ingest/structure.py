from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Tuple

from openpyxl import load_workbook
from openpyxl.utils import get_column_letter

from excel_ingest.validation import _resolve_local_path


class FileStatus(Enum):
    VALID = "VALID"
    NO_HEADERS = "NO_HEADERS"
    NO_DATA = "NO_DATA"
    EMPTY_FILE = "EMPTY_FILE"
    INVALID_STRUCTURE = "INVALID_STRUCTURE"
    SHEET_NOT_SPECIFIED = "SHEET_NOT_SPECIFIED"   # multi-sheet file, no sheet_name given


@dataclass
class MergedCellInfo:
    min_row: int
    max_row: int
    min_col: int
    max_col: int
    span_rows: int
    span_cols: int
    top_left_value: Optional[str]

    @property
    def col_letter_range(self) -> str:
        return f"{get_column_letter(self.min_col)}:{get_column_letter(self.max_col)}"


@dataclass
class HeaderStructure:
    header_row_indices: List[int]          # 1-based row numbers
    num_header_rows: int
    data_start_row: int                    # 1-based
    columns_per_row: Dict[int, int]        # {row_idx: column_count}
    raw_headers: Dict[int, List[Optional[str]]]  # {row_idx: [cell values]}


@dataclass
class FileStructureMetadata:
    sheet_name: str
    status: FileStatus
    header_structure: Optional[HeaderStructure]
    merged_cells: List[MergedCellInfo]
    blank_column_indices: List[int]        # 1-based
    hidden_column_indices: List[int]       # 1-based
    total_rows: int
    total_cols: int
    data_row_count: int
    messages: List[str] = field(default_factory=list)


@dataclass
class FileProcessingConfig:
    sheet_name: Optional[str] = None
    static_header_rows: Optional[List[int]] = None   # 1-based; if known, skip auto-detect
    data_start_row: Optional[int] = None             # 1-based; if known, skip auto-detect
    ignore_rows: Optional[List[int]] = None          # 1-based rows to skip
    ignore_row_ranges: Optional[List[Tuple[int, int]]] = None
    ignore_columns: Optional[List[int]] = None       # 1-based
    max_rows_to_scan: int = 20
    header_population_threshold: float = 0.3


# ---------------------------------------------------------------------------
# Internal detection helpers
# ---------------------------------------------------------------------------

def _detect_merged_cells(worksheet) -> List[MergedCellInfo]:
    result = []
    for merge in worksheet.merged_cells.ranges:
        top_left = worksheet.cell(merge.min_row, merge.min_col).value
        result.append(
            MergedCellInfo(
                min_row=merge.min_row, max_row=merge.max_row,
                min_col=merge.min_col, max_col=merge.max_col,
                span_rows=merge.max_row - merge.min_row + 1,
                span_cols=merge.max_col - merge.min_col + 1,
                top_left_value=str(top_left).strip() if top_left is not None else None,
            )
        )
    return result


def _detect_blank_and_hidden_columns(worksheet) -> Tuple[List[int], List[int]]:
    blank_cols: List[int] = []
    hidden_cols: List[int] = []
    max_col = worksheet.max_column or 0
    for col_idx in range(1, max_col + 1):
        letter = get_column_letter(col_idx)
        dim = worksheet.column_dimensions.get(letter)
        if dim and dim.hidden:
            hidden_cols.append(col_idx)
        col_values = [
            worksheet.cell(row, col_idx).value
            for row in range(1, min(worksheet.max_row or 1, 20) + 1)
        ]
        if all(v is None or str(v).strip() == "" for v in col_values):
            blank_cols.append(col_idx)
    return blank_cols, hidden_cols


def _detect_header_rows(
    worksheet,
    merged_cells: List[MergedCellInfo],
    config: FileProcessingConfig,
) -> HeaderStructure:
    max_col = worksheet.max_column or 1
    header_rows: List[int] = []

    if config.static_header_rows:
        header_rows = sorted(config.static_header_rows)
    else:
        scan_limit = min(config.max_rows_to_scan, worksheet.max_row or 1)
        for row_idx in range(1, scan_limit + 1):
            row_values = [worksheet.cell(row_idx, c).value for c in range(1, max_col + 1)]
            non_empty = sum(1 for v in row_values if v is not None and str(v).strip() != "")
            population = non_empty / max_col if max_col else 0
            if population >= config.header_population_threshold:
                header_rows.append(row_idx)
            elif header_rows:
                break  # first gap after headers found = end of header zone

    if not header_rows:
        header_rows = [1]

    data_start = config.data_start_row or (max(header_rows) + 1)
    raw_headers: Dict[int, List[Optional[str]]] = {}
    columns_per_row: Dict[int, int] = {}
    for r in header_rows:
        row_vals = [worksheet.cell(r, c).value for c in range(1, max_col + 1)]
        raw_headers[r] = [str(v).strip() if v is not None else None for v in row_vals]
        columns_per_row[r] = max_col

    return HeaderStructure(
        header_row_indices=header_rows,
        num_header_rows=len(header_rows),
        data_start_row=data_start,
        columns_per_row=columns_per_row,
        raw_headers=raw_headers,
    )


def _count_visible_sheets(workbook) -> int:
    return sum(1 for ws in workbook.worksheets if ws.sheet_state == "visible")


def _resolve_sheet(workbook, config: FileProcessingConfig) -> Tuple[Optional[str], List[str]]:
    msgs: List[str] = []
    if config.sheet_name:
        if config.sheet_name in workbook.sheetnames:
            return config.sheet_name, msgs
        msgs.append(f"Sheet '{config.sheet_name}' not found. Available: {workbook.sheetnames}")
        return None, msgs
    visible_count = _count_visible_sheets(workbook)
    if visible_count == 1:
        sheet = next(ws for ws in workbook.worksheets if ws.sheet_state == "visible")
        msgs.append(f"Single visible sheet auto-selected: '{sheet.title}'")
        return sheet.title, msgs
    msgs.append(
        f"Multiple visible sheets ({visible_count}) — provide sheet_name in FileProcessingConfig."
    )
    return None, msgs


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def analyze_excel_structure(
    file_path: str,
    config: Optional[FileProcessingConfig] = None,
    password: Optional[str] = None,
) -> FileStructureMetadata:
    config = config or FileProcessingConfig()
    local_path = _resolve_local_path(file_path)
    msgs: List[str] = []

    if password is not None:
        _use_kwarg = False
        try:
            import io, msoffcrypto
            with open(local_path, "rb") as f:
                office_file = msoffcrypto.OfficeFile(f)
                office_file.load_key(password=password)
                decrypted = io.BytesIO()
                office_file.decrypt(decrypted)
            decrypted.seek(0)
            wb = load_workbook(decrypted, read_only=False, data_only=True)
        except ImportError:
            _use_kwarg = True
        except Exception as exc:
            msg = str(exc).lower()
            if "password" in msg or "decrypt" in msg:
                raise
            _use_kwarg = True  # not AES-encrypted — fall through to openpyxl worksheet kwarg
        if _use_kwarg:
            wb = load_workbook(local_path, read_only=False, data_only=True, password=password)
    else:
        wb = load_workbook(local_path, read_only=False, data_only=True)

    sheet_name, sheet_msgs = _resolve_sheet(wb, config)
    msgs.extend(sheet_msgs)

    if sheet_name is None:
        wb.close()
        return FileStructureMetadata(
            sheet_name="",
            status=FileStatus.SHEET_NOT_SPECIFIED,
            header_structure=None,
            merged_cells=[], blank_column_indices=[], hidden_column_indices=[],
            total_rows=0, total_cols=0, data_row_count=0, messages=msgs,
        )

    ws = wb[sheet_name]
    total_rows = ws.max_row or 0
    total_cols = ws.max_column or 0

    if total_rows == 0 or total_cols == 0:
        wb.close()
        return FileStructureMetadata(
            sheet_name=sheet_name, status=FileStatus.EMPTY_FILE,
            header_structure=None,
            merged_cells=[], blank_column_indices=[], hidden_column_indices=[],
            total_rows=total_rows, total_cols=total_cols, data_row_count=0, messages=msgs,
        )

    merged = _detect_merged_cells(ws)
    blank_cols, hidden_cols = _detect_blank_and_hidden_columns(ws)
    header_struct = _detect_header_rows(ws, merged, config)

    data_row_count = max(0, total_rows - header_struct.data_start_row + 1)
    if data_row_count == 0:
        status = FileStatus.NO_DATA
    elif not header_struct.header_row_indices:
        status = FileStatus.NO_HEADERS
    else:
        status = FileStatus.VALID

    msgs.append(
        f"Sheet '{sheet_name}': {header_struct.num_header_rows} header row(s), "
        f"{data_row_count} data row(s), {len(merged)} merged region(s)."
    )
    wb.close()

    return FileStructureMetadata(
        sheet_name=sheet_name,
        status=status,
        header_structure=header_struct,
        merged_cells=merged,
        blank_column_indices=blank_cols,
        hidden_column_indices=hidden_cols,
        total_rows=total_rows,
        total_cols=total_cols,
        data_row_count=data_row_count,
        messages=msgs,
    )
