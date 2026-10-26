import csv
import os

from openpyxl import Workbook
from openpyxl.styles import Alignment, Font
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.worksheet import Worksheet

REPORT_FILENAME = "benchmark_report.xlsx"
QUERY_RUNS_SHEET = "Query Runs"
RUN_SUMMARY_SHEET = "Run Summary"
VALIDATION_SHEET = "Validation"
QUERY_RUNS_CSV = "query_runs.csv"
RUN_SUMMARY_CSV = "run_summary.csv"

# Display width of each column in the Excel report, by column name. Columns
# not listed here get DEFAULT_COLUMN_WIDTH.
DEFAULT_COLUMN_WIDTH = 15
COLUMN_WIDTHS = {
    "run_id": 36,
    "started_at": 28,
    "query_id": 10,
    "query_file": 18,
    "query_version": 14,
    "statement_part": 15,
    "min_seconds": 14,
    "max_seconds": 14,
    "avg_seconds": 14,
    "status": 12,
    "row_count": 12,
    "error": 60,
    "scale_factor": 14,
    "runs_per_query": 16,
    "parquet_dir": 50,
    "queries_dir": 50,
    "run_elapsed_seconds": 20,
    "successful_query_seconds": 22,
    "average_statement_seconds": 22,
    "attempted_statements": 20,
    "successful_statements": 20,
    "failed_statements": 20,
    "original_avg_seconds": 20,
    "changed_avg_seconds": 20,
    "optimization_gain_percent": 24,
    "results_identical": 18,
    "original_only_sample": 60,
    "changed_only_sample": 60,
}


def _table_rows(records: list[dict]) -> list[list]:
    """Header row (keys of the first record) followed by one row per record."""
    if not records:
        return []
    headers = list(records[0])
    return [headers] + [[record[header] for header in headers] for record in records]


def _format_worksheet(worksheet: Worksheet) -> None:
    # Make the first row, which contains the column names, bold, and set each
    # column's width from its name.
    column_indexes = {}
    for cell in worksheet[1]:
        cell.font = Font(bold=True)
        worksheet.column_dimensions[get_column_letter(cell.column)].width = (
            COLUMN_WIDTHS.get(cell.value, DEFAULT_COLUMN_WIDTH)
        )
        column_indexes[cell.value] = cell.column

    # Wrap long cell contents and align data rows to the top of each cell.
    for row in worksheet.iter_rows(min_row=2):
        for cell in row:
            cell.alignment = Alignment(wrap_text=True, vertical="top")

    gain_column = column_indexes.get("optimization_gain_percent")
    if gain_column is not None:
        for row in worksheet.iter_rows(
            min_row=2, min_col=gain_column, max_col=gain_column
        ):
            row[0].number_format = '0.00"%"'

    # Keep the header row visible while scrolling down the worksheet.
    worksheet.freeze_panes = "A2"


def _write_excel(path: str, sheets: list[tuple[str, list[list]]]) -> None:
    workbook = Workbook()
    workbook.remove(workbook.active)
    for title, rows in sheets:
        worksheet = workbook.create_sheet(title)
        for row in rows:
            worksheet.append(row)
        _format_worksheet(worksheet)
    workbook.save(path)


def _write_csv(path: str, rows: list[list]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as csv_file:
        csv.writer(csv_file).writerows(rows)


def write_reports(
    run_directory: str,
    query_records: list[dict],
    summary: dict,
    validation_records: list[dict] | None = None,
) -> None:
    """
    Save the benchmark results in run_directory: one Excel workbook with a
    sheet per table, plus each table as a separate CSV for downstream analysis.
    """
    query_rows = _table_rows(query_records)
    summary_rows = _table_rows([summary])

    report_path = os.path.join(run_directory, REPORT_FILENAME)
    sheets = [(QUERY_RUNS_SHEET, query_rows), (RUN_SUMMARY_SHEET, summary_rows)]
    if validation_records is not None:
        sheets.append((VALIDATION_SHEET, _table_rows(validation_records)))
    _write_excel(report_path, sheets)
    print(f"\nReport saved to {report_path}")

    for rows, csv_filename in (
        (query_rows, QUERY_RUNS_CSV),
        (summary_rows, RUN_SUMMARY_CSV),
    ):
        csv_path = os.path.join(run_directory, csv_filename)
        _write_csv(csv_path, rows)
        print(f"CSV saved to {csv_path}")
