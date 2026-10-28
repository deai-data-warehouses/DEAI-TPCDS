import csv
import os

from openpyxl import Workbook
from openpyxl.styles import Alignment, Font
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.worksheet import Worksheet

REPORT_FILENAME = "benchmark_report.xlsx"

QUERY_RUNS_SHEET   = "Query Runs"
RUN_SUMMARY_SHEET  = "Run Summary"
RUN_CONFIG_SHEET   = "Run Config"
SPARK_CONFIG_SHEET = "Spark Config"
QUERY_RUNS_CSV     = "query_runs.csv"
RUN_SUMMARY_CSV    = "run_summary.csv"
RUN_CONFIG_CSV     = "run_config.csv"
SPARK_CONFIG_CSV   = "spark_config.csv"

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
    "status": 12,
    "row_count": 12,
    "error": 60,
    "scale_factor": 14,
    "query_selection": 24,
    "runs_per_query": 16,
    "parquet_dir": 50,
    "queries_dir": 50,
    "run_elapsed_seconds": 20,
    "total_seconds": 14,
    "statements_succeeded": 20,
    "statements_failed": 18,
    "successful_query_seconds": 22,
    "average_statement_seconds": 22,
    "attempted_statements": 20,
    "successful_statements": 20,
    "failed_statements": 20,
    "section": 12,
    "option": 32,
    "value": 30,
    "parameter": 60,
    "description": 100,
    "session_value": 30,
    "default_value": 30,
    "changed": 10,
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
    run_config: list[dict],
    spark_config: list[dict],
) -> None:
    """
    Save the benchmark results in run_directory: one Excel workbook with a
    sheet per table, plus each table as a separate CSV for downstream analysis.
    run_config holds the options from config.toml and spark_config every
    setting of the Spark session.
    """
    tables = [
        (QUERY_RUNS_SHEET,   QUERY_RUNS_CSV,   _table_rows(query_records)),
        (RUN_SUMMARY_SHEET,  RUN_SUMMARY_CSV,  _table_rows([summary])),
        (RUN_CONFIG_SHEET,   RUN_CONFIG_CSV,   _table_rows(run_config)),
        (SPARK_CONFIG_SHEET, SPARK_CONFIG_CSV, _table_rows(spark_config)),
    ]

    report_path = os.path.join(run_directory, REPORT_FILENAME)
    _write_excel(report_path, [(sheet, rows) for sheet, _, rows in tables])
    print(f"\nReport saved to {report_path}")

    for _, csv_filename, rows in tables:
        csv_path = os.path.join(run_directory, csv_filename)
        _write_csv(csv_path, rows)
        print(f"CSV saved to {csv_path}")
