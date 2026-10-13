import argparse
import csv
import os
import subprocess
import time
import uuid
from datetime import datetime, timezone

from openpyxl import Workbook
from openpyxl.styles import Alignment, Font

from constants import RUNS_DIR, SPARK_SESSION, get_scenario_paths
from utils import normalize_sql_for_spark, register_tables_in_spark, sort_query_files

parser = argparse.ArgumentParser(description="Run the TPC-DS benchmark queries.")
parser.add_argument(
    "--limit", type=int, help="run only the first N queries (in query order)"
)
parser.add_argument(
    "--query", type=int, help="run only the file query<QUERY>.sql (e.g. --query 5)"
)
parser.add_argument(
    "--start", type=int, help="run queries from query<START> onwards (inclusive)"
)
parser.add_argument(
    "--end", type=int, help="run queries up to query<END> (inclusive)"
)
parser.add_argument(
    "--scale-factor",
    type=float,
    required=True,
    help="TPC-DS data scale factor used for this run",
)
args = parser.parse_args()
if args.query is not None and any(
    v is not None for v in (args.limit, args.start, args.end)
):
    parser.error("--query cannot be combined with --limit, --start or --end")

_, PARQUET_DIR, QUERIES_DIR = get_scenario_paths(args.scale_factor)

run_id = str(uuid.uuid4())
run_started_at = datetime.now(timezone.utc)
started_at = run_started_at.isoformat()
run_directory = os.path.join(
    RUNS_DIR,
    run_started_at.strftime("%Y-%m-%d"),
    f"{run_started_at.strftime('%H%M%S')}_{run_id}",
)
os.makedirs(run_directory, exist_ok=True)
report_path = os.path.join(run_directory, "benchmark_report.xlsx")

try:
    repository_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    # Save the short commit hash, control code version
    code_version = subprocess.run(
        ["git", "rev-parse", "--short", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
        cwd=repository_root,
    ).stdout.strip()
    # Record in the reporting if the run used coded different from the current commit
    # If a file has uncommited edits it will be True
    working_tree_dirty = bool(
        subprocess.run(
            ["git", "status", "--porcelain", "--untracked-files=no"],
            check=True,
            capture_output=True,
            text=True,
            cwd=repository_root,
        ).stdout.strip()
    )
except (subprocess.CalledProcessError, FileNotFoundError):
    code_version = "unknown"
    working_tree_dirty = None
# Measure durations
run_start = time.perf_counter()
query_records = []
# Reduce Spark log verbosity to keep the terminal clean
SPARK_SESSION.sparkContext.setLogLevel("ERROR")

print("Registering temporary views from Parquet files...")
register_tables_in_spark(SPARK_SESSION, PARQUET_DIR)
print("All tables are ready for SQL queries!\n")

print("=================================")
print("  TPC-DS BENCHMARK START   ")
print("=================================\n")

# Create in-memory Excel file
report = Workbook()
query_sheet = report.active
# Worksheet tab to store results fro the wuery runs
query_sheet.title = "Query Runs"
query_headers = [
    "run_id",
    "started_at",
    "query_id",
    "query_file",
    "statement_part",
    "label",
    "elapsed_seconds",
    "status",
    "row_count",
    "error",
    "scale_factor",
    "code_version",
    "working_tree_dirty",
    "sql",
]
query_sheet.append(query_headers)

query_files = sort_query_files(os.listdir(QUERIES_DIR))
if args.query is not None:
    query_file = f"query{args.query}.sql"
    if query_file not in query_files:
        raise SystemExit(f"{query_file} not found in {QUERIES_DIR}")
    query_files = [query_file]
else:
    def query_number(query_file):
        return int(query_file.removeprefix("query").removesuffix(".sql"))

    if args.start is not None:
        query_files = [q for q in query_files if query_number(q) >= args.start]
    if args.end is not None:
        query_files = [q for q in query_files if query_number(q) <= args.end]
    if args.limit is not None:
        query_files = query_files[:args.limit]

for query_file in query_files:
    query_path = os.path.join(QUERIES_DIR, query_file)
    with open(query_path, "r") as f:
        raw_sql = normalize_sql_for_spark(f.read())

    # Some files (e.g. query14) contain several statements separated by ";"
    statements = [stmt.strip() for stmt in raw_sql.split(";") if stmt.strip()]
    if len(statements) > 1:
        print(f"{query_file} contains {len(statements)} queries, executing all of them")

    for i, sql in enumerate(statements, start=1):
        label = f"{query_file} (part {i}/{len(statements)})" if len(statements) > 1 else query_file
        print(f"Executing {label}...")
        start_time = time.perf_counter()
        try:
            rows = SPARK_SESSION.sql(sql).count()
            elapsed = time.perf_counter() - start_time
            print(f"✅ OK! Time: {elapsed:.2f} s (Rows: {rows})")
            status, error = "success", ""
        except Exception as e:
            elapsed = time.perf_counter() - start_time
            print(f"❌ FAILED in {elapsed:.2f} s")
            rows = None
            status, error = "error", str(e)
            if len(query_files) == 1:
                print(f"\n{error}\n")

        # Construct individual record for every query after success or failure
        query_number = int(query_file.removeprefix("query").removesuffix(".sql"))

        query_records.append({
            "run_id": run_id,
            "started_at": started_at,
            "query_id": query_number,
            "query_file": query_file,
            "statement_part": i,
            "label": label,
            "elapsed_seconds": elapsed,
            "status": status,
            "row_count": rows,
            "error": error[:32767],
            "scale_factor": args.scale_factor,
            "code_version": code_version,
            "working_tree_dirty": working_tree_dirty,
            "sql": sql[:32767],
        })

        query_sheet.append([record_value for record_value in query_records[-1].values()])

# Capture benchmark duration before preparing and saving the workbook.
run_elapsed_seconds = time.perf_counter() - run_start
successful_records = [r for r in query_records if r["status"] == "success"]
all_query_times = [r["elapsed_seconds"] for r in query_records]
successful_query_times = [r["elapsed_seconds"] for r in successful_records]

# TODO: Modularize the code better, this could be in a different file
summary = {
    "run_id": run_id,
    "started_at": started_at,
    "scale_factor": args.scale_factor,
    "parquet_dir": PARQUET_DIR,
    "queries_dir": QUERIES_DIR,
    "code_version": code_version,
    "working_tree_dirty": working_tree_dirty,
    "run_elapsed_seconds": run_elapsed_seconds,
    "total_query_seconds": sum(all_query_times),
    "successful_query_seconds": sum(successful_query_times),
    "average_statement_seconds": (
        sum(successful_query_times) / len(successful_query_times)
        if successful_query_times
        else None
    ),
    "attempted_statements": len(query_records),
    "successful_statements": len(successful_records),
    "failed_statements": len(query_records) - len(successful_records),
}
summary_sheet = report.create_sheet("Run Summary")
summary_headers = list(summary)
summary_sheet.append(summary_headers)
summary_sheet.append(list(summary.values()))

# Format both Excel worksheets. Each tuple pairs a worksheet with the
# display width for each of its columns.
for worksheet, widths in (
    (query_sheet, (36, 28, 10, 18, 15, 30, 18, 12, 12, 60, 14, 14, 18, 60)),
    (summary_sheet, (36, 28, 14, 50, 50, 14, 18, 20, 20, 22, 22, 20, 20, 20)),
):
    # Make the first row, which contains the column names, bold.
    for cell in worksheet[1]:
        cell.font = Font(bold=True)

    # Set each column's width. Excel column numbers start at 1, while
    # openpyxl converts those numbers to letters such as A, B, and C.
    for column, width in enumerate(widths, start=1):
        worksheet.column_dimensions[
            worksheet.cell(row=1, column=column).column_letter
        ].width = width
    
    # Wrap long cell contents and align data rows to the top of each cell.
    for row in worksheet.iter_rows(min_row=2):
        for cell in row:
            cell.alignment = Alignment(wrap_text=True, vertical="top")
    
    # Keep the header row visible while scrolling down the worksheet.
    worksheet.freeze_panes = "A2"


report.save(report_path)
print(f"\nReport saved to {report_path}")

# Save the two sheets as separate csv files for downstream analysis
for worksheet, csv_filename in (
    (query_sheet, "query_runs.csv"),
    (summary_sheet, "run_summary.csv"),
):
    csv_path = os.path.join(run_directory, csv_filename)
    with open(csv_path, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerows(
            [cell.value for cell in row]
            for row in worksheet.iter_rows()
        )
    print(f"CSV saved to {csv_path}")

print("\nBenchmark completed!")
try:
    SPARK_SESSION.sparkContext.cancelAllJobs()
    SPARK_SESSION.stop()
except:
    pass