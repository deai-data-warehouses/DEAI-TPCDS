import argparse
import os
import time

from openpyxl import Workbook
from openpyxl.styles import Alignment, Font

from constants import PARQUET_DIR, QUERIES_DIR, REPORT_PATH, SPARK_SESSION
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
args = parser.parse_args()
if args.query is not None and any(
    v is not None for v in (args.limit, args.start, args.end)
):
    parser.error("--query cannot be combined with --limit, --start or --end")

# Reduce Spark log verbosity to keep the terminal clean
SPARK_SESSION.sparkContext.setLogLevel("ERROR")

print("Registering temporary views from Parquet files...")
register_tables_in_spark(SPARK_SESSION, PARQUET_DIR)
print("All tables are ready for SQL queries!\n")

print("=================================")
print("  TPC-DS BENCHMARK START   ")
print("=================================\n")

report = Workbook()
sheet = report.active
sheet.title = "Benchmark"
sheet.append(["query file", "query", "status", "result", "error", "time (s)"])

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
        start_time = time.time()
        try:
            rows = SPARK_SESSION.sql(sql).count()
            elapsed = time.time() - start_time
            print(f"✅ OK! Time: {elapsed:.2f} s (Rows: {rows})")
            status, result, error = "success", f"{rows} rows", ""
        except Exception as e:
            elapsed = time.time() - start_time
            print(f"❌ FAILED in {elapsed:.2f} s")
            status, result, error = "error", "", str(e)
            if len(query_files) == 1:
                print(f"\n{error}\n")

        # Excel cells are limited to 32767 characters
        sheet.append([
            label, sql[:32767], status, result, error[:32767], round(elapsed, 2)
        ])

# Format the report: bold header, fixed column widths, wrapped text
for cell in sheet[1]:
    cell.font = Font(bold=True)
for column, width in zip("ABCDEF", (20, 60, 10, 15, 60, 10)):
    sheet.column_dimensions[column].width = width
for row in sheet.iter_rows(min_row=2):
    for cell in row:
        cell.alignment = Alignment(wrap_text=True, vertical="top")
sheet.freeze_panes = "A2"

report.save(REPORT_PATH)
print(f"\nReport saved to {REPORT_PATH}")

print("\nBenchmark completed!")
try:
    SPARK_SESSION.sparkContext.cancelAllJobs()
    SPARK_SESSION.stop()
except:
    pass