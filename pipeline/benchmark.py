import argparse
import os
import time
from pathlib import Path

from openpyxl import Workbook
from openpyxl.styles import Alignment, Font

from constants import (
    PARQUET_DIR,
    QUERIES_DIR,
    REPORT_PATH,
    SQL_SCHEMA_PATH,
    create_spark_session,
)
from utils import (
    get_table_names,
    normalize_sql_for_spark,
    parquet_table_is_complete,
    register_tables_in_spark,
    sort_query_files,
)


def _select_query_files(parser: argparse.ArgumentParser, args: argparse.Namespace) -> list[str]:
    if args.query is not None and any(
        value is not None for value in (args.limit, args.start, args.end)
    ):
        parser.error("--query cannot be combined with --limit, --start or --end")

    query_dir = Path(QUERIES_DIR)
    if not query_dir.is_dir():
        raise FileNotFoundError(
            f"Query directory not found at {query_dir}; run pipeline/prepare.py first."
        )
    query_files = sort_query_files(os.listdir(query_dir))
    if args.query is not None:
        query_file = f"query{args.query}.sql"
        if query_file not in query_files:
            raise FileNotFoundError(f"{query_file} not found in {query_dir}")
        return [query_file]

    def query_number(filename: str) -> int:
        return int(filename.removeprefix("query").removesuffix(".sql"))

    if args.start is not None:
        query_files = [name for name in query_files if query_number(name) >= args.start]
    if args.end is not None:
        query_files = [name for name in query_files if query_number(name) <= args.end]
    if args.limit is not None:
        query_files = query_files[:args.limit]
    if not query_files:
        raise SystemExit(f"No query files selected in {query_dir}")
    empty_queries = [
        name
        for name in query_files
        if not (query_dir / name).is_file() or (query_dir / name).stat().st_size == 0
    ]
    if empty_queries:
        raise FileNotFoundError(
            f"Missing or empty SQL query files in {query_dir}: {empty_queries}"
        )
    return query_files


def main() -> None:
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
    query_files = _select_query_files(parser, args)

    schema_path = Path(SQL_SCHEMA_PATH)
    if not schema_path.is_file():
        raise FileNotFoundError(
            f"TPC-DS schema not found at {schema_path}; run pipeline/prepare.py first."
        )
    table_names = get_table_names(schema_path.read_text())
    missing_tables = [
        name
        for name in table_names
        if not parquet_table_is_complete(os.path.join(PARQUET_DIR, name))
    ]
    if missing_tables:
        raise FileNotFoundError(
            f"Missing or incomplete Parquet tables in {PARQUET_DIR}: "
            + ", ".join(missing_tables)
            + ". Run pipeline/convert.py first."
        )

    Path(REPORT_PATH).parent.mkdir(parents=True, exist_ok=True)
    spark = create_spark_session("TPCDS_Benchmark")
    try:
        spark.sparkContext.setLogLevel("ERROR")
        print("Registering temporary views from Parquet files...")
        register_tables_in_spark(spark, PARQUET_DIR)
        print("All tables are ready for SQL queries!\n")

        print("=================================")
        print("  TPC-DS BENCHMARK START   ")
        print("=================================\n")

        report = Workbook()
        sheet = report.active
        sheet.title = "Benchmark"
        sheet.append(["query file", "query", "status", "result", "error", "time (s)"])

        for query_file in query_files:
            query_path = os.path.join(QUERIES_DIR, query_file)
            with open(query_path, "r") as query_stream:
                raw_sql = normalize_sql_for_spark(query_stream.read())

            statements = [stmt.strip() for stmt in raw_sql.split(";") if stmt.strip()]
            if len(statements) > 1:
                print(
                    f"{query_file} contains {len(statements)} queries, "
                    "executing all of them"
                )

            for index, sql in enumerate(statements, start=1):
                label = (
                    f"{query_file} (part {index}/{len(statements)})"
                    if len(statements) > 1
                    else query_file
                )
                print(f"Executing {label}...")
                start_time = time.time()
                try:
                    rows = spark.sql(sql).count()
                    elapsed = time.time() - start_time
                    print(f"✅ OK! Time: {elapsed:.2f} s (Rows: {rows})")
                    status, result, error = "success", f"{rows} rows", ""
                except Exception as exception:
                    elapsed = time.time() - start_time
                    print(f"❌ FAILED in {elapsed:.2f} s")
                    status, result, error = "error", "", str(exception)
                    if len(query_files) == 1:
                        print(f"\n{error}\n")

                sheet.append(
                    [
                        label,
                        sql[:32767],
                        status,
                        result,
                        error[:32767],
                        round(elapsed, 2),
                    ]
                )

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
    finally:
        spark.stop()


if __name__ == "__main__":
    main()