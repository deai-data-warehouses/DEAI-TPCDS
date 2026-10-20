import argparse
import csv
import os
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from constants import (
    RUNS_DIR,
    SQL_SCHEMA_PATH,
    create_spark_session,
    get_scenario_paths,
)
from jinja2 import StrictUndefined, Template
from openpyxl import Workbook
from openpyxl.styles import Alignment, Font
from pyspark.sql import SparkSession
from utils import (
    get_table_names,
    normalize_sql_for_spark,
    parquet_table_is_complete,
    register_tables_in_spark,
    sort_query_files,
)

# Make the repository-root config package importable when run as a script.
project_root = str(Path(__file__).resolve().parent.parent)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from config.config import options


def set_session_configurations(
    spark_session: SparkSession,
) -> dict[str, int | bool]:
    """
    Apply run options to Spark and return them for inclusion in run metadata.
    """
    run_configuration: dict[str, int | bool] = {
        "num_runs": options.general.num_runs,
        "use_partition": options.spark.use_partition,
        "use_broadcast_joins_hints": options.spark.use_broadcast_joins_hints,
        "spark_adaptive_enabled": options.spark.adaptative_enabled,
        "spark_adaptive_coalesce_partitions_enabled": (
            options.spark.adaptative_coalesce_partitions
        ),
        "spark_adaptive_skew_join_enabled": options.spark.adapatative_skew_join,
    }
    spark_settings = {
        "spark.sql.adaptive.enabled": "spark_adaptive_enabled",
        "spark.sql.adaptive.coalescePartitions.enabled": (
            "spark_adaptive_coalesce_partitions_enabled"
        ),
        "spark.sql.adaptive.skewJoin.enabled": "spark_adaptive_skew_join_enabled",
    }

    for spark_key, configuration_key in spark_settings.items():
        spark_session.conf.set(
            spark_key, str(run_configuration[configuration_key]).lower()
        )
    return run_configuration


def _select_query_files(
    parser: argparse.ArgumentParser, args: argparse.Namespace
) -> list[str]:
    if args.query is not None and any(
        value is not None for value in (args.limit, args.start, args.end)
    ):
        parser.error("--query cannot be combined with --limit, --start or --end")

    query_dir = Path(get_scenario_paths(args.scale_factor)[2])
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
        query_files = query_files[: args.limit]
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
    parser.add_argument(
        "--scale-factor",
        type=int,
        required=True,
        help="TPC-DS data scale factor used for this run",
    )
    parser.add_argument(
        "--test-run",
        action="store_true",
        help="run the benchmark in test mode, saving results in a separate folder",
    )
    args = parser.parse_args()

    if args.scale_factor <= 0:
        parser.error("--scale-factor must be a positive integer")

    query_files = _select_query_files(parser, args)
    _, parquet_dir, queries_dir = get_scenario_paths(args.scale_factor)

    schema_path = Path(SQL_SCHEMA_PATH)
    if not schema_path.is_file():
        raise FileNotFoundError(
            f"TPC-DS schema not found at {schema_path}; run pipeline/prepare.py first."
        )
    table_names = get_table_names(schema_path.read_text())
    missing_tables = [
        name
        for name in table_names
        if not parquet_table_is_complete(os.path.join(parquet_dir, name))
    ]
    if missing_tables:
        raise FileNotFoundError(
            f"Missing or incomplete Parquet tables in {parquet_dir}: "
            + ", ".join(missing_tables)
            + ". Run pipeline/convert.py first."
        )

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
        code_version = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
            cwd=repository_root,
        ).stdout.strip()
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

    # Create session and configurations
    spark = create_spark_session("TPCDS_Benchmark")
    run_configuration = set_session_configurations(spark)

    run_start = time.perf_counter()
    query_records = []
    spark.sparkContext.setLogLevel("ERROR")
    print("Registering temporary views from Parquet files...")
    register_tables_in_spark(spark, parquet_dir)
    print("All tables are ready for SQL queries!\n")

    print("=================================")
    print("  TPC-DS BENCHMARK START   ")
    print("=================================\n")

    # Create in-memory Excel file
    report = Workbook()
    query_sheet = report.active
    # Worksheet tab to store results from the query runs
    query_sheet.title = "Query Runs"

    query_headers = [
        "run_id",
        "started_at",
        "query_id",
        "query_file",
        "statement_part",
        "label",
        "elapsed_seconds",
        "min_seconds",
        "max_seconds",
        "avg_seconds",
        "status",
        "row_count",
        "error",
        "scale_factor",
        "code_version",
        "working_tree_dirty",
        "sql",
        "test_run",
    ] + list(run_configuration)
    query_sheet.append(query_headers)

    # multiple runs for each query to get more stable timing results
    NUM_RUNS = 5

    for query_file in query_files:
        query_path = os.path.join(queries_dir, query_file)
        with open(query_path, "r", encoding="utf-8") as query_stream:
            # a
            sql_template = query_stream.read()
            rendered_sql = Template(sql_template, undefined=StrictUndefined).render(
                options=options
            )
            # Normalize the SQL for Spark (remove comments, etc.)
            raw_sql = normalize_sql_for_spark(rendered_sql)

        statements = [stmt.strip() for stmt in raw_sql.split(";") if stmt.strip()]
        if len(statements) > 1:
            print(
                f"{query_file} contains {len(statements)} queries, executing all of them"
            )

        for i, sql in enumerate(statements, start=1):
            label = (
                f"{query_file} (part {i}/{len(statements)})"
                if len(statements) > 1
                else query_file
            )
            print(f"Executing {label} ({options.general.num_runs} runs)...")

            run_times = []
            status, error, rows = "success", "", None

            for run_iter in range(options.general.num_runs):
                start_time = time.perf_counter()
                try:
                    rows = spark.sql(sql).count()
                    elapsed = time.perf_counter() - start_time
                    run_times.append(elapsed)
                    print(f"  - Run {run_iter + 1}: ✅ {elapsed:.2f} s (Rows: {rows})")
                except Exception as e:
                    elapsed = time.perf_counter() - start_time
                    print(f"  - Run {run_iter + 1}: ❌ FAILED in {elapsed:.2f} s")
                    status, error = "error", str(e)
                    if len(query_files) == 1:
                        print(f"\n{error}\n")
                    break

            if status == "success" and run_times:
                avg_time = sum(run_times) / len(run_times)
                min_time = min(run_times)
                max_time = max(run_times)
                final_elapsed = avg_time
            else:
                final_elapsed = elapsed
                min_time, max_time, avg_time = None, None, None

            query_number = int(query_file.removeprefix("query").removesuffix(".sql"))

            query_records.append(
                {
                    "run_id": run_id,
                    "started_at": started_at,
                    "query_id": query_number,
                    "query_file": query_file,
                    "statement_part": i,
                    "label": label,
                    "elapsed_seconds": final_elapsed,
                    "min_seconds": min_time,
                    "max_seconds": max_time,
                    "avg_seconds": avg_time,
                    "status": status,
                    "row_count": rows,
                    "error": error[:32767],
                    "scale_factor": args.scale_factor,
                    "code_version": code_version,
                    "working_tree_dirty": working_tree_dirty,
                    "sql": sql[:32767],
                    "test_run": args.test_run,
                    **run_configuration,
                }
            )
            query_sheet.append(list(query_records[-1].values()))

        # Force a full JVM garbage collection between statements (outside
        # the timed section). Otherwise the heap keeps the memory grown by
        # earlier queries and, over a long run, the host can run out of RAM
        # and the OOM killer kills the Spark JVM. Together with the heap
        # free ratios set in create_spark_session, this lets the JVM return
        # unused memory to the OS.
        try:
            spark._jvm.System.gc()
        except Exception:
            pass

    run_elapsed_seconds = time.perf_counter() - run_start
    successful_records = [r for r in query_records if r["status"] == "success"]
    all_query_times = [r["elapsed_seconds"] for r in query_records]
    successful_query_times = [r["elapsed_seconds"] for r in successful_records]

    summary = {
        "run_id": run_id,
        "started_at": started_at,
        "scale_factor": args.scale_factor,
        "parquet_dir": parquet_dir,
        "queries_dir": queries_dir,
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
        "test_run": args.test_run,
        **run_configuration,
    }
    summary_sheet = report.create_sheet("Run Summary")
    summary_headers = list(summary)
    summary_sheet.append(summary_headers)
    summary_sheet.append(list(summary.values()))

    for worksheet, widths in (
        (
            query_sheet,
            (36, 28, 10, 18, 15, 30, 18, 14, 14, 14, 12, 12, 60, 14, 14, 18, 60),
        ),
        (summary_sheet, (36, 28, 14, 50, 50, 14, 18, 20, 20, 22, 22, 20, 20, 20)),
    ):
        for cell in worksheet[1]:
            cell.font = Font(bold=True)

        for column, width in enumerate(widths, start=1):
            worksheet.column_dimensions[
                worksheet.cell(row=1, column=column).column_letter
            ].width = width

        for row in worksheet.iter_rows(min_row=2):
            for cell in row:
                cell.alignment = Alignment(wrap_text=True, vertical="top")

        worksheet.freeze_panes = "A2"

    if args.test_run:
        run_directory = os.path.join(run_directory, "test")
        os.makedirs(run_directory, exist_ok=True)
        report_path = os.path.join(run_directory, "benchmark_report.xlsx")

    report.save(report_path)
    print(f"\nReport saved to {report_path}")

    for worksheet, csv_filename in (
        (query_sheet, "query_runs.csv"),
        (summary_sheet, "run_summary.csv"),
    ):
        csv_path = os.path.join(run_directory, csv_filename)
        with open(csv_path, "w", newline="", encoding="utf-8") as csv_file:
            writer = csv.writer(csv_file, delimiter=";")
            writer.writerows(
                [cell.value for cell in row] for row in worksheet.iter_rows()
            )
        print(f"CSV saved to {csv_path}")

    if args.test_run:
        input(
            f"Spark UI still up at {spark.sparkContext.uiWebUrl} - press Enter to exit..."
        )
        try:
            spark.sparkContext.cancelAllJobs()
            spark.stop()
        except:
            print(
                f"\nError stopping Spark session. It may have already been stopped or not started properly."
            )


if __name__ == "__main__":
    main()
