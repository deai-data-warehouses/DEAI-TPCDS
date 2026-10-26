import argparse
import os
import time
import uuid

from datetime import datetime, timezone
from jinja2 import StrictUndefined, Template
from pathlib import Path

from config_loader import options
from constants import (
    QUERY_VERSIONS,
    RUNS_DIR,
    SQL_SCHEMA_PATH,
    get_benchmark_queries_dir,
    get_scenario_paths,
)
from reporting.report import write_reports
from reporting.summary import build_summary
from spark_session import (
    apply_run_options,
    create_spark_session,
    register_tables_in_spark,
)
from toolkit.queries import normalize_sql_for_spark, select_query_files
from toolkit.schema import get_table_names, incomplete_parquet_tables


def _print_run_configuration(run_configuration: dict[str, int | bool | str]) -> None:
    """Print the run configuration as an aligned key/value table."""
    width = max(len(key) for key in run_configuration)
    print("=================================")
    print("  RUN CONFIGURATION")
    print("=================================")
    for key, value in run_configuration.items():
        print(f"  {key:<{width}} : {value}")
    print()


def _parse_args() -> argparse.Namespace:
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
        "--queries",
        choices=QUERY_VERSIONS,
        default="original",
        help="read queries from queries/<QUERIES>/queries_<N>GB (default: original)",
    )
    args = parser.parse_args()

    if args.scale_factor <= 0:
        parser.error("--scale-factor must be a positive integer")
    if args.query is not None and any(
        value is not None for value in (args.limit, args.start, args.end)
    ):
        parser.error("--query cannot be combined with --limit, --start or --end")
    return args


def _query_selection_label(args: argparse.Namespace) -> str:
    """Describe the query selection arguments, e.g. "full", "query5", "from10_first3"."""
    parts = []
    if args.query is not None:
        parts.append(f"query{args.query}")
    elif args.start is not None and args.end is not None:
        parts.append(f"query{args.start}-{args.end}")
    elif args.start is not None:
        parts.append(f"from{args.start}")
    elif args.end is not None:
        parts.append(f"to{args.end}")
    if args.limit is not None:
        parts.append(f"first{args.limit}")
    label = "_".join(parts) or "full"
    # Original queries keep the plain label; other versions are suffixed
    if args.queries != "original":
        label += f"_{args.queries}"
    return label


def main() -> None:
    args = _parse_args()
    _, parquet_dir, _ = get_scenario_paths(args.scale_factor)
    queries_dir = get_benchmark_queries_dir(args.scale_factor, args.queries)
    query_files = select_query_files(args, queries_dir)

    # 1. Check that the TPC-DS schema is present and that all Parquet tables are
    #    complete
    schema_path = Path(SQL_SCHEMA_PATH)
    table_names = get_table_names(schema_path)
    missing_tables = incomplete_parquet_tables(parquet_dir, table_names)
    if missing_tables:
        raise FileNotFoundError(
            f"Missing or incomplete Parquet tables in {parquet_dir}: "
            + ", ".join(missing_tables)
            + ". Run pipeline/convert.py first."
        )

    run_id = str(uuid.uuid4())
    run_started_at = datetime.now(timezone.utc)
    started_at = run_started_at.isoformat()
    # Folder name: <HHMMSS>_<scale>GB_<query selection>, e.g. 145347_1GB_full
    run_directory = os.path.join(
        RUNS_DIR,
        run_started_at.strftime("%Y-%m-%d"),
        f"{run_started_at.strftime('%H%M%S')}_{args.scale_factor:g}GB_"
        f"{_query_selection_label(args)}",
    )
    os.makedirs(run_directory, exist_ok=True)

    # Create session and configurations
    spark = create_spark_session("TPCDS_Benchmark")
    spark.sparkContext.setLogLevel("ERROR")

    run_configuration = apply_run_options(spark, options)
    _print_run_configuration(run_configuration)

    run_start = time.perf_counter()
    query_records = []

    print(f"Registering Parquet files as {options.spark.register_tables_as}...")
    register_tables_in_spark(spark, parquet_dir, options.spark.register_tables_as)
    print("All tables are ready for SQL queries!\n")

    print("=================================")
    print("  TPC-DS BENCHMARK START   ")
    print("=================================\n")

    for query_file in query_files:
        query_path = os.path.join(queries_dir, query_file)
        with open(query_path, "r", encoding="utf-8") as query_stream:
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
            print(f"Executing {label} ({options.general.runs_per_query} runs)...")

            run_times = []
            status, error, rows = "success", "", None

            for run_iter in range(options.general.runs_per_query):
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
            else:
                min_time, max_time, avg_time = None, None, None

            query_number = int(query_file.removeprefix("query").removesuffix(".sql"))

            query_records.append(
                {
                    "run_id": run_id,
                    "started_at": started_at,
                    "query_id": query_number,
                    "query_file": query_file,
                    "query_version": args.queries,
                    "statement_part": i,
                    "min_seconds": min_time,
                    "max_seconds": max_time,
                    "avg_seconds": avg_time,
                    "status": status,
                    "row_count": rows,
                    "error": error[:32767],
                    "scale_factor": args.scale_factor,
                    **run_configuration,
                }
            )

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
    summary = build_summary(
        query_records,
        run_id=run_id,
        started_at=started_at,
        scale_factor=args.scale_factor,
        parquet_dir=parquet_dir,
        queries_dir=queries_dir,
        run_elapsed_seconds=run_elapsed_seconds,
    )
    summary.update(run_configuration)
    write_reports(run_directory, query_records, summary)

    input(f"\n⚠️ Spark UI still up at {spark.sparkContext.uiWebUrl} - press Enter to exit...")
    try:
        spark.sparkContext.cancelAllJobs()
        spark.stop()
    except:
        print(f"\nError stopping Spark session. It may have already been stopped or not started properly.")


if __name__ == "__main__":
    main()
