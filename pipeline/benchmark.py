import argparse
import os
import time
import uuid

from datetime import datetime, timezone
from pathlib import Path

from py4j.protocol import Py4JError
from pyspark.errors import PySparkException

from config_loader import options
from constants import (
    QUERY_VERSIONS,
    RUNS_DIR,
    SQL_SCHEMA_PATH,
    get_benchmark_queries_dir,
    get_results_dir,
    get_scenario_paths,
)
from reporting.report import write_reports
from reporting.summary import build_summary
from reporting.models import QueryRow
from spark_session import (
    apply_run_options,
    create_spark_session,
    get_session_configuration,
    register_tables_in_spark,
)
from toolkit.queries import normalize_sql_for_spark, select_query_files, save_query_plan
from toolkit.results import compare_results
from toolkit.schema import get_table_names, incomplete_parquet_tables


def _print_run_configuration(run_configuration: dict[str, int | bool]) -> None:
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
        "--query-list",
        type=int,
        nargs="+",
        help="run a specific list of query numbers (e.g. --query-list 14 23 72)",
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
    if args.query_list is not None and any(
        value is not None for value in (args.query, args.limit, args.start, args.end)
    ):
        parser.error("--query-list cannot be combined with --query, --limit, --start or --end")
    return args

def _query_selection_label(args: argparse.Namespace) -> str:
    """
    Describe the query selection and version, e.g. "full_original",
    "query5_changed", "from10_first3_original".
    """
    parts = []
    if args.query_list is not None:
        # e.g. "queries_14_23_72"
        parts.append(f"queries_{'_'.join(str(q) for q in args.query_list)}")
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
    return f"{label}_{args.queries}"


def main() -> None:
    args = _parse_args()
    _, parquet_dir, _ = get_scenario_paths(
        args.scale_factor, options.spark.use_partition, options.spark.use_repartition
    )
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
    query_selection = _query_selection_label(args)
    # Folder name: <HHMMSS>_<scale>GB_<query selection>, e.g. 145347_1GB_full_original
    run_directory = os.path.join(
        RUNS_DIR,
        run_started_at.strftime("%Y-%m-%d"),
        f"{run_started_at.strftime('%H%M%S')}_{args.scale_factor:g}GB_"
        f"{query_selection}",
    )
    os.makedirs(run_directory, exist_ok=True)

    spark = create_spark_session(
        "TPCDS_Benchmark",
        use_all_cores=options.spark.use_all_cores,
        tune_memory_allocation=options.spark.tune_memory_allocation
    )
    spark.sparkContext.setLogLevel("ERROR")

    run_configuration = apply_run_options(spark, options)
    _print_run_configuration(run_configuration)

    print(f"Registering Parquet files as {options.spark.register_tables_as}...")
    if options.spark.cbo_enabled:
        print("Computing table and column statistics for the CBO...")
    register_tables_in_spark(
        spark_session=spark,
        parquet_dir=parquet_dir,
        register_as=options.spark.register_tables_as,
        analyze=options.spark.cbo_enabled,
        enable_static_caching=options.spark.enable_static_caching
    )
    print("All tables are ready for SQL queries!\n")

    print("=================================")
    print("  TPC-DS BENCHMARK START   ")
    print("=================================\n")

    query_records = []
    runs_per_query = options.general.runs_per_query
    for query_file in query_files:
        query_path = os.path.join(queries_dir, query_file)
        with open(query_path, "r", encoding="utf-8") as f:
            clean_sql = normalize_sql_for_spark(f.read())

        statements = [stmt.strip() for stmt in clean_sql.split(";") if stmt.strip()]
        if len(statements) > 1:
            print(
                f"{query_file} contains {len(statements)} queries, "
                "executing all of them"
            )

        for i, sql in enumerate(statements, start=1):
            label = f"{query_file} (part {i}/{len(statements)})"
            print(f"Executing {label} ({runs_per_query} runs)...")

            query_number = int(query_file.removeprefix("query").removesuffix(".sql"))
            for run_iter in range(1, runs_per_query + 1):
                try:
                    df = spark.sql(sql)
                    start_time = time.perf_counter()
                    result = df.collect()
                    elapsed = time.perf_counter() - start_time
                    rows = len(result)
                    print(f"  - Run {run_iter}: ✅ {elapsed:.2f} s (Rows: {rows})")
                    status, error, rows = "success", "", rows

                    # We save the plan onfly for the first run of each statement,
                    # after it has been executed as AQE could have changed it
                    if run_iter == 1:
                        try:
                            plan_paths = save_query_plan(
                                df,
                                run_directory,
                                query_file,
                                i if len(statements) > 1 else None,
                            )
                            for plan_path in plan_paths:
                                print(f"  Query plan saved to {plan_path}")
                        except (OSError, Py4JError, PySparkException) as e:
                            print(f"  - Could not save query plan: {str(e)}")

                    # Compare the result with the baseline from save_baseline.py,
                    # outside the timed section
                    result_check = ""
                    if run_iter == 1:
                        try:
                            comparison = compare_results(
                                spark,
                                result,
                                df.schema,
                                get_results_dir(args.scale_factor),
                                query_file,
                                i if len(statements) > 1 else None,
                            )
                            result_check = comparison["status"]
                            mark = "✅" if result_check == "match" else "⚠️"
                            print(f"  {mark} Result vs baseline: {result_check}")
                            if result_check != "match":
                                for key, value in comparison.items():
                                    if key != "status":
                                        print(f"      {key}: {value}")
                        # A failed comparison must not mark the query run as failed
                        except Exception as e:
                            result_check = "error"
                            print(f"  - Could not compare with baseline: {str(e)}")

                except Exception as e:
                    elapsed = time.perf_counter() - start_time
                    print(f"  - Run {run_iter}: ❌ FAILED in {elapsed:.2f} s")
                    status, error, rows, result_check = "error", str(e), None, ""

                query_records.append(
                    QueryRow(
                        run_id=run_id,
                        query_filename=query_file,
                        query_number=query_number,
                        query_version=args.queries,
                        statement_part=i,
                        run_number=run_iter,
                        seconds=elapsed,
                        status=status,
                        row_count=rows,
                        result_check=result_check,
                        error=error[:32767],  # Truncate to fit in CSV column
                    )
                )

            # Force a full JVM garbage collection between statements. Otherwise
            # the heap keeps the memory grown by earlier queries and, over a
            # long run, the host can run out of RAM and the OOM killer kills the
            # Spark JVM. Together with the heap free ratios set in
            # create_spark_session, this lets the JVM return unused memory to
            # the OS.
            try:
                spark._jvm.System.gc()
            except Exception:
                pass

    summary = build_summary(
        query_records=query_records,
        run_id=run_id,
        started_at=started_at,
        scale_factor=args.scale_factor,
        query_selection=query_selection,
        parquet_dir=parquet_dir,
        queries_dir=queries_dir,
        options=options
    )
    run_config = [
        {"section": section, "option": option, "value": value}
        for section, section_options in options.model_dump().items()
        for option, value in section_options.items()
    ]
    write_reports(
        run_directory,
        [r.model_dump() for r in query_records],
        summary,
        run_config=run_config,
        spark_config=get_session_configuration(spark),
    )

    input(f"\n⚠️ Spark UI still up at {spark.sparkContext.uiWebUrl} - press Enter to exit...")
    try:
        spark.sparkContext.cancelAllJobs()
        spark.stop()
    except:
        print(f"\nError stopping Spark session. It may have already been stopped or not started properly.")


if __name__ == "__main__":
    main()
