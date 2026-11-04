import argparse
import os

from pathlib import Path

from config_loader import load_config
from constants import (
    DEFAULT_SPARK_CONFIG_PATH,
    SQL_SCHEMA_PATH,
    get_benchmark_queries_dir,
    get_results_dir,
    get_scenario_paths,
)
from spark_session import apply_run_options, create_spark_session, register_tables_in_spark
from toolkit.queries import normalize_sql_for_spark, select_query_files
from toolkit.results import results_are_saved, save_results
from toolkit.schema import get_table_names, incomplete_parquet_tables


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the original TPC-DS queries once with the default Spark "
        "settings and save their results, as the baseline benchmark.py compares against."
    )
    parser.add_argument(
        "--scale-factor",
        type=int,
        required=True,
        help="TPC-DS data scale factor",
    )
    parser.add_argument("--query", type=int, help="only query<QUERY>.sql (e.g. --query 72)")
    parser.add_argument(
        "--query-list",
        type=int,
        nargs="+",
        help="a specific list of query numbers (e.g. --query-list 14 23 72)",
    )
    parser.add_argument("--start", type=int, help="queries from query<START> onwards (inclusive)")
    parser.add_argument("--end", type=int, help="queries up to query<END> (inclusive)")
    parser.add_argument("--limit", type=int, help="only the first N queries (in query order)")
    parser.add_argument(
        "--force", action="store_true", help="overwrite results that are already saved"
    )
    args = parser.parse_args()

    if args.scale_factor <= 0:
        parser.error("--scale-factor must be a positive integer")
    if args.query is not None and args.query_list is not None:
        parser.error("--query cannot be combined with --query-list")
    if (args.query is not None or args.query_list is not None) and any(
        value is not None for value in (args.limit, args.start, args.end)
    ):
        parser.error("--query and --query-list cannot be combined with --limit, --start or --end")
    return args


def main() -> None:
    args = _parse_args()
    # Plain Spark settings, so the baseline doesn't depend on any tuning
    options = load_config(DEFAULT_SPARK_CONFIG_PATH)
    _, parquet_dir, _ = get_scenario_paths(
        args.scale_factor, options.spark.use_partition, options.spark.use_repartition
    )
    queries_dir = get_benchmark_queries_dir(args.scale_factor, "original")
    query_files = select_query_files(args, queries_dir)
    results_dir = get_results_dir(args.scale_factor)

    missing_tables = incomplete_parquet_tables(parquet_dir, get_table_names(Path(SQL_SCHEMA_PATH)))
    if missing_tables:
        raise FileNotFoundError(
            f"Missing or incomplete Parquet tables in {parquet_dir}: "
            + ", ".join(missing_tables)
            + ". Run pipeline/convert.py first."
        )

    spark = create_spark_session(
        "TPCDS_Baseline",
        use_all_cores=options.spark.use_all_cores,
        tune_memory_allocation=options.spark.tune_memory_allocation,
    )
    spark.sparkContext.setLogLevel("ERROR")
    apply_run_options(spark, options)
    register_tables_in_spark(
        spark_session=spark,
        parquet_dir=parquet_dir,
        register_as=options.spark.register_tables_as,
    )

    failed = []
    for query_file in query_files:
        with open(os.path.join(queries_dir, query_file), "r", encoding="utf-8") as f:
            clean_sql = normalize_sql_for_spark(f.read())
        statements = [stmt.strip() for stmt in clean_sql.split(";") if stmt.strip()]

        for i, sql in enumerate(statements, start=1):
            # Only files with several statements get a _part<N> suffix
            statement_part = i if len(statements) > 1 else None
            label = query_file if statement_part is None else f"{query_file} (part {i})"
            if not args.force and results_are_saved(results_dir, query_file, statement_part):
                print(f"⏭️ {label}: already saved, skipping (--force to overwrite)")
                continue
            try:
                df = spark.sql(sql)
                path = save_results(
                    spark, df.collect(), df.schema, results_dir, query_file, statement_part
                )
                print(f"✅ {label}: saved to {path}")
            except Exception as e:
                failed.append(label)
                print(f"❌ {label}: {str(e).splitlines()[0]}")

            # Same as benchmark.py: release JVM memory between statements
            try:
                spark._jvm.System.gc()
            except Exception:
                pass

    spark.stop()
    if failed:
        raise SystemExit(f"Could not save the results of: {', '.join(failed)}")


if __name__ == "__main__":
    main()
