import os
import re
import time
import uuid

from datetime import datetime, timezone
from pathlib import Path

from jinja2 import StrictUndefined, Template

from benchmark import _parse_args, _query_selection_label
from config_loader import RunConfig, SparkConfig, options
from constants import (
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


DIFFERENCE_SAMPLE_SIZE = 5
DIFFERENCE_SAMPLE_MAX_LENGTH = 3000


def _render_statements(query_path: str, query_options: RunConfig) -> list[str]:
    with open(query_path, "r", encoding="utf-8") as query_stream:
        rendered_sql = Template(
            query_stream.read(), undefined=StrictUndefined
        ).render(options=query_options)
    raw_sql = normalize_sql_for_spark(rendered_sql)
    return [statement.strip() for statement in raw_sql.split(";") if statement.strip()]


def _baseline_run_configuration(spark, baseline_options: RunConfig) -> dict:
    """Report baseline settings without applying config.toml to the session."""
    threshold = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
    threshold_match = re.match(r"-?\d+", threshold)
    if threshold_match is None:
        raise ValueError(f"Unexpected Spark broadcast threshold value: {threshold}")
    threshold_bytes = int(threshold_match.group())
    return {
        "runs_per_query": options.general.runs_per_query,
        "use_partition": baseline_options.spark.use_partition,
        "use_broadcast_joins_hints": (
            baseline_options.spark.use_broadcast_joins_hints
        ),
        "spark_adaptive_enabled": (
            spark.conf.get("spark.sql.adaptive.enabled").lower() == "true"
        ),
        "spark_adaptive_coalesce_partitions_enabled": (
            spark.conf.get("spark.sql.adaptive.coalescePartitions.enabled").lower()
            == "true"
        ),
        "spark_adaptive_skew_join_enabled": (
            spark.conf.get("spark.sql.adaptive.skewJoin.enabled").lower() == "true"
        ),
        "spark_auto_broadcast_join_threshold_mb": threshold_bytes / (1024 * 1024),
    }


def _sample_rows(rows) -> str:
    sample = "\n".join(str(row) for row in rows)
    return sample[:DIFFERENCE_SAMPLE_MAX_LENGTH]


def _compare_results(original_spark, changed_spark, original_sql: str, changed_sql: str):
    original = original_spark.sql(original_sql)
    changed = changed_spark.sql(changed_sql)

    original_schema = [
        (field.name, field.dataType.simpleString()) for field in original.schema.fields
    ]
    changed_schema = [
        (field.name, field.dataType.simpleString()) for field in changed.schema.fields
    ]
    if original_schema != changed_schema:
        return (
            False,
            f"Schema: {original_schema}\n"
            f"Rows:\n{_sample_rows(original.limit(DIFFERENCE_SAMPLE_SIZE).collect())}",
            f"Schema: {changed_schema}\n"
            f"Rows:\n{_sample_rows(changed.limit(DIFFERENCE_SAMPLE_SIZE).collect())}",
        )

    original_only = original.exceptAll(changed).limit(DIFFERENCE_SAMPLE_SIZE).collect()
    changed_only = changed.exceptAll(original).limit(DIFFERENCE_SAMPLE_SIZE).collect()
    return (
        not original_only and not changed_only,
        _sample_rows(original_only),
        _sample_rows(changed_only),
    )


def _append_query_record(
    records: list[dict],
    *,
    run_id: str,
    started_at: str,
    query_file: str,
    query_number: int,
    statement_part: int,
    version: str,
    result: dict,
    scale_factor: int,
    run_configuration: dict,
) -> None:
    records.append(
        {
            "run_id": run_id,
            "started_at": started_at,
            "query_id": query_number,
            "query_file": query_file,
            "query_version": version,
            "statement_part": statement_part,
            "min_seconds": result["min_seconds"],
            "max_seconds": result["max_seconds"],
            "avg_seconds": result["avg_seconds"],
            "status": result["status"],
            "row_count": result["row_count"],
            "error": result["error"][:32767],
            "scale_factor": scale_factor,
            **run_configuration,
        }
    )


def main() -> None:
    args = _parse_args()
    _, parquet_dir, _ = get_scenario_paths(args.scale_factor)
    changed_dir = get_benchmark_queries_dir(args.scale_factor, "changed")
    original_dir = get_benchmark_queries_dir(args.scale_factor, "original")
    query_files = select_query_files(args, changed_dir)

    missing_original = [
        query_file
        for query_file in query_files
        if not (Path(original_dir) / query_file).is_file()
    ]
    if missing_original:
        raise FileNotFoundError(
            f"Missing original query counterparts in {original_dir}: "
            + ", ".join(missing_original)
        )
    empty_queries = [
        os.path.join(directory, query_file)
        for directory in (changed_dir, original_dir)
        for query_file in query_files
        if (Path(directory) / query_file).stat().st_size == 0
    ]
    if empty_queries:
        raise FileNotFoundError(f"Empty SQL query files: {', '.join(empty_queries)}")

    table_names = get_table_names(Path(SQL_SCHEMA_PATH))
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
    run_directory = os.path.join(
        RUNS_DIR,
        run_started_at.strftime("%Y-%m-%d"),
        f"{run_started_at.strftime('%H%M%S')}_{args.scale_factor:g}GB_"
        f"{_query_selection_label(args)}_validate",
    )
    os.makedirs(run_directory, exist_ok=True)

    baseline_options = RunConfig(
        general=options.general,
        spark=SparkConfig(),
    )
    spark = create_spark_session("TPCDS_Validate")
    spark.sparkContext.setLogLevel("ERROR")
    original_spark = spark.newSession()
    changed_spark = spark.newSession()

    try:
        changed_configuration = apply_run_options(changed_spark, options)
        original_configuration = _baseline_run_configuration(
            original_spark, baseline_options
        )
        print(
            f"Registering temporary views from Parquet files in both sessions..."
        )
        register_tables_in_spark(original_spark, parquet_dir)
        register_tables_in_spark(changed_spark, parquet_dir)

        query_records = []
        validation_records = []
        run_start = time.perf_counter()

        for query_file in query_files:
            original_sql = _render_statements(
                os.path.join(original_dir, query_file), baseline_options
            )
            changed_sql = _render_statements(
                os.path.join(changed_dir, query_file), options
            )
            query_number = int(
                query_file.removeprefix("query").removesuffix(".sql")
            )
            runs = options.general.runs_per_query

            if len(original_sql) != len(changed_sql):
                mismatch = (
                    f"Statement count mismatch: original has {len(original_sql)}, "
                    f"changed has {len(changed_sql)}"
                )
                print(f"{query_file}: {mismatch}")
                for statement_part in range(
                    1, max(len(original_sql), len(changed_sql)) + 1
                ):
                    for version, statements, configuration in (
                        ("original", original_sql, original_configuration),
                        ("changed", changed_sql, changed_configuration),
                    ):
                        if statement_part <= len(statements):
                            _append_query_record(
                                query_records,
                                run_id=run_id,
                                started_at=started_at,
                                query_file=query_file,
                                query_number=query_number,
                                statement_part=statement_part,
                                version=version,
                                result={
                                    "status": "error",
                                    "error": mismatch,
                                    "row_count": None,
                                    "min_seconds": None,
                                    "max_seconds": None,
                                    "avg_seconds": None,
                                },
                                scale_factor=args.scale_factor,
                                run_configuration=configuration,
                            )
                    validation_records.append(
                        {
                            "query_id": query_number,
                            "query_file": query_file,
                            "statement_part": statement_part,
                            "original_avg_seconds": None,
                            "changed_avg_seconds": None,
                            "optimization_gain_percent": None,
                            "results_identical": "not compared",
                            "status": "statement count mismatch",
                            "original_only_sample": mismatch,
                            "changed_only_sample": "",
                            "error": mismatch,
                        }
                    )
                continue

            for statement_part, (original_statement, changed_statement) in enumerate(
                zip(original_sql, changed_sql), start=1
            ):
                label = (
                    f"{query_file} (part {statement_part}/{len(original_sql)})"
                    if len(original_sql) > 1
                    else query_file
                )
                print(f"Validating {label} ({runs} runs per version)...")
                results = {
                    version: {
                        "run_times": [],
                        "row_count": None,
                        "error": "",
                        "status": "success",
                    }
                    for version in ("original", "changed")
                }
                sessions = {
                    "original": (original_spark, original_statement),
                    "changed": (changed_spark, changed_statement),
                }
                for run_index in range(runs):
                    order = (
                        ("original", "changed")
                        if run_index % 2 == 0
                        else ("changed", "original")
                    )
                    for version in order:
                        session, statement = sessions[version]
                        started = time.perf_counter()
                        try:
                            row_count = session.sql(statement).count()
                            elapsed = time.perf_counter() - started
                            results[version]["run_times"].append(elapsed)
                            results[version]["row_count"] = row_count
                            print(
                                f"  {version} run {run_index + 1}: "
                                f"{elapsed:.2f} s (Rows: {row_count})"
                            )
                        except Exception as exception:
                            elapsed = time.perf_counter() - started
                            results[version]["status"] = "error"
                            results[version]["error"] = str(exception)
                            print(
                                f"  {version} run {run_index + 1}: FAILED "
                                f"in {elapsed:.2f} s: {exception}"
                            )

                for version, configuration in (
                    ("original", original_configuration),
                    ("changed", changed_configuration),
                ):
                    result = results[version]
                    succeeded = (
                        result["status"] == "success"
                        and len(result["run_times"]) == runs
                    )
                    summary_result = {
                        "status": "success" if succeeded else "error",
                        "error": result["error"],
                        "row_count": result["row_count"],
                        "min_seconds": (
                            min(result["run_times"]) if succeeded else None
                        ),
                        "max_seconds": (
                            max(result["run_times"]) if succeeded else None
                        ),
                        "avg_seconds": (
                            sum(result["run_times"]) / len(result["run_times"])
                            if succeeded
                            else None
                        ),
                    }
                    _append_query_record(
                        query_records,
                        run_id=run_id,
                        started_at=started_at,
                        query_file=query_file,
                        query_number=query_number,
                        statement_part=statement_part,
                        version=version,
                        result=summary_result,
                        scale_factor=args.scale_factor,
                        run_configuration=configuration,
                    )

                original_result = results["original"]
                changed_result = results["changed"]
                if all(
                    result["status"] == "success"
                    and len(result["run_times"]) == runs
                    for result in (original_result, changed_result)
                ):
                    original_avg = sum(original_result["run_times"]) / runs
                    changed_avg = sum(changed_result["run_times"]) / runs
                    gain_percent = (
                        (original_avg - changed_avg) / original_avg * 100
                        if original_avg
                        else None
                    )
                    try:
                        identical, original_sample, changed_sample = _compare_results(
                            original_spark,
                            changed_spark,
                            original_statement,
                            changed_statement,
                        )
                        validation_status = "success"
                        error = ""
                        result_label = "identical" if identical else "different"
                        print(f"  Results are {result_label}.")
                    except Exception as exception:
                        identical = False
                        original_sample = ""
                        changed_sample = ""
                        validation_status = "comparison error"
                        error = str(exception)
                        gain_percent = None
                        print(f"  Could not compare results: {error}")
                else:
                    identical = False
                    original_sample = ""
                    changed_sample = ""
                    original_avg = (
                        sum(original_result["run_times"]) / len(original_result["run_times"])
                        if original_result["status"] == "success"
                        and len(original_result["run_times"]) == runs
                        else None
                    )
                    changed_avg = (
                        sum(changed_result["run_times"]) / len(changed_result["run_times"])
                        if changed_result["status"] == "success"
                        and len(changed_result["run_times"]) == runs
                        else None
                    )
                    gain_percent = None
                    validation_status = "execution error"
                    error = "; ".join(
                        f"{version}: {result['error']}"
                        for version, result in (
                            ("original", original_result),
                            ("changed", changed_result),
                        )
                        if result["status"] != "success"
                    )
                    print(f"  Results not compared because one version failed.")

                validation_records.append(
                    {
                        "query_id": query_number,
                        "query_file": query_file,
                        "statement_part": statement_part,
                        "original_avg_seconds": original_avg,
                        "changed_avg_seconds": changed_avg,
                        "optimization_gain_percent": gain_percent,
                        "results_identical": (
                            "yes" if identical else "no"
                        ) if validation_status == "success" else "not compared",
                        "status": validation_status,
                        "original_only_sample": original_sample,
                        "changed_only_sample": changed_sample,
                        "error": error,
                    }
                )

        run_elapsed_seconds = time.perf_counter() - run_start
        summary = build_summary(
            query_records,
            run_id=run_id,
            started_at=started_at,
            scale_factor=args.scale_factor,
            parquet_dir=parquet_dir,
            queries_dir=f"{original_dir} | {changed_dir}",
            run_elapsed_seconds=run_elapsed_seconds,
        )
        summary.update(changed_configuration)
        write_reports(
            run_directory, query_records, summary, validation_records
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()