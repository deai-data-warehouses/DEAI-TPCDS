import argparse
import re
from datetime import datetime
from pathlib import Path
from time import perf_counter

from constants import (
    SQL_SCHEMA_PATH,
    create_spark_session,
    get_scenario_paths,
)
from utils import clean_sql_comments, get_table_names, parquet_table_is_complete

from config.config import options


def write_table_partitioned(dataframe, table_name, output_path):
    if table_name == "catalog_sales":
        dataframe.write.mode("overwrite").partitionBy("cs_sold_date_sk").parquet(str(output_path))
    elif table_name == "inventory":
        dataframe.write.mode("overwrite").partitionBy("inv_date_sk").parquet(str(output_path))
    else:
        dataframe.write.mode("overwrite").parquet(str(output_path))

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Convert TPC-DS raw data files to Parquet."
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="rebuild all Parquet tables, overwriting existing outputs",
    )
    parser.add_argument(
        "--scale-factor",
        type=int,
        required=True,
        help="TPC-DS data scale factor used for this run",
    )
    args = parser.parse_args()
    if args.scale_factor <= 0:
        parser.error("--scale-factor must be a positive integer")

    data_dir_str, parquet_dir_str, _ = get_scenario_paths(args.scale_factor)
    data_dir = Path(data_dir_str)
    parquet_dir = Path(parquet_dir_str)

    schema_path = Path(SQL_SCHEMA_PATH)
    if not schema_path.is_file():
        raise FileNotFoundError(
            f"TPC-DS schema not found at {schema_path}; run pipeline/prepare.py first."
        )
    if not data_dir.is_dir():
        raise FileNotFoundError(
            f"Raw data directory not found at {data_dir}; run pipeline/prepare.py first."
        )

    schema_sql = clean_sql_comments(schema_path.read_text())
    statements = [statement.strip() for statement in schema_sql.split(";") if statement.strip()]
    table_statements: list[tuple[str, str]] = []
    for statement in statements:
        match = re.search(r"\bcreate\s+table\s+([A-Za-z_]\w*)", statement, re.IGNORECASE)
        if match:
            table_name = match.group(1).lower()
            if table_name != "dbgen_version":
                table_statements.append((table_name, statement))

    table_names = get_table_names(schema_sql)
    if not table_names:
        raise RuntimeError(f"No TPC-DS tables found in {schema_path}")
    missing_data = [
        name
        for name in table_names
        if not (data_dir / f"{name}.dat").is_file()
        or (data_dir / f"{name}.dat").stat().st_size == 0
    ]
    if missing_data:
        raise FileNotFoundError(
            f"Missing or empty raw data files in {data_dir}: "
            + ", ".join(f"{name}.dat" for name in missing_data)
            + ". Run pipeline/prepare.py to generate them."
        )

    completed = [
        name
        for name in table_names
        if parquet_table_is_complete(str(parquet_dir / name))
    ]
    if not args.force and len(completed) == len(table_names):
        print(f"All Parquet tables are already complete in {parquet_dir}; nothing to do.")
        return

    parquet_dir.mkdir(parents=True, exist_ok=True)
    parquet_generation_started = perf_counter()
    print(
        f"[{datetime.now():%Y-%m-%d %H:%M:%S}] "
        "Generate parquet files started."
    )
    spark = create_spark_session("TPCDS_Parquet_Conversion")
    try:
        spark.sparkContext.setLogLevel("ERROR")
        for table_name, statement in table_statements:
            output_path = parquet_dir / table_name
            if not args.force and parquet_table_is_complete(str(output_path)):
                print(f"Skipping completed table: {table_name}")
                continue

            print(f"Processing table: {table_name}...")
            clean_lines = [
                line for line in statement.splitlines()
                if "primary key" not in line.lower()
            ]
            clean_statement = re.sub(
                r",\s*\)", "\n)", "\n".join(clean_lines)
            )

            spark.sql(clean_statement)
            table_schema = spark.table(table_name).schema
            dataframe = spark.read.csv(
                path=str(data_dir / f"{table_name}.dat"),
                sep="|",
                schema=table_schema,
            )

            # Option to use partition
            if options.spark.use_partition:
                write_table_partitioned(dataframe, table_name, output_path)
            else:
                dataframe.write.mode("overwrite").parquet(str(output_path))
            
            if not parquet_table_is_complete(str(output_path)):
                raise RuntimeError(f"Parquet output is incomplete for table {table_name}")
            print(f"Parquet export completed: {table_name}")
        input(f"Spark UI still up at {spark.sparkContext.uiWebUrl} - press Enter to exit...")
    finally:
        spark.stop()

    incomplete = [
        name
        for name in table_names
        if not parquet_table_is_complete(str(parquet_dir / name))
    ]
    if incomplete:
        raise RuntimeError(f"Conversion left incomplete Parquet tables: {incomplete}")
    print(f"All tables have been converted to Parquet in {parquet_dir}.")
    print(
        f"[{datetime.now():%Y-%m-%d %H:%M:%S}] "
        "Generate parquet files finalized "
        f"(elapsed: {perf_counter() - parquet_generation_started:.2f}s)."
    )


if __name__ == "__main__":
    main()
