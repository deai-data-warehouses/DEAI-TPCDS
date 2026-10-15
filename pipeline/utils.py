import os
import re
from pathlib import Path

def clean_sql_comments(sql_str: str) -> str:
     """
     Removes comments from a SQL string.

     Args:
         sql_str (str): The SQL string to clean. Comments are lines that start
         with '--'.
    Returns:
        str: The SQL string without comments.
    """
     return "\n".join(
        line for line in sql_str.splitlines() if not line.strip().startswith("--")
    )


def get_table_names(schema_sql: str) -> list[str]:
    """Return table names declared in the TPC-DS schema, excluding metadata."""
    names = re.findall(
        r"\bcreate\s+table\s+([A-Za-z_]\w*)",
        clean_sql_comments(schema_sql),
        flags=re.IGNORECASE,
    )
    return [name.lower() for name in names if name.lower() != "dbgen_version"]


def parquet_table_is_complete(table_path: str) -> bool:
    path = Path(table_path)
    return (
        (path / "_SUCCESS").is_file()
        and any(file.is_file() and file.stat().st_size > 0 for file in path.rglob("*.parquet"))
    )


def normalize_sql_for_spark(sql_str: str) -> str:
    """
    Adapts a TPC-DS query to run in Spark SQL:
      - Removes day units from integer date arithmetic.
      - Replaces the non-existent column `c_last_review_date_sk` (query 30)
        with `c_last_review_date`.
    """
    sql_str = re.sub(r"([+-]\s*\d+)\s+days?\b", r"\1", sql_str, flags=re.IGNORECASE)
    return re.sub(
        r"\bc_last_review_date_sk\b", "c_last_review_date", sql_str, flags=re.IGNORECASE
    )


def register_tables_in_spark(spark_session, parquet_dir):
    """
    Registers Parquet tables as SQL temporary views in a Spark session.

    Args:
        spark_session: The Spark session to register the views in.
        parquet_dir: The directory containing the Parquet files.
    """
    for table_name in os.listdir(parquet_dir):
        table_path = os.path.join(parquet_dir, table_name)
        if os.path.isdir(table_path):
            # Read parquet dataset
            df = spark_session.read.parquet(table_path)
            # Register with its original table name (e.g., "store_sales") for
            # query resolution
            df.createOrReplaceTempView(table_name)


def sort_query_files(query_files):
    """
    Sorts a list of query files based on their numeric order.

    Args:
        query_files (list): A list of query file names.

    Returns:
        list: A sorted list of query file names.
    """
    def query_number(filename):
        match = re.search(r"\d+", filename)
        return int(match.group()) if match else float("inf")

    return sorted(
        (f for f in query_files if f.endswith(".sql")),
        key=lambda f: (query_number(f), f),
    )