import os
import re

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