import argparse
import re
import os

from pathlib import Path

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


def select_query_files(args: argparse.Namespace, query_dir: str) -> list[str]:
    query_dir = Path(query_dir)
    if not query_dir.is_dir():
        raise FileNotFoundError(f"Query directory not found at {query_dir}")
    query_files = sort_query_files(os.listdir(query_dir))
    # Single query execution
    if args.query is not None:
        query_file = f"query{args.query}.sql"
        if query_file not in query_files:
            raise FileNotFoundError(f"{query_file} not found in {query_dir}")
        return [query_file]
    # Execute a list of queries
    if getattr(args, "query_list", None) is not None:
        selected_files = []
        missing_files = []
        for q in args.query_list:
            filename = f"query{q}.sql"
            if filename not in query_files:
                missing_files.append(filename)
            else:
                selected_files.append(filename)
        if missing_files:
            raise FileNotFoundError(
                f"Missing query files in {query_dir}: {missing_files}"
            )
        return sort_query_files(selected_files)

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
