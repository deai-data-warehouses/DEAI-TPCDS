import re

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