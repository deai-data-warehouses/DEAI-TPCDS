import re

from pathlib import Path

from toolkit.queries import clean_sql_comments

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
