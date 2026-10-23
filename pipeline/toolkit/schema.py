import os
import re

from pathlib import Path

from toolkit.queries import clean_sql_comments


def get_table_names(schema_path: Path) -> list[str]:
    """Return table names declared in the TPC-DS schema, excluding metadata."""
    if not schema_path.is_file():
        raise FileNotFoundError(
            f"TPC-DS schema not found at {schema_path}; run pipeline/prepare.py first."
        )
    schema_sql = schema_path.read_text()
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


def incomplete_parquet_tables(parquet_dir, table_names: list[str]) -> list[str]:
    return [
        name for name in table_names
        if not parquet_table_is_complete(os.path.join(parquet_dir, name))
    ]

