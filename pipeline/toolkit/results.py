import csv

from collections.abc import Iterable, Sequence
from pathlib import Path

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.types import StructType

# Rows from each side kept as examples when results differ
SAMPLE_SIZE = 5


def results_dataframe(spark: SparkSession, rows: list[Row], schema: StructType) -> DataFrame:
    """
    Collected rows as a single-partition DataFrame, keeping their order.

    Spark can't write duplicate column names (e.g. query 64 returns `syear`
    twice), so repeated names get their position appended (`syear_9`).
    """
    names = [field.name for field in schema.fields]
    unique_names = [
        f"{name}_{i}" if names.count(name) > 1 else name for i, name in enumerate(names)
    ]
    return spark.createDataFrame(rows, schema).toDF(*unique_names).coalesce(1)


def results_path(results_dir: str, query_file: str, statement_part: int | None) -> Path:
    """query72.parquet, or query14_part2.parquet for files with several statements."""
    suffix = f"_part{statement_part}" if statement_part is not None else ""
    return Path(results_dir) / f"{Path(query_file).stem}{suffix}.parquet"


def results_are_saved(results_dir: str, query_file: str, statement_part: int | None) -> bool:
    """Whether the results were fully written (Spark's _SUCCESS marker)."""
    return (results_path(results_dir, query_file, statement_part) / "_SUCCESS").is_file()


def write_results_csv(
    parquet_path: Path, column_names: Sequence[str], rows: Iterable[Sequence]
) -> Path:
    """
    Readable copy of a saved result, next to its Parquet (query72.csv).
    Comparisons use the Parquet, not this.

    Strings are always quoted, so trailing spaces stay visible and NULL (bare)
    can't be mistaken for the text "NULL"; numbers and dates are left bare.
    """
    class _Null:
        """
        Written as a bare NULL: not a string, so the CSV writer leaves it
        unquoted.
        """
        def __str__(self) -> str:
            return "NULL"
    _NULL = _Null()

    csv_path = parquet_path.with_suffix(".csv")
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_STRINGS)
        writer.writerow(column_names)
        writer.writerows(
            [_NULL if value is None else value for value in row] for row in rows
        )
    return csv_path


def save_results(
    spark: SparkSession,
    rows: list[Row],
    schema: StructType,
    results_dir: str,
    query_file: str,
    statement_part: int | None = None,
) -> Path:
    """
    Save the collected rows of one statement as Parquet, plus a readable CSV
    copy. statement_part is only given for query files with several statements.
    """
    path = results_path(results_dir, query_file, statement_part)
    df = results_dataframe(spark, rows, schema)
    df.write.mode("overwrite").parquet(str(path))
    write_results_csv(path, df.columns, rows)
    return path


def compare_results(
    spark: SparkSession,
    rows: list[Row],
    schema: StructType,
    results_dir: str,
    query_file: str,
    statement_part: int | None = None,
) -> dict:
    """
    Compare the collected rows of one statement with its saved baseline.

    Returns a dict whose "status" is one of:
      - "no_baseline": nothing saved for this statement.
      - "schema_mismatch": column names or types differ.
      - "match": same rows, in the same order.
      - "order_differs": same rows (duplicates counted), different order.
      - "mismatch": different rows, with samples from each side.
    """
    path = results_path(results_dir, query_file, statement_part)
    if not path.exists():
        return {"status": "no_baseline"}
    baseline = spark.read.parquet(str(path))
    actual = results_dataframe(spark, rows, schema)

    def columns(df: DataFrame) -> list[tuple[str, str]]:
        return [
            (field.name, field.dataType.simpleString())
            for field in df.schema.fields
        ]

    if columns(baseline) != columns(actual):
        return {
            "status": "schema_mismatch",
            "baseline_columns": columns(baseline),
            "actual_columns": columns(actual),
        }

    if [tuple(row) for row in baseline.collect()] == [tuple(row) for row in rows]:
        return {"status": "match"}

    # exceptAll keeps duplicates, so both sides empty means the same multiset
    only_in_baseline = baseline.exceptAll(actual).limit(SAMPLE_SIZE).collect()
    only_in_actual   = actual.exceptAll(baseline).limit(SAMPLE_SIZE).collect()
    if not only_in_baseline and not only_in_actual:
        return {"status": "order_differs"}

    return {
        "status": "mismatch",
        "baseline_rows": baseline.count(),
        "actual_rows": len(rows),
        "only_in_baseline": only_in_baseline,
        "only_in_actual": only_in_actual,
    }
