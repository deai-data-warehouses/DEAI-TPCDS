import os

from pyspark.sql import SparkSession


def create_spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder \
        .appName(app_name) \
        .master("local[4]") \
        .config("spark.ui.port", "4040") \
        .config("spark.port.maxRetries", "0") \
        .config("spark.driver.memory", "6g") \
        # Shrink the heap after a full GC so the JVM returns unused memory to
        # the OS instead of holding on to it (keeps at most 30% of the heap
        # free).
        .config("spark.driver.extraJavaOptions",
                "-XX:MinHeapFreeRatio=10 -XX:MaxHeapFreeRatio=30") \
        .config("spark.sql.ansi.enable", "true") \
        .config("spark.sql.ansi.doubleQuotedIdentifiers", "true") \
        .getOrCreate()
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