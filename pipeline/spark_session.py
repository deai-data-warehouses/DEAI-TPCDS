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


def register_tables_in_spark(spark_session, parquet_dir, register_as="views"):
    """
    Registers Parquet tables in a Spark session, as temporary views or as
    catalog tables.

    Args:
        spark_session: The Spark session to register the tables in.
        parquet_dir: The directory containing the Parquet files.
        register_as: "views" for temporary views, or "tables" for external
            catalog tables, which (unlike views) support ANALYZE TABLE.
    """
    for table_name in os.listdir(parquet_dir):
        table_path = os.path.join(parquet_dir, table_name)
        if os.path.isdir(table_path):
            # Register with its original table name (e.g., "store_sales") for
            # query resolution
            if register_as == "tables":
                # A temp view with the same name would shadow the table
                spark_session.catalog.dropTempView(table_name)
                # External table: dropping it leaves the Parquet files intact
                spark_session.sql(f"DROP TABLE IF EXISTS {table_name}")
                spark_session.catalog.createTable(
                    table_name, path=table_path, source="parquet"
                )
            else:
                df = spark_session.read.parquet(table_path)
                df.createOrReplaceTempView(table_name)


def apply_run_options(spark_session, options) -> dict[str, int | bool | str]:
    """
    Applies the run options from config.toml to a Spark session.

    Args:
        spark_session: The Spark session to configure.
        options: The loaded run configuration (config_loader.RunConfig).

    Returns:
        The applied options as a flat dict, for inclusion in the run reports.
    """
    run_configuration: dict[str, int | bool | str] = {
        "runs_per_query": options.general.runs_per_query,
        "use_partition": options.spark.use_partition,
        "use_broadcast_joins_hints": options.spark.use_broadcast_joins_hints,
        "spark_adaptive_enabled": options.spark.adaptative_enabled,
        "spark_adaptive_coalesce_partitions_enabled": (
            options.spark.adaptative_coalesce_partitions
        ),
        "spark_adaptive_skew_join_enabled": options.spark.adapatative_skew_join,
        "spark_auto_broadcast_join_threshold_mb": options.spark.broadcast_threshold_mb,
        "register_tables_as": options.spark.register_tables_as,
    }
    boolean_settings = {
        "spark.sql.adaptive.enabled": "spark_adaptive_enabled",
        "spark.sql.adaptive.coalescePartitions.enabled": (
            "spark_adaptive_coalesce_partitions_enabled"
        ),
        "spark.sql.adaptive.skewJoin.enabled": "spark_adaptive_skew_join_enabled",
    }
    for spark_key, configuration_key in boolean_settings.items():
        spark_session.conf.set(
            spark_key, str(run_configuration[configuration_key]).lower()
        )

    # Spark expects the threshold in bytes; -1 disables automatic broadcast joins
    threshold_mb = options.spark.broadcast_threshold_mb
    threshold_bytes = threshold_mb * 1024 * 1024 if threshold_mb > 0 else -1
    spark_session.conf.set("spark.sql.autoBroadcastJoinThreshold", str(threshold_bytes))

    return run_configuration
