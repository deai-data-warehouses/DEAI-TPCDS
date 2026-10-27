import os

from pyspark.sql import SparkSession


def create_spark_session(app_name: str, options) -> SparkSession:

    master_val = "local[*]" if options.spark.use_all_cores else "local[4]" 
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(master_val)
        .config("spark.ui.port", "4040")
        .config("spark.port.maxRetries", "0")
        .config("spark.driver.memory", "6g")
        .config("spark.driver.extraJavaOptions",
                "-XX:MinHeapFreeRatio=10 -XX:MaxHeapFreeRatio=30")
        .config("spark.sql.ansi.enable", "true")
        .config("spark.sql.ansi.doubleQuotedIdentifiers", "true")
    )
    if options.spark.tune_memory_allocation:
        builder = (
            builder.config("spark.memory.fraction", "0.8")
                   .config("spark.memory.storageFraction", "0.3")
        )

    return builder.getOrCreate()

STATIC_DIMENSIONS = {"date_dim", "warehouse", "ship_mode", "store", "item"}

def register_tables_in_spark(spark_session, parquet_dir, options):
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
            if options.spark.enable_static_caching and table_name in STATIC_DIMENSIONS:
                df = df.cache()
                df.count()  # Materialize the cache to RAM immediately, to avoid lazy evaluation later
                print(f"  -> Dimension '{table_name}' loaded into memory (cache).")

            df.createOrReplaceTempView(table_name)


def apply_run_options(spark_session, options) -> dict[str, int | bool]:
    """
    Applies the run options from config.toml to a Spark session.

    Args:
        spark_session: The Spark session to configure.
        options: The loaded run configuration (config_loader.RunConfig).

    Returns:
        The applied options as a flat dict, for inclusion in the run reports.
    """
    run_configuration: dict[str, int | bool] = {
        "runs_per_query": options.general.runs_per_query,
        "use_partition": options.spark.use_partition,
        "use_broadcast_joins_hints": options.spark.use_broadcast_joins_hints,
        "spark_adaptive_enabled": options.spark.adaptative_enabled,
        "spark_adaptive_coalesce_partitions_enabled": (
            options.spark.adaptative_coalesce_partitions
        ),
        "spark_adaptive_skew_join_enabled": options.spark.adapatative_skew_join,
        "spark_auto_broadcast_join_threshold_mb": options.spark.broadcast_threshold_mb,
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
    if threshold_mb < 0:
        bytes_val = -1
    else:
        bytes_val = threshold_mb * 1024 * 1024
    
    spark_session.conf.set("spark.sql.autoBroadcastJoinThreshold", str(bytes_val))
    run_configuration["broadcast_threshold_bytes"] = bytes_val

    # Shuffling parts
    shuffle_parts = options.spark.shuffle_partitions
    if shuffle_parts <= 0:
        # 2 particions per nucli lògic disponible a la màquina local
        cores = os.cpu_count() or 4
        shuffle_parts = cores * 2
    
    spark_session.conf.set("spark.sql.shuffle.partitions", str(shuffle_parts))
    run_configuration["shuffle_partitions"] = shuffle_parts

    run_configuration["use_all_cores"] = options.spark.use_all_cores
    run_configuration["tune_memory_allocation"] = options.spark.tune_memory_allocation
    run_configuration["enable_static_caching"] = options.spark.enable_static_caching

    return run_configuration
