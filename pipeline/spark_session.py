import os

from pyspark.sql import SparkSession


def create_spark_session(
    app_name: str,
    use_all_cores: bool,
    tune_memory_allocation: bool
) -> SparkSession:
    master_val = "local[*]" if use_all_cores else "local[4]"
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(master_val)
        .config("spark.ui.port", "4040")
        .config("spark.port.maxRetries", "0")
        .config("spark.driver.memory", "6g")
        .config("spark.driver.extraJavaOptions",
                "-XX:MinHeapFreeRatio=10 -XX:MaxHeapFreeRatio=30")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.ansi.doubleQuotedIdentifiers", "true")
    )
    if tune_memory_allocation:
        builder = (
            builder.config("spark.memory.fraction", "0.8")
                   .config("spark.memory.storageFraction", "0.3")
        )

    return builder.getOrCreate()

STATIC_DIMENSIONS = {"date_dim", "warehouse", "ship_mode", "store", "item"}

def register_tables_in_spark(
    spark_session,
    parquet_dir,
    register_as="views",
    analyze=False,
    enable_static_caching=False
):
    """
    Registers Parquet tables in a Spark session, as temporary views or as
    catalog tables.

    Args:
        spark_session: The Spark session to register the tables in.
        parquet_dir: The directory containing the Parquet files.
        register_as: "views" for temporary views, or "tables" for external
            catalog tables, which (unlike views) support ANALYZE TABLE.
        analyze: Whether to compute table and column statistics for the CBO
            after creating each table. Requires register_as="tables".
    """
    if analyze and register_as != "tables":
        raise ValueError('analyze=True requires register_as="tables"')

    for table_name in os.listdir(parquet_dir):
        table_path = os.path.join(parquet_dir, table_name)
        if os.path.isdir(table_path):
            # Register with its original table name (e.g., "store_sales") for
            # query resolution
            if register_as == "tables":
                print(f"Registering {table_name} as an external table")
                # A temp view with the same name would shadow the table
                spark_session.catalog.dropTempView(table_name)
                # External table: dropping it leaves the Parquet files intact
                spark_session.sql(f"DROP TABLE IF EXISTS {table_name}")
                spark_session.catalog.createTable(
                    table_name, path=table_path, source="parquet"
                )
                if analyze:
                    # Table-level statistics
                    spark_session.sql(f"ANALYZE TABLE {table_name} COMPUTE STATISTICS")
                    # Column-level statistics
                    spark_session.sql(
                        f"ANALYZE TABLE {table_name} COMPUTE STATISTICS FOR ALL COLUMNS"
                    )
            else:
                print(f"Registering {table_name} as a temporary view...")
                df = spark_session.read.parquet(table_path)
                df.createOrReplaceTempView(table_name)

            # Works for both temp views and catalog tables
            if enable_static_caching and table_name in STATIC_DIMENSIONS:
                spark_session.catalog.cacheTable(table_name)
                # Materialize the cache to RAM immediately, to avoid lazy evaluation later
                spark_session.table(table_name).count()
                print(f"  -> Dimension '{table_name}' loaded into memory (cache).")


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
        "use_repartition": options.spark.use_repartition,
        "use_broadcast_joins_hints": options.spark.use_broadcast_joins_hints,
        "spark_adaptive_enabled": options.spark.adaptative_enabled,
        "spark_adaptive_coalesce_partitions_enabled": (
            options.spark.adaptative_coalesce_partitions
        ),
        "spark_adaptive_skew_join_enabled": options.spark.adapatative_skew_join,
        "spark_auto_broadcast_join_threshold_mb": options.spark.broadcast_threshold_mb,
        "register_tables_as": options.spark.register_tables_as,
        "spark_cbo_enabled": options.spark.cbo_enabled,
        "spark_cbo_join_reorder_enabled": options.spark.cbo_join_reorder,
        "spark_cbo_plan_stats_enabled": options.spark.cbo_plan_stats_enabled,
    }
    boolean_settings = {
        "spark.sql.cbo.enabled": "spark_cbo_enabled",
        "spark.sql.cbo.joinReorder.enabled": "spark_cbo_join_reorder_enabled",
        "spark.sql.cbo.planStats.enabled": "spark_cbo_plan_stats_enabled",
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
        # 2 partitions per available logical core on the local machine
        cores = os.cpu_count() or 4
        shuffle_parts = cores * 2

    spark_session.conf.set("spark.sql.shuffle.partitions", str(shuffle_parts))
    run_configuration["shuffle_partitions"] = shuffle_parts

    run_configuration["use_all_cores"] = options.spark.use_all_cores
    run_configuration["tune_memory_allocation"] = options.spark.tune_memory_allocation
    run_configuration["enable_static_caching"] = options.spark.enable_static_caching

    return run_configuration


def get_session_configuration(spark_session) -> list[dict[str, str | bool]]:
    """
    Every configuration parameter of a Spark session, sorted by name, for
    inclusion in the run reports. Each row holds the parameter, its
    description, its value in this session, Spark's default value, and whether
    it was changed: explicitly set (by us in create_spark_session /
    apply_run_options, or by Spark itself at startup, e.g. spark.app.id) to a
    value other than the default. Values are compared as text, so 10485760 vs
    the default 10MB counts as changed.

    The parameter list combines two sources:
    - SET -v: every documented SQL parameter with its current value, defaults
      included. The Python API has no equivalent.
    - spark.conf.getAll: every parameter explicitly set in this session,
      including core ones (spark.master, spark.driver.memory, ...) and ones
      Spark does not document, which SET -v leaves out; their values win.
    Default values and descriptions come from Spark's registry of known
    parameters (internal JVM API, the only place that exposes defaults). Keys
    Spark does not know, such as spark.app.id or a misspelled key, have none.
    """
    session_values = {
        row["key"]: row["value"] for row in spark_session.sql("SET -v").collect()
    }
    explicitly_set = spark_session.conf.getAll
    session_values.update(explicitly_set)

    config_entry = spark_session._jvm.org.apache.spark.internal.config.ConfigEntry
    rows = []
    for key in sorted(session_values):
        entry = config_entry.findEntry(key)
        rows.append(
            {
                "parameter": key,
                "description": entry.doc() if entry is not None else "",
                "session_value": session_values[key],
                "default_value": (
                    entry.defaultValueString() if entry is not None else ""
                ),
                "changed": key in explicitly_set and (
                    entry is None
                    or session_values[key] != entry.defaultValueString()
                ),
            }
        )
    return rows
