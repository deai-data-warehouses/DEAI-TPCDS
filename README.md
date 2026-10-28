# DEAI-TPCDS
DEAI TPC-DS Benchmark Implementation for Data Warehouse Project

TPC-DS benchmark pipeline using Apache Spark. Docker builds the TPC-DS utilities,
generates the synthetic data and query files, converts the data to Parquet, and
runs the benchmark. The unzipped TPC-DS tool source is supplied by the user and
is not included in this repository or image.

## Requirements

- Docker Engine and Docker Compose.
- The TPC-DS tool archive downloaded from the
  [TPC current specifications page](https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp)
  and unzipped. The resulting directory must contain `tools/` (including
  `tpcds.sql`) and `query_templates/`.
- Enough host disk space for the generated raw data, Parquet data, and reports.
  These can require substantial space, especially at scales larger than 1 GB.

The container builds a writable copy of `tools/` and `query_templates/` in the
scenarios directory. Compatibility fixes and compiled executables are applied
to that copy, leaving the supplied TPC-DS source unchanged.

## Configure

The suggested layout keeps the repository as parent to TPC-DS source and generated data folders:

```text
workspace/
└── DEAI-TPCDS/       # this repository
    ├── tpcds-tool/
    │   ├── tools/    # (contains tpcds.sql, dsdgen source, ...)
    │   └── query_templates/
    └── scenarios/        # generated files and benchmark reports

```

From the repository directory, create `.env` from the template:

```powershell
Copy-Item .env.template .env
```

Edit `.env` if the folders are elsewhere:

```dotenv
TPCDS_TOOL_DIR=tcpds-tool
SCENARIOS_DIR=scenarios
```

Paths can be relative to the Compose file or absolute. On Windows, forward
slashes are accepted in absolute Docker bind-mount paths, for example
`C:/Users/name/data/tcpds-tool`. 

## Run the stages

Build the image once, and rebuild it when the Dockerfile, the Python
dependencies, or the code in `pipeline/` or `scripts/` change (those are copied
into the image). `queries/` and `config/` are bind-mounted, so editing them
needs no rebuild:

```powershell
docker compose build
```

The first build took my computer about 20 tries to run successfully, because the package downloads were timing out. Don't know if it's Bel Air's bad wifi or if the image size is so massive (500MB) that it's going to be like this every first time. Good luck.

### 1. Prepare

On the stage's first execution, some compatibility fixes are automatically applied as follows:
1. In the `scenarios/.tpcds-tools/tools` dir:
    - `date.c` — `getDateWeightFromJulian(jDay, nDistribution)` →
      `getDateWeightFromJulian(int jDay, int nDistribution)`
    - `tdefs.c` — `getSimpleTdefsByNumber(nTable)` → `getSimpleTdefsByNumber(int
      nTable)`
    - `tdef_functions.c` — `getTdefFunctionsByNumber(nTable)` →
      `getTdefFunctionsByNumber(int nTable)`
    - `Makefile` and `makefile` — added `-fcommon` to `LINUX_CFLAGS`
2. In the `scenarios/.tpcds-tools/query_templates` dir:
    - `netezza.tpl` - Fix bug by appending 'define _END = "";' to file

To build `dsdgen` and `dsqgen`, generate the raw `.dat` data (for 1GB scale), and generate one SQL
file per query template:

```powershell
docker compose run --rm pipeline python pipeline/prepare.py --scale-factor 1
```

Raw data generation can take a while and needs disk space appropriate to the
selected scale factor. One execution on Gustavo's computer took
3 hours for 1GB scale.

Preparation stage checks for complete existing outputs and skips already completed
work. Use `--force` to rebuild the tools and regenerate all raw data and query
files if necessary:

```powershell
docker compose run --rm pipeline python pipeline/prepare.py --scale-factor 1 --force
```

It is possible to have multiple scales' data coexisting in the project's folder without problems.
Just change the --scale-factor value and run the prepare stage again.

### 2. Convert

Convert the raw `.dat` files to Parquet (for the case of 1GB scale):

```powershell
docker compose run --rm --service-ports pipeline python pipeline/convert.py --scale-factor 1
```

Completed Parquet tables are reused on subsequent runs. Use `--force` to
overwrite and rebuild every table if necessary:

```powershell
docker compose run --rm --service-ports pipeline python pipeline/convert.py --scale-factor 1 --force
```

Conversion fails with a list of missing raw inputs instead of silently
producing an incomplete dataset.

The `use_partition` setting in `config/config.toml` selects the Parquet output
layout. Convert each scale factor once with `use_partition = false` and once
with `use_partition = true`; these are kept separately as
`scenarios/parquet_<N>GB` and `scenarios/parquet_partitioned_<N>GB`. The
experiment runner requires both layouts for every scale factor before it
starts.

### 3. Benchmark

Run all generated queries, or select a subset:

```powershell
docker compose run --rm --service-ports pipeline python pipeline/benchmark.py --scale-factor 1
docker compose run --rm --service-ports pipeline python pipeline/benchmark.py --scale-factor 1 --query 5
docker compose run --rm --service-ports pipeline python pipeline/benchmark.py --scale-factor 1 --limit 20
docker compose run --rm --service-ports pipeline python pipeline/benchmark.py --scale-factor 1 --start 10 --end 20
docker compose run --rm --service-ports pipeline python pipeline/benchmark.py --scale-factor 1 --start 10 --limit 3
docker compose run --rm --service-ports pipeline python pipeline/benchmark.py --scale-factor 1 --query 5 --queries changed
```

| Parameter | Description |
|---|---|
| `--scale-factor N` | Required. Set the data scale factor for this run (for example, `1`). It does not generate the dataset. |
| `--limit N` | Run only the first `N` queries (in query order). |
| `--query N` | Run only the file `queryN.sql`. Cannot be combined with `--limit`, `--start` or `--end`. |
| `--start N` | Run the queries from `queryN.sql` onwards (inclusive). |
| `--end N` | Run the queries up to `queryN.sql` (inclusive). |
| `--queries V` | Which query set to read: `original` (default, `queries/original/queries_<N>GB/`) or `changed` (`queries/changed/queries_<N>GB/`, only the modified queries). Recorded as `query_version` in each row; `changed` runs get a `_changed` suffix in the run folder name. |

Run options that are not command-line flags live in `config/config.toml`,
validated by `pipeline/config_loader.py` (unknown keys are rejected):

To run the seven configured experiments over scales 1, 2, 5, and 10 using three
timed runs per query, with the flat and partitioned Parquet layouts already
converted, run:

```bash
bash scripts/run_experiments.sh
```

The suite includes an unoptimized baseline, five single-option comparisons
(core parallelism, shuffle partitions, broadcast threshold, memory allocation,
and static dimension caching), and a final experiment combining the
optimizations. The script temporarily updates all relevant Spark options in
`config/config.toml` for each experiment and restores its original contents
when it exits. It skips a scale-factor run only when its saved CSV reports
show that all selected queries completed successfully with the matching
configuration. By default, it checks reports from `2026-10-26`; set
`RESUME_DATE=YYYY-MM-DD` when resuming reports saved under another date.

| Option | Description |
|---|---|
| `[general] runs_per_query` | Execute each query this many times (default `5`). Timings are reported as min/max/avg over the runs. |
| `[spark] adaptative_enabled` | Sets `spark.sql.adaptive.enabled`. |
| `[spark] adaptative_coalesce_partitions` | Sets `spark.sql.adaptive.coalescePartitions.enabled`. |
| `[spark] adapatative_skew_join` | Sets `spark.sql.adaptive.skewJoin.enabled`. |
| `[spark] broadcast_threshold_mb` | Sets `spark.sql.autoBroadcastJoinThreshold`, in MB (default `10`; `0` or `-1` disables automatic broadcast joins; explicit `BROADCAST` hints still apply). |
| `[spark] register_tables_as` | How the Parquet tables are registered: `"views"` (default, temporary views) or `"tables"` (external catalog tables over the same Parquet files, which support `ANALYZE TABLE`). |
| `[spark] cbo_enabled` | Sets `spark.sql.cbo.enabled`, and runs `ANALYZE TABLE ... COMPUTE STATISTICS FOR ALL COLUMNS` on every table after registering it (default `false`). Requires `register_tables_as = "tables"`. The analysis time counts toward the run's total elapsed time, not toward any query's timings. |
| `[spark] cbo_join_reorder` | Sets `spark.sql.cbo.joinReorder.enabled`, letting the CBO reorder joins using the collected statistics (default `false`). Requires `cbo_enabled = true`. |
| `[spark] use_partition`, `use_broadcast_joins_hints` | Flags available to query templates (see below); not Spark settings. |
| `[spark] shuffle_partitions` | Sets `spark.sql.shuffle.partitions`; `-1` selects twice the detected logical CPU count. |
| `[spark] use_all_cores` | Uses `local[*]` when true and `local[4]` when false. |
| `[spark] tune_memory_allocation` | Enables the configured Spark memory and storage fractions. |
| `[spark] enable_static_caching` | Caches the selected dimension tables before benchmark queries run. |
| `[spark] use_partition`, `use_broadcast_joins_hints` | `use_partition` selects the Parquet layout; `use_broadcast_joins_hints` is a flag available to query templates (see below). Neither is itself a Spark setting. |

Before running, each query file is rendered as a Jinja template with these
options as `options`, so a query can adapt to the configuration, e.g.
`{% if options.spark.use_broadcast_joins_hints %}/*+ BROADCAST(d) */{% endif %}`.
Referencing an undefined option fails the run. In Docker, `config/` is
bind-mounted read-only, so edits to `config.toml` apply on the next run without
rebuilding.

Benchmarking reads existing Parquet from `SCENARIOS_DIR` and the
version-controlled queries in `queries/original/queries_<N>GB/` (not the
`SCENARIOS_DIR/queries_<N>GB/` output of the prepare stage, which stays as the
untouched dsqgen reference). Edits to those query files are tracked by git and
picked up on the next run; in Docker the folder is bind-mounted, so no rebuild
is needed. `queries/changed/queries_<N>GB/` holds modified versions of individual
queries and is used with `--queries changed`. It never regenerates data, queries, or Parquet.
Each invocation creates a folder under `SCENARIOS_DIR/runs/<UTC date>/` named
`<HHMMSS>_<scale>GB_<queries>`, where `<queries>` describes the selection
arguments:

| Arguments | Folder name |
|---|---|
| *(none)* | `145347_1GB_full` |
| `--query 5` | `145347_1GB_query5` |
| `--start 10` | `145347_1GB_from10` |
| `--end 20` | `145347_1GB_to20` |
| `--start 10 --end 20` | `145347_1GB_query10-20` |
| `--limit 20` | `145347_1GB_first20` |
| `--start 10 --limit 3` | `145347_1GB_from10_first3` |
| `--end 20 --limit 3` | `145347_1GB_to20_first3` |
| `--start 10 --end 20 --limit 3` | `145347_1GB_query10-20_first3` |

The unique run ID is still recorded in the `run_id` column of the reports.
The folder holds one Excel workbook named
`benchmark_report.xlsx` there. The workbook has a `Query Runs` sheet with one
row per executed SQL statement and a `Run Summary` sheet with one row for the
whole invocation. The same folder also contains `query_runs.csv` and
`run_summary.csv`, exported from those respective sheets for analysis in a
notebook or other tools. Both outputs include the run configuration from
`config.toml` (`runs_per_query`, the broadcast threshold, the table registration
mode, and the partitioning, broadcast-hint, and adaptive query execution options).

## Generated layout

After preparation and conversion, the scenarios directory contains (if scale = 1GB):

```text
scenarios/
├── .tpcds-tools/
│   ├── tools/                  # patched sources, schema, dsdgen, dsqgen
│   └── query_templates/        # patched template copy
├── data_1GB/
│   └── *.dat
├── queries_1GB/
│   └── query*.sql
├── parquet_1GB/
│   └── <table-name>/
│       ├── part-*.parquet
│       └── _SUCCESS
└── runs
    └── 2026-10-14
        └── 145347_1GB_full
            ├── benchmark_report.xlsx
            ├── query_runs.csv
            └── run_summary.csv
```

This directory is bind-mounted from the host, so all generated files persist
after the containers exit. The repository ignores local scenario artifacts and
`.env`; do not commit generated data or local paths.

## Run locally

The individual Python stages should also be able to run outside Docker if Python, Java,
Spark prerequisites, and their dependencies are installed. Set the same
`TPCDS_TOOL_DIR` and `SCENARIOS_DIR` environment variables.
The preparation stage additionally requires Linux build tools (`make`, a C
compiler, `flex`, and `bison`).

## References

- [TPC-DS Benchmark Specification](https://www.tpc.org/tpcds/)
- [Spark SQL API](https://spark.apache.org/docs/latest/api/sql/)
- [Mastering Broadcast Joins in PySpark: Optimizing Performance for Large-Scale Data Processing](https://www.sparkcodehub.com/pyspark/performance/broadcast-joins)
- [https://www.sparkcodehub.com/pyspark/performance/broadcast-joins](https://medium.com/@sksami1997/spark-sql-engine-how-queries-are-optimized-and-executed-62cf5b3d3f84)
- [Spark Configuration (`spark.driver.memory`, `spark.driver.extraJavaOptions`)](https://spark.apache.org/docs/latest/configuration.html)
- [Java 21 `System.gc()` API](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/lang/System.html#gc())
- [Java 21 `java` command options (`-XX:MinHeapFreeRatio`, `-XX:MaxHeapFreeRatio`)](https://docs.oracle.com/en/java/javase/21/docs/specs/man/java.html)
- [Java 21 Garbage-First (G1) Garbage Collector Tuning](https://docs.oracle.com/en/java/javase/21/gctuning/garbage-first-garbage-collector-tuning.html)


# TO DO
## Pending Fixes
1. Evaluate if Python Notebooks still work under the new project structure. Fix if not.
2. Validate if all current installations during first build are really necessary, to reduce the image size and speed up package download/install.
3. Decide if we add the unzipped TPC-DS downloaded folder into the project repo, to make the setup trivial and be less prone to problems for future versions of TPC-DS (currently 4.0.0)
4. See if it's necessary to have single spark session as global variable or if we transplant from constants.py to utils.py

## Future Tasks
- Implement module to get the results, both in csv and visualization, so when we execute the queries at a scale and at a version of the code we store the results in a proper way, so we can see how the execution times change in every new version of the code
