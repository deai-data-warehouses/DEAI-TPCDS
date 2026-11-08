# DEAI-TPCDS
DEAI TPC-DS Benchmark Implementation for Data Warehouse Project

TPC-DS benchmark pipeline using Apache Spark. Docker builds the TPC-DS utilities,
generates the synthetic data and query files, converts the data to Parquet, and
runs the benchmark. The zipped TPC-DS tool source is included in this repository, for improved reproducibility.

## Requirements

- Docker Engine and Docker Compose.
- Enough host disk space for the generated raw data, Parquet data, and reports.
  These can require substantial space, especially at scales larger than 1 GB.
- Unzipping the TPC-DS tool folder included in this repository
- Cloning .env.template into a .env with the local paths.

The container builds a writable copy of `tools/` and `query_templates/` in the
scenarios directory. Compatibility fixes and compiled executables are applied
to that copy, leaving the supplied TPC-DS source unchanged.

## TL;DR

To run the benchmark once, execute this:

```powershell
docker compose build
docker compose run --rm pipeline python pipeline/prepare.py --scale-factor 1
docker compose run --rm --service-ports pipeline python pipeline/convert.py --scale-factor 1
docker compose run --rm --service-ports pipeline python pipeline/benchmark.py --scale-factor 1
```

This will prepare the 1GB TPC-DS data, convert it to Parquet, run all 99 queries,
and produce a spreasheet with the results.

The rest of this README will explain how to run the project for other parameters.

## To run the stages

Build the image first:
```powershell
docker compose build
```

### 1. Prepare

Do this for the desired scale factor (example for 1GB):

```powershell
docker compose run --rm pipeline python pipeline/prepare.py --scale-factor 1
```

Raw data generation can take a while and needs disk space appropriate to the
selected scale factor.

Preparation stage checks for complete existing outputs and skips already completed
work. Use `--force` to rebuild the tools and regenerate all raw data and query
files if necessary.

It is possible to have multiple scales' data coexisting in the project's folder without problems.
Just change the --scale-factor value and run the prepare stage again.

### 2. Convert

Convert the raw `.dat` files to Parquet (example for 1GB):

```powershell
docker compose run --rm --service-ports pipeline python pipeline/convert.py --scale-factor 1
```

Completed Parquet tables are reused on subsequent runs. Use `--force` to
overwrite and rebuild every table if necessary.

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
optimizations. By default, it checks reports from `2026-10-26`; set
`RESUME_DATE=YYYY-MM-DD` when resuming reports saved under another date.

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

## Run locally

The individual Python stages should also be able to run outside Docker if Python, Java,
Spark prerequisites, and their dependencies are installed. Set the same
`TPCDS_TOOL_DIR` and `SCENARIOS_DIR` environment variables.

## References

- [TPC-DS Benchmark Specification](https://www.tpc.org/tpcds/)
- [Spark SQL API](https://spark.apache.org/docs/latest/api/sql/)
- [Mastering Broadcast Joins in PySpark: Optimizing Performance for Large-Scale Data Processing](https://www.sparkcodehub.com/pyspark/performance/broadcast-joins)
- [https://www.sparkcodehub.com/pyspark/performance/broadcast-joins](https://medium.com/@sksami1997/spark-sql-engine-how-queries-are-optimized-and-executed-62cf5b3d3f84)
- [Spark Configuration (`spark.driver.memory`, `spark.driver.extraJavaOptions`)](https://spark.apache.org/docs/latest/configuration.html)
- [Java 21 `System.gc()` API](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/lang/System.html#gc())
- [Java 21 `java` command options (`-XX:MinHeapFreeRatio`, `-XX:MaxHeapFreeRatio`)](https://docs.oracle.com/en/java/javase/21/docs/specs/man/java.html)
- [Java 21 Garbage-First (G1) Garbage Collector Tuning](https://docs.oracle.com/en/java/javase/21/gctuning/garbage-first-garbage-collector-tuning.html)

