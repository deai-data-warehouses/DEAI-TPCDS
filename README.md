# Performance Analysis and Optimization of Apache Spark SQL Using TPC-DS

Date: November 8th, 2026

Authors:
- Bas Peralta, Benjamín
- Cisbani, Davide
- Guedes, Gustavo
- Samper Arguelagués, Guillem

This repository contains all the source code used for the running the
experiments explained in the report.

TPC-DS benchmark pipeline using Apache Spark. Docker builds the TPC-DS
utilities, generates the synthetic data and query files, converts the data to
Parquet, and runs the benchmark. The zipped TPC-DS tool source is included in
this repository, for improved reproducibility.

## Requirements

- Docker Engine and Docker Compose.
- Enough host disk space for the generated raw data, Parquet data, and reports.
  These can require substantial space, especially at scales larger than 1 GB.
- Unzipping the TPC-DS tool folder included in this repository
- Cloning `.env.template` into a `.env` with the local paths.

The container builds a writable copy of `tools/` and `query_templates/` in the
scenarios directory. Compatibility fixes and compiled executables are applied to
that copy, leaving the supplied TPC-DS source unchanged.

## TL;DR

Before running anything, set the run options in `config/config.toml` (Parquet
layout, runs per query, Spark settings, etc.; see [Configuration](#configuration)).
Both `convert.py` and `benchmark.py` read it; changing `use_partition` or
`use_repartition` afterwards requires running `convert.py` again.

The first conversion of each scale factor must use `use_partition = false`:
`save_baseline.py` always reads the non-partitioned layout (`parquet_<N>GB`) and
fails if it doesn't exist. To benchmark the partitioned layout, set
`use_partition = true` afterwards and run `convert.py` again.

To run the benchmark once, execute this:

```powershell
docker compose build
docker compose run --rm pipeline python pipeline/prepare.py --scale-factor 1
docker compose run --rm --service-ports pipeline python pipeline/convert.py --scale-factor 1
docker compose run --rm --service-ports pipeline python pipeline/save_baseline.py --scale-factor 1
docker compose run --rm --service-ports pipeline python pipeline/benchmark.py --scale-factor 1
```

This will prepare the 1GB TPC-DS data, convert it to Parquet, save the baseline
query results, run all 99 queries, and produce a spreadsheet with the results.

Alternatively, `run_benchmark.sh` runs all the stages for a given scale factor
in one go (see [Run everything with one script](#run-everything-with-one-script)):

```bash
bash run_benchmark.sh 1
```

The rest of this README will explain how to run the project for other parameters.

## Run everything with one script

`run_benchmark.sh` takes the scale factor as its first argument and runs, in order:

1. `docker compose build`
2. `prepare.py` (generate raw data and queries)
3. `convert.py` (convert `.dat` to Parquet)
4. `save_baseline.py` (save the reference results that the benchmark's `result_check` column compares against)
5. `benchmark.py` (run the queries and write the report)

```bash
bash run_benchmark.sh 1                          # 1GB, all original queries
bash run_benchmark.sh 1 --queries changed        # extra arguments go to benchmark.py
bash run_benchmark.sh 5 --start 10 --end 20
```

- Any argument after the scale factor is passed to `benchmark.py` only (see the
  [Benchmark](#4-benchmark) parameters table).
- Each stage skips work that is already complete, so running the script again for
  the same scale factor is cheap. It never passes `--force`; run the stages by hand
  for that.
- The script stops at the first stage that fails.
- It needs an interactive terminal: at the end `benchmark.py` keeps the Spark UI
  alive at http://localhost:4040 until you press Enter.
- Reports are written under `SCENARIOS_DIR/runs/` (see [Benchmark](#4-benchmark)).

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

Preparation stage checks for complete existing outputs and skips already
completed work. Use `--force` to rebuild the tools and regenerate all raw data
and query files if necessary.

It is possible to have multiple scales' data coexisting in the project's folder
without problems. Just change the --scale-factor value and run the prepare stage
again.

### 2. Convert

Convert the raw `.dat` files to Parquet (example for 1GB):

```bash
docker compose run --rm --service-ports pipeline python pipeline/convert.py --scale-factor 1
```

Completed Parquet tables are reused on subsequent runs. Use `--force` to
overwrite and rebuild every table if necessary.

The `use_partition` setting in `config/config.toml` selects the Parquet output
layout. Convert each scale factor once with `use_partition = false` and once
with `use_partition = true`; these are kept separately as
`scenarios/parquet_<N>GB` and `scenarios/parquet_partitioned_<N>GB`. The
experiment runner requires both layouts for every scale factor before it starts.

### 3. Save the baseline results

Run the original queries once with plain Spark settings and save their results,
so the benchmark can check that its results are correct (example for 1GB):

```bash
docker compose run --rm --service-ports pipeline python pipeline/save_baseline.py --scale-factor 1
```

The results are saved as Parquet under `SCENARIOS_DIR/results_<N>GB/`, one
`queryN.parquet` per query (with a `_part<N>` suffix for files with several
statements), plus a readable `queryN.csv` copy. The baseline always uses
`config/default_spark.toml`, not `config/config.toml`, so it doesn't depend on
any tuning. This also means it always reads the non-partitioned Parquet
(`parquet_<N>GB`), so `convert.py` must have been run at least once with
`use_partition = false` for that scale factor.

On the first run of each query, `benchmark.py` compares its result with this
baseline and records the outcome in the `result_check` column of the report:
`match`, `order_differs`, `mismatch`, `schema_mismatch`, or `no_baseline` if
this stage wasn't run for that query.

Queries whose results are already saved are skipped; use `--force` to overwrite
them. It accepts the same selection flags as the benchmark (`--query`, `--start`,
`--end`, `--limit`), plus `--query-list` for a specific list (e.g.
`--query-list 14 23 72`).

### 4. Benchmark

Run all generated queries, or select a subset:

```bash
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

Run options that are not command-line flags (number of runs per query, Spark
settings, etc.) live in `config/config.toml`; see [Configuration](#configuration).

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

## Configuration

Edit `config/config.toml` to change how the benchmark runs. It is read by
`convert.py` (only `use_partition`, `use_repartition`, `use_all_cores` and
`tune_memory_allocation`) and by `benchmark.py`, so it also applies when using
`run_benchmark.sh`. In Docker the `config/` folder
is bind-mounted, so changes take effect on the next run without rebuilding the
image. The values used in each run are recorded in the report, next to every
query.

The file is validated by `pipeline/config_loader.py`: unknown keys and invalid
combinations are rejected before anything runs.

```toml
[general]
runs_per_query = 6

[spark]
use_partition = false
use_repartition = false
adaptative_enabled = true
adaptative_coalesce_partitions = true
adapatative_skew_join = true
broadcast_threshold_mb = 10
shuffle_partitions = 200
use_all_cores = false
tune_memory_allocation = false
enable_static_caching = false
register_tables_as = "views"
cbo_enabled = false
cbo_join_reorder = false
cbo_plan_stats_enabled = false
```

| Option | Description |
|---|---|
| `runs_per_query` | How many times each query is run. The run summary uses the median time of each query. |
| `use_partition` | Read (and, in `convert.py`, write) the partitioned Parquet layout, `parquet_partitioned_<N>GB`. |
| `use_repartition` | When writing partitioned tables, write one file per partition value. Requires `use_partition = true`. |
| `adaptative_enabled` | Adaptive Query Execution (`spark.sql.adaptive.enabled`). |
| `adaptative_coalesce_partitions` | AQE partition coalescing (`spark.sql.adaptive.coalescePartitions.enabled`). |
| `adapatative_skew_join` | AQE skew join handling (`spark.sql.adaptive.skewJoin.enabled`). |
| `broadcast_threshold_mb` | Maximum table size in MB for broadcast joins (`spark.sql.autoBroadcastJoinThreshold`). `-1` disables them. |
| `shuffle_partitions` | Number of shuffle partitions (`spark.sql.shuffle.partitions`). `-1` uses 2 per CPU core. |
| `use_all_cores` | `true` uses as many threads as CPU cores (`local[*]`); `false` uses 4 (`local[4]`). |
| `tune_memory_allocation` | Sets `spark.memory.fraction = 0.8` and `spark.memory.storageFraction = 0.3`. |
| `enable_static_caching` | Caches the small dimension tables (`date_dim`, `warehouse`, `ship_mode`, `store`, `item`) in memory before running the queries. |
| `register_tables_as` | `"views"` (temporary views) or `"tables"` (catalog tables, needed for the CBO). |
| `cbo_enabled` | Cost-based optimizer (`spark.sql.cbo.enabled`); computes table and column statistics with `ANALYZE TABLE` first. Requires `register_tables_as = "tables"`. |
| `cbo_join_reorder` | CBO join reordering (`spark.sql.cbo.joinReorder.enabled`). Requires `cbo_enabled = true`. |
| `cbo_plan_stats_enabled` | Use plan statistics in the CBO (`spark.sql.cbo.planStats.enabled`). Requires `cbo_enabled = true`. |

`save_baseline.py` ignores this file and always uses `config/default_spark.toml`,
which holds Spark's own defaults.

## Run locally

The individual Python stages should also be able to run outside Docker if Python, Java,
Spark prerequisites, and their dependencies are installed. Set the same
`TPCDS_TOOL_DIR` and `SCENARIOS_DIR` environment variables.

## Disclosure

This `README.md` was originally written by the authors, and as the project
started growing, it was updated using AI, but constantly revised by the authors.
