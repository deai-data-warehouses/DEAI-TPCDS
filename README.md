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

Build the image once, and rebuild it when the Dockerfile or Python dependencies
change:

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

### 3. Benchmark

Run all generated queries, or select a subset:

```powershell
docker compose run --rm --service-ports pipeline python pipeline/benchmark.py --scale-factor 1
docker compose run --rm --service-ports pipeline python pipeline/benchmark.py --scale-factor 1 --query 5
docker compose run --rm --service-ports pipeline python pipeline/benchmark.py --scale-factor 1 --limit 20
docker compose run --rm --service-ports pipeline python pipeline/benchmark.py --scale-factor 1 --start 10 --end 20
docker compose run --rm --service-ports pipeline python pipeline/benchmark.py --scale-factor 1 --start 10 --limit 3
```

| Parameter | Description |
|---|---|
| `--scale-factor N` | Required. Set the data scale factor for this run (for example, `1`). It does not generate the dataset. |
| `--limit N` | Run only the first `N` queries (in query order). |
| `--query N` | Run only the file `queryN.sql`. Cannot be combined with `--limit`, `--start` or `--end`. |
| `--start N` | Run the queries from `queryN.sql` onwards (inclusive). |
| `--end N` | Run the queries up to `queryN.sql` (inclusive). |

Benchmarking reads existing Parquet
and query files; it never regenerates data, queries, or Parquet.
Each invocation creates a unique folder under `SCENARIOS_DIR/runs/`, organized
by UTC date and run ID, and saves one Excel workbook named
`benchmark_report.xlsx` there. The workbook has a `Query Runs` sheet with one
row per executed SQL statement and a `Run Summary` sheet with one row for the
whole invocation. The same folder also contains `query_runs.csv` and
`run_summary.csv`, exported from those respective sheets for analysis in a
notebook or other tools. Both outputs include the run configuration, including
the number of repetitions and the enabled partitioning, broadcast-hint, and
adaptive query execution options. Results are saved for both normal and test
runs; test-run results are stored in a `test/` subfolder, and both CSV files
include a `test_run` column indicating the run type.

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
        └── 145347_7b3b9cb7-562d-4081-9f35-2417f32ca6d8
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
