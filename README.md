# DEAI-TPCDS

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

The suggested layout keeps the repository, TPC-DS source, and generated data as
sibling directories:

```text
workspace/
├── DEAI-TPCDS/       # this repository
├── tcpds-tool/       # unzipped TPC-DS download
└── scenarios/        # generated files and benchmark reports
```

From the repository directory, create `.env` from the template:

```powershell
Copy-Item .env.template .env
```

Edit `.env` if the folders are elsewhere:

```dotenv
TPCDS_TOOL_DIR=../tcpds-tool
SCENARIOS_DIR=../scenarios
TPCDS_SCALE=1
```

Paths can be relative to the Compose file or absolute. On Windows, forward
slashes are accepted in absolute Docker bind-mount paths, for example
`C:/Users/name/data/tcpds-tool`. `TPCDS_SCALE` is a positive integer used as the TPC-DS
scale factor; the default `1` stores artifacts in folders named `data_1GB`,
`queries_1GB`, and `parquet_1GB`.

## Run the stages

Build the image once, and rebuild it when the Dockerfile or Python dependencies
change:

```powershell
docker compose build
```

### 1. Prepare

Build `dsdgen` and `dsqgen`, generate the raw `.dat` data, and generate one SQL
file per query template:

```powershell
docker compose run --rm pipeline python pipeline/prepare.py
```

Preparation checks for complete existing outputs and skips already completed
work. Use `--force` to rebuild the tools and regenerate all raw data and query
files:

```powershell
docker compose run --rm pipeline python pipeline/prepare.py --force
```

Raw data generation can take a while and needs disk space appropriate to the
selected scale factor.

### 2. Convert

Convert the raw `.dat` files to Parquet:

```powershell
docker compose run --rm pipeline python pipeline/convert.py
```

Completed Parquet tables are reused on subsequent runs. Use `--force` to
overwrite and rebuild every table:

```powershell
docker compose run --rm pipeline python pipeline/convert.py --force
```

Conversion fails with a list of missing raw inputs instead of silently
producing an incomplete dataset.

### 3. Benchmark

Run all generated queries, or select a subset:

```powershell
docker compose run --rm pipeline python pipeline/benchmark.py
docker compose run --rm pipeline python pipeline/benchmark.py --query 5
docker compose run --rm pipeline python pipeline/benchmark.py --limit 20
docker compose run --rm pipeline python pipeline/benchmark.py --start 10 --end 20
docker compose run --rm pipeline python pipeline/benchmark.py --start 10 --limit 3
```

`--query N` runs only `queryN.sql` and cannot be combined with `--limit`,
`--start`, or `--end`. The range flags use the query number in the filename,
and `--limit` is applied after filtering. Benchmarking reads existing Parquet
and query files; it never regenerates data, queries, or Parquet. Each run writes
a timestamped Excel report into the scenarios directory.

## Generated layout

After preparation and conversion, the scenarios directory contains:

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
└── benchmark_report_1GB_<timestamp>.xlsx
```

This directory is bind-mounted from the host, so all generated files persist
after the containers exit. The repository ignores local scenario artifacts and
`.env`; do not commit generated data or local paths.

## Run locally

The individual Python stages can also be run outside Docker if Python, Java,
Spark prerequisites, and their dependencies are installed. Set the same
`TPCDS_TOOL_DIR`, `SCENARIOS_DIR`, and `TPCDS_SCALE` environment variables.
The preparation stage additionally requires Linux build tools (`make`, a C
compiler, `flex`, and `bison`).

## References

- [TPC-DS Benchmark Specification](https://www.tpc.org/tpcds/)
- [Spark SQL API](https://spark.apache.org/docs/latest/api/sql/)
