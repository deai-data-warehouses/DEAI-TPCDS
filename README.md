# DEAI-TPCDS
DEAI TPC-DS Benchmark Implementation for Data Warehouse Project

## Prerrequisites (to run LOCALLY)

### TPC-DS Tools
Download the TCP-DS tool zip folder at
[https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp](https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp)
and unzip it.

During this `README`, the resulting unzipped folder, will be referred to as
`tcpds-tool`.

### TPC-DS synthetic data
1. Make the following modifications to the files at the `tpcds-tools/tools` dir:
    - `date.c` — `getDateWeightFromJulian(jDay, nDistribution)` →
      `getDateWeightFromJulian(int jDay, int nDistribution)`
    - `tdefs.c` — `getSimpleTdefsByNumber(nTable)` → `getSimpleTdefsByNumber(int
      nTable)`
    - `tdef_functions.c` — `getTdefFunctionsByNumber(nTable)` →
      `getTdefFunctionsByNumber(int nTable)`
    - `Makefile` and `makefile` — added `-fcommon` to `LINUX_CFLAGS`

3. Build the tool running `make`. This command generated two tools:
  - `dsdgen`: for generating the synthetic data.
  - `dsqgen`: for generating the queries.

4. Generate the synthetic data at the desired folder (in this case I am using
   `data`, which you need to create **beforehand**) using the generated tool.
   This will create `.dat` files inside the specified folder.
    ```bash
    ./dsdgen -SCALE 1 -DIR ./data FORCE
    ```

### TPC-DS queries
1. Go inside the `tpcsds-tool/query_templates` dir and fix the Netezza dialect
   bug by appending the missing variable definition:
   ```bash
   echo 'define _END = "";' >> netezza.tpl
    ```

2. Generate the queries using the `scripts/generate_queries.sh` script in this
   repo:`./scripts/generate_queries.sh --tpcds-tools-dir <dir> --output-dir <dir>`.
   This will create 99 `sql` files inside the specified output folder.


> ⚠️ **Up to this moment, we have the data, the schema (`tpcds-tool/tools/tpcds.sql`)
and the queries, but we haven't connected anything yet.**

Ideally, the folder structure at this point should be the following:

```
.
├── DEAI-TPCDS    (this repo)
├── scenarios     (or any name you want)
│   ├── data_1GB
│   └── queries_1GB
└── tpcds-tool    (the unzipped TPC-DS tool folder)
```

### Set environment variables
Using `env.template` as a guide, fill in the dirs with the corresponding values:
- `TPCDS_TOOLS_DIR`: the path to the unzipped TPC-DS tool folder. Using the
  folder structure above, this would be `tpcds-tool/tools`.
- `SCENARIOS_DIR`: the path to the folder containing the data and queries. Using
  the folder structure above, this would be `scenarios`.


### Virtual environment
Options: `uv`, `venv`, etc.

---

## How to run

## Run locally

**Only once you have done everything in the prerequisites section, you can run the benchmark locally.**

- `pipeline/main.py` will:
  1. Read the schema from `tpcds-tool/tools/tpcds.sql` and create empty tables
     in Spark with the correct schema.
  2. Read the raw `.dat` files from the `data_1GB` folder, attach the schema to
     them, and write them to Parquet format in the `parquet_1GB` folder.

- `pipeline/benchmark.py` will:
  1. Read the Parquet files from the `parquet_1GB` folder and register them as
     temporary views in Spark.
  2. Read the queries from the `queries_1GB` folder and execute them one by one,
     measuring the execution time for each query.

#### `benchmark.py` parameters

`--scale-factor` is required and records the scale factor of the dataset being
benchmarked. It does not generate or select the dataset; `PARQUET_DIR` in
`pipeline/constants.py` determines which Parquet data is loaded. Query filters
are optional; if none are provided, all queries are executed.

| Parameter | Description |
|---|---|
| `--scale-factor N` | Required. Record the data scale factor for this run (for example, `1`). |
| `--limit N` | Run only the first `N` queries (in query order). |
| `--query N` | Run only the file `queryN.sql`. Cannot be combined with `--limit`, `--start` or `--end`. |
| `--start N` | Run the queries from `queryN.sql` onwards (inclusive). |
| `--end N` | Run the queries up to `queryN.sql` (inclusive). |

`--start`, `--end` and `--limit` can be combined; `--limit` is applied last.
`--start` and `--end` use the query number from the file name, not its position.
If exactly one query is executed, its error (if any) is also printed in the
terminal, not only saved in the Excel report.

```bash
python pipeline/benchmark.py --scale-factor 1 --query 5
python pipeline/benchmark.py --scale-factor 1 --limit 20
python pipeline/benchmark.py --scale-factor 1 --start 10 --end 20
python pipeline/benchmark.py --scale-factor 1 --start 10 --limit 3
```

Each invocation creates a unique folder under `SCENARIOS_DIR/runs/`, organized
by UTC date and run ID, and saves one Excel workbook named
`benchmark_report.xlsx` there. The workbook has a `Query Runs` sheet with one
row per executed SQL statement and a `Run Summary` sheet with one row for the
whole invocation. The same folder also contains `query_runs.csv` and
`run_summary.csv`, exported from those respective sheets for analysis in a
notebook or other tools.

With Docker, add the parameters after the command:

```bash
docker compose run --rm pipeline python pipeline/benchmark.py --scale-factor 1 --query 5
```


### Run with Docker

Docker runs the pipeline in Linux with Python, Java, and PySpark installed.
First prepare the inputs described below and in the prerequisites:

- Make sure the generated raw `.dat` files are in the  `data` folder inside
  `tools`. If you wish for the data, Parquet, and query files to be elsewhere,
  adjust both the 'volume' part of docker-compose.yml and the .env file to make
  them talk to eachother
- Keep the TPC-DS tools directory, including `tpcds.sql`, at
  `DSGen-software-code-4.0.0/tools/`.
- Generate `queries_1GB/query_0.sql` with `dsqgen` before running the benchmark.

Build the image and convert the raw data files to Parquet:

```bash
docker compose build
docker compose run --rm pipeline
```
Or just docker compose run --build --rm pipeline

Run the benchmark after the Parquet files and query file are ready:

```bash
docker compose run --rm pipeline python pipeline/benchmark.py --scale-factor 1
```

The Compose service bind-mounts `data` and `parquet_1GB/` from the project
directory. Generated Parquet files and query files therefore remain on the host
after the container exits.

---

## References
- [TPC-DS Benchmark Specification](https://www.tpc.org/tpcds/)
- [Spark SQL API](https://spark.apache.org/docs/latest/api/sql/)
- Claude (Sonnet 5.5 Medium)