# DEAI-TPCDS
DEAI TPC-DS Benchmark Implementation for Data Warehouse Project

## Prerrequisites

### TCP-DS synthetic data
1. Download the TCP-DS tool zip folder at [https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp](https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp)
2. Unzip the folder and make the following modifications to the files at the
`tools` dir:
    - `date.c` — `getDateWeightFromJulian(jDay, nDistribution)` → `getDateWeightFromJulian(int jDay, int nDistribution)`
    - `tdefs.c` — `getSimpleTdefsByNumber(nTable)` → `getSimpleTdefsByNumber(int nTable)`
    - `tdef_functions.c` — `getTdefFunctionsByNumber(nTable)` → `getTdefFunctionsByNumber(int nTable)`
    - `Makefile` and `makefile` — added `-fcommon` to `LINUX_CFLAGS`
3. Build the tool running `make`.
4. Generate the synthetic data at the desired folder (in this case I am using `data`, which you need to create **beforehand**) using the generated tool. This will create `.dat` files inside the specified folder.
    ```bash
    ./dsdgen -SCALE 1 -DIR ./data FORCE
    ```

### Virtual environment
Options: `uv`, `venv`, etc.


## How to run

### Run with Docker

Docker runs the pipeline in Linux with Python, Java, and PySpark installed. First
prepare the inputs described below and in the prerequisites:

- Make sure the generated raw `.dat` files are in the  `data` folder inside `tools`. If you wish for the data, Parquet, and query files to be elsewhere, adjust both the 'volume' part of docker-compose.yml and the .env file to make them talk to eachother
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
docker compose run --rm pipeline python pipeline/benchmark.py
```

The Compose service bind-mounts `data` and `parquet_1GB/` from the project directory. Generated Parquet files and query files therefore remain on the host after the container exits.
