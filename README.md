# DEAI-TPCDS
DEAI TPC-DS Benchmark Implementation for Data Warehouse Project

## How to run

## Prerrequisites

### TCP-DS synthetic data
1. Download the TCP-DS tool zip folder at [https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp](https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp)
2. Unzip the folder and make the following modifications to the files at the
`tools` dir:
    > `date.c` — `getDateWeightFromJulian(jDay, nDistribution)` → `getDateWeightFromJulian(int jDay, int nDistribution)`
    > `tdefs.c` — `getSimpleTdefsByNumber(nTable)` → `getSimpleTdefsByNumber(int nTable)` 
    > `tdef_functions.c` — `getTdefFunctionsByNumber(nTable)` → `getTdefFunctionsByNumber(int nTable)`
    > `Makefile` and `makefile` — added `-fcommon` to `LINUX_CFLAGS`
3. Build the tool running `make`.
4. Generate the synthetic data at the desired folder (in this case I am using `data`, which you need to create **beforehand**) using the generated tool. This will create `.dat` files inside the specified folder.
    ```bash
    ./dsdgen -SCALE 1 -DIR ./data FORCE
    ```

### Virtual environment
Options: `uv`, `venv`, etc.
