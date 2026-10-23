# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A TPC-DS benchmark pipeline on PySpark (local mode). Three stages, each a standalone script under `pipeline/`, run in order per scale factor:

1. `prepare.py` — stage + patch the TPC-DS toolkit, compile `dsdgen`/`dsqgen`, generate raw `.dat` data and one `queryN.sql` per template.
2. `convert.py` — convert `.dat` → Parquet using schemas derived from `tpcds.sql`.
3. `benchmark.py` — register Parquet tables as temp views, run the queries, write a report.

The TPC-DS toolkit itself is user-supplied (not in the repo, gitignored as `tpcds-tool/`). There are no tests or linters.

## Commands

Normally everything runs through Docker Compose (requires `.env` from `.env.template` with `TPCDS_TOOL_DIR` and `SCENARIOS_DIR`):

```bash
docker compose build
docker compose run --rm pipeline python pipeline/prepare.py --scale-factor 1 [--force]
docker compose run --rm --service-ports pipeline python pipeline/convert.py --scale-factor 1 [--force]
docker compose run --rm --service-ports pipeline python pipeline/benchmark.py --scale-factor 1 [--query N | --limit N | --start N --end M] [--runs N]
```

- `--service-ports` is needed for Spark stages: the Spark UI is pinned to port 4040 with `spark.port.maxRetries=0`, so only one Spark session can run at a time and a busy port fails immediately.
- `convert.py` and `benchmark.py` end with an `input()` prompt to keep the Spark UI alive — they block until Enter is pressed, so they need an interactive TTY.
- `--query N` is the way to run a single query; it can't be combined with `--limit/--start/--end`.
- The container has no default stage: `docker compose run pipeline` with no command only prints usage.

Running outside Docker: dependencies are managed with `uv` (`uv sync`, Python 3.13), plus Java 21 and, for `prepare.py`, `make`/gcc/`flex`/`bison`. Run scripts from the repo root as `python pipeline/<stage>.py` — the modules import each other relative to `pipeline/` (`from constants import ...`, `from toolkit.schema import ...`), and `scripts/generate_queries.sh` is resolved relative to the repo. Python deps in the `Dockerfile` are pinned separately from `pyproject.toml`; keep them in sync when changing dependencies.

## Architecture notes

- **Config**: `pipeline/constants.py` reads `TPCDS_TOOL_DIR`/`SCENARIOS_DIR` at import time (via `dotenv`) and derives all paths; `get_scenario_paths(scale)` returns `(data_<N>GB, parquet_<N>GB, queries_<N>GB)` under `SCENARIOS_DIR`. `pipeline/spark_session.py` owns `create_spark_session` (local[4], 6g driver, ANSI mode + double-quoted identifiers, JVM heap free-ratio flags) and `register_tables_in_spark`. Multiple scale factors coexist side by side.
- **Staging**: `prepare.py` copies the read-only toolkit into `SCENARIOS_DIR/.tpcds-tools/` and patches the copy idempotently via `toolkit/patches.py` (C function signatures, `-fcommon` in the Makefile, `define _END = "";` in `netezza.tpl`). Patch helpers raise if the source doesn't match the expected old or new form — update them if targeting a different TPC-DS version (currently 4.0.0).
- **Idempotency via markers**: stages skip finished work based on marker files — `.build-complete` (tools), `.complete` (in data and query dirs), and Spark's `_SUCCESS` per Parquet table (`toolkit/schema.parquet_table_is_complete`; `incomplete_parquet_tables` lists the ones not done, used by both `convert.py` and `benchmark.py`). `--force` ignores them. Downstream stages validate upstream outputs and fail with a list of what's missing rather than regenerating.
- **Table list source of truth**: `toolkit/schema.get_table_names(schema_path)` checks the schema exists ("run prepare.py first") and parses `CREATE TABLE` statements from the staged `tpcds.sql` (excluding `dbgen_version`). `convert.py` executes those DDL statements in Spark (after stripping `primary key` lines) to obtain each table's schema, then reads the pipe-delimited `.dat` with it.
- **Query dialect fixes**: queries are generated with the `netezza` dialect. Spark-incompatibilities are fixed at benchmark time in `toolkit/queries.normalize_sql_for_spark` (strip `N days` units, `c_last_review_date_sk` → `c_last_review_date`), not by editing generated files. Files with multiple statements (queries 14, 23, 24, 39) are split on `;` and each part is timed separately. See `docs/CHANGES.md` for the rationale.
- **Benchmark measurement**: each statement runs `--runs` times (default 5, recorded as `runs_per_query` in the summary) via `spark.sql(sql).count()`; `min_seconds`/`max_seconds`/`avg_seconds` are recorded on success (left empty on failure), and the first failure stops further runs of that statement. A forced JVM GC (`spark._jvm.System.gc()`) runs between statements to prevent host OOM on long runs — background in `docs/spark-java-foundations.md`.
- **Reports**: each benchmark invocation writes to `SCENARIOS_DIR/runs/<UTC date>/<HHMMSS>_<scale>GB_<selection>/` (selection label from `_query_selection_label`, e.g. `full`, `query5`, `from10_first3`; the UUID run ID lives only in the `run_id` column): `benchmark_report.xlsx` (sheets `Query Runs` and `Run Summary`) plus `query_runs.csv` / `run_summary.csv`. Report writing lives in `pipeline/reporting/` (`summary.build_summary` aggregates the run; `report.write_reports` writes the workbook and CSVs, with Excel column widths looked up by column name in `COLUMN_WIDTHS`, so add an entry there when adding a column).
- **Container permissions**: the compose entrypoint runs as root and `chown`s `/app/scenarios` back to `HOST_UID:HOST_GID` (default 1000) on exit.

Generated data (`scenarios/`), `.env`, and the toolkit are gitignored — never commit them.
