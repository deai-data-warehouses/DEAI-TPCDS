import os

from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

TPCDS_TOOL_DIR = os.getenv("TPCDS_TOOL_DIR")
SCENARIOS_DIR  = os.getenv("SCENARIOS_DIR")

STAGED_TPCDS_DIR = os.path.join(SCENARIOS_DIR, ".tpcds-tools")
STAGED_TOOLS_DIR = os.path.join(STAGED_TPCDS_DIR, "tools")
QUERY_TEMPLATES_DIR = os.path.join(STAGED_TPCDS_DIR, "query_templates")
SQL_SCHEMA_PATH = os.path.join(STAGED_TOOLS_DIR, "tpcds.sql")
RUNS_DIR = os.path.join(SCENARIOS_DIR, "runs")

# Version-controlled benchmark queries and run configuration, at the repository
# root
QUERIES_DIR = str(Path(__file__).resolve().parent.parent / "queries")
CONFIG_PATH = str(Path(__file__).resolve().parent.parent / "config" / "config.toml")
# Subfolders of QUERIES_DIR the benchmark can read from
QUERY_VERSIONS = ("original", "changed")

def get_scenario_paths(scale_factor, use_partition=False):
    label = f"{scale_factor:g}GB"
    parquet_label = f"parquet_partitioned_{label}" if use_partition else f"parquet_{label}"
    return (
        os.path.join(SCENARIOS_DIR, f"data_{label}"),
        os.path.join(SCENARIOS_DIR, parquet_label),
        os.path.join(SCENARIOS_DIR, f"queries_{label}"),
    )

def get_benchmark_queries_dir(scale_factor, version="original"):
    return os.path.join(QUERIES_DIR, version, f"queries_{scale_factor:g}GB")
