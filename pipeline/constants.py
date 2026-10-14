import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()

TPCDS_TOOL_DIR = os.getenv("TPCDS_TOOL_DIR")
SCENARIOS_DIR = os.getenv("SCENARIOS_DIR")

STAGED_TPCDS_DIR = os.path.join(SCENARIOS_DIR, ".tpcds-tools")
STAGED_TOOLS_DIR = os.path.join(STAGED_TPCDS_DIR, "tools")
QUERY_TEMPLATES_DIR = os.path.join(STAGED_TPCDS_DIR, "query_templates")
SQL_SCHEMA_PATH = os.path.join(STAGED_TOOLS_DIR, "tpcds.sql")
RUNS_DIR = os.path.join(SCENARIOS_DIR, "runs")

def get_scenario_paths(scale_factor):
    label = f"{scale_factor:g}GB"
    return (
        os.path.join(SCENARIOS_DIR, f"data_{label}"),
        os.path.join(SCENARIOS_DIR, f"parquet_{label}"),
        os.path.join(SCENARIOS_DIR, f"queries_{label}"),
    )

def create_spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[4]")
        .config("spark.driver.memory", "6g")
        .config("spark.sql.ansi.enable", "true")
        .config("spark.sql.ansi.doubleQuotedIdentifiers", "true")
        .getOrCreate()
    )
