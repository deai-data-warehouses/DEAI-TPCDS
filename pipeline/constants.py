import os
import re
from datetime import datetime

from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()

TPCDS_TOOL_DIR = os.getenv("TPCDS_TOOL_DIR", "/input/tpcds-tool")
SCENARIOS_DIR = os.getenv("SCENARIOS_DIR", "/app/scenarios")
TPCDS_SCALE = os.getenv("TPCDS_SCALE", "1")

if not re.fullmatch(r"0*[1-9][0-9]*", TPCDS_SCALE):
    raise ValueError("TPCDS_SCALE must be a positive integer, such as 1 or 10")

STAGED_TPCDS_DIR = os.path.join(SCENARIOS_DIR, ".tpcds-tools")
STAGED_TOOLS_DIR = os.path.join(STAGED_TPCDS_DIR, "tools")
QUERY_TEMPLATES_DIR = os.path.join(STAGED_TPCDS_DIR, "query_templates")
SQL_SCHEMA_PATH = os.path.join(STAGED_TOOLS_DIR, "tpcds.sql")

DATA_DIR = os.path.join(SCENARIOS_DIR, f"data_{TPCDS_SCALE}GB")
PARQUET_DIR = os.path.join(SCENARIOS_DIR, f"parquet_{TPCDS_SCALE}GB")
QUERIES_DIR = os.path.join(SCENARIOS_DIR, f"queries_{TPCDS_SCALE}GB")
REPORT_PATH = os.path.join(
    SCENARIOS_DIR,
    f"benchmark_report_{TPCDS_SCALE}GB_{datetime.now():%Y%m%d_%H%M%S}.xlsx",
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
