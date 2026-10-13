import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()

TPCDS_TOOLS_DIR = os.getenv("TPCDS_TOOLS_DIR")
SCENARIOS_DIR   = os.getenv("SCENARIOS_DIR")

SQL_SCHEMA_PATH = os.path.join(TPCDS_TOOLS_DIR, "tpcds.sql")

def get_scenario_paths(scale_factor):
    label = f"{scale_factor:g}GB"
    return (
        os.path.join(SCENARIOS_DIR, f"data_{label}"),
        os.path.join(SCENARIOS_DIR, f"parquet_{label}"),
        os.path.join(SCENARIOS_DIR, f"queries_{label}"),
    )


DATA_DIR, PARQUET_DIR, QUERIES_DIR = get_scenario_paths(1)
RUNS_DIR = os.path.join(SCENARIOS_DIR, "runs")

SPARK_SESSION = SparkSession.builder \
    .appName("TPCDS_Parquet_Conversion") \
    .master("local[4]") \
    .config("spark.driver.memory", "6g") \
    .config("spark.sql.ansi.enable", "true") \
    .config("spark.sql.ansi.doubleQuotedIdentifiers", "true") \
    .getOrCreate()
