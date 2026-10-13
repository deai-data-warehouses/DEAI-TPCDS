import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()

TPCDS_TOOLS_DIR = os.getenv("TPCDS_TOOLS_DIR")
SCENARIOS_DIR   = os.getenv("SCENARIOS_DIR")

SQL_SCHEMA_PATH = os.path.join(TPCDS_TOOLS_DIR, "tpcds.sql")

DATA_DIR    = os.path.join(SCENARIOS_DIR, "data_1GB")
PARQUET_DIR = os.path.join(SCENARIOS_DIR, "parquet_1GB")
QUERIES_DIR = os.path.join(SCENARIOS_DIR, "queries_1GB")
RUNS_DIR = os.path.join(SCENARIOS_DIR, "runs")

SPARK_SESSION = SparkSession.builder \
    .appName("TPCDS_Parquet_Conversion") \
    .master("local[4]") \
    .config("spark.driver.memory", "6g") \
    .config("spark.sql.ansi.enable", "true") \
    .config("spark.sql.ansi.doubleQuotedIdentifiers", "true") \
    .getOrCreate()
