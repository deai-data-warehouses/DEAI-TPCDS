"""
PREREQUISITE:
Before running this benchmark, you must generate the query_0.sql file using the dsqgen tool.
1. Fix the Netezza dialect bug by appending the missing variable definition:
   echo 'define _END = "";' >> ../query_templates/netezza.tpl
2. Generate the 99 queries by running this command in the tools directory:
   ./dsqgen -DIRECTORY ../query_templates -INPUT ../query_templates/templates.lst -VERBOSE Y -QUALIFY Y -SCALE 1 -DIALECT netezza -OUTPUT_DIR <your_queries_output_dir>
"""

import os
import time
from dotenv import load_dotenv
from pyspark.sql import SparkSession

# 1. Load environment paths
load_dotenv()
DATA_DIR = os.getenv("DATA_DIR")
BASE_DIR = os.path.dirname(DATA_DIR)

PARQUET_DIR = os.path.join(BASE_DIR, "parquet_1GB")
QUERIES_FILE = os.path.join(BASE_DIR, "queries_1GB", "query_0.sql")

# 2. Initialize Spark Session
spark = SparkSession.builder \
    .appName("TPCDS_Benchmark") \
    .master("local[*]") \
    .getOrCreate()

# Reduce Spark log verbosity to keep the terminal clean
spark.sparkContext.setLogLevel("ERROR")

# 3. Register Parquet tables as SQL temporary views
print("Registering temporary views from Parquet files...")
for table_name in os.listdir(PARQUET_DIR):
    table_path = os.path.join(PARQUET_DIR, table_name)
    if os.path.isdir(table_path):
        # Read parquet dataset
        df = spark.read.parquet(table_path)
        # Register with its original table name (e.g., "store_sales") for query resolution
        df.createOrReplaceTempView(table_name)
print("All tables are ready for SQL queries!\n")

# 4. Read and split the 99 benchmark queries
with open(QUERIES_FILE, "r") as f:
    raw_sql = f.read()

# Split the file using query headers or comments
import re
queries = re.split(r'--\s*(?:start\s+)?query', raw_sql, flags=re.IGNORECASE)

print("=================================")
print("  TPC-DS BENCHMARK START (1GB)   ")
print("=================================\n")

# Fallback mechanism if the file wasn't split correctly by comments
if len(queries) <= 1:
    queries = [q + ";" for q in raw_sql.split(";") if q.strip()]
    for i, q_text in enumerate(queries[:99], 1):
        print(f"Executing Query {i}...", end=" ", flush=True)
        start_time = time.time()
        try:
            rows = spark.sql(q_text).count()
            elapsed = time.time() - start_time
            print(f"✅ OK! Time: {elapsed:.2f} s (Rows: {rows})")
        except Exception as e:
            elapsed = time.time() - start_time
            print(f"❌ FAILED in {elapsed:.2f} s")
else:
    # Standard execution handling separated blocks (strictly limited from 1 to 99)
    for idx, q in enumerate(queries[1:100], 1):
        print(f"Executing Query {idx}...", end=" ", flush=True)
        
        # Reconstruct the query adding back the prefix trimmed by the regex
        sql_to_run = "SELECT" + q if not q.strip().upper().startswith("WITH") else "WITH" + q
        
        start_time = time.time()
        try:
            result_df = spark.sql(sql_to_run)
            rows = result_df.count()
            elapsed = time.time() - start_time
            print(f"✅ OK! Time: {elapsed:.2f} s (Rows: {rows})")
        except Exception as e:
            elapsed = time.time() - start_time
            print(f"❌ FAILED in {elapsed:.2f} s")

print("\nBenchmark completed!")
try:
    spark.stop()
except:
    pass