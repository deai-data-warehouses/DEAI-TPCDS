import os
import re
import time

from constants import PARQUET_DIR, QUERIES_DIR, SPARK_SESSION

# Reduce Spark log verbosity to keep the terminal clean
SPARK_SESSION.sparkContext.setLogLevel("ERROR")

# Register Parquet tables as SQL temporary views
print("Registering temporary views from Parquet files...")
for table_name in os.listdir(PARQUET_DIR):
    table_path = os.path.join(PARQUET_DIR, table_name)
    if os.path.isdir(table_path):
        # Read parquet dataset
        df = SPARK_SESSION.read.parquet(table_path)
        # Register with its original table name (e.g., "store_sales") for query
        # resolution
        df.createOrReplaceTempView(table_name)
print("All tables are ready for SQL queries!\n")

print("=================================")
print("  TPC-DS BENCHMARK START   ")
print("=================================\n")

def query_number(filename):
    match = re.search(r"\d+", filename)
    return int(match.group()) if match else float("inf")


# Run the queries in numeric order (query1, query2, ..., query10)
query_files = sorted(
    (f for f in os.listdir(QUERIES_DIR) if f.endswith(".sql")),
    key=lambda f: (query_number(f), f),
)

for query_file in query_files:
    query_path = os.path.join(QUERIES_DIR, query_file)
    with open(query_path, "r") as f:
        raw_sql = f.read()

    print(f"Executing {query_file}...")
    start_time = time.time()
    try:
        rows = SPARK_SESSION.sql(raw_sql).count()
        elapsed = time.time() - start_time
        print(f"✅ OK! Time: {elapsed:.2f} s (Rows: {rows})")
    except Exception as e:
        elapsed = time.time() - start_time
        print(f"❌ FAILED in {elapsed:.2f} s")

print("\nBenchmark completed!")
try:
    SPARK_SESSION.stop()
except:
    pass