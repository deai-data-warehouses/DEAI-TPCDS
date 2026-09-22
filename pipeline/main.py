""" 
PREREQUISITE:
Before running this script, you must generate the raw .dat files using the official TPC-DS dsdgen tool.
Navigate to the DSGen tools directory and run:
./dsdgen -SCALE 1 -DIR <your_data_output_dir>
"""


import os
import re
from dotenv import load_dotenv
from pyspark.sql import SparkSession

# 1. Load environment variables
load_dotenv()
TPCDS_TOOLS_DIR = os.getenv("TPCDS_TOOLS_DIR")
DATA_DIR = os.getenv("DATA_DIR")

# Dynamically create the output path for Parquet files
BASE_DIR = os.path.dirname(DATA_DIR)
PARQUET_DIR = os.path.join(BASE_DIR, "parquet_1GB")

# 2. Initialize Spark Session
spark = SparkSession.builder \
    .appName("TPCDS_Parquet_Conversion") \
    .master("local[*]") \
    .getOrCreate()

# 3. Read the official SQL file and clean it from comments
sql_schema_path = os.path.join(TPCDS_TOOLS_DIR, "tpcds.sql")
with open(sql_schema_path, "r") as f:
    sql_schema = f.read()

no_comments = "\n".join(
    line for line in sql_schema.splitlines() if not line.strip().startswith("--")
)
# Split the file into individual statements (each semicolon separates a table)
statements = [stmt.strip() for stmt in no_comments.split(";") if stmt.strip()]

print(f"Starting conversion. Output directory: {PARQUET_DIR}\n")

# 4. Loop through all SQL statements
for stmt in statements:
    if "create table" in stmt.lower():
        # Extract the table name
        first_line = stmt.splitlines()[0].lower()
        table_name = first_line.split("table")[1].strip().split()[0]
        
        # Skip dbgen_version due to unsupported TIME data type
        if table_name == "dbgen_version":
            print("Skipping dbgen_version (metadata not required for queries)...")
            continue
            
        print(f"Processing table: {table_name}...")
        
        # --- Remove Primary Key constraints ---
        clean_lines = []
        for line in stmt.splitlines():
            if "primary key" not in line.lower():
                clean_lines.append(line)
        
        clean_stmt = "\n".join(clean_lines)
        # Remove trailing commas before the closing parenthesis
        clean_stmt = re.sub(r',\s*\)', '\n)', clean_stmt)
        # --------------------------------------
        
        # Register the empty table in Spark using the cleaned SQL
        spark.sql(clean_stmt)
        
        # Extract the perfect schema object for this specific table
        table_schema = spark.table(table_name).schema
        
        # Define the path for the corresponding .dat file
        dat_file_path = os.path.join(DATA_DIR, f"{table_name}.dat")
        
        if not os.path.exists(dat_file_path):
            print(f"  -> WARNING: File {dat_file_path} not found. Skipping table.")
            continue

        # Read the raw text file using the "|" separator and the extracted schema
        df = spark.read.csv(
            path=dat_file_path,
            sep='|',
            schema=table_schema
        )
        
        # Save data in optimized Parquet format
        output_path = os.path.join(PARQUET_DIR, table_name)
        df.write.mode("overwrite").parquet(output_path)
        print(f"  -> Parquet export completed successfully.")

print("\nAll tables have been converted to Parquet!")
spark.stop()