import os
import re

from constants import SPARK_SESSION, SQL_SCHEMA_PATH, DATA_DIR, PARQUET_DIR
from utils import clean_sql_comments

print(f"Data input directory: {DATA_DIR}")
print(f"Parquet output directory: {PARQUET_DIR}")
print(f"SQL schema file: {SQL_SCHEMA_PATH}")

# Read the official SQL file and clean it from comments
print(f"Reading SQL schema from: {SQL_SCHEMA_PATH}")
with open(SQL_SCHEMA_PATH, "r") as f:
    sql_schema = f.read()

no_comments = clean_sql_comments(sql_schema)

# Split the file into individual statements (each semicolon separates a table)
statements = [stmt.strip() for stmt in no_comments.split(";") if stmt.strip()]

print(f"Starting conversion. Output directory: {PARQUET_DIR}\n")

# Loop through all SQL statements
for stmt in statements:
    if "create table" in stmt.lower():
        # Extract the table name
        first_line = stmt.splitlines()[0].lower()
        table_name = re.search(r"create table (\w+)", first_line).group(1)

        # Skip dbgen_version due to unsupported TIME data type
        # It is not used in the queries, it is only metadata for the TPC-DS benchmark
        if table_name == "dbgen_version":
            print("Skipping dbgen_version (metadata not required for queries)...")
            continue

        print(f"Processing table: {table_name}...")

        # --- Remove Primary Key constraints (Spark SQL does not support them) ---
        clean_lines = []
        for line in stmt.splitlines():
            if "primary key" not in line.lower():
                clean_lines.append(line)

        clean_stmt = "\n".join(clean_lines)
        # Remove trailing commas before the closing parenthesis
        clean_stmt = re.sub(r',\s*\)', '\n)', clean_stmt)
        # --------------------------------------

        # Register the empty table in Spark using the cleaned SQL. This creates a
        # table named `table_name` with the correct schema but no data
        SPARK_SESSION.sql(clean_stmt)

        # Extract the perfect schema object for this specific table
        table_schema = SPARK_SESSION.table(table_name).schema

        dat_file_path = os.path.join(DATA_DIR, f"{table_name}.dat")

        if not os.path.exists(dat_file_path):
            print(f"  -> WARNING: File {dat_file_path} not found. Skipping table.")
            continue

        df = SPARK_SESSION.read.csv(
            path=dat_file_path,
            sep='|',
            schema=table_schema
        )

        # Save data in optimized Parquet format
        output_path = os.path.join(PARQUET_DIR, table_name)
        df.write.mode("overwrite").parquet(output_path)
        print(f"  -> Parquet export completed successfully.")

print("\nAll tables have been converted to Parquet!")
SPARK_SESSION.stop()