# Databricks notebook source
# MAGIC %md
# MAGIC # AI Query Processing
# MAGIC
# MAGIC This notebook:
# MAGIC 1. Reads parsed file content from the parsed_files table
# MAGIC 2. Processes each file's content through the ai_query function
# MAGIC 3. Uses the specified AI endpoint for analysis
# MAGIC 4. Stores the results in the ai_query_results table

# COMMAND ----------

# MAGIC %run ./_validators

# COMMAND ----------

dbutils.widgets.text("catalog_name", "main", "Catalog Name")
dbutils.widgets.text("schema_name", "pipeline_integrity", "Schema Name")
dbutils.widgets.text("parsed_table", "", "Parsed Table")
dbutils.widgets.text("ai_query_table", "", "AI Query Table")
dbutils.widgets.text("kie_endpoint_name", "", "KIE Endpoint Name")

catalog_name = validate_identifier(dbutils.widgets.get("catalog_name"), "catalog_name")
schema_name = validate_identifier(dbutils.widgets.get("schema_name"), "schema_name")
parsed_table = validate_identifier(dbutils.widgets.get("parsed_table"), "parsed_table")
ai_query_table = validate_identifier(dbutils.widgets.get("ai_query_table"), "ai_query_table")
kie_endpoint_name = validate_endpoint_name(dbutils.widgets.get("kie_endpoint_name"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check KIE Endpoint Availability

# COMMAND ----------

import requests

def is_endpoint_ready(endpoint_name):
    """Check if the serving endpoint is in READY state."""
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    host = spark.conf.get("spark.databricks.workspaceUrl")
    resp = requests.get(
        f"https://{host}/api/2.0/serving-endpoints/{endpoint_name}",
        headers={"Authorization": f"Bearer {token}"},
        timeout=10,
    )
    if resp.status_code != 200:
        return False, f"HTTP {resp.status_code}: {resp.text[:200]}"
    state = resp.json().get("state", {}).get("ready", "NOT_READY")
    return state == "READY", f"Endpoint state: {state}"

endpoint_ready, endpoint_status = is_endpoint_ready(kie_endpoint_name)
print(f"KIE endpoint '{kie_endpoint_name}': {endpoint_status}")

if not endpoint_ready:
    msg = f"SKIPPED: KIE endpoint '{kie_endpoint_name}' is not available. {endpoint_status}"
    print(msg)
    dbutils.notebook.exit(msg)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# COMMAND ----------

# Set current catalog and schema
spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE SCHEMA {schema_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Parsed Files

# COMMAND ----------

# Read all parsed files
parsed_df = spark.table(parsed_table)

if parsed_df.isEmpty():
    print("No parsed files to process. Exiting.")
    dbutils.notebook.exit("No parsed files to process")

total_parsed = parsed_df.count()
print(f"Total parsed files: {total_parsed}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Identify Files Not Yet Processed by AI Query

# COMMAND ----------

# Get files already processed by AI query
try:
    ai_processed_df = spark.table(ai_query_table).select("file_path")

    # Find files not yet processed
    files_to_process_df = parsed_df.join(
        ai_processed_df,
        on="file_path",
        how="left_anti"
    )
except Exception as e:
    print(f"AI query table is empty or doesn't exist: {str(e)}")
    files_to_process_df = parsed_df

if files_to_process_df.isEmpty():
    print("No new files to process. Exiting.")
    dbutils.notebook.exit("No new files for AI query")

files_to_process_count = files_to_process_df.count()
print(f"Files to process with AI query: {files_to_process_count}")

# Display sample
files_to_process_df.select("file_path", "file_name", "file_extension").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process Files with ai_query Function
# MAGIC
# MAGIC The ai_query function has a text limit. We'll truncate to 131072 characters (128KB) per the example.

# COMMAND ----------

# Prepare data for AI query
# Truncate text to 131072 characters (128KB limit)
input_df = files_to_process_df.select(
    "file_path",
    "file_name",
    F.substring(F.col("text"), 1, 131072).alias("text")
).filter(
    F.col("text").isNotNull() & (F.length(F.col("text")) > 0)
)

if input_df.isEmpty():
    print("No files with valid text to process. Exiting.")
    dbutils.notebook.exit("No valid text for AI query")

input_count = input_df.count()
print(f"Files with valid text for AI query: {input_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute AI Query
# MAGIC
# MAGIC Using SQL with ai_query function for processing

# COMMAND ----------

# Create temporary view for SQL processing
input_df.createOrReplaceTempView("files_to_query")

# Execute ai_query using SQL
# Store response as VARIANT for native JSON navigation downstream
ai_query_results_df = spark.sql(f"""
WITH query_results AS (
    SELECT
        file_path,
        file_name,
        text AS input,
        ai_query(
            '{kie_endpoint_name}',
            text,
            failOnError => false
        ) AS response
    FROM files_to_query
)
SELECT
    file_path,
    file_name,
    input AS input_text,
    response.result AS response,
    CAST(response.errorMessage AS STRING) AS error_message,
    current_timestamp() AS query_timestamp
FROM query_results
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Results to AI Query Table

# COMMAND ----------

# Write using MERGE for idempotent upserts (safe on job retry)
ai_query_results_df.createOrReplaceTempView("ai_query_batch")
spark.sql(f"""
    MERGE INTO {ai_query_table} t
    USING ai_query_batch s ON t.file_path = s.file_path
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print(f"✓ Saved AI query results to {ai_query_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analyze Results

# COMMAND ----------

# Read back only the current batch from the written Delta table using
# the input file paths, so stats reflect this run without re-calling ai_query()
input_df.select("file_path").createOrReplaceTempView("current_batch_paths")

current_batch_df = spark.sql(f"""
    SELECT r.*
    FROM {ai_query_table} r
    JOIN current_batch_paths p ON r.file_path = p.file_path
""")

# Compute stats in a single aggregation pass
stats = current_batch_df.agg(
    F.count(F.when(F.col("response").isNotNull() & F.col("error_message").isNull(), 1)).alias("success_count"),
    F.count(F.when(F.col("error_message").isNotNull(), 1)).alias("error_count")
).first()

success_count = stats["success_count"]
error_count = stats["error_count"]

print(f"Successful AI queries: {success_count}")
print(f"Failed AI queries: {error_count}")

# Display sample results
print("\nSample results:")
current_batch_df.select(
    "file_path",
    "file_name",
    F.substring(F.col("response").cast("STRING"), 1, 200).alias("response_preview"),
    "error_message"
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print("AI Query Processing Complete!")
print("=" * 60)
print(f"Total files processed: {input_count}")
print(f"Successful queries: {success_count}")
print(f"Failed queries: {error_count}")
print(f"Results saved to: {ai_query_table}")
print("=" * 60)

# Show table statistics
spark.sql(f"""
SELECT
    COUNT(*) as total_records,
    COUNT(CASE WHEN response IS NOT NULL AND error_message IS NULL THEN 1 END) as successful,
    COUNT(CASE WHEN error_message IS NOT NULL THEN 1 END) as failed
FROM {ai_query_table}
""").display()

# COMMAND ----------

# Return the count of successfully processed files
dbutils.notebook.exit(str(success_count))
