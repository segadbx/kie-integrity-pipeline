# Databricks notebook source
# MAGIC %md
# MAGIC # Setup Genie Space and Dashboard
# MAGIC
# MAGIC This notebook provides instructions and helpers for:
# MAGIC 1. Creating a Databricks Genie Space for the AI query results
# MAGIC 2. Setting up an AI/BI (Lakeview) Dashboard

# COMMAND ----------

# MAGIC %run ./_validators

# COMMAND ----------

dbutils.widgets.text("catalog_name", "main", "Catalog Name")
dbutils.widgets.text("schema_name", "pipeline_integrity", "Schema Name")
dbutils.widgets.text("ai_query_table", "", "AI Query Table")
dbutils.widgets.text("checkpoint_table", "", "Checkpoint Table")
dbutils.widgets.text("parsed_table", "", "Parsed Table")

catalog_name = validate_identifier(dbutils.widgets.get("catalog_name"), "catalog_name")
schema_name = validate_identifier(dbutils.widgets.get("schema_name"), "schema_name")
ai_query_table = validate_identifier(dbutils.widgets.get("ai_query_table"), "ai_query_table")
checkpoint_table = validate_identifier(dbutils.widgets.get("checkpoint_table"), "checkpoint_table")
parsed_table = validate_identifier(dbutils.widgets.get("parsed_table"), "parsed_table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Tables

# COMMAND ----------

# Set current catalog and schema
spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE SCHEMA {schema_name}")

# Check if tables exist
tables = spark.sql(f"SHOW TABLES IN {catalog_name}.{schema_name}").collect()
print("Available tables:")
for table in tables:
    print(f"  - {table.tableName}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Genie Space
# MAGIC
# MAGIC To create a Genie Space for natural language queries:
# MAGIC
# MAGIC ### Option 1: Via UI (Manual)
# MAGIC 1. Navigate to the **AI/BI** section in your Databricks workspace
# MAGIC 2. Click on **Genie Spaces**
# MAGIC 3. Click **Create Genie Space**
# MAGIC 4. Configure the space:
# MAGIC    - **Name**: `Pipeline Integrity Analysis`
# MAGIC    - **Description**: `Natural language interface for querying pipeline integrity results`
# MAGIC    - **Tables**: Add the following tables:
# MAGIC      - `${ai_query_table}` (main table)
# MAGIC      - Optionally add the parsed_files table
# MAGIC      - Optionally add the checkpoint table
# MAGIC 5. Click **Create**
# MAGIC
# MAGIC ### Option 2: Via API (Programmatic)
# MAGIC Use the Databricks REST API to create a Genie Space programmatically.
# MAGIC See: https://docs.databricks.com/en/genie/api-reference.html

# COMMAND ----------

print("=" * 60)
print("Genie Space Configuration")
print("=" * 60)
print(f"Recommended Name: Pipeline Integrity Analysis")
print(f"Main Table: {ai_query_table}")
print(f"Additional Tables:")
print(f"  - {parsed_table}")
print(f"  - {checkpoint_table}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Queries for Genie
# MAGIC
# MAGIC Once the Genie Space is created, you can ask questions like:
# MAGIC - "How many files have been processed?"
# MAGIC - "Show me the most recent AI query results"
# MAGIC - "What is the success rate of AI queries?"
# MAGIC - "Show me all failed AI queries"
# MAGIC - "What types of files have been processed?"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create AI/BI Dashboard
# MAGIC
# MAGIC To create a Lakeview Dashboard:
# MAGIC
# MAGIC ### Option 1: Via UI (Manual)
# MAGIC 1. Navigate to **Dashboards** in your Databricks workspace
# MAGIC 2. Click **Create Dashboard**
# MAGIC 3. Choose **AI/BI Dashboard**
# MAGIC 4. Name it: `Pipeline Integrity Dashboard`
# MAGIC 5. Add visualizations using the queries in `dashboards/dashboard_queries.sql`
# MAGIC
# MAGIC ### Recommended Dashboard Sections:
# MAGIC
# MAGIC #### 1. Summary Section (KPI Cards)
# MAGIC - Total Files Processed
# MAGIC - Files Successfully Parsed
# MAGIC - AI Queries Executed
# MAGIC - Success Rate
# MAGIC
# MAGIC #### 2. Processing Status (Bar/Pie Charts)
# MAGIC - Files by Type (PDF vs XLSX)
# MAGIC - Processing Status Distribution
# MAGIC - AI Query Success vs Failed
# MAGIC
# MAGIC #### 3. Timeline (Line Chart)
# MAGIC - Files Processed Over Time
# MAGIC - Daily Processing Volume
# MAGIC
# MAGIC #### 4. Details Table
# MAGIC - Recent AI Query Results
# MAGIC - Failed Files (if any)
# MAGIC - Error Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Dashboard Query

# COMMAND ----------

# Example: Pipeline Health Summary
spark.sql(f"""
WITH stats AS (
    SELECT
        COUNT(DISTINCT c.file_path) as total_files,
        COUNT(DISTINCT p.file_path) as parsed_files,
        COUNT(DISTINCT aq.file_path) as ai_queried_files,
        COUNT(DISTINCT CASE WHEN aq.error_message IS NULL AND aq.response IS NOT NULL THEN aq.file_path END) as successful_ai_queries
    FROM {checkpoint_table} c
    LEFT JOIN {parsed_table} p ON c.file_path = p.file_path
    LEFT JOIN {ai_query_table} aq ON p.file_path = aq.file_path
    WHERE c.processing_status = 'success'
)
SELECT
    total_files,
    parsed_files,
    ROUND(parsed_files * 100.0 / NULLIF(total_files, 0), 2) as parsing_success_rate,
    ai_queried_files,
    successful_ai_queries,
    ROUND(successful_ai_queries * 100.0 / NULLIF(ai_queried_files, 0), 2) as ai_success_rate
FROM stats
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dashboard Configuration File
# MAGIC
# MAGIC For reference, see the SQL queries in:
# MAGIC ```
# MAGIC dashboards/dashboard_queries.sql
# MAGIC ```
# MAGIC
# MAGIC These queries can be used as the basis for your dashboard visualizations.

# COMMAND ----------

print("=" * 60)
print("Next Steps")
print("=" * 60)
print("1. Create a Genie Space using the UI or API")
print("2. Create an AI/BI Dashboard")
print("3. Add visualizations using the queries in dashboards/dashboard_queries.sql")
print("4. Share the dashboard with your team")
print("=" * 60)
