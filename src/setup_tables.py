# Databricks notebook source
# MAGIC %md
# MAGIC # Setup Tables for Pipeline Integrity
# MAGIC
# MAGIC This notebook creates the necessary tables and schema for the pipeline:
# MAGIC 1. Checkpoint table for tracking processed files
# MAGIC 2. Parsed files table for storing extracted content
# MAGIC 3. AI query results table for storing analysis output

# COMMAND ----------

# MAGIC %run ./_validators

# COMMAND ----------

dbutils.widgets.text("catalog_name", "main", "Catalog Name")
dbutils.widgets.text("schema_name", "pipeline_integrity", "Schema Name")
dbutils.widgets.text("checkpoint_table", "", "Checkpoint Table")
dbutils.widgets.text("parsed_table", "", "Parsed Table")
dbutils.widgets.text("ai_query_table", "", "AI Query Table")

catalog_name = validate_identifier(dbutils.widgets.get("catalog_name"), "catalog_name")
schema_name = validate_identifier(dbutils.widgets.get("schema_name"), "schema_name")
checkpoint_table = validate_identifier(dbutils.widgets.get("checkpoint_table"), "checkpoint_table")
parsed_table = validate_identifier(dbutils.widgets.get("parsed_table"), "parsed_table")
ai_query_table = validate_identifier(dbutils.widgets.get("ai_query_table"), "ai_query_table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Catalog and Schema

# COMMAND ----------

# Set current catalog first
spark.sql(f"USE CATALOG {catalog_name}")

# Create schema (catalog is expected to exist in production;
# CREATE CATALOG requires elevated privileges and should be done
# via workspace admin tooling, not from a pipeline notebook)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
print(f"✓ Schema '{catalog_name}.{schema_name}' is ready")

# Set current schema
spark.sql(f"USE SCHEMA {schema_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Checkpoint Table
# MAGIC
# MAGIC This table tracks which files have been processed to avoid reprocessing

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {checkpoint_table} (
    file_path STRING NOT NULL
        COMMENT 'Full path to the source file in the Databricks Volume, e.g. /Volumes/catalog/schema/volume/report.pdf. Primary key.',
    file_name STRING
        COMMENT 'Base file name including extension, e.g. report.pdf or recoat_data.xlsx.',
    file_extension STRING
        COMMENT 'Lowercase file extension including the dot, e.g. .pdf, .xlsx, .xls, or .xlsm.',
    file_size_bytes BIGINT
        COMMENT 'Size of the source file in bytes at the time of processing.',
    processed_timestamp TIMESTAMP
        COMMENT 'UTC timestamp when the file was processed by the pipeline.',
    processing_status STRING
        COMMENT 'Outcome of file processing. Values: success (file parsed and stored), failed (parsing error occurred).',
    error_message STRING
        COMMENT 'Detailed error message if processing_status is failed; NULL on success.',
    CONSTRAINT pk_checkpoint PRIMARY KEY (file_path)
)
USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.columnMapping.mode' = 'name'
)
COMMENT 'Pipeline processing checkpoint table. Tracks every file discovered in the input Volume to prevent duplicate processing. Each row represents one source file (PDF or Excel) and records whether it was successfully parsed or failed. Use this table to monitor ingestion completeness, identify failed files, and audit processing history.'
""")

print(f"✓ Checkpoint table '{checkpoint_table}' is ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Parsed Files Table
# MAGIC
# MAGIC This table stores the extracted content from PDF and XLSX files

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {parsed_table} (
    file_path STRING NOT NULL
        COMMENT 'Full path to the source file in the Databricks Volume. Primary key. Joins to checkpoint table and AI query results on this column.',
    file_name STRING
        COMMENT 'Base file name including extension, e.g. report.pdf or recoat_data.xlsx.',
    file_extension STRING
        COMMENT 'Lowercase file extension including the dot, e.g. .pdf, .xlsx, .xls, or .xlsm.',
    text STRING
        COMMENT 'Full extracted text content from the source file. For PDFs this is the output of ai_parse_document (structured text with tables). For Excel files this is a Markdown representation with sheet headings and tables.',
    num_pages INT
        COMMENT 'Number of pages (PDFs) or sheets (Excel files) found in the document. 0 if unknown or parsing failed.',
    metadata STRING
        COMMENT 'JSON string with additional parsing metadata. For Excel files contains sheet names and count. For errors contains the error details.',
    parsed_timestamp TIMESTAMP
        COMMENT 'UTC timestamp when the file content was extracted and stored.',
    CONSTRAINT pk_parsed PRIMARY KEY (file_path)
)
USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.columnMapping.mode' = 'name'
)
COMMENT 'Stores extracted text content from pipeline inspection documents (PDF and Excel files). PDFs are parsed using Databricks ai_parse_document; Excel files are converted to Markdown via the unstructured library. This table is the input to the AI query processing step. Join to the checkpoint table on file_path to correlate parsing results with processing status.'
""")

print(f"✓ Parsed files table '{parsed_table}' is ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create AI Query Results Table
# MAGIC
# MAGIC This table stores the output from ai_query function

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ai_query_table} (
    file_path STRING NOT NULL
        COMMENT 'Full path to the source file in the Databricks Volume. Primary key. Joins to parsed_files and checkpoint tables on this column.',
    file_name STRING
        COMMENT 'Base file name including extension, e.g. report.pdf or recoat_data.xlsx.',
    input_text STRING
        COMMENT 'The text content sent to the AI endpoint, truncated to 128 KB. This is the parsed document text used as input for knowledge extraction.',
    response VARIANT
        COMMENT 'Structured JSON response from the KIE (Knowledge Information Extraction) AI endpoint stored as VARIANT. Contains extracted fields such as operating_zone, pipeline_name, site_id, pipe_diameter_mm, repairs array, recoating details, and other pipeline inspection attributes. Access fields via VARIANT path syntax, e.g. response:pipeline_name::STRING.',
    error_message STRING
        COMMENT 'Error message returned by the AI endpoint if the query failed; NULL on success. Common errors include rate limiting, token limit exceeded, or endpoint unavailability.',
    query_timestamp TIMESTAMP
        COMMENT 'UTC timestamp when the AI query was executed.',
    CONSTRAINT pk_ai_query PRIMARY KEY (file_path)
)
USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.columnMapping.mode' = 'name'
)
COMMENT 'Stores AI-powered Knowledge Information Extraction (KIE) results for pipeline inspection documents. Each row contains the structured data extracted from one source file by the AI endpoint, including pipeline metadata (name, owner, location, dimensions), inspection dates, and a detailed repairs array with repair types, positions, and reference girth welds. The response column is VARIANT for flexible querying. The kie_results view provides a typed, column-level projection of this data. Join to parsed_files on file_path to see the original document text alongside the AI extraction.'
""")

print(f"✓ AI query results table '{ai_query_table}' is ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print("Setup Complete!")
print("=" * 60)
print(f"Catalog: {catalog_name}")
print(f"Schema: {schema_name}")
print(f"Checkpoint Table: {checkpoint_table}")
print(f"Parsed Table: {parsed_table}")
print(f"AI Query Table: {ai_query_table}")
print("=" * 60)

# Verify tables exist
tables = spark.sql(f"SHOW TABLES IN {catalog_name}.{schema_name}").collect()
print(f"\nTables in {catalog_name}.{schema_name}:")
for table in tables:
    print(f"  - {table.tableName}")
