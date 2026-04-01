# Databricks notebook source
# MAGIC %md
# MAGIC # Parse Files from Volume
# MAGIC
# MAGIC This notebook:
# MAGIC 1. Scans the Databricks Volume for new files (PDF and XLSX)
# MAGIC 2. Checks against the checkpoint table to identify unprocessed files
# MAGIC 3. Parses PDF files using ai_parse function
# MAGIC 4. Parses XLSX files using Python libraries and converts to HTML
# MAGIC 5. Stores parsed content in the parsed_files table
# MAGIC 6. Updates the checkpoint table

# COMMAND ----------

# MAGIC %run ./_validators

# COMMAND ----------

dbutils.widgets.text("catalog_name", "main", "Catalog Name")
dbutils.widgets.text("schema_name", "pipeline_integrity", "Schema Name")
dbutils.widgets.text("volume_path", "", "Volume Path")
dbutils.widgets.text("checkpoint_table", "", "Checkpoint Table")
dbutils.widgets.text("parsed_table", "", "Parsed Table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Library Requirements
# MAGIC
# MAGIC This notebook requires `unstructured[xlsx]` and `markdownify`.
# MAGIC When running as a job task, these are installed via `libraries` in `pipeline_job.yml`.
# MAGIC For interactive use, run: `%pip install "unstructured[xlsx]" markdownify` then restart Python.

# COMMAND ----------

import os
import tempfile
import traceback
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
from unstructured.partition.xlsx import partition_xlsx
from markdownify import markdownify as md

MAX_FILE_SIZE_BYTES = 50 * 1024 * 1024  # 50 MB — skip files larger than this

# COMMAND ----------

# Validate and extract widget values
catalog_name = validate_identifier(dbutils.widgets.get("catalog_name"), "catalog_name")
schema_name = validate_identifier(dbutils.widgets.get("schema_name"), "schema_name")
volume_path = validate_volume_path(dbutils.widgets.get("volume_path"))
checkpoint_table = validate_identifier(dbutils.widgets.get("checkpoint_table"), "checkpoint_table")
parsed_table = validate_identifier(dbutils.widgets.get("parsed_table"), "parsed_table")

# COMMAND ----------

# Set current catalog and schema
spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE SCHEMA {schema_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scan Volume for Files

# COMMAND ----------

def list_files_in_volume(volume_path):
    """List all supported files in the volume using os.walk on FUSE mount.

    Uses the local FUSE mount instead of recursive dbutils.fs.ls() calls,
    which issues one REST API call per directory and becomes slow for large
    volumes with deep directory trees.
    """
    SUPPORTED_EXTENSIONS = {'.pdf', '.xlsx', '.xls', '.xlsm'}
    files = []

    try:
        for dirpath, dirnames, filenames in os.walk(volume_path):
            # Skip the exports folder written by the export_to_excel task
            dirnames[:] = [d for d in dirnames if d != 'exports']
            for fname in filenames:
                ext = os.path.splitext(fname)[1].lower()
                if ext in SUPPORTED_EXTENSIONS:
                    full_path = os.path.join(dirpath, fname)
                    files.append({
                        'file_path': full_path,
                        'file_name': fname,
                        'file_extension': ext,
                        'file_size_bytes': os.path.getsize(full_path),
                    })
    except Exception as e:
        print(f"Error listing files in {volume_path}: {str(e)}")

    return files

# List all files in the volume
print(f"Scanning volume: {volume_path}")
all_files = list_files_in_volume(volume_path)
print(f"Found {len(all_files)} files")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Identify New Files

# COMMAND ----------

if len(all_files) == 0:
    print("No files found in volume. Exiting.")
    dbutils.notebook.exit("No files to process")

# Convert to DataFrame
files_df = spark.createDataFrame(all_files)

# Get already processed files from checkpoint table
try:
    processed_files_df = spark.table(checkpoint_table).select("file_path")

    # Find new files (not in checkpoint)
    new_files_df = files_df.join(
        processed_files_df,
        on="file_path",
        how="left_anti"
    )
except Exception as e:
    print(f"Checkpoint table is empty or doesn't exist: {str(e)}")
    new_files_df = files_df

# Filter out files exceeding the size limit
oversized_df = new_files_df.filter(F.col("file_size_bytes") > MAX_FILE_SIZE_BYTES)
oversized_count = oversized_df.count()
if oversized_count > 0:
    print(f"Skipping {oversized_count} file(s) exceeding {MAX_FILE_SIZE_BYTES // (1024*1024)} MB size limit")
    oversized_df.select("file_path", "file_name", "file_size_bytes").display()
    # Record oversized files in checkpoint as skipped
    oversized_checkpoint = oversized_df \
        .withColumn("processed_timestamp", F.current_timestamp()) \
        .withColumn("processing_status", F.lit("skipped_too_large")) \
        .withColumn("error_message", F.lit(f"File exceeds {MAX_FILE_SIZE_BYTES} byte limit"))
    oversized_checkpoint.createOrReplaceTempView("oversized_batch")
    spark.sql(f"""
        MERGE INTO {checkpoint_table} t
        USING oversized_batch s ON t.file_path = s.file_path
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

new_files_df = new_files_df.filter(F.col("file_size_bytes") <= MAX_FILE_SIZE_BYTES)
new_files_count = new_files_df.count()
print(f"New files to process: {new_files_count}")

if new_files_count == 0:
    print("No new files to process. Exiting.")
    dbutils.notebook.exit("No new files to process")

new_files_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parse PDF Files using ai_parse

# COMMAND ----------

def parse_pdf_files():
    """Parse PDF files using Databricks ai_parse_document SQL function"""

    # Filter PDF files
    pdf_files_df = new_files_df.filter(F.col("file_extension") == ".pdf")
    pdf_count = pdf_files_df.count()

    if pdf_count == 0:
        print("No PDF files to process")
        return None

    print(f"Processing {pdf_count} PDF files...")

    # Read binary content only for the new PDF files (not the entire volume)
    pdf_paths = [row.file_path for row in pdf_files_df.select("file_path").collect()]
    pdf_binary_df = spark.read.format("binaryFile").load(pdf_paths)

    # Join with file metadata and parse
    pdf_files_df.createOrReplaceTempView("pdf_files_to_parse")
    pdf_binary_df.createOrReplaceTempView("pdf_binary")

    parsed_pdf_df = spark.sql(f"""
        WITH pdf_to_process AS (
            SELECT
                p.file_path,
                p.file_name,
                p.file_extension,
                b.content
            FROM pdf_files_to_parse p
            INNER JOIN pdf_binary b ON p.file_path = replace(b.path, 'dbfs:', '')
        ),
        pdf_parsed AS (
            SELECT
                file_path,
                file_name,
                file_extension,
                CAST(ai_parse_document(
                    content,
                    map('version', '2.0', 'descriptionElementTypes', '')
                ) AS STRING) as text
            FROM pdf_to_process
        )
        SELECT
            file_path,
            file_name,
            file_extension,
            text,
            0 as num_pages,  -- ai_parse_document does not return page count
            '' as metadata,
            current_timestamp() as parsed_timestamp
        FROM pdf_parsed
    """)

    return parsed_pdf_df

pdf_results = parse_pdf_files()
if pdf_results:
    print(f"✓ Parsed {pdf_results.count()} PDF files")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parse XLSX Files using Python

# COMMAND ----------

def xlsx_to_markdown(file_path):
    """
    Parse XLSX file using unstructured and convert output to markdown.
    Table elements are converted from their HTML representation via markdownify.
    Returns a dictionary with text (markdown) and metadata.
    """
    try:
        # unstructured needs a local POSIX path; strip the dbfs: prefix
        local_path = file_path.replace("dbfs:", "")

        elements = partition_xlsx(filename=local_path)

        md_parts = []
        seen_sheets_set = set()
        seen_sheets_order = []

        for element in elements:
            page_name = getattr(element.metadata, 'page_name', None)

            # Emit a heading when entering a new sheet
            if page_name and page_name not in seen_sheets_set:
                seen_sheets_set.add(page_name)
                seen_sheets_order.append(page_name)
                md_parts.append(f"\n## Sheet: {page_name}\n")

            if element.category == 'Table':
                html = getattr(element.metadata, 'text_as_html', None)
                if html:
                    md_parts.append(md(html))
                elif element.text:
                    md_parts.append(element.text)
            elif element.category in ('Title', 'Header'):
                md_parts.append(f"### {element.text}\n")
            elif element.text:
                md_parts.append(element.text)

        return {
            'text': '\n'.join(md_parts),
            'num_pages': len(seen_sheets_order),
            'metadata': {
                'sheets': seen_sheets_order,
                'total_sheets': len(seen_sheets_order)
            }
        }

    except Exception as e:
        print(f"Error parsing {file_path}: {traceback.format_exc()}")
        return {
            'text': None,
            'num_pages': 0,
            'metadata': {'error': str(e), 'traceback': traceback.format_exc()}
        }

# COMMAND ----------

def parse_xlsx_files():
    """Parse XLSX files using unstructured and convert to markdown"""

    # Filter XLSX files
    xlsx_files_df = new_files_df.filter(
        F.col("file_extension").isin([".xlsx", ".xls", ".xlsm"])
    )
    xlsx_count = xlsx_files_df.count()

    if xlsx_count == 0:
        print("No XLSX files to process")
        return None

    print(f"Processing {xlsx_count} XLSX files...")

    # Process XLSX files iteratively (more reliable than UDF with serverless)
    xlsx_results = []

    for row in xlsx_files_df.toLocalIterator():
        file_path = row['file_path']
        file_name = row['file_name']
        file_extension = row['file_extension']

        try:
            result = xlsx_to_markdown(file_path)
            xlsx_results.append({
                'file_path': file_path,
                'file_name': file_name,
                'file_extension': file_extension,
                'text': result['text'],
                'num_pages': result['num_pages'],
                'metadata': str(result['metadata']),
            })
            print(f"✓ Parsed: {file_name}")
        except Exception as e:
            xlsx_results.append({
                'file_path': file_path,
                'file_name': file_name,
                'file_extension': file_extension,
                'text': None,
                'num_pages': 0,
                'metadata': str({'error': str(e)}),
            })
            print(f"✗ Failed to parse: {file_name} - {str(e)}")

    if len(xlsx_results) == 0:
        return None

    # Convert to DataFrame with explicit schema
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType

    xlsx_schema = StructType([
        StructField("file_path", StringType(), False),
        StructField("file_name", StringType(), True),
        StructField("file_extension", StringType(), True),
        StructField("text", StringType(), True),
        StructField("num_pages", IntegerType(), True),
        StructField("metadata", StringType(), True),
    ])

    # Use current_timestamp() for consistent timezone handling with Spark
    parsed_xlsx_df = spark.createDataFrame(xlsx_results, schema=xlsx_schema) \
        .withColumn("parsed_timestamp", F.current_timestamp())

    return parsed_xlsx_df

xlsx_results = parse_xlsx_files()
if xlsx_results:
    print(f"✓ Parsed {xlsx_results.count()} XLSX files")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Parsed Results

# COMMAND ----------

# Combine PDF and XLSX results
if pdf_results and xlsx_results:
    combined_results = pdf_results.union(xlsx_results)
elif pdf_results:
    combined_results = pdf_results
elif xlsx_results:
    combined_results = xlsx_results
else:
    print("No files were successfully parsed")
    dbutils.notebook.exit("No files parsed")

# Filter out rows with null text (failed parsing)
successful_results = combined_results.filter(F.col("text").isNotNull())

# Write using MERGE for idempotent upserts (safe on job retry)
successful_results.createOrReplaceTempView("parsed_batch")
spark.sql(f"""
    MERGE INTO {parsed_table} t
    USING parsed_batch s ON t.file_path = s.file_path
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
print(f"✓ Saved parsed content to {parsed_table}")

# Determine success/failure counts by reading the written Delta table —
# no re-parsing, just a cheap file_path scan
new_files_df.select("file_path").createOrReplaceTempView("new_batch_paths")

written_batch_paths = spark.sql(f"""
    SELECT file_path FROM {parsed_table}
    JOIN new_batch_paths USING (file_path)
""")

successful_count = written_batch_paths.count()
failed_count = new_files_count - successful_count

print(f"Successfully parsed {successful_count} files")
if failed_count > 0:
    print(f"Failed to parse {failed_count} files")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update Checkpoint Table

# COMMAND ----------

# Build checkpoint rows from new_files_df (already materialized) — no re-parsing
checkpoint_updates = new_files_df.join(written_batch_paths, on="file_path") \
    .withColumn("processed_timestamp", F.current_timestamp()) \
    .withColumn("processing_status", F.lit("success")) \
    .withColumn("error_message", F.lit(None).cast(StringType()))

if failed_count > 0:
    checkpoint_failures = new_files_df.join(written_batch_paths, on="file_path", how="left_anti") \
        .withColumn("processed_timestamp", F.current_timestamp()) \
        .withColumn("processing_status", F.lit("failed")) \
        .withColumn("error_message", F.lit("Failed to parse file"))
    checkpoint_updates = checkpoint_updates.union(checkpoint_failures)

checkpoint_updates.createOrReplaceTempView("checkpoint_batch")
spark.sql(f"""
    MERGE INTO {checkpoint_table} t
    USING checkpoint_batch s ON t.file_path = s.file_path
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
print(f"✓ Updated checkpoint table with {new_files_count} entries")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print("File Parsing Complete!")
print("=" * 60)
print(f"Total new files found: {new_files_count}")
print(f"Successfully parsed: {successful_count}")
print(f"Failed to parse: {failed_count}")
print(f"Parsed files saved to: {parsed_table}")
print(f"Checkpoint updated: {checkpoint_table}")
print("=" * 60)

# Return the count of successfully parsed files
dbutils.notebook.exit(str(successful_count))
