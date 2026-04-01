# Databricks notebook source
# MAGIC %md
# MAGIC # Export KIE Results to Excel
# MAGIC
# MAGIC This notebook:
# MAGIC 1. Reads from the KIE results view (typed, pre-parsed fields)
# MAGIC 2. Explodes the repairs array to produce one row per repair
# MAGIC 3. Maps view columns to the required Excel columns
# MAGIC 4. Exports the result as an Excel file to the Databricks Volume

# COMMAND ----------

# MAGIC %run ./_validators

# COMMAND ----------

dbutils.widgets.text("catalog_name", "main", "Catalog Name")
dbutils.widgets.text("schema_name", "pipeline_integrity", "Schema Name")
dbutils.widgets.text("kie_view_name", "", "KIE View Name")
dbutils.widgets.text("export_volume_path", "", "Export Volume Path")
dbutils.widgets.text("max_export_rows", "100000", "Max Export Rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Library Requirements
# MAGIC
# MAGIC This notebook requires `openpyxl`.
# MAGIC When running as a job task, it is installed via `libraries` in `pipeline_job.yml`.
# MAGIC For interactive use, run: `%pip install openpyxl` then restart Python.

# COMMAND ----------

# Validate and extract widget values
catalog_name = validate_identifier(dbutils.widgets.get("catalog_name"), "catalog_name")
schema_name = validate_identifier(dbutils.widgets.get("schema_name"), "schema_name")
kie_view_name = validate_identifier(dbutils.widgets.get("kie_view_name"), "kie_view_name")
export_volume_path = validate_volume_path(dbutils.widgets.get("export_volume_path"))
max_export_rows = int(dbutils.widgets.get("max_export_rows"))

# COMMAND ----------

import os
import shutil
import tempfile
from datetime import datetime, timezone
import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read and Flatten KIE View

# COMMAND ----------

spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE SCHEMA {schema_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check KIE View Availability

# COMMAND ----------

try:
    has_data = not spark.table(kie_view_name).limit(1).isEmpty()
except Exception:
    has_data = False

if not has_data:
    msg = "SKIPPED: KIE view has no data — upstream processing may have been skipped."
    print(msg)
    dbutils.notebook.exit(msg)

# COMMAND ----------

# Explode the typed repairs array — one row per repair per file.
# Recoat repairs (repair_type = 'Recoat') populate the recoat position columns;
# all other repair types populate the repair position columns.
export_spark_df = spark.sql(f"""
    SELECT
        site_id                                                                                 AS `Dig Number`,
        repair.repair_end                                                                       AS `Repair Date`,
        repair.repair_type                                                                      AS `Repair Type`,
        repair.rgw                                                                              AS `Reference Girth Weld`,
        CASE WHEN repair.repair_type != 'Recoat' THEN repair.relative_repair_start END         AS `Relative Repair Start`,
        CASE WHEN repair.repair_type != 'Recoat' THEN repair.relative_repair_end   END         AS `Relative Repair End`,
        CASE WHEN repair.repair_type  = 'Recoat' THEN repair.relative_repair_start END         AS `Relative Recoat Start`,
        CASE WHEN repair.repair_type  = 'Recoat' THEN repair.relative_repair_end   END         AS `Relative Recoat End`
    FROM {kie_view_name}
    LATERAL VIEW OUTER EXPLODE(repairs) AS repair
    WHERE repair IS NOT NULL
""")

# Guard against collecting too many rows to the driver
row_count = export_spark_df.count()
if row_count > max_export_rows:
    raise ValueError(
        f"Export has {row_count} rows, exceeding the {max_export_rows} limit. "
        "Consider filtering or partitioning the export."
    )

export_df = export_spark_df.toPandas()
total = len(export_df)
print(f"Total repair rows to export: {total}")

if total == 0:
    print("No records to export. Exiting.")
    dbutils.notebook.exit("No records to export")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Excel File to Volume

# COMMAND ----------

os.makedirs(export_volume_path.rstrip("/"), exist_ok=True)

timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
excel_filename = f"recoat_analysis_{timestamp}.xlsx"
excel_path = os.path.join(export_volume_path.rstrip("/"), excel_filename)

# openpyxl uses zipfile internally which requires seekable I/O.
# Databricks Volume FUSE mounts don't support seek, so write to a
# local temp file first and then copy to the volume.
with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
    tmp_path = tmp.name

try:
    export_df.to_excel(tmp_path, index=False, engine="openpyxl")
    shutil.copy2(tmp_path, excel_path)
finally:
    os.unlink(tmp_path)

if not os.path.exists(excel_path) or os.path.getsize(excel_path) == 0:
    raise IOError(f"Excel write verification failed: file missing or empty at {excel_path}")

print(f"✓ Exported {total} records to {excel_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print("Excel Export Complete!")
print("=" * 60)
print(f"Output file : {excel_path}")
print(f"Records     : {total}")
print("=" * 60)

dbutils.notebook.exit(excel_path)
