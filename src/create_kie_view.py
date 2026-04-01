# Databricks notebook source
# MAGIC %md
# MAGIC # Create KIE Results View
# MAGIC
# MAGIC Creates a Unity Catalog view on top of ai_query_results that parses
# MAGIC the JSON response field into typed columns per the KIE schema.

# COMMAND ----------

# MAGIC %run ./_validators

# COMMAND ----------

dbutils.widgets.text("catalog_name", "main", "Catalog Name")
dbutils.widgets.text("schema_name", "pipeline_integrity", "Schema Name")
dbutils.widgets.text("ai_query_table", "", "AI Query Table")
dbutils.widgets.text("kie_view_name", "", "KIE View Name")

catalog_name = validate_identifier(dbutils.widgets.get("catalog_name"), "catalog_name")
schema_name = validate_identifier(dbutils.widgets.get("schema_name"), "schema_name")
ai_query_table = validate_identifier(dbutils.widgets.get("ai_query_table"), "ai_query_table")
kie_view_name = validate_identifier(dbutils.widgets.get("kie_view_name"), "kie_view_name")

# COMMAND ----------

spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE SCHEMA {schema_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check AI Query Results Availability

# COMMAND ----------

if spark.table(ai_query_table).filter("response IS NOT NULL").isEmpty():
    msg = "SKIPPED: No AI query results available — KIE endpoint may not have been running."
    print(msg)
    dbutils.notebook.exit(msg)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create KIE Results View

# COMMAND ----------

# Column comments for Genie / Databricks Assistant discoverability
column_comments = {
    "file_path":          "Full path to the source inspection document in the Databricks Volume. Joins to parsed_files and checkpoint tables.",
    "file_name":          "Base file name with extension, e.g. report.pdf or recoat_data.xlsx.",
    "query_timestamp":    "UTC timestamp when the AI extraction was performed.",
    "error_message":      "AI endpoint error message if extraction failed; NULL on success.",
    "operating_zone":     "Geographical or operational zone where the pipeline is located, e.g. a city, region, or designated area.",
    "pipeline_name":      "Official name of the pipeline as designated in the project documentation.",
    "project_name":       "Name of the inspection or maintenance project.",
    "pipeline_system":    "Name or code of the broader pipeline system this pipeline belongs to.",
    "pipeline_owner":     "Organization or entity that owns the pipeline.",
    "location":           "Specific geographic location of the inspection site, may include city, province, landmarks, or coordinates.",
    "site_id":            "Unique site identifier, also known as Dig Number or Site Name. Examples: GWD 16, Dig #1, JT 388.",
    "dig_number":         "Dig number identifier for the inspection site. May duplicate site_id or provide an alternative reference.",
    "project_manager":    "Full name of the project manager overseeing the inspection.",
    "field_integrity_supervisor": "Full name of the field integrity supervisor on site.",
    "client_project_engineer":    "Full name of the client-side project engineer.",
    "inspection_start":   "Date or datetime when the inspection began. Format varies: ISO 8601, e.g. 2017-10-17, or natural language, e.g. October 17, 2017.",
    "inspection_end":     "Date or datetime when the inspection concluded. Same format as inspection_start.",
    "pipe_diameter_mm":   "Nominal pipe diameter in millimeters (mm).",
    "nominal_wall_thickness_mm": "Nominal pipe wall thickness in millimeters (mm).",
    "material_grade_SMYS_MPa":   "Specified Minimum Yield Strength (SMYS) of the pipe material in megapascals (MPa).",
    "max_allowable_operating_pressure_kPa": "Maximum Allowable Operating Pressure (MAOP) of the pipeline in kilopascals (kPa).",
    "recoating":          "Complex nested structure with recoating details including coating type, application method, primer, and axial positions. Stored as VARIANT.",
    "repairs":            "Array of repair records. Each element is a struct with: repair_start (date), repair_end (date), relative_repair_start (axial position), relative_repair_end (axial position), reference_units (mm or m), repair_type (e.g. Recoat, Compression Sleeve, Patch, Clock Spring), and rgw (Reference Girth Weld number). Use EXPLODE(repairs) to flatten into one row per repair.",
}

# Build the column list with inline COMMENT clauses from the map
column_defs = ",\n    ".join(
    f"{col} COMMENT '{comment.replace(chr(39), chr(92) + chr(39))}'"
    for col, comment in column_comments.items()
)

spark.sql(f"""
CREATE OR REPLACE VIEW {kie_view_name} (
    {column_defs}
)
COMMENT 'Typed, column-level projection of the AI-extracted pipeline inspection data (KIE schema). Each row represents one successfully processed inspection document. Scalar fields are cast to their native types; the repairs array is a typed struct array that can be exploded for per-repair analysis. This is the primary view for Genie Spaces, dashboards, and Excel exports. Source: ai_query_results table (response VARIANT column).'
AS
SELECT
    file_path,
    file_name,
    query_timestamp,
    error_message,
    -- Pipeline and project identification
    response:operating_zone::STRING                        AS operating_zone,
    response:pipeline_name::STRING                         AS pipeline_name,
    response:project_name::STRING                          AS project_name,
    response:pipeline_system::STRING                       AS pipeline_system,
    response:pipeline_owner::STRING                        AS pipeline_owner,
    response:location::STRING                              AS location,
    response:site_id::STRING                               AS site_id,
    response:dig_number::STRING                            AS dig_number,
    -- Personnel
    response:project_manager::STRING                       AS project_manager,
    response:field_integrity_supervisor::STRING             AS field_integrity_supervisor,
    response:client_project_engineer::STRING                AS client_project_engineer,
    -- Inspection dates
    response:inspection_start::STRING                      AS inspection_start,
    response:inspection_end::STRING                        AS inspection_end,
    -- Pipe physical properties
    response:pipe_diameter_mm::DOUBLE                      AS pipe_diameter_mm,
    response:nominal_wall_thickness_mm::DOUBLE             AS nominal_wall_thickness_mm,
    response:material_grade_SMYS_MPa::DOUBLE               AS material_grade_SMYS_MPa,
    response:max_allowable_operating_pressure_kPa::INT     AS max_allowable_operating_pressure_kPa,
    -- Recoating details (complex nested structure)
    response:recoating                                     AS recoating,
    -- Repairs: typed struct array for per-repair analysis
    from_json(
        CAST(response:repairs AS STRING),
        'ARRAY<STRUCT<
            repair_start: STRING,
            repair_end: STRING,
            relative_repair_start: DOUBLE,
            relative_repair_end: DOUBLE,
            reference_units: STRING,
            repair_type: STRING,
            rgw: INT
        >>'
    ) AS repairs
FROM {ai_query_table}
WHERE response IS NOT NULL
""")

print(f"✓ View '{kie_view_name}' created successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print("KIE View Creation Complete!")
print("=" * 60)
print(f"View:         {kie_view_name}")
print(f"Source table: {ai_query_table}")
print("=" * 60)

dbutils.notebook.exit(kie_view_name)
