# Databricks notebook source
# MAGIC %md
# MAGIC # Shared Validation Helpers
# MAGIC
# MAGIC Reusable input validation functions for all pipeline notebooks.
# MAGIC Import via `%run ./_validators` in any notebook.

# COMMAND ----------

import re

def validate_identifier(name, label="identifier"):
    """Validate that a string is a safe SQL identifier (catalog.schema.table)."""
    if not re.match(r'^[a-zA-Z_]\w*(\.[a-zA-Z_]\w*)*$', name):
        raise ValueError(f"Invalid {label}: {name!r}")
    return name

def validate_volume_path(path):
    """Validate that a volume path looks legitimate."""
    if not path.startswith("/Volumes/"):
        raise ValueError(f"Invalid volume path (must start with /Volumes/): {path!r}")
    return path
