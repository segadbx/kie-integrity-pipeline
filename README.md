# Pipeline Integrity

A Databricks Asset Bundle (DAB) for processing and analyzing files from a Databricks Volume using AI-powered parsing and querying capabilities.

## Overview

This pipeline automates the ingestion, parsing, and AI-powered analysis of PDF and XLSX/XLSM files stored in Databricks Volume. It provides:

- **Automated File Processing**: Monitors a Databricks Volume for new files
- **Smart Parsing**: Extracts content from PDFs (using `ai_parse`) and XLSX/XLSM files (converted to Markfown)
- **AI-Powered Analysis**: Extracts structured data using Databricks `ai_extract` with a JSON schema
- **Data Tracking**: Maintains checkpoints to avoid reprocessing files
- **Analytics Ready**: Outputs structured data ready for Genie Spaces and AI/BI Dashboards

## Architecture

The solution follows a **2-stage workflow** running on **Databricks Serverless Compute**:

**Stage 1 — Ingestion Job** (`ingestion_job`):
1. **Setup Tables**: Creates the necessary Unity Catalog tables and schema
2. **Parse Files**: Scans the volume, identifies new files, and extracts content
3. **Trigger Processing**: Automatically chains the processing job (see below)

**Stage 2 — Processing Job** (`processing_job`):
4. **AI Extract Processing**: Extracts structured data from parsed content using `ai_extract` with the JSON schema defined in [`kie_schema.json`](kie_schema.json)
5. **Create KIE View**: Creates a typed view from extraction results
6. **Export to Excel**: Exports repairs data to Excel

The ingestion job automatically triggers the processing job via a `run_job_task` after new files are parsed. This means the full pipeline — from file arrival to Excel export — runs end-to-end without manual intervention.

All automated tasks use serverless compute for automatic scaling, fast startup, and cost optimization.

### Data Flow

```
Stage 1 — Ingestion Job (file arrival trigger):
  Databricks Volume → Setup Tables → Parse Files → Parsed Table
                                          ↓               ↓
                                   Checkpoint Table   Triggers Processing Job

Stage 2 — Processing Job (auto-triggered by ingestion):
  Parsed Table → AI Extract (kie_schema.json) → Results Table → KIE View → Excel Export
                                                                    ↓
                                                           Genie Space/Dashboard
```

## Prerequisites

- Databricks Workspace with Unity Catalog enabled
- **Databricks Serverless Compute enabled**
- Databricks Runtime 15.4+ (for AI functions like `ai_extract`)
- A Databricks Volume for input files
- A Databricks Volume for Excel exports
- Databricks CLI installed for deployment

## Quick Start

### 1. Install Databricks CLI

```bash
# Using Homebrew (macOS)
brew tap databricks/tap
brew install databricks

# Or using pip
pip install databricks-cli
```

### 2. Authenticate with Databricks

```bash
databricks auth login --host <your-workspace-url>
```

### 3. Clone and Configure

```bash
git clone <repository-url>
cd pipeline_integrity
```

### 4. Set Environment Variables (Required for Deployment)

The bundle uses `BUNDLE_VAR_*` environment variables. A template file (`.env.local`) and a loader script (`set_env.sh`) are provided:

```bash
# Copy the template and fill in your values
cp .env.local .env
```

Edit `.env` with your workspace-specific values:

```dotenv
BUNDLE_VAR_catalog_name=my_catalog
BUNDLE_VAR_schema_name=pipeline_integrity
BUNDLE_VAR_volume_path=/Volumes/my_catalog/pipeline_integrity/reports/
BUNDLE_VAR_export_volume_path=/Volumes/my_catalog/pipeline_integrity/exports/
BUNDLE_VAR_service_principal_name=
```

Then load the variables into your shell:

```bash
source set_env.sh
```

| Variable | Description | Example |
|----------|-------------|---------|
| `BUNDLE_VAR_catalog_name` | Unity Catalog name for storing tables | `my_catalog` |
| `BUNDLE_VAR_schema_name` | Schema name for storing tables | `pipeline_integrity` |
| `BUNDLE_VAR_volume_path` | Full path to the Databricks Volume with input files | `/Volumes/my_catalog/my_schema/reports/` |
| `BUNDLE_VAR_export_volume_path` | Full path to the Databricks Volume for Excel exports | `/Volumes/my_catalog/my_schema/exports/` |
| `BUNDLE_VAR_service_principal_name` | Service principal for prod runs (optional) | `""` |

> **Note**: You can also pass variables inline with `--var` flags if preferred.

### 5. Deploy and Run

```bash
# Load environment variables
source set_env.sh

# Validate and deploy the bundle
databricks bundle validate
databricks bundle deploy -t dev

# Run the ingestion job (processing job triggers automatically)
databricks bundle run ingestion_job -t dev

# The processing job can also be run independently if needed:
databricks bundle run processing_job -t dev
```

The extraction schema is defined in [`kie_schema.json`](kie_schema.json) and is automatically deployed with the bundle. The processing job's `ai_extract` function reads this schema at runtime to extract structured fields from the parsed file content.

## Configuration

### Variables

All configurable values are defined in `databricks.yml`. The deployment-time variables are loaded from your `.env` file via `source set_env.sh` (see [Quick Start](#4-set-environment-variables-required-for-deployment)). The remaining variables have defaults derived from the required ones:

| Variable | Description | Default |
|----------|-------------|---------|
| `catalog_name` | Unity Catalog name | **Required (deploy time)** |
| `schema_name` | Schema name | **Required (deploy time)** |
| `volume_path` | Path to input files volume | **Required (deploy time)** |
| `checkpoint_table` | Table tracking processed files | `<catalog>.<schema>.processed_files_checkpoint` |
| `parsed_table` | Table storing parsed content | `<catalog>.<schema>.parsed_files` |
| `ai_query_table` | Table storing AI analysis results | `<catalog>.<schema>.ai_query_results` |
| `kie_view_name` | View for typed KIE results | `<catalog>.<schema>.kie_results` |
| `export_volume_path` | Path to volume for Excel exports | `/Volumes/<catalog>/<schema>/exports/` |
| `service_principal_name` | Service principal for prod runs | `""` |

### Targets

- **dev**: Development environment (default)
- **prod**: Production environment with service principal

## Usage

### Running the Pipeline

Once deployed (see [Quick Start](#5-deploy-and-run)), the pipeline runs automatically:

```bash
# Upload files to the volume — the ingestion job triggers automatically
# on file arrival, parses new files, then chains the processing job.

# Or trigger the ingestion job manually via CLI:
databricks bundle run ingestion_job -t dev

# The processing job can also be run independently:
databricks bundle run processing_job -t dev
```

The ingestion job's `trigger_processing` task automatically launches the processing job after new files are parsed. Both jobs remain independently runnable for debugging or reprocessing.

### Updating the Extraction Schema

The extraction schema is defined in [`kie_schema.json`](kie_schema.json) and deployed with the bundle. To update it:

1. Edit `kie_schema.json` with the desired field definitions
2. Redeploy the bundle (`databricks bundle deploy -t dev`)

The processing job reads the schema at runtime, so changes take effect on the next run.

### Monitoring

Monitor pipeline execution through:

- **Databricks Jobs UI**: View run history and logs
- **Unity Catalog Tables**: Query the tables directly
- **AI/BI Dashboard**: Visual analytics (see setup instructions below)
- **Genie Space**: Natural language queries

### Supported File Types

- **PDF Files** (`.pdf`): Parsed using Databricks `ai_parse` function
- **Excel Files** (`.xlsx`, `.xls, .xlsm`): Converted to Markdown format

## Tables Schema

### 1. Checkpoint Table (`processed_files_checkpoint`)

Tracks all processed files to prevent reprocessing.


| Column                | Type      | Description                |
| --------------------- | --------- | -------------------------- |
| `file_path`           | STRING    | Full path to the file (PK) |
| `file_name`           | STRING    | File name                  |
| `file_extension`      | STRING    | File extension             |
| `file_size_bytes`     | BIGINT    | File size                  |
| `processed_timestamp` | TIMESTAMP | When processed             |
| `processing_status`   | STRING    | success/failed             |
| `error_message`       | STRING    | Error details if failed    |


### 2. Parsed Files Table (`parsed_files`)

Stores extracted content from files.


| Column             | Type      | Description                |
| ------------------ | --------- | -------------------------- |
| `file_path`        | STRING    | Full path to the file (PK) |
| `file_name`        | STRING    | File name                  |
| `file_extension`   | STRING    | File extension             |
| `text`             | STRING    | Extracted text content     |
| `num_pages`        | INT       | Number of pages            |
| `metadata`         | STRING    | Additional metadata (JSON) |
| `parsed_timestamp` | TIMESTAMP | When parsed                |


### 3. AI Query Results Table (`ai_query_results`)

Stores AI analysis results.


| Column            | Type      | Description                |
| ----------------- | --------- | -------------------------- |
| `file_path`       | STRING    | Full path to the file (PK) |
| `file_name`       | STRING    | File name                  |
| `input_text`      | STRING    | Input text sent to AI      |
| `response`        | STRING    | AI response                |
| `error_message`   | STRING    | Error details if failed    |
| `query_timestamp` | TIMESTAMP | When queried               |


## Analytics Setup

### Creating a Genie Space

1. Navigate to **AI/BI** → **Genie Spaces** in your workspace
2. Click **Create Genie Space**
3. Configure:
  - **Name**: `Pipeline Integrity Analysis`
  - **Tables**: Add `ai_query_results`, `parsed_files`, and `processed_files_checkpoint`
4. Start asking natural language questions!

Example questions:

- "How many files have been processed?"
- "Show me recent AI query results"
- "What is the success rate?"

### Creating an AI/BI Dashboard

1. Navigate to **Dashboards** in your workspace
2. Click **Create Dashboard** → **AI/BI Dashboard**
3. Use the queries in `dashboards/dashboard_queries.sql` to create visualizations

Or run the helper notebook:

```bash
databricks workspace import src/setup_genie_and_dashboard.py
```

## Development

### Project Structure

```
paa_pipeline_integrity/
├── databricks.yml              # Bundle configuration (variables, targets)
├── .env.local                  # Environment variable template (copy to .env)
├── .gitignore
├── set_env.sh                  # Loads .env into the shell
├── kie_schema.json             # KIE JSON schema definition
├── resources/
│   ├── pipeline_job.yml        # Job definitions (ingestion + processing)
│   └── uc.yml                  # Unity Catalog resources (schema, volume)
├── src/
│   ├── _validators.py          # Shared input validation helpers
│   ├── setup_tables.py         # Creates UC tables
│   ├── parse_files.py          # File parsing logic (PDF + Excel)
│   ├── ai_query_processing.py  # AI extract processing via kie_schema.json
│   ├── create_kie_view.py      # Creates typed view from AI results
│   ├── export_to_excel.py      # Exports repairs data to Excel
│   └── setup_genie_and_dashboard.py  # Analytics setup helper
├── dashboards/
│   └── dashboard_queries.sql   # Dashboard query templates
└── README.md
```

### Local Development

To test notebooks locally:

```bash
# Export notebooks
databricks workspace export-dir src/ ./local_notebooks/

# Edit locally and import back
databricks workspace import-dir ./local_notebooks/ src/
```

### Testing

1. Create a test volume with sample files
2. Update `volume_path` variable
3. Run the pipeline
4. Verify results in the tables

## Troubleshooting

### Common Issues

**Issue**: Files not being detected

- Check that `volume_path` is correct
- Verify file extensions are `.pdf`, `.xlsx`, or `.xls`
- Check permissions on the volume

**Issue**: AI extract/parse failures

- Check that the `kie_schema.json` is valid JSON and deployed with the bundle
- Verify that Databricks Runtime supports `ai_extract` (15.4+)
- Review error messages in the `ai_query_results` table

**Issue**: XLSX parsing errors

- Ensure `openpyxl` library is installed (handled by notebook)
- Check file is not corrupted
- Verify file size is within limits

### Viewing Logs

```bash
# Get recent job runs
databricks jobs list-runs --job-id <job-id>

# View specific run logs
databricks jobs get-run-output --run-id <run-id>
```

## Maintenance

### Reprocessing Files

To reprocess specific files, delete their entries from the checkpoint table:

```sql
DELETE FROM <catalog>.<schema>.processed_files_checkpoint
WHERE file_path = '<path-to-file>';
```

### Table Optimization

Periodically optimize Delta tables:

```sql
OPTIMIZE <catalog>.<schema>.<table_name>;
VACUUM <catalog>.<schema>.<table_name> RETAIN 168 HOURS;
```

## Deployment Best Practices

1. **Use Service Principals in Production**: Configure `run_as` in the prod target
2. **Set Appropriate Permissions**: Grant necessary permissions to tables and volumes
3. **Monitor Costs**: AI endpoints and compute can incur costs
4. **Configure Alerts**: Set up job failure notifications
5. **Version Control**: Commit changes to Git before deploying

