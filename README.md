# Pipeline Integrity

A Databricks Asset Bundle (DAB) for processing and analyzing files from a Databricks Volume using AI-powered parsing and querying capabilities.

## Overview

This pipeline automates the ingestion, parsing, and AI-powered analysis of PDF and XLSX/XLSM files stored in Databricks Volume. It provides:

- **Automated File Processing**: Monitors a Databricks Volume for new files
- **Smart Parsing**: Extracts content from PDFs (using `ai_parse`) and XLSX/XLSM files (converted to Markfown)
- **AI-Powered Analysis**: Processes extracted content through Databricks AI endpoints
- **Data Tracking**: Maintains checkpoints to avoid reprocessing files
- **Analytics Ready**: Outputs structured data ready for Genie Spaces and AI/BI Dashboards

## Architecture

The solution follows a **3-stage workflow** running on **Databricks Serverless Compute**:

**Stage 1 — One-Time Setup** (manual, via Databricks UI):
1. **Create Agent Bricks KIE Endpoint**: Create an Agent Bricks Information Extraction endpoint, pointing it at the `parsed_files` table's `text` column as the data source. This produces a KIE serving endpoint name needed by the processing job.

**Stage 2 — Ingestion Job** (`ingestion_job`):
2. **Setup Tables**: Creates the necessary Unity Catalog tables and schema
3. **Parse Files**: Scans the volume, identifies new files, and extracts content
4. **Trigger Processing**: Automatically chains the processing job (see below)

**Stage 3 — Processing Job** (`processing_job`):
5. **AI Query Processing**: Analyzes parsed content using the KIE endpoint from Stage 1
6. **Create KIE View**: Creates a typed view from AI results
7. **Export to Excel**: Exports repairs data to Excel

The ingestion job automatically triggers the processing job via a `run_job_task` after new files are parsed. This means the full pipeline — from file arrival to Excel export — runs end-to-end without manual intervention once the KIE endpoint is set up.

All automated tasks use serverless compute for automatic scaling, fast startup, and cost optimization.

### Data Flow

```
Stage 1 — One-Time Setup (manual):
  Parsed Table (text column) → Databricks UI → KIE Serving Endpoint

Stage 2 — Ingestion Job (file arrival trigger):
  Databricks Volume → Setup Tables → Parse Files → Parsed Table
                                          ↓               ↓
                                   Checkpoint Table   Triggers Processing Job

Stage 3 — Processing Job (auto-triggered by ingestion):
  Parsed Table → AI Query (KIE endpoint) → Results Table → KIE View → Excel Export
                                                               ↓
                                                      Genie Space/Dashboard
```

## Prerequisites

- Databricks Workspace with Unity Catalog enabled
- **Databricks Serverless Compute enabled**
- Databricks Runtime 15.4+ (for AI functions)
- Access to the AI endpoint
- A Databricks Volume for input files
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
BUNDLE_VAR_kie_endpoint_name=kie-extraction
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
| `BUNDLE_VAR_kie_endpoint_name` | KIE serving endpoint name (from Stage 1 setup) | `kie-extraction` |
| `BUNDLE_VAR_service_principal_name` | Service principal for prod runs (optional) | `""` |

> **Note**: `kie_endpoint_name` must be set before deployment so that the processing job can reference it when auto-triggered by the ingestion job. You can also pass variables inline with `--var` flags if preferred.

### 5. Deploy and Run

```bash
# Stage 1 (one-time): Create an Agent Bricks KIE endpoint via the Databricks UI
#   1. Navigate to Machine Learning → Serving in the Databricks UI
#   2. Create a new Agent Bricks Information Extraction endpoint
#   3. Point it at the `parsed_files` table, `text` column as the data source
#   4. Note the endpoint name and set it in your .env file

# Load environment variables (if not already loaded)
source set_env.sh

# Validate and deploy the bundle
databricks bundle validate
databricks bundle deploy -t dev

# Stage 2: Run the ingestion job (processing job triggers automatically)
databricks bundle run ingestion_job -t dev

# The ingestion job will automatically trigger the processing job after
# new files are parsed. No need to run processing_job manually.

# The processing job can still be run independently if needed:
databricks bundle run processing_job -t dev
```

## Configuration

### Variables

All configurable values are defined in `databricks.yml`. The deployment-time variables are loaded from your `.env` file via `source set_env.sh` (see [Quick Start](#4-set-environment-variables-required-for-deployment)). The remaining variables have defaults derived from the required ones:

| Variable | Description | Default |
|----------|-------------|---------|
| `catalog_name` | Unity Catalog name | **Required (deploy time)** |
| `schema_name` | Schema name | **Required (deploy time)** |
| `volume_path` | Path to input files volume | **Required (deploy time)** |
| `kie_endpoint_name` | KIE AI endpoint name | **Required (deploy time)** — set via `BUNDLE_VAR_kie_endpoint_name` |
| `checkpoint_table` | Table tracking processed files | `<catalog>.<schema>.processed_files_checkpoint` |
| `parsed_table` | Table storing parsed content | `<catalog>.<schema>.parsed_files` |
| `ai_query_table` | Table storing AI analysis results | `<catalog>.<schema>.ai_query_results` |
| `kie_view_name` | View for typed KIE results | `<catalog>.<schema>.kie_results` |
| `service_principal_name` | Service principal for prod runs | `""` |

### Targets

- **dev**: Development environment (default)
- **prod**: Production environment with service principal

## Usage

### Running the Pipeline

The pipeline runs automatically once deployed:

**One-Time Setup — Create an Agent Bricks KIE Endpoint**

1. Open the Databricks UI
2. Navigate to **Machine Learning** → **Serving**
3. Create a new **Agent Bricks Information Extraction** endpoint
4. Configure the data source: select the `parsed_files` table with the `text` column
5. Deploy the endpoint and set `BUNDLE_VAR_kie_endpoint_name` to its name in your `.env` file

**Automated Flow — Ingestion Triggers Processing**

```bash
# Upload files to the volume — the ingestion job triggers automatically
# on file arrival, parses new files, then chains the processing job.

# Or trigger the ingestion job manually via CLI:
databricks bundle run ingestion_job -t dev

# The processing job can also be run independently:
databricks bundle run processing_job -t dev
```

The ingestion job's `trigger_processing` task automatically launches the processing job after new files are parsed. Both jobs remain independently runnable for debugging or reprocessing.

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
│   ├── ai_query_processing.py  # AI query processing via KIE endpoint
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

**Issue**: AI parse/query failures

- Verify AI endpoint is accessible
- Check endpoint name is correct
- Review error messages in checkpoint/results tables

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

