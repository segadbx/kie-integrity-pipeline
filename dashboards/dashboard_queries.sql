-- ===================================================================
-- Pipeline Integrity Dashboard Queries
-- ===================================================================
-- These queries can be used to create an AI/BI (Lakeview) Dashboard
-- Replace the table names with your actual table names from variables
-- ===================================================================

-- -------------------------------------------------------------------
-- Query 1: Processing Summary
-- -------------------------------------------------------------------
-- Overview of total files, parsed files, and AI query results
SELECT
    'Total Files Processed' as metric,
    COUNT(DISTINCT file_path) as count,
    MAX(processed_timestamp) as last_updated
FROM ${catalog_name}.${schema_name}.processed_files_checkpoint
WHERE processing_status = 'success'

UNION ALL

SELECT
    'Files Parsed' as metric,
    COUNT(DISTINCT file_path) as count,
    MAX(parsed_timestamp) as last_updated
FROM ${catalog_name}.${schema_name}.parsed_files

UNION ALL

SELECT
    'AI Queries Executed' as metric,
    COUNT(DISTINCT file_path) as count,
    MAX(query_timestamp) as last_updated
FROM ${catalog_name}.${schema_name}.ai_query_results;

-- -------------------------------------------------------------------
-- Query 2: File Processing Status by Type
-- -------------------------------------------------------------------
SELECT
    file_extension,
    COUNT(*) as file_count,
    processing_status,
    MAX(processed_timestamp) as last_processed
FROM ${catalog_name}.${schema_name}.processed_files_checkpoint
GROUP BY file_extension, processing_status
ORDER BY file_extension, processing_status;

-- -------------------------------------------------------------------
-- Query 3: AI Query Success Rate
-- -------------------------------------------------------------------
SELECT
    CASE
        WHEN error_message IS NULL AND response IS NOT NULL THEN 'Success'
        WHEN error_message IS NOT NULL THEN 'Failed'
        ELSE 'Unknown'
    END as status,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM ${catalog_name}.${schema_name}.ai_query_results
GROUP BY
    CASE
        WHEN error_message IS NULL AND response IS NOT NULL THEN 'Success'
        WHEN error_message IS NOT NULL THEN 'Failed'
        ELSE 'Unknown'
    END;

-- -------------------------------------------------------------------
-- Query 4: Recent AI Query Results
-- -------------------------------------------------------------------
-- Shows the most recent AI query results
SELECT
    file_name,
    LEFT(response, 500) as response_preview,
    error_message,
    query_timestamp
FROM ${catalog_name}.${schema_name}.ai_query_results
ORDER BY query_timestamp DESC
LIMIT 50;

-- -------------------------------------------------------------------
-- Query 5: Processing Timeline
-- -------------------------------------------------------------------
-- Shows file processing over time
SELECT
    DATE(processed_timestamp) as processing_date,
    file_extension,
    COUNT(*) as files_processed
FROM ${catalog_name}.${schema_name}.processed_files_checkpoint
WHERE processing_status = 'success'
GROUP BY DATE(processed_timestamp), file_extension
ORDER BY processing_date DESC;

-- -------------------------------------------------------------------
-- Query 6: File Size Distribution
-- -------------------------------------------------------------------
SELECT
    file_extension,
    ROUND(AVG(file_size_bytes) / 1024 / 1024, 2) as avg_size_mb,
    ROUND(MIN(file_size_bytes) / 1024 / 1024, 2) as min_size_mb,
    ROUND(MAX(file_size_bytes) / 1024 / 1024, 2) as max_size_mb,
    COUNT(*) as file_count
FROM ${catalog_name}.${schema_name}.processed_files_checkpoint
GROUP BY file_extension
ORDER BY avg_size_mb DESC;

-- -------------------------------------------------------------------
-- Query 7: Failed Files Analysis
-- -------------------------------------------------------------------
-- Shows files that failed processing
SELECT
    file_name,
    file_extension,
    error_message,
    processed_timestamp
FROM ${catalog_name}.${schema_name}.processed_files_checkpoint
WHERE processing_status = 'failed'
ORDER BY processed_timestamp DESC;

-- -------------------------------------------------------------------
-- Query 8: AI Query Error Analysis
-- -------------------------------------------------------------------
-- Analyzes errors from AI queries
SELECT
    error_message,
    COUNT(*) as error_count,
    MAX(query_timestamp) as last_occurrence
FROM ${catalog_name}.${schema_name}.ai_query_results
WHERE error_message IS NOT NULL
GROUP BY error_message
ORDER BY error_count DESC;

-- -------------------------------------------------------------------
-- Query 9: Detailed Results View
-- -------------------------------------------------------------------
-- Comprehensive view joining all tables
SELECT
    p.file_name,
    p.file_extension,
    p.num_pages,
    p.parsed_timestamp,
    CASE
        WHEN aq.error_message IS NULL AND aq.response IS NOT NULL THEN 'Success'
        WHEN aq.error_message IS NOT NULL THEN 'Failed'
        ELSE 'Not Processed'
    END as ai_query_status,
    LEFT(aq.response, 200) as response_preview,
    aq.query_timestamp
FROM ${catalog_name}.${schema_name}.parsed_files p
LEFT JOIN ${catalog_name}.${schema_name}.ai_query_results aq
    ON p.file_path = aq.file_path
ORDER BY p.parsed_timestamp DESC;

-- -------------------------------------------------------------------
-- Query 10: Pipeline Health Summary
-- -------------------------------------------------------------------
-- Overall pipeline health metrics
WITH stats AS (
    SELECT
        COUNT(DISTINCT c.file_path) as total_files,
        COUNT(DISTINCT p.file_path) as parsed_files,
        COUNT(DISTINCT aq.file_path) as ai_queried_files,
        COUNT(DISTINCT CASE WHEN aq.error_message IS NULL AND aq.response IS NOT NULL THEN aq.file_path END) as successful_ai_queries
    FROM ${catalog_name}.${schema_name}.processed_files_checkpoint c
    LEFT JOIN ${catalog_name}.${schema_name}.parsed_files p ON c.file_path = p.file_path
    LEFT JOIN ${catalog_name}.${schema_name}.ai_query_results aq ON p.file_path = aq.file_path
    WHERE c.processing_status = 'success'
)
SELECT
    total_files,
    parsed_files,
    ROUND(parsed_files * 100.0 / NULLIF(total_files, 0), 2) as parsing_success_rate,
    ai_queried_files,
    successful_ai_queries,
    ROUND(successful_ai_queries * 100.0 / NULLIF(ai_queried_files, 0), 2) as ai_success_rate
FROM stats;
