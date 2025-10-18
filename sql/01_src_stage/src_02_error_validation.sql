-- ============================================
-- STAGE 1: SRC - ERROR & VALIDATION CHECKS
-- Purpose: Validate data quality AFTER loading
-- Location: sql/stage_1_src/src_02_error_validation.sql
-- Run: AFTER data is loaded to src_daily_spending
-- Usage: Executed by scripts/run_validation.py
-- ============================================

-- ============================================
-- PREREQUISITE: Data must exist in src_daily_spending
-- This SQL will fail if table is empty!
-- ============================================

-- ============================================
-- STEP 1: TRUNCATE PREVIOUS RESULTS
-- ============================================

TRUNCATE TABLE log_validation_results;

-- ============================================
-- STEP 2: ERROR CHECKS (Critical - Must Fix)
-- These MUST be fixed before proceeding to Stage 2
-- ============================================

-- ERROR CHECK 1: NULL in Required Fields
INSERT INTO log_validation_results (
    validation_run_id,
    validation_timestamp,
    stage,
    table_name,
    check_name,
    check_type,
    check_status,
    records_checked,
    records_failed,
    failure_percentage,
    error_message,
    sample_failed_ids
)
SELECT 
    :validation_run_id,
    CURRENT_TIMESTAMP,
    'SRC',
    'src_daily_spending',
    'NULL_CHECK_REQUIRED_FIELDS',
    'ERROR',
    CASE WHEN COUNT(*) FILTER (WHERE person_name IS NULL OR spending_date IS NULL OR category IS NULL OR amount IS NULL) = 0 
         THEN 'PASSED' ELSE 'FAILED' END,
    COUNT(*),
    COUNT(*) FILTER (WHERE person_name IS NULL OR spending_date IS NULL OR category IS NULL OR amount IS NULL),
    ROUND(100.0 * COUNT(*) FILTER (WHERE person_name IS NULL OR spending_date IS NULL OR category IS NULL OR amount IS NULL) / NULLIF(COUNT(*), 0), 2),
    'Critical fields (person_name, spending_date, category, amount) cannot be NULL',
    (SELECT STRING_AGG(src_id::TEXT, ', ') FROM (
        SELECT src_id FROM src_daily_spending 
        WHERE person_name IS NULL OR spending_date IS NULL OR category IS NULL OR amount IS NULL 
        LIMIT 10
    ) sub)
FROM src_daily_spending;

-- ERROR CHECK 2: Empty Strings in Required Fields
INSERT INTO log_validation_results (
    validation_run_id,
    validation_timestamp,
    stage,
    table_name,
    check_name,
    check_type,
    check_status,
    records_checked,
    records_failed,
    failure_percentage,
    error_message,
    sample_failed_ids
)
SELECT 
    :validation_run_id,
    CURRENT_TIMESTAMP,
    'SRC',
    'src_daily_spending',
    'EMPTY_STRING_CHECK',
    'ERROR',
    CASE WHEN COUNT(*) FILTER (WHERE TRIM(person_name) = '' OR TRIM(spending_date) = '' OR TRIM(category) = '' OR TRIM(amount) = '') = 0 
         THEN 'PASSED' ELSE 'FAILED' END,
    COUNT(*),
    COUNT(*) FILTER (WHERE TRIM(person_name) = '' OR TRIM(spending_date) = '' OR TRIM(category) = '' OR TRIM(amount) = ''),
    ROUND(100.0 * COUNT(*) FILTER (WHERE TRIM(person_name) = '' OR TRIM(spending_date) = '' OR TRIM(category) = '' OR TRIM(amount) = '') / NULLIF(COUNT(*), 0), 2),
    'Required fields cannot be empty strings or whitespace',
    (SELECT STRING_AGG(src_id::TEXT, ', ') FROM (
        SELECT src_id FROM src_daily_spending 
        WHERE TRIM(person_name) = '' OR TRIM(spending_date) = '' OR TRIM(category) = '' OR TRIM(amount) = ''
        LIMIT 10
    ) sub)
FROM src_daily_spending
WHERE person_name IS NOT NULL;

-- ERROR CHECK 3: Duplicate Records
INSERT INTO log_validation_results (
    validation_run_id,
    validation_timestamp,
    stage,
    table_name,
    check_name,
    check_type,
    check_status,
    records_checked,
    records_failed,
    failure_percentage,
    error_message,
    sample_failed_ids
)
WITH duplicates AS (
    SELECT 
        person_name,
        spending_date,
        category,
        amount,
        location,
        COUNT(*) as dup_count,
        STRING_AGG(src_id::TEXT, ', ') as duplicate_ids
    FROM src_daily_spending
    GROUP BY person_name, spending_date, category, amount, location
    HAVING COUNT(*) > 1
),
total AS (
    SELECT COUNT(*) as total_records FROM src_daily_spending
)
SELECT 
    :validation_run_id,
    CURRENT_TIMESTAMP,
    'SRC',
    'src_daily_spending',
    'DUPLICATE_RECORDS_CHECK',
    'ERROR',
    CASE WHEN COUNT(*) = 0 THEN 'PASSED' ELSE 'FAILED' END,
    (SELECT total_records FROM total),
    COALESCE(SUM(dup_count), 0),
    ROUND(100.0 * COALESCE(SUM(dup_count), 0) / NULLIF((SELECT total_records FROM total), 0), 2),
    'Duplicate records found based on (person_name, spending_date, category, amount, location)',
    (SELECT STRING_AGG(SUBSTRING(duplicate_ids, 1, 50), '; ') FROM (SELECT duplicate_ids FROM duplicates LIMIT 3) sub)
FROM duplicates;

-- ERROR CHECK 4: Missing Metadata (ETL Tracking Fields)
INSERT INTO log_validation_results (
    validation_run_id,
    validation_timestamp,
    stage,
    table_name,
    check_name,
    check_type,
    check_status,
    records_checked,
    records_failed,
    failure_percentage,
    error_message,
    sample_failed_ids
)
SELECT 
    :validation_run_id,
    CURRENT_TIMESTAMP,
    'SRC',
    'src_daily_spending',
    'METADATA_COMPLETENESS_CHECK',
    'ERROR',
    CASE WHEN COUNT(*) FILTER (WHERE source_file IS NULL OR load_batch_id IS NULL) = 0 
         THEN 'PASSED' ELSE 'FAILED' END,
    COUNT(*),
    COUNT(*) FILTER (WHERE source_file IS NULL OR load_batch_id IS NULL),
    ROUND(100.0 * COUNT(*) FILTER (WHERE source_file IS NULL OR load_batch_id IS NULL) / NULLIF(COUNT(*), 0), 2),
    'ETL metadata (source_file, load_batch_id) must not be NULL',
    (SELECT STRING_AGG(src_id::TEXT, ', ') FROM (
        SELECT src_id FROM src_daily_spending 
        WHERE source_file IS NULL OR load_batch_id IS NULL
        LIMIT 10
    ) sub)
FROM src_daily_spending;

-- ============================================
-- STEP 3: WARNING CHECKS (Non-Critical)
-- These are data quality issues that should be reviewed
-- but don't prevent proceeding to Stage 2
-- ============================================

-- WARNING CHECK 1: Unusual Amount Format
INSERT INTO log_validation_results (
    validation_run_id,
    validation_timestamp,
    stage,
    table_name,
    check_name,
    check_type,
    check_status,
    records_checked,
    records_failed,
    failure_percentage,
    error_message,
    sample_failed_ids
)
SELECT 
    :validation_run_id,
    CURRENT_TIMESTAMP,
    'SRC',
    'src_daily_spending',
    'AMOUNT_FORMAT_CHECK',
    'WARNING',
    CASE WHEN COUNT(*) FILTER (WHERE amount !~ '^[0-9.,$ SGD]+$') = 0 
         THEN 'PASSED' ELSE 'FAILED' END,
    COUNT(*),
    COUNT(*) FILTER (WHERE amount !~ '^[0-9.,$ SGD]+$'),
    ROUND(100.0 * COUNT(*) FILTER (WHERE amount !~ '^[0-9.,$ SGD]+$') / NULLIF(COUNT(*), 0), 2),
    'Amount field contains unusual characters or format (expected: numbers, dots, commas, $, SGD)',
    (SELECT STRING_AGG(src_id::TEXT || ':' || amount, ', ') FROM (
        SELECT src_id, amount FROM src_daily_spending 
        WHERE amount !~ '^[0-9.,$ SGD]+$'
        LIMIT 10
    ) sub)
FROM src_daily_spending
WHERE amount IS NOT NULL;

-- WARNING CHECK 2: Missing Optional Fields
INSERT INTO log_validation_results (
    validation_run_id,
    validation_timestamp,
    stage,
    table_name,
    check_name,
    check_type,
    check_status,
    records_checked,
    records_failed,
    failure_percentage,
    error_message,
    sample_failed_ids
)
SELECT 
    :validation_run_id,
    CURRENT_TIMESTAMP,
    'SRC',
    'src_daily_spending',
    'OPTIONAL_FIELDS_COMPLETENESS',
    'WARNING',
    CASE WHEN COUNT(*) FILTER (WHERE location IS NULL OR description IS NULL OR payment_method IS NULL) = 0 
         THEN 'PASSED' ELSE 'FAILED' END,
    COUNT(*),
    COUNT(*) FILTER (WHERE location IS NULL OR description IS NULL OR payment_method IS NULL),
    ROUND(100.0 * COUNT(*) FILTER (WHERE location IS NULL OR description IS NULL OR payment_method IS NULL) / NULLIF(COUNT(*), 0), 2),
    'Optional fields (location, description, payment_method) have missing values',
    (SELECT STRING_AGG(src_id::TEXT, ', ') FROM (
        SELECT src_id FROM src_daily_spending 
        WHERE location IS NULL OR description IS NULL OR payment_method IS NULL
        LIMIT 10
    ) sub)
FROM src_daily_spending;

-- WARNING CHECK 3: Data Freshness
INSERT INTO log_validation_results (
    validation_run_id,
    validation_timestamp,
    stage,
    table_name,
    check_name,
    check_type,
    check_status,
    records_checked,
    records_failed,
    failure_percentage,
    error_message,
    sample_failed_ids
)
WITH freshness AS (
    SELECT 
        MAX(loaded_at) as latest_load,
        EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MAX(loaded_at))) / 3600 as hours_since_load
    FROM src_daily_spending
)
SELECT 
    :validation_run_id,
    CURRENT_TIMESTAMP,
    'SRC',
    'src_daily_spending',
    'DATA_FRESHNESS_CHECK',
    'WARNING',
    CASE WHEN hours_since_load < 48 THEN 'PASSED' ELSE 'FAILED' END,
    1,
    CASE WHEN hours_since_load >= 48 THEN 1 ELSE 0 END,
    CASE WHEN hours_since_load >= 48 THEN 100 ELSE 0 END,
    'Data is older than 48 hours. Last load: ' || TO_CHAR(latest_load, 'YYYY-MM-DD HH24:MI:SS') || ' (' || ROUND(hours_since_load::NUMERIC, 1) || ' hours ago)',
    NULL
FROM freshness;

-- ============================================
-- STEP 4: SUMMARY QUERIES (For Reference)
-- These are run by the Python script, not here
-- ============================================

-- Summary by check type
/*
SELECT 
    check_type,
    COUNT(*) as total_checks,
    COUNT(*) FILTER (WHERE check_status = 'PASSED') as passed,
    COUNT(*) FILTER (WHERE check_status = 'FAILED') as failed,
    ROUND(100.0 * COUNT(*) FILTER (WHERE check_status = 'PASSED') / COUNT(*), 2) as pass_rate_pct
FROM log_validation_results
WHERE validation_run_id = :validation_run_id
GROUP BY check_type
ORDER BY check_type;
*/

-- Failed checks detail
/*
SELECT 
    check_name,
    check_type,
    records_checked,
    records_failed,
    failure_percentage,
    error_message,
    sample_failed_ids
FROM log_validation_results
WHERE validation_run_id = :validation_run_id
AND check_status = 'FAILED'
ORDER BY 
    CASE WHEN check_type = 'ERROR' THEN 1 ELSE 2 END,
    failure_percentage DESC;
*/

-- ============================================
-- END OF VALIDATION CHECKS
-- Total checks: 7 (4 ERRORS + 3 WARNINGS)
-- ============================================