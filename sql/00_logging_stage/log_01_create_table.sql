-- ============================================
-- LOGGING TABLE - CREATE
-- Purpose: Store validation and error check results
-- Location: sql/logging/log_01_create_table.sql
-- ============================================

-- Drop existing table if needed (for clean setup)
-- DROP TABLE IF EXISTS log_validation_results CASCADE;

-- ============================================
-- TABLE: log_validation_results
-- Purpose: Store all validation and error check results
-- ============================================

CREATE TABLE IF NOT EXISTS log_validation_results (
    -- Primary key
    log_id BIGSERIAL PRIMARY KEY,
    
    -- Validation metadata
    validation_run_id VARCHAR(100) NOT NULL,
    validation_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- What was validated
    stage VARCHAR(50) NOT NULL,              -- 'SRC', 'STG', 'CURATED', etc.
    table_name VARCHAR(100) NOT NULL,        -- Table being validated
    check_name VARCHAR(200) NOT NULL,        -- Name of the validation check
    check_type VARCHAR(50) NOT NULL,         -- 'ERROR' or 'WARNING'
    
    -- Validation results
    check_status VARCHAR(20) NOT NULL,       -- 'PASSED' or 'FAILED'
    records_checked INTEGER,                 -- How many records were checked
    records_failed INTEGER,                  -- How many failed the check
    failure_percentage NUMERIC(5,2),         -- Percentage that failed
    
    -- Details
    error_message TEXT,                      -- Description of the issue
    sample_failed_ids TEXT,                  -- Example IDs that failed (comma-separated)
    
    -- SQL used for the check (for documentation)
    validation_query TEXT
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_log_validation_run 
    ON log_validation_results(validation_run_id);

CREATE INDEX IF NOT EXISTS idx_log_validation_timestamp 
    ON log_validation_results(validation_timestamp);

CREATE INDEX IF NOT EXISTS idx_log_validation_stage 
    ON log_validation_results(stage);

CREATE INDEX IF NOT EXISTS idx_log_validation_status 
    ON log_validation_results(check_status);

CREATE INDEX IF NOT EXISTS idx_log_validation_type 
    ON log_validation_results(check_type);

-- ============================================
-- COMMENTS FOR DOCUMENTATION
-- ============================================

COMMENT ON TABLE log_validation_results IS 'Stores results of all validation and error checks across all ETL stages';
COMMENT ON COLUMN log_validation_results.validation_run_id IS 'Unique identifier for each validation run (format: VAL_YYYYMMDD_HHMMSS)';
COMMENT ON COLUMN log_validation_results.check_type IS 'ERROR = will break ETL, WARNING = data quality issue';
COMMENT ON COLUMN log_validation_results.check_status IS 'PASSED = check passed, FAILED = issues found';
COMMENT ON COLUMN log_validation_results.sample_failed_ids IS 'First 10 record IDs that failed, comma-separated';

-- ============================================
-- HELPER VIEW: Latest Validation Summary
-- ============================================

CREATE OR REPLACE VIEW v_latest_validation_summary AS
WITH latest_run AS (
    SELECT MAX(validation_run_id) as run_id
    FROM log_validation_results
)
SELECT 
    stage,
    table_name,
    COUNT(*) as total_checks,
    COUNT(*) FILTER (WHERE check_status = 'PASSED') as checks_passed,
    COUNT(*) FILTER (WHERE check_status = 'FAILED') as checks_failed,
    COUNT(*) FILTER (WHERE check_type = 'ERROR' AND check_status = 'FAILED') as errors,
    COUNT(*) FILTER (WHERE check_type = 'WARNING' AND check_status = 'FAILED') as warnings,
    ROUND(100.0 * COUNT(*) FILTER (WHERE check_status = 'PASSED') / COUNT(*), 2) as pass_rate_pct
FROM log_validation_results
WHERE validation_run_id = (SELECT run_id FROM latest_run)
GROUP BY stage, table_name
ORDER BY stage, table_name;

COMMENT ON VIEW v_latest_validation_summary IS 'Summary of most recent validation run';

-- ============================================
-- VERIFICATION
-- ============================================

-- Verify table created
SELECT 
    table_name,
    column_name,
    data_type,
    is_nullable
FROM information_schema.columns
WHERE table_name = 'log_validation_results'
ORDER BY ordinal_position;