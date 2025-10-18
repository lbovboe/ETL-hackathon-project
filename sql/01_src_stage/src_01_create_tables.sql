-- ============================================
-- STAGE 1: SRC (SOURCE) - CREATE TABLES
-- Purpose: Raw landing zone for data from .parquet files
-- ============================================

-- ============================================
-- TABLE: src_daily_spending
-- Purpose: Raw daily spending data from parquet files
-- All columns are TEXT to preserve original format
-- ============================================

CREATE TABLE IF NOT EXISTS src_daily_spending (
    -- Primary key
    src_id BIGSERIAL PRIMARY KEY,
    
    -- Raw data columns (as-is from parquet)
    person_name TEXT,
    spending_date TEXT,
    category TEXT,
    amount TEXT,
    location TEXT,
    description TEXT,
    payment_method TEXT,
    
    -- ETL metadata for audit trail
    source_file VARCHAR(255) NOT NULL,
    load_batch_id VARCHAR(100) NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for frequently queried columns
CREATE INDEX IF NOT EXISTS idx_src_spending_batch 
    ON src_daily_spending(load_batch_id);

CREATE INDEX IF NOT EXISTS idx_src_spending_loaded_at 
    ON src_daily_spending(loaded_at);

CREATE INDEX IF NOT EXISTS idx_src_spending_source_file 
    ON src_daily_spending(source_file);

-- ============================================
-- COMMENTS FOR DOCUMENTATION
-- ============================================

COMMENT ON TABLE src_daily_spending IS 'Raw landing zone for daily spending data from parquet files. All data stored as TEXT to preserve original format.';
COMMENT ON COLUMN src_daily_spending.src_id IS 'Auto-generated primary key';
COMMENT ON COLUMN src_daily_spending.source_file IS 'Name of the parquet file this data came from';
COMMENT ON COLUMN src_daily_spending.load_batch_id IS 'Batch identifier for grouping records from same ETL run';
COMMENT ON COLUMN src_daily_spending.loaded_at IS 'Timestamp when data was loaded into database';