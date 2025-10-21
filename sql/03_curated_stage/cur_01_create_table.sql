-- ============================================================================
-- CURATED STAGE - TABLE CREATION
-- ============================================================================
-- Purpose: Create table for versioned snapshots of ALL historical spending data
-- Each version contains complete copy of all data from STG at that point in time
-- Running on Oct 20 → Version 1 with ALL data up to Oct 20
-- Running on Oct 21 → Version 2 with ALL data up to Oct 21
-- Latest version has is_latest = 1, all previous versions have is_latest = 0
-- ============================================================================

-- Drop table if exists (for clean recreation during development)
DROP TABLE IF EXISTS curated_spending_snapshots CASCADE;

-- Create the main curated table
CREATE TABLE curated_spending_snapshots (
    -- ========================================
    -- PRIMARY IDENTIFIER
    -- ========================================
    snapshot_id BIGSERIAL PRIMARY KEY,
    
    -- ========================================
    -- VERSION METADATA
    -- ========================================
    snapshot_version INTEGER NOT NULL,
    snapshot_date DATE NOT NULL,
    snapshot_batch_id VARCHAR(100) NOT NULL,
    is_latest SMALLINT NOT NULL DEFAULT 0 CHECK (is_latest IN (0, 1)),
    
    -- ========================================
    -- SOURCE TRACKING (Data Lineage)
    -- ========================================
    src_id INTEGER NOT NULL,
    stg_spending_id INTEGER NOT NULL,
    
    -- ========================================
    -- DIMENSION FOREIGN KEYS (for optional joins if needed)
    -- ========================================
    person_id INTEGER NOT NULL,
    category_id INTEGER NOT NULL,
    location_id INTEGER NOT NULL,
    payment_method_id INTEGER NOT NULL,
    
    -- ========================================
    -- DENORMALIZED DIMENSION VALUES (for fast queries without joins)
    -- ========================================
    person_name VARCHAR(255),
    category_name VARCHAR(100),
    category_group VARCHAR(100),
    location_name VARCHAR(255),
    location_type VARCHAR(50),
    payment_method_name VARCHAR(100),
    payment_type VARCHAR(50),
    
    -- ========================================
    -- TIME DIMENSIONS
    -- ========================================
    spending_date DATE NOT NULL,
    spending_year INTEGER NOT NULL,
    spending_month INTEGER NOT NULL,
    spending_quarter INTEGER NOT NULL,
    spending_day_of_week INTEGER NOT NULL,
    
    -- ========================================
    -- SPENDING DETAILS
    -- ========================================
    amount_cleaned NUMERIC(12,2) NOT NULL,
    currency_code VARCHAR(3),
    description TEXT,
    
    -- ========================================
    -- QUALITY METRICS
    -- ========================================
    data_quality_score INTEGER,
    
    -- ========================================
    -- AUDIT COLUMNS
    -- ========================================
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- ========================================
    -- CONSTRAINTS
    -- ========================================
    CONSTRAINT unique_version_spending UNIQUE (snapshot_version, stg_spending_id),
    CONSTRAINT check_amount_positive CHECK (amount_cleaned >= 0)
);

-- ============================================================================
-- INDEXES FOR PERFORMANCE
-- ============================================================================

-- Index on snapshot version (for querying specific versions)
CREATE INDEX idx_curated_snapshot_version 
ON curated_spending_snapshots(snapshot_version);

-- Index on is_latest (for quickly getting current version - most common query)
CREATE INDEX idx_curated_is_latest 
ON curated_spending_snapshots(is_latest) 
WHERE is_latest = 1;

-- Index on snapshot_date (for tracking when versions were created)
CREATE INDEX idx_curated_snapshot_date 
ON curated_spending_snapshots(snapshot_date);

-- Index on spending_date (for transaction date queries)
CREATE INDEX idx_curated_spending_date 
ON curated_spending_snapshots(spending_date);

-- Composite index for latest version date range queries
CREATE INDEX idx_curated_latest_spending 
ON curated_spending_snapshots(is_latest, spending_date) 
WHERE is_latest = 1;

-- Index on stg_spending_id for joins back to STG
CREATE INDEX idx_curated_stg_spending_id 
ON curated_spending_snapshots(stg_spending_id);

-- Index on person_name for filtering by person (denormalized queries)
CREATE INDEX idx_curated_person_name 
ON curated_spending_snapshots(person_name);

-- Index on category_name for category analysis (denormalized queries)
CREATE INDEX idx_curated_category_name 
ON curated_spending_snapshots(category_name);

-- ============================================================================
-- COMMENTS FOR DOCUMENTATION
-- ============================================================================

COMMENT ON TABLE curated_spending_snapshots IS 
'Versioned snapshots - each version contains complete copy of ALL historical spending data from STG at that point in time. Version 1 = all data up to first snapshot date, Version 2 = all data up to second snapshot date, etc.';

COMMENT ON COLUMN curated_spending_snapshots.snapshot_id IS 
'Unique identifier for each record in CURATED table (auto-incrementing)';

COMMENT ON COLUMN curated_spending_snapshots.snapshot_version IS 
'Sequential version number (1, 2, 3...) - increments with each new snapshot run';

COMMENT ON COLUMN curated_spending_snapshots.snapshot_date IS 
'Date when this snapshot version was created';

COMMENT ON COLUMN curated_spending_snapshots.snapshot_batch_id IS 
'ETL batch tracking ID for this snapshot creation run';

COMMENT ON COLUMN curated_spending_snapshots.is_latest IS 
'Flag indicating if this is the latest version: 1 = latest, 0 = historical. Only ONE version should have is_latest = 1 at any time';

COMMENT ON COLUMN curated_spending_snapshots.src_id IS 
'Link back to original SRC table for complete data lineage';

COMMENT ON COLUMN curated_spending_snapshots.stg_spending_id IS 
'Link back to STG fact table - used for version comparisons and data validation';

COMMENT ON COLUMN curated_spending_snapshots.person_id IS 
'Foreign key to stg_dim_person - kept for optional joins';

COMMENT ON COLUMN curated_spending_snapshots.category_id IS 
'Foreign key to stg_dim_category - kept for optional joins';

COMMENT ON COLUMN curated_spending_snapshots.person_name IS 
'Denormalized person name - stored at snapshot time for fast queries and historical accuracy';

COMMENT ON COLUMN curated_spending_snapshots.category_name IS 
'Denormalized category name - preserves exact category as it existed when snapshot was created';

COMMENT ON COLUMN curated_spending_snapshots.category_group IS 
'Denormalized category group - enables fast category rollup queries without joins';

COMMENT ON COLUMN curated_spending_snapshots.location_name IS 
'Denormalized location name - stored for fast geographic analysis';

COMMENT ON COLUMN curated_spending_snapshots.payment_method_name IS 
'Denormalized payment method name - enables fast payment analysis without joins';

COMMENT ON COLUMN curated_spending_snapshots.data_quality_score IS 
'Data quality score from STG layer (0-100) - helps track data quality improvements over time';

-- ============================================================================
-- VERIFICATION QUERY
-- ============================================================================

-- Verify table was created successfully
SELECT 
    table_name,
    column_name,
    data_type,
    character_maximum_length,
    is_nullable
FROM information_schema.columns
WHERE table_name = 'curated_spending_snapshots'
ORDER BY ordinal_position;

-- Check indexes
SELECT 
    indexname,
    indexdef
FROM pg_indexes
WHERE tablename = 'curated_spending_snapshots';

-- ============================================================================
-- EXPECTED OUTPUT
-- ============================================================================
-- Table should have:
-- - 28 columns total (hybrid: foreign keys + denormalized values)
-- - 8 indexes (optimized for version queries and denormalized lookups)
-- - Unique constraint on (snapshot_version, stg_spending_id)
-- - Check constraint on is_latest (0 or 1 only)
-- - Check constraint on amount_cleaned (>= 0)
--
-- Key Features:
-- 1. Version Management: snapshot_version tracks incremental versions (1, 2, 3...)
-- 2. Latest Flag: is_latest = 1 for current version, 0 for historical versions
-- 3. Hybrid Approach: Has BOTH foreign keys AND denormalized values
--    - Foreign keys (person_id, category_id, etc.) for optional joins
--    - Denormalized values (person_name, category_name, etc.) for fast queries
-- 4. Historical Accuracy: Denormalized values preserve exact state at snapshot time
-- 5. Data Lineage: Links back to both SRC (src_id) and STG (stg_spending_id)
-- 6. Time Travel: Can query any historical version to see data as it existed then
--
-- Example Query (NO JOINS NEEDED - Fast!):
-- SELECT person_name, category_name, SUM(amount_cleaned) as total
-- FROM curated_spending_snapshots
-- WHERE is_latest = 1
-- GROUP BY person_name, category_name
-- ORDER BY total DESC;
-- ============================================================================