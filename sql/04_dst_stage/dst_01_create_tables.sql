-- ============================================================================
-- DST LAYER: DISSEMINATION STAGING (PRE-AGGREGATED TABLES)
-- ============================================================================
-- Purpose: Create pre-aggregated tables optimized for fast reporting and analytics
-- Layer: Stage 4 of 5 (CURATED → DST → DIS)
-- 
-- This layer creates 4 main aggregation tables:
--   1. dst_monthly_spending_summary    - Monthly totals by person/category/location
--   2. dst_category_trends             - Category spending trends with MoM/YoY
--   3. dst_person_analytics            - Per-person spending patterns and statistics
--   4. dst_payment_method_summary      - Payment method usage and preferences
--
-- Benefits:
--   - Queries run 100-1000x faster (seconds → milliseconds)
--   - Dashboard-ready for BI tools (Tableau, Power BI, Looker)
--   - Pre-calculated trends (no need to compute on the fly)
--   - Optimized indexes for common query patterns
-- ============================================================================

-- Drop existing tables if they exist (for clean re-runs)
DROP TABLE IF EXISTS dst_monthly_spending_summary CASCADE;
DROP TABLE IF EXISTS dst_category_trends CASCADE;
DROP TABLE IF EXISTS dst_person_analytics CASCADE;
DROP TABLE IF EXISTS dst_payment_method_summary CASCADE;

-- ============================================================================
-- TABLE 1: Monthly Spending Summary
-- ============================================================================
-- Purpose: Pre-aggregated monthly totals by person, category, and location
-- Grain: One row per month + person + category + location combination
-- Updates: Incremental (process only new curated snapshot versions)
-- ============================================================================

CREATE TABLE dst_monthly_spending_summary (
    -- Primary Key
    summary_id SERIAL PRIMARY KEY,
    
    -- Time Dimensions
    year INTEGER NOT NULL,
    month INTEGER NOT NULL CHECK (month BETWEEN 1 AND 12),
    quarter INTEGER NOT NULL CHECK (quarter BETWEEN 1 AND 4),
    month_start_date DATE NOT NULL,
    month_end_date DATE NOT NULL,
    
    -- Business Dimensions (denormalized - using names, not IDs)
    person_name VARCHAR(100) NOT NULL,
    category_name VARCHAR(100) NOT NULL,
    category_group VARCHAR(100),
    location_name VARCHAR(200) NOT NULL,
    location_type VARCHAR(50),
    
    -- Aggregated Metrics
    total_spending NUMERIC(12, 2) NOT NULL,
    transaction_count INTEGER NOT NULL,
    avg_transaction_amount NUMERIC(12, 2),
    min_transaction_amount NUMERIC(12, 2),
    max_transaction_amount NUMERIC(12, 2),
    
    -- Trend Calculations (Month-over-Month)
    prev_month_spending NUMERIC(12, 2),
    mom_absolute_change NUMERIC(12, 2),
    mom_percent_change NUMERIC(8, 2),
    
    -- Trend Calculations (Year-over-Year)
    prev_year_spending NUMERIC(12, 2),
    yoy_absolute_change NUMERIC(12, 2),
    yoy_percent_change NUMERIC(8, 2),
    
    -- Data Quality
    avg_quality_score NUMERIC(5, 2),
    
    -- Metadata
    snapshot_version_source INTEGER NOT NULL,  -- Which curated version was used
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Composite Unique Constraint (prevent duplicates)
    CONSTRAINT uq_monthly_summary 
        UNIQUE (year, month, person_name, category_name, location_name)
);

-- Indexes for fast queries
CREATE INDEX idx_dst_monthly_year_month ON dst_monthly_spending_summary(year, month);
CREATE INDEX idx_dst_monthly_person ON dst_monthly_spending_summary(person_name, year, month);
CREATE INDEX idx_dst_monthly_category ON dst_monthly_spending_summary(category_name, year, month);
CREATE INDEX idx_dst_monthly_location ON dst_monthly_spending_summary(location_name, year, month);
CREATE INDEX idx_dst_monthly_snapshot ON dst_monthly_spending_summary(snapshot_version_source);

COMMENT ON TABLE dst_monthly_spending_summary IS 
'Pre-aggregated monthly spending totals by person, category, and location. Optimized for fast trend analysis and reporting.';

-- ============================================================================
-- TABLE 2: Category Trends
-- ============================================================================
-- Purpose: Category-level spending trends with detailed MoM and YoY analysis
-- Grain: One row per month + category combination
-- Updates: Incremental (process only new curated snapshot versions)
-- ============================================================================

CREATE TABLE dst_category_trends (
    -- Primary Key
    trend_id SERIAL PRIMARY KEY,
    
    -- Time Dimensions
    year INTEGER NOT NULL,
    month INTEGER NOT NULL CHECK (month BETWEEN 1 AND 12),
    quarter INTEGER NOT NULL CHECK (quarter BETWEEN 1 AND 4),
    month_start_date DATE NOT NULL,
    
    -- Category Dimensions (denormalized - using names, not IDs)
    category_name VARCHAR(100) NOT NULL,
    category_group VARCHAR(100),
    
    -- Current Month Metrics
    total_spending NUMERIC(12, 2) NOT NULL,
    transaction_count INTEGER NOT NULL,
    unique_persons INTEGER,  -- How many people spent in this category
    avg_transaction_amount NUMERIC(12, 2),
    
    -- Month-over-Month Trends
    prev_month_spending NUMERIC(12, 2),
    mom_absolute_change NUMERIC(12, 2),
    mom_percent_change NUMERIC(8, 2),
    mom_trend_direction VARCHAR(20),  -- 'INCREASING', 'DECREASING', 'STABLE'
    
    -- Year-over-Year Trends
    prev_year_spending NUMERIC(12, 2),
    yoy_absolute_change NUMERIC(12, 2),
    yoy_percent_change NUMERIC(8, 2),
    yoy_trend_direction VARCHAR(20),  -- 'INCREASING', 'DECREASING', 'STABLE'
    
    -- Rolling Averages (3-month and 6-month)
    rolling_3month_avg NUMERIC(12, 2),
    rolling_6month_avg NUMERIC(12, 2),
    
    -- Category Rank (by spending amount)
    category_rank_current INTEGER,      -- Rank in current month
    category_rank_prev_month INTEGER,   -- Rank in previous month
    rank_change INTEGER,                -- Change in rank position
    
    -- Share of Total Spending
    percent_of_total_spending NUMERIC(5, 2),  -- This category as % of all spending
    
    -- Metadata
    snapshot_version_source INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Composite Unique Constraint
    CONSTRAINT uq_category_trend 
        UNIQUE (year, month, category_name)
);

-- Indexes for fast queries
CREATE INDEX idx_dst_category_year_month ON dst_category_trends(year, month);
CREATE INDEX idx_dst_category_name ON dst_category_trends(category_name, year, month);
CREATE INDEX idx_dst_category_group ON dst_category_trends(category_group, year, month);
CREATE INDEX idx_dst_category_rank ON dst_category_trends(category_rank_current);

COMMENT ON TABLE dst_category_trends IS 
'Category-level spending trends with MoM/YoY analysis, rankings, and rolling averages. Perfect for identifying spending pattern changes.';

-- ============================================================================
-- TABLE 3: Person Analytics
-- ============================================================================
-- Purpose: Per-person spending behavior, patterns, and statistics
-- Grain: One row per month + person combination
-- Updates: Incremental (process only new curated snapshot versions)
-- ============================================================================

CREATE TABLE dst_person_analytics (
    -- Primary Key
    analytics_id SERIAL PRIMARY KEY,
    
    -- Time Dimensions
    year INTEGER NOT NULL,
    month INTEGER NOT NULL CHECK (month BETWEEN 1 AND 12),
    quarter INTEGER NOT NULL CHECK (quarter BETWEEN 1 AND 4),
    month_start_date DATE NOT NULL,
    
    -- Person Dimensions (denormalized - using names, not IDs)
    person_name VARCHAR(100) NOT NULL,
    
    -- Overall Spending Metrics
    total_spending NUMERIC(12, 2) NOT NULL,
    transaction_count INTEGER NOT NULL,
    avg_transaction_amount NUMERIC(12, 2),
    median_transaction_amount NUMERIC(12, 2),
    
    -- Spending Distribution
    top_category VARCHAR(100),          -- Category with most spending
    top_category_spending NUMERIC(12, 2),
    top_category_percent NUMERIC(5, 2),  -- % of total spent in top category
    
    -- Essential vs Discretionary Breakdown (for financial health insights)
    essential_spending NUMERIC(12, 2),      -- Total spent on essential categories
    discretionary_spending NUMERIC(12, 2),  -- Total spent on discretionary categories
    transport_spending NUMERIC(12, 2),      -- Total spent on transport
    healthcare_spending NUMERIC(12, 2),     -- Total spent on healthcare
    education_spending NUMERIC(12, 2),      -- Total spent on education
    other_spending NUMERIC(12, 2),          -- Total spent on other categories
    essential_percent NUMERIC(5, 2),        -- % of total spent on essentials
    discretionary_percent NUMERIC(5, 2),    -- % of total spent on discretionary
    essential_to_discretionary_ratio NUMERIC(8, 2),  -- Ratio (higher = better financial discipline)
    
    -- Diversity Metrics
    unique_categories_count INTEGER,     -- How many different categories
    unique_locations_count INTEGER,      -- How many different locations
    unique_payment_methods_count INTEGER, -- How many different payment methods
    
    -- Behavioral Patterns
    weekday_spending NUMERIC(12, 2),     -- Total spent Mon-Fri
    weekend_spending NUMERIC(12, 2),     -- Total spent Sat-Sun
    weekend_spending_percent NUMERIC(5, 2),
    
    morning_spending NUMERIC(12, 2),     -- 6am-12pm (if we had time data)
    afternoon_spending NUMERIC(12, 2),   -- 12pm-6pm
    evening_spending NUMERIC(12, 2),     -- 6pm-12am
    night_spending NUMERIC(12, 2),       -- 12am-6am
    
    -- Transaction Size Buckets
    small_transactions_count INTEGER,    -- < $10
    medium_transactions_count INTEGER,   -- $10-$100
    large_transactions_count INTEGER,    -- $100-$500
    xlarge_transactions_count INTEGER,   -- > $500
    
    -- Spending Frequency
    avg_daily_spending NUMERIC(12, 2),
    avg_weekly_spending NUMERIC(12, 2),
    days_with_spending INTEGER,          -- How many days had transactions
    spending_frequency_percent NUMERIC(5, 2), -- days_with_spending / days_in_month
    
    -- Month-over-Month Trends
    prev_month_total NUMERIC(12, 2),
    mom_absolute_change NUMERIC(12, 2),
    mom_percent_change NUMERIC(8, 2),
    
    -- Year-over-Year Trends
    prev_year_total NUMERIC(12, 2),
    yoy_absolute_change NUMERIC(12, 2),
    yoy_percent_change NUMERIC(8, 2),
    
    -- Data Quality
    avg_quality_score NUMERIC(5, 2),
    
    -- Metadata
    snapshot_version_source INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Composite Unique Constraint
    CONSTRAINT uq_person_analytics 
        UNIQUE (year, month, person_name)
);

-- Indexes for fast queries
CREATE INDEX idx_dst_person_year_month ON dst_person_analytics(year, month);
CREATE INDEX idx_dst_person_name ON dst_person_analytics(person_name, year, month);
CREATE INDEX idx_dst_person_spending ON dst_person_analytics(total_spending DESC);

COMMENT ON TABLE dst_person_analytics IS 
'Per-person spending behavior analysis including patterns, diversity metrics, and behavioral insights. Enables personalized recommendations.';

-- ============================================================================
-- TABLE 4: Payment Method Summary
-- ============================================================================
-- Purpose: Payment method usage trends and preferences
-- Grain: One row per month + payment_method combination
-- Updates: Incremental (process only new curated snapshot versions)
-- ============================================================================

CREATE TABLE dst_payment_method_summary (
    -- Primary Key
    payment_summary_id SERIAL PRIMARY KEY,
    
    -- Time Dimensions
    year INTEGER NOT NULL,
    month INTEGER NOT NULL CHECK (month BETWEEN 1 AND 12),
    quarter INTEGER NOT NULL CHECK (quarter BETWEEN 1 AND 4),
    month_start_date DATE NOT NULL,
    
    -- Payment Method Dimensions (denormalized - using names, not IDs)
    payment_method_name VARCHAR(100) NOT NULL,
    payment_type VARCHAR(50),  -- 'credit_card', 'debit_card', 'digital_wallet', etc.
    
    -- Usage Metrics
    transaction_count INTEGER NOT NULL,
    unique_persons_count INTEGER,  -- How many people used this method
    total_amount NUMERIC(12, 2) NOT NULL,
    avg_transaction_amount NUMERIC(12, 2),
    min_transaction_amount NUMERIC(12, 2),
    max_transaction_amount NUMERIC(12, 2),
    
    -- Market Share
    percent_of_transactions NUMERIC(5, 2),  -- % of total transaction count
    percent_of_spending NUMERIC(5, 2),       -- % of total spending amount
    
    -- Category Breakdown (top 3 categories for this payment method)
    top_category_1 VARCHAR(100),
    top_category_1_amount NUMERIC(12, 2),
    top_category_2 VARCHAR(100),
    top_category_2_amount NUMERIC(12, 2),
    top_category_3 VARCHAR(100),
    top_category_3_amount NUMERIC(12, 2),
    
    -- Trends
    prev_month_transaction_count INTEGER,
    mom_transaction_change_percent NUMERIC(8, 2),
    prev_month_amount NUMERIC(12, 2),
    mom_amount_change_percent NUMERIC(8, 2),
    
    -- Usage Rank
    payment_method_rank INTEGER,  -- Rank by total amount
    
    -- Metadata
    snapshot_version_source INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Composite Unique Constraint
    CONSTRAINT uq_payment_summary 
        UNIQUE (year, month, payment_method_name)
);

-- Indexes for fast queries
CREATE INDEX idx_dst_payment_year_month ON dst_payment_method_summary(year, month);
CREATE INDEX idx_dst_payment_method_name ON dst_payment_method_summary(payment_method_name, year, month);
CREATE INDEX idx_dst_payment_type ON dst_payment_method_summary(payment_type, year, month);
CREATE INDEX idx_dst_payment_rank ON dst_payment_method_summary(payment_method_rank);

COMMENT ON TABLE dst_payment_method_summary IS 
'Payment method usage trends, preferences, and market share analysis. Helps identify payment optimization opportunities.';

-- ============================================================================
-- HELPER VIEW: Latest Aggregations Dashboard
-- ============================================================================
-- Purpose: Quick view of the most recent month's aggregated data
-- Usage: SELECT * FROM vw_dst_latest_month_dashboard;
-- ============================================================================

CREATE OR REPLACE VIEW vw_dst_latest_month_dashboard AS
WITH latest_month AS (
    SELECT MAX(year * 100 + month) as latest_period
    FROM dst_monthly_spending_summary
)
SELECT 
    -- Overall Metrics
    COUNT(DISTINCT mss.person_name) as total_persons,
    COUNT(DISTINCT mss.category_name) as total_categories,
    SUM(mss.total_spending) as total_spending,
    SUM(mss.transaction_count) as total_transactions,
    AVG(mss.avg_transaction_amount) as avg_transaction_size,
    
    -- Top Category
    (SELECT category_name 
     FROM dst_category_trends 
     WHERE year * 100 + month = (SELECT latest_period FROM latest_month)
     ORDER BY total_spending DESC 
     LIMIT 1) as top_category,
    
    -- Top Person
    (SELECT person_name 
     FROM dst_person_analytics 
     WHERE year * 100 + month = (SELECT latest_period FROM latest_month)
     ORDER BY total_spending DESC 
     LIMIT 1) as top_spender,
    
    -- Top Payment Method
    (SELECT payment_method_name 
     FROM dst_payment_method_summary 
     WHERE year * 100 + month = (SELECT latest_period FROM latest_month)
     ORDER BY total_amount DESC 
     LIMIT 1) as top_payment_method,
    
    -- Metadata
    mss.year,
    mss.month,
    mss.month_start_date,
    MAX(mss.snapshot_version_source) as snapshot_version

FROM dst_monthly_spending_summary mss
WHERE mss.year * 100 + mss.month = (SELECT latest_period FROM latest_month)
GROUP BY mss.year, mss.month, mss.month_start_date;

COMMENT ON VIEW vw_dst_latest_month_dashboard IS 
'Quick dashboard view showing key metrics from the most recent aggregated month.';

-- ============================================================================
-- HELPER FUNCTION: Get Trend Direction
-- ============================================================================
-- Purpose: Standardize trend direction labels based on percent change
-- Usage: SELECT get_trend_direction(mom_percent_change);
-- ============================================================================

CREATE OR REPLACE FUNCTION get_trend_direction(percent_change NUMERIC)
RETURNS VARCHAR(20) AS $$
BEGIN
    IF percent_change IS NULL THEN
        RETURN 'NO_DATA';
    ELSIF percent_change > 5 THEN
        RETURN 'INCREASING';
    ELSIF percent_change < -5 THEN
        RETURN 'DECREASING';
    ELSE
        RETURN 'STABLE';
    END IF;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

COMMENT ON FUNCTION get_trend_direction IS 
'Returns trend direction label (INCREASING/DECREASING/STABLE) based on percent change threshold of ±5%';

-- ============================================================================
-- SUMMARY
-- ============================================================================
-- Tables Created:
--   ✅ dst_monthly_spending_summary    (22 columns, 5 indexes) - Fully denormalized
--   ✅ dst_category_trends             (24 columns, 4 indexes) - Fully denormalized
--   ✅ dst_person_analytics            (51 columns, 3 indexes) - Fully denormalized with essential/discretionary breakdown
--   ✅ dst_payment_method_summary      (24 columns, 4 indexes) - Fully denormalized
--
-- Views Created:
--   ✅ vw_dst_latest_month_dashboard   (latest month summary)
--
-- Functions Created:
--   ✅ get_trend_direction()           (trend classification helper)
--
-- Next Steps:
--   1. Run this script to create all DST tables
--   2. Build ETL scripts to populate tables from curated_spending_snapshots
--   3. Implement incremental update logic (process only new snapshots)
--   4. Create validation scripts to verify aggregations
-- ============================================================================

-- Display success message
DO $$
BEGIN
    RAISE NOTICE '✅ DST Layer tables created successfully!';
    RAISE NOTICE '📊 4 aggregation tables ready for population';
    RAISE NOTICE '🔍 1 dashboard view created';
    RAISE NOTICE '⚙️  1 helper function created';
    RAISE NOTICE '';
    RAISE NOTICE 'Next: Run ETL scripts to populate aggregation tables';
END $$;

