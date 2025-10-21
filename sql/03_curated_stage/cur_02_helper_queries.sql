-- ============================================================================
-- CURATED STAGE - HELPER QUERIES
-- ============================================================================
-- Purpose: Common analytical queries for the curated snapshot layer
-- These queries demonstrate how to leverage the versioned snapshot data
-- ============================================================================

-- ============================================================================
-- SECTION 1: BASIC SNAPSHOT QUERIES
-- ============================================================================

-- Query 1: Get Latest Snapshot (Most Common Query)
-- Returns: All records from the current/latest version
SELECT 
    person_name,
    category_name,
    spending_date,
    amount_cleaned,
    description
FROM curated_spending_snapshots
WHERE is_latest = 1
ORDER BY spending_date DESC
LIMIT 100;

-- Query 2: Get Specific Version (Time-Travel Query)
-- Returns: All records from a specific historical version
SELECT 
    person_name,
    category_name,
    spending_date,
    amount_cleaned
FROM curated_spending_snapshots
WHERE snapshot_version = 1  -- Change to desired version number
ORDER BY spending_date DESC
LIMIT 100;

-- Query 3: List All Available Versions
-- Returns: Summary of all snapshot versions
SELECT 
    snapshot_version,
    snapshot_date,
    is_latest,
    COUNT(*) as record_count,
    MIN(spending_date) as earliest_transaction,
    MAX(spending_date) as latest_transaction,
    SUM(amount_cleaned) as total_amount,
    ROUND(AVG(data_quality_score), 2) as avg_quality_score
FROM curated_spending_snapshots
GROUP BY snapshot_version, snapshot_date, is_latest
ORDER BY snapshot_version DESC;

-- ============================================================================
-- SECTION 2: SPENDING ANALYSIS (Latest Version)
-- ============================================================================

-- Query 4: Top 10 Spending Categories (Latest)
-- Returns: Highest spending categories from latest snapshot
SELECT 
    category_name,
    category_group,
    COUNT(*) as transaction_count,
    SUM(amount_cleaned) as total_spent,
    ROUND(AVG(amount_cleaned), 2) as avg_transaction,
    MIN(amount_cleaned) as min_spent,
    MAX(amount_cleaned) as max_spent
FROM curated_spending_snapshots
WHERE is_latest = 1
GROUP BY category_name, category_group
ORDER BY total_spent DESC
LIMIT 10;

-- Query 5: Spending by Person (Latest)
-- Returns: Total spending per person
SELECT 
    person_name,
    COUNT(*) as transaction_count,
    SUM(amount_cleaned) as total_spent,
    ROUND(AVG(amount_cleaned), 2) as avg_per_transaction,
    COUNT(DISTINCT category_name) as unique_categories,
    COUNT(DISTINCT payment_method_name) as payment_methods_used
FROM curated_spending_snapshots
WHERE is_latest = 1
GROUP BY person_name
ORDER BY total_spent DESC;

-- Query 6: Monthly Spending Trend (Latest)
-- Returns: Spending aggregated by month
SELECT 
    spending_year,
    spending_month,
    COUNT(*) as transaction_count,
    SUM(amount_cleaned) as total_spent,
    ROUND(AVG(amount_cleaned), 2) as avg_transaction,
    COUNT(DISTINCT person_name) as active_persons
FROM curated_spending_snapshots
WHERE is_latest = 1
GROUP BY spending_year, spending_month
ORDER BY spending_year DESC, spending_month DESC;

-- Query 7: Spending by Day of Week (Latest)
-- Returns: Average spending patterns by day
SELECT 
    spending_day_of_week,
    CASE spending_day_of_week
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
        WHEN 7 THEN 'Sunday'
    END as day_name,
    COUNT(*) as transaction_count,
    SUM(amount_cleaned) as total_spent,
    ROUND(AVG(amount_cleaned), 2) as avg_per_transaction
FROM curated_spending_snapshots
WHERE is_latest = 1
GROUP BY spending_day_of_week
ORDER BY spending_day_of_week;

-- Query 8: Top Locations by Spending (Latest)
-- Returns: Where people spend the most
SELECT 
    location_name,
    location_type,
    COUNT(*) as visit_count,
    SUM(amount_cleaned) as total_spent,
    ROUND(AVG(amount_cleaned), 2) as avg_per_visit
FROM curated_spending_snapshots
WHERE is_latest = 1
  AND location_name IS NOT NULL
GROUP BY location_name, location_type
ORDER BY total_spent DESC
LIMIT 20;

-- ============================================================================
-- SECTION 3: VERSION COMPARISON QUERIES
-- ============================================================================

-- Query 9: Compare Two Versions (Growth Analysis)
-- Returns: Record count and spending changes between versions
WITH version_summary AS (
    SELECT 
        snapshot_version,
        snapshot_date,
        COUNT(*) as record_count,
        SUM(amount_cleaned) as total_spending
    FROM curated_spending_snapshots
    GROUP BY snapshot_version, snapshot_date
)
SELECT 
    v1.snapshot_version as older_version,
    v1.snapshot_date as older_date,
    v1.record_count as older_records,
    v1.total_spending as older_spending,
    v2.snapshot_version as newer_version,
    v2.snapshot_date as newer_date,
    v2.record_count as newer_records,
    v2.total_spending as newer_spending,
    v2.record_count - v1.record_count as record_growth,
    ROUND(v2.total_spending - v1.total_spending, 2) as spending_growth,
    ROUND(((v2.record_count - v1.record_count)::NUMERIC / v1.record_count * 100), 2) as growth_pct
FROM version_summary v1
CROSS JOIN version_summary v2
WHERE v2.snapshot_version = v1.snapshot_version + 1
ORDER BY v1.snapshot_version DESC;

-- Query 10: Version-over-Version Growth
-- Returns: Sequential growth between all versions
WITH version_stats AS (
    SELECT 
        snapshot_version,
        snapshot_date,
        COUNT(*) as record_count,
        SUM(amount_cleaned) as total_spending,
        LAG(COUNT(*)) OVER (ORDER BY snapshot_version) as prev_count,
        LAG(SUM(amount_cleaned)) OVER (ORDER BY snapshot_version) as prev_spending
    FROM curated_spending_snapshots
    GROUP BY snapshot_version, snapshot_date
)
SELECT 
    snapshot_version,
    snapshot_date,
    record_count,
    ROUND(total_spending, 2) as total_spending,
    CASE 
        WHEN prev_count IS NULL THEN record_count
        ELSE record_count - prev_count
    END as record_growth,
    CASE 
        WHEN prev_spending IS NULL THEN ROUND(total_spending, 2)
        ELSE ROUND(total_spending - prev_spending, 2)
    END as spending_growth
FROM version_stats
ORDER BY snapshot_version DESC;

-- Query 11: New Transactions per Version
-- Returns: Transactions that appeared in each version (not in previous)
WITH numbered_versions AS (
    SELECT 
        snapshot_version,
        stg_spending_id,
        spending_date,
        amount_cleaned
    FROM curated_spending_snapshots
)
SELECT 
    v.snapshot_version,
    COUNT(*) as new_transaction_count,
    SUM(v.amount_cleaned) as new_spending_amount,
    MIN(v.spending_date) as earliest_new_transaction,
    MAX(v.spending_date) as latest_new_transaction
FROM numbered_versions v
LEFT JOIN numbered_versions prev ON 
    v.stg_spending_id = prev.stg_spending_id 
    AND prev.snapshot_version = v.snapshot_version - 1
WHERE prev.stg_spending_id IS NULL
GROUP BY v.snapshot_version
ORDER BY v.snapshot_version DESC;

-- ============================================================================
-- SECTION 4: ADVANCED ANALYTICS
-- ============================================================================

-- Query 12: Person-Category Matrix (Latest)
-- Returns: Spending breakdown per person per category
SELECT 
    person_name,
    category_name,
    COUNT(*) as transaction_count,
    SUM(amount_cleaned) as total_spent,
    ROUND(AVG(amount_cleaned), 2) as avg_per_transaction
FROM curated_spending_snapshots
WHERE is_latest = 1
GROUP BY person_name, category_name
ORDER BY person_name, total_spent DESC;

-- Query 13: Payment Method Preferences (Latest)
-- Returns: How people prefer to pay
SELECT 
    person_name,
    payment_method_name,
    payment_type,
    COUNT(*) as usage_count,
    SUM(amount_cleaned) as total_spent,
    ROUND(AVG(amount_cleaned), 2) as avg_transaction
FROM curated_spending_snapshots
WHERE is_latest = 1
GROUP BY person_name, payment_method_name, payment_type
ORDER BY person_name, usage_count DESC;

-- Query 14: High-Value Transactions (Latest)
-- Returns: Top 50 largest transactions
SELECT 
    person_name,
    spending_date,
    category_name,
    location_name,
    amount_cleaned,
    description,
    payment_method_name
FROM curated_spending_snapshots
WHERE is_latest = 1
ORDER BY amount_cleaned DESC
LIMIT 50;

-- Query 15: Quarterly Spending Summary (Latest)
-- Returns: Spending by quarter
SELECT 
    spending_year,
    spending_quarter,
    COUNT(*) as transaction_count,
    SUM(amount_cleaned) as total_spent,
    ROUND(AVG(amount_cleaned), 2) as avg_transaction,
    COUNT(DISTINCT person_name) as active_persons,
    COUNT(DISTINCT category_name) as categories_used
FROM curated_spending_snapshots
WHERE is_latest = 1
GROUP BY spending_year, spending_quarter
ORDER BY spending_year DESC, spending_quarter DESC;

-- ============================================================================
-- SECTION 5: DATA QUALITY QUERIES
-- ============================================================================

-- Query 16: Data Quality Distribution (Latest)
-- Returns: How many records at each quality level
SELECT 
    CASE 
        WHEN data_quality_score >= 90 THEN 'A+ (90-100)'
        WHEN data_quality_score >= 80 THEN 'A (80-89)'
        WHEN data_quality_score >= 70 THEN 'B (70-79)'
        WHEN data_quality_score >= 60 THEN 'C (60-69)'
        WHEN data_quality_score >= 50 THEN 'D (50-59)'
        ELSE 'F (<50)'
    END as quality_grade,
    COUNT(*) as record_count,
    ROUND(COUNT(*)::NUMERIC / SUM(COUNT(*)) OVER () * 100, 2) as percentage
FROM curated_spending_snapshots
WHERE is_latest = 1
GROUP BY quality_grade
ORDER BY MIN(data_quality_score) DESC;

-- Query 17: Quality Score by Category (Latest)
-- Returns: Average quality score per category
SELECT 
    category_name,
    COUNT(*) as record_count,
    ROUND(AVG(data_quality_score), 2) as avg_quality_score,
    MIN(data_quality_score) as min_quality,
    MAX(data_quality_score) as max_quality
FROM curated_spending_snapshots
WHERE is_latest = 1
GROUP BY category_name
ORDER BY avg_quality_score DESC;

-- ============================================================================
-- SECTION 6: TIME-TRAVEL QUERIES
-- ============================================================================

-- Query 18: Compare Person Spending Across Versions
-- Returns: How a person's spending changed over versions
SELECT 
    snapshot_version,
    snapshot_date,
    person_name,
    COUNT(*) as transaction_count,
    SUM(amount_cleaned) as total_spent,
    ROUND(AVG(amount_cleaned), 2) as avg_per_transaction
FROM curated_spending_snapshots
WHERE person_name = 'John Doe'  -- Change to specific person
GROUP BY snapshot_version, snapshot_date, person_name
ORDER BY snapshot_version DESC;

-- Query 19: Category Spending Evolution
-- Returns: How category spending changed across versions
SELECT 
    snapshot_version,
    snapshot_date,
    category_name,
    COUNT(*) as transaction_count,
    SUM(amount_cleaned) as total_spent
FROM curated_spending_snapshots
WHERE category_name = 'Dining Out'  -- Change to specific category
GROUP BY snapshot_version, snapshot_date, category_name
ORDER BY snapshot_version DESC;

-- Query 20: Full Snapshot at Specific Date
-- Returns: Complete data state at a historical point
SELECT 
    snapshot_version,
    snapshot_date,
    COUNT(*) as total_transactions,
    SUM(amount_cleaned) as total_spending,
    COUNT(DISTINCT person_name) as unique_persons,
    COUNT(DISTINCT category_name) as unique_categories,
    MIN(spending_date) as earliest_transaction,
    MAX(spending_date) as latest_transaction
FROM curated_spending_snapshots
WHERE snapshot_date = '2025-10-21'  -- Change to specific date
GROUP BY snapshot_version, snapshot_date;

-- ============================================================================
-- SECTION 7: EXPORT QUERIES
-- ============================================================================

-- Query 21: Export Latest Snapshot for Analysis
-- Returns: Full dataset ready for export to CSV/Excel
SELECT 
    snapshot_version,
    snapshot_date,
    person_name,
    category_name,
    category_group,
    location_name,
    location_type,
    payment_method_name,
    payment_type,
    spending_date,
    spending_year,
    spending_month,
    spending_quarter,
    spending_day_of_week,
    amount_cleaned,
    currency_code,
    description,
    data_quality_score
FROM curated_spending_snapshots
WHERE is_latest = 1
ORDER BY spending_date DESC, person_name;

-- Query 22: Export Version Comparison
-- Returns: Side-by-side comparison of two versions
SELECT 
    COALESCE(v1.stg_spending_id, v2.stg_spending_id) as spending_id,
    v1.person_name as v1_person,
    v2.person_name as v2_person,
    v1.category_name as v1_category,
    v2.category_name as v2_category,
    v1.spending_date as v1_date,
    v2.spending_date as v2_date,
    v1.amount_cleaned as v1_amount,
    v2.amount_cleaned as v2_amount,
    CASE 
        WHEN v1.stg_spending_id IS NULL THEN 'New in V2'
        WHEN v2.stg_spending_id IS NULL THEN 'Removed in V2'
        ELSE 'Exists in Both'
    END as status
FROM 
    (SELECT * FROM curated_spending_snapshots WHERE snapshot_version = 1) v1
FULL OUTER JOIN 
    (SELECT * FROM curated_spending_snapshots WHERE snapshot_version = 2) v2
    ON v1.stg_spending_id = v2.stg_spending_id
ORDER BY status, COALESCE(v1.spending_date, v2.spending_date) DESC;

-- ============================================================================
-- END OF HELPER QUERIES
-- ============================================================================
-- 
-- USAGE TIPS:
-- 1. Replace 'John Doe' with actual person names from your data
-- 2. Replace category names with your actual categories
-- 3. Adjust date ranges and version numbers as needed
-- 4. Add WHERE clauses to filter by date ranges
-- 5. Combine queries to create custom analytics
--
-- For more complex analysis, consider creating materialized views or
-- exporting to a BI tool like Tableau, Power BI, or Looker
-- ============================================================================

