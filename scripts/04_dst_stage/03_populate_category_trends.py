"""
DST Stage - Step 3: Populate Category Trends
Purpose: Create category-level spending trends with MoM/YoY analysis, rankings, and rolling averages
Source: curated_spending_snapshots (latest version)
Target: dst_category_trends
"""

from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
from datetime import datetime
import uuid

# Setup connection
load_dotenv('../../.env')
connection_string = os.getenv('SUPABASE_CONNECTION_STRING')
engine = create_engine(connection_string)

print("=" * 80)
print("DST STAGE - POPULATE CATEGORY TRENDS")
print("=" * 80)
print(f"⏰ Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

# Generate batch ID
batch_id = f"category_trends_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
print(f"📦 Batch ID: {batch_id}\n")

try:
    with engine.connect() as conn:
        # ============================================
        # STEP 1: Get source snapshot version
        # ============================================
        print("📊 STEP 1: Checking source data...")
        print("-" * 80)
        
        result = conn.execute(text("""
            SELECT snapshot_version, COUNT(*)
            FROM curated_spending_snapshots
            WHERE is_latest = 1
            GROUP BY snapshot_version
        """))
        
        snapshot_info = result.fetchone()
        if not snapshot_info:
            print("❌ No data found in curated_spending_snapshots")
            exit(1)
        
        snapshot_version = snapshot_info[0]
        record_count = snapshot_info[1]
        
        print(f"✅ Source snapshot version: {snapshot_version}")
        print(f"   Records: {record_count:,}\n")
        
        # ============================================
        # STEP 2: Clear existing data
        # ============================================
        print("🗑️  STEP 2: Clearing existing category trends data...")
        print("-" * 80)
        
        delete_result = conn.execute(text("""
            DELETE FROM dst_category_trends
            WHERE snapshot_version_source = :version
        """), {"version": snapshot_version})
        
        print(f"✅ Deleted {delete_result.rowcount} existing records\n")
        
        # ============================================
        # STEP 3: Aggregate category trends
        # ============================================
        print("📊 STEP 3: Aggregating category trends with rankings...")
        print("-" * 80)
        
        insert_query = text("""
            WITH category_base AS (
                -- Base aggregation by year, month, category
                SELECT 
                    spending_year as year,
                    spending_month as month,
                    spending_quarter as quarter,
                    DATE_TRUNC('month', spending_date)::DATE as month_start_date,
                    category_name,
                    category_group,
                    SUM(amount_cleaned) as total_spending,
                    COUNT(*) as transaction_count,
                    COUNT(DISTINCT person_name) as unique_persons,
                    AVG(amount_cleaned) as avg_transaction_amount,
                    MAX(snapshot_version) as snapshot_version_source
                FROM curated_spending_snapshots
                WHERE is_latest = 1
                GROUP BY spending_year, spending_month, spending_quarter, 
                         DATE_TRUNC('month', spending_date), category_name, category_group
            ),
            prev_month AS (
                -- Previous month for MoM
                SELECT 
                    year, month, category_name,
                    total_spending as prev_month_spending,
                    CASE WHEN month = 1 THEN year - 1 ELSE year END as prev_year,
                    CASE WHEN month = 1 THEN 12 ELSE month - 1 END as prev_month
                FROM category_base
            ),
            prev_year AS (
                -- Previous year same month for YoY
                SELECT year, month, category_name, total_spending as prev_year_spending
                FROM category_base
            ),
            monthly_totals AS (
                -- Total spending per month for percentage calculation
                SELECT year, month, SUM(total_spending) as month_total
                FROM category_base
                GROUP BY year, month
            ),
            rolling_avgs AS (
                -- Rolling 3-month and 6-month averages
                SELECT 
                    cb.year,
                    cb.month,
                    cb.category_name,
                    AVG(cb2.total_spending) FILTER (
                        WHERE cb2.year * 12 + cb2.month BETWEEN (cb.year * 12 + cb.month - 2) 
                        AND (cb.year * 12 + cb.month)
                    ) as rolling_3month_avg,
                    AVG(cb2.total_spending) FILTER (
                        WHERE cb2.year * 12 + cb2.month BETWEEN (cb.year * 12 + cb.month - 5) 
                        AND (cb.year * 12 + cb.month)
                    ) as rolling_6month_avg
                FROM category_base cb
                CROSS JOIN category_base cb2
                WHERE cb.category_name = cb2.category_name
                GROUP BY cb.year, cb.month, cb.category_name
            ),
            current_ranks AS (
                -- Current month rankings
                SELECT 
                    year, month, category_name,
                    ROW_NUMBER() OVER (PARTITION BY year, month ORDER BY total_spending DESC) as category_rank_current
                FROM category_base
            ),
            prev_ranks AS (
                -- Previous month rankings
                SELECT 
                    year, month, category_name,
                    ROW_NUMBER() OVER (PARTITION BY year, month ORDER BY total_spending DESC) as category_rank_prev
                FROM category_base
            )
            INSERT INTO dst_category_trends (
                year, month, quarter, month_start_date,
                category_name, category_group,
                total_spending, transaction_count, unique_persons, avg_transaction_amount,
                prev_month_spending, mom_absolute_change, mom_percent_change, mom_trend_direction,
                prev_year_spending, yoy_absolute_change, yoy_percent_change, yoy_trend_direction,
                rolling_3month_avg, rolling_6month_avg,
                category_rank_current, category_rank_prev_month, rank_change,
                percent_of_total_spending,
                snapshot_version_source, created_at, updated_at
            )
            SELECT 
                cb.year, cb.month, cb.quarter, cb.month_start_date,
                cb.category_name, cb.category_group,
                cb.total_spending, cb.transaction_count, cb.unique_persons, cb.avg_transaction_amount,
                
                -- MoM trends
                pm.prev_month_spending,
                cb.total_spending - COALESCE(pm.prev_month_spending, 0) as mom_absolute_change,
                CASE 
                    WHEN pm.prev_month_spending IS NOT NULL AND pm.prev_month_spending > 0
                    THEN ROUND(((cb.total_spending - pm.prev_month_spending) / pm.prev_month_spending * 100)::NUMERIC, 2)
                    ELSE NULL
                END as mom_percent_change,
                CASE 
                    WHEN pm.prev_month_spending IS NULL THEN 'NO_DATA'
                    WHEN ((cb.total_spending - pm.prev_month_spending) / NULLIF(pm.prev_month_spending, 0) * 100) > 5 THEN 'INCREASING'
                    WHEN ((cb.total_spending - pm.prev_month_spending) / NULLIF(pm.prev_month_spending, 0) * 100) < -5 THEN 'DECREASING'
                    ELSE 'STABLE'
                END as mom_trend_direction,
                
                -- YoY trends
                py.prev_year_spending,
                cb.total_spending - COALESCE(py.prev_year_spending, 0) as yoy_absolute_change,
                CASE 
                    WHEN py.prev_year_spending IS NOT NULL AND py.prev_year_spending > 0
                    THEN ROUND(((cb.total_spending - py.prev_year_spending) / py.prev_year_spending * 100)::NUMERIC, 2)
                    ELSE NULL
                END as yoy_percent_change,
                CASE 
                    WHEN py.prev_year_spending IS NULL THEN 'NO_DATA'
                    WHEN ((cb.total_spending - py.prev_year_spending) / NULLIF(py.prev_year_spending, 0) * 100) > 5 THEN 'INCREASING'
                    WHEN ((cb.total_spending - py.prev_year_spending) / NULLIF(py.prev_year_spending, 0) * 100) < -5 THEN 'DECREASING'
                    ELSE 'STABLE'
                END as yoy_trend_direction,
                
                -- Rolling averages
                ROUND(ra.rolling_3month_avg::NUMERIC, 2) as rolling_3month_avg,
                ROUND(ra.rolling_6month_avg::NUMERIC, 2) as rolling_6month_avg,
                
                -- Rankings
                cr.category_rank_current,
                pr.category_rank_prev,
                COALESCE(pr.category_rank_prev, cr.category_rank_current) - cr.category_rank_current as rank_change,
                
                -- Percentage of total
                ROUND((cb.total_spending / NULLIF(mt.month_total, 0) * 100)::NUMERIC, 2) as percent_of_total_spending,
                
                cb.snapshot_version_source,
                CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP
                
            FROM category_base cb
            LEFT JOIN prev_month pm ON 
                cb.category_name = pm.category_name
                AND pm.year = pm.prev_year
                AND pm.month = pm.prev_month
            LEFT JOIN prev_year py ON 
                py.year = cb.year - 1
                AND py.month = cb.month
                AND cb.category_name = py.category_name
            LEFT JOIN monthly_totals mt ON 
                mt.year = cb.year AND mt.month = cb.month
            LEFT JOIN rolling_avgs ra ON 
                ra.year = cb.year AND ra.month = cb.month AND ra.category_name = cb.category_name
            LEFT JOIN current_ranks cr ON 
                cr.year = cb.year AND cr.month = cb.month AND cr.category_name = cb.category_name
            LEFT JOIN prev_ranks pr ON 
                pr.year = CASE WHEN cb.month = 1 THEN cb.year - 1 ELSE cb.year END
                AND pr.month = CASE WHEN cb.month = 1 THEN 12 ELSE cb.month - 1 END
                AND pr.category_name = cb.category_name
        """)
        
        result = conn.execute(insert_query)
        conn.commit()
        
        inserted_count = result.rowcount
        print(f"✅ Inserted {inserted_count:,} category trend records\n")
        
        # ============================================
        # STEP 4: Verify and show insights
        # ============================================
        print("🔍 STEP 4: Category trend insights...")
        print("-" * 80)
        
        # Top categories
        print("\n📊 Top 5 categories by spending:")
        top_cats = conn.execute(text("""
            SELECT 
                category_name, 
                SUM(total_spending) as total,
                AVG(percent_of_total_spending) as avg_share,
                STRING_AGG(DISTINCT mom_trend_direction, ', ') as trends
            FROM dst_category_trends
            WHERE snapshot_version_source = :version
            GROUP BY category_name
            ORDER BY total DESC
            LIMIT 5
        """), {"version": snapshot_version})
        
        for row in top_cats:
            print(f"   {row[0]:20} ${row[1]:10,.2f}  ({row[2]:5.2f}% share)  Trends: {row[3]}")
        
        # Growing categories
        print("\n📈 Fastest growing categories (MoM):")
        growing = conn.execute(text("""
            SELECT category_name, mom_percent_change, total_spending
            FROM dst_category_trends
            WHERE snapshot_version_source = :version
              AND mom_percent_change IS NOT NULL
            ORDER BY mom_percent_change DESC
            LIMIT 5
        """), {"version": snapshot_version})
        
        for row in growing:
            print(f"   {row[0]:20} +{row[1]:6.2f}%  (${row[2]:,.2f})")
        
        # Declining categories
        print("\n📉 Declining categories (MoM):")
        declining = conn.execute(text("""
            SELECT category_name, mom_percent_change, total_spending
            FROM dst_category_trends
            WHERE snapshot_version_source = :version
              AND mom_percent_change IS NOT NULL
            ORDER BY mom_percent_change ASC
            LIMIT 5
        """), {"version": snapshot_version})
        
        for row in declining:
            print(f"   {row[0]:20} {row[1]:6.2f}%  (${row[2]:,.2f})")

except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()
    exit(1)

print("\n" + "=" * 80)
print("✅ CATEGORY TRENDS POPULATION COMPLETED")
print("=" * 80)
print(f"⏰ Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"📦 Batch ID: {batch_id}")
print("\n📝 Next step: Run 04_populate_person_analytics.py")
print("=" * 80)

