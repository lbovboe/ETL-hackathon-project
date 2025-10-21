"""
DST Stage - Step 4: Populate Person Analytics
Purpose: Create per-person spending behavior analysis with essential/discretionary breakdown
This is the KEY table for financial recommendations in Stage 5 (DIS)
Source: curated_spending_snapshots (latest version)
Target: dst_person_analytics
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
print("DST STAGE - POPULATE PERSON ANALYTICS")
print("=" * 80)
print(f"⏰ Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

# Generate batch ID
batch_id = f"person_analytics_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
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
        print("🗑️  STEP 2: Clearing existing person analytics data...")
        print("-" * 80)
        
        delete_result = conn.execute(text("""
            DELETE FROM dst_person_analytics
            WHERE snapshot_version_source = :version
        """), {"version": snapshot_version})
        
        print(f"✅ Deleted {delete_result.rowcount} existing records\n")
        
        # ============================================
        # STEP 3: Aggregate person analytics
        # ============================================
        print("📊 STEP 3: Aggregating person analytics with essential/discretionary breakdown...")
        print("-" * 80)
        
        insert_query = text("""
            WITH person_base AS (
                -- Base aggregation by year, month, person
                SELECT 
                    spending_year as year,
                    spending_month as month,
                    spending_quarter as quarter,
                    DATE_TRUNC('month', spending_date)::DATE as month_start_date,
                    person_name,
                    SUM(amount_cleaned) as total_spending,
                    COUNT(*) as transaction_count,
                    AVG(amount_cleaned) as avg_transaction_amount,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount_cleaned) as median_transaction_amount,
                    AVG(data_quality_score) as avg_quality_score,
                    MAX(snapshot_version) as snapshot_version_source
                FROM curated_spending_snapshots
                WHERE is_latest = 1
                GROUP BY spending_year, spending_month, spending_quarter,
                         DATE_TRUNC('month', spending_date), person_name
            ),
            category_breakdown AS (
                -- Essential vs Discretionary breakdown by category_group
                SELECT 
                    spending_year as year,
                    spending_month as month,
                    person_name,
                    SUM(CASE WHEN category_group = 'Essential' THEN amount_cleaned ELSE 0 END) as essential_spending,
                    SUM(CASE WHEN category_group = 'Discretionary' THEN amount_cleaned ELSE 0 END) as discretionary_spending,
                    SUM(CASE WHEN category_group = 'Transport' THEN amount_cleaned ELSE 0 END) as transport_spending,
                    SUM(CASE WHEN category_group = 'Healthcare' THEN amount_cleaned ELSE 0 END) as healthcare_spending,
                    SUM(CASE WHEN category_group = 'Education' THEN amount_cleaned ELSE 0 END) as education_spending,
                    SUM(CASE WHEN category_group = 'Other' OR category_group IS NULL THEN amount_cleaned ELSE 0 END) as other_spending
                FROM curated_spending_snapshots
                WHERE is_latest = 1
                GROUP BY spending_year, spending_month, person_name
            ),
            top_category AS (
                -- Find top spending category per person per month
                SELECT DISTINCT ON (spending_year, spending_month, person_name)
                    spending_year as year,
                    spending_month as month,
                    person_name,
                    category_name as top_category,
                    SUM(amount_cleaned) as top_category_spending
                FROM curated_spending_snapshots
                WHERE is_latest = 1
                GROUP BY spending_year, spending_month, person_name, category_name
                ORDER BY spending_year, spending_month, person_name, SUM(amount_cleaned) DESC
            ),
            diversity_metrics AS (
                -- Count unique dimensions per person per month
                SELECT 
                    spending_year as year,
                    spending_month as month,
                    person_name,
                    COUNT(DISTINCT category_name) as unique_categories_count,
                    COUNT(DISTINCT location_name) as unique_locations_count,
                    COUNT(DISTINCT payment_method_name) as unique_payment_methods_count
                FROM curated_spending_snapshots
                WHERE is_latest = 1
                GROUP BY spending_year, spending_month, person_name
            ),
            weekday_weekend AS (
                -- Weekday vs weekend spending
                SELECT 
                    spending_year as year,
                    spending_month as month,
                    person_name,
                    SUM(CASE WHEN spending_day_of_week BETWEEN 1 AND 5 THEN amount_cleaned ELSE 0 END) as weekday_spending,
                    SUM(CASE WHEN spending_day_of_week IN (6, 7) THEN amount_cleaned ELSE 0 END) as weekend_spending
                FROM curated_spending_snapshots
                WHERE is_latest = 1
                GROUP BY spending_year, spending_month, person_name
            ),
            transaction_buckets AS (
                -- Transaction size distribution
                SELECT 
                    spending_year as year,
                    spending_month as month,
                    person_name,
                    COUNT(*) FILTER (WHERE amount_cleaned < 10) as small_transactions_count,
                    COUNT(*) FILTER (WHERE amount_cleaned >= 10 AND amount_cleaned < 100) as medium_transactions_count,
                    COUNT(*) FILTER (WHERE amount_cleaned >= 100 AND amount_cleaned < 500) as large_transactions_count,
                    COUNT(*) FILTER (WHERE amount_cleaned >= 500) as xlarge_transactions_count
                FROM curated_spending_snapshots
                WHERE is_latest = 1
                GROUP BY spending_year, spending_month, person_name
            ),
            spending_frequency AS (
                -- Days with spending activity
                SELECT 
                    spending_year as year,
                    spending_month as month,
                    person_name,
                    COUNT(DISTINCT spending_date) as days_with_spending
                FROM curated_spending_snapshots
                WHERE is_latest = 1
                GROUP BY spending_year, spending_month, person_name
            ),
            prev_month AS (
                -- Previous month for MoM
                SELECT 
                    year, month, person_name, total_spending as prev_month_total,
                    CASE WHEN month = 1 THEN year - 1 ELSE year END as prev_year,
                    CASE WHEN month = 1 THEN 12 ELSE month - 1 END as prev_month
                FROM person_base
            ),
            prev_year AS (
                -- Previous year same month for YoY
                SELECT year, month, person_name, total_spending as prev_year_total
                FROM person_base
            )
            INSERT INTO dst_person_analytics (
                year, month, quarter, month_start_date, person_name,
                total_spending, transaction_count, avg_transaction_amount, median_transaction_amount,
                top_category, top_category_spending, top_category_percent,
                essential_spending, discretionary_spending, transport_spending, 
                healthcare_spending, education_spending, other_spending,
                essential_percent, discretionary_percent, essential_to_discretionary_ratio,
                unique_categories_count, unique_locations_count, unique_payment_methods_count,
                weekday_spending, weekend_spending, weekend_spending_percent,
                morning_spending, afternoon_spending, evening_spending, night_spending,
                small_transactions_count, medium_transactions_count, 
                large_transactions_count, xlarge_transactions_count,
                avg_daily_spending, avg_weekly_spending, 
                days_with_spending, spending_frequency_percent,
                prev_month_total, mom_absolute_change, mom_percent_change,
                prev_year_total, yoy_absolute_change, yoy_percent_change,
                avg_quality_score, snapshot_version_source, created_at, updated_at
            )
            SELECT 
                pb.year, pb.month, pb.quarter, pb.month_start_date, pb.person_name,
                pb.total_spending, pb.transaction_count, pb.avg_transaction_amount, pb.median_transaction_amount,
                
                -- Top category
                tc.top_category,
                tc.top_category_spending,
                ROUND((tc.top_category_spending / NULLIF(pb.total_spending, 0) * 100)::NUMERIC, 2) as top_category_percent,
                
                -- Essential vs Discretionary breakdown
                cb.essential_spending,
                cb.discretionary_spending,
                cb.transport_spending,
                cb.healthcare_spending,
                cb.education_spending,
                cb.other_spending,
                ROUND((cb.essential_spending / NULLIF(pb.total_spending, 0) * 100)::NUMERIC, 2) as essential_percent,
                ROUND((cb.discretionary_spending / NULLIF(pb.total_spending, 0) * 100)::NUMERIC, 2) as discretionary_percent,
                ROUND((cb.essential_spending / NULLIF(cb.discretionary_spending, 0))::NUMERIC, 2) as essential_to_discretionary_ratio,
                
                -- Diversity
                dm.unique_categories_count,
                dm.unique_locations_count,
                dm.unique_payment_methods_count,
                
                -- Weekday/Weekend
                ww.weekday_spending,
                ww.weekend_spending,
                ROUND((ww.weekend_spending / NULLIF(pb.total_spending, 0) * 100)::NUMERIC, 2) as weekend_spending_percent,
                
                -- Time of day (placeholder - we don't have time data)
                NULL::NUMERIC as morning_spending,
                NULL::NUMERIC as afternoon_spending,
                NULL::NUMERIC as evening_spending,
                NULL::NUMERIC as night_spending,
                
                -- Transaction buckets
                tb.small_transactions_count,
                tb.medium_transactions_count,
                tb.large_transactions_count,
                tb.xlarge_transactions_count,
                
                -- Frequency metrics
                ROUND((pb.total_spending / EXTRACT(DAY FROM (pb.month_start_date + INTERVAL '1 month - 1 day')::DATE))::NUMERIC, 2) as avg_daily_spending,
                ROUND((pb.total_spending / 4.33)::NUMERIC, 2) as avg_weekly_spending,
                sf.days_with_spending,
                ROUND((sf.days_with_spending::NUMERIC / EXTRACT(DAY FROM (pb.month_start_date + INTERVAL '1 month - 1 day')::DATE) * 100), 2) as spending_frequency_percent,
                
                -- MoM trends
                pm.prev_month_total,
                pb.total_spending - COALESCE(pm.prev_month_total, 0) as mom_absolute_change,
                CASE 
                    WHEN pm.prev_month_total IS NOT NULL AND pm.prev_month_total > 0
                    THEN ROUND(((pb.total_spending - pm.prev_month_total) / pm.prev_month_total * 100)::NUMERIC, 2)
                    ELSE NULL
                END as mom_percent_change,
                
                -- YoY trends
                py.prev_year_total,
                pb.total_spending - COALESCE(py.prev_year_total, 0) as yoy_absolute_change,
                CASE 
                    WHEN py.prev_year_total IS NOT NULL AND py.prev_year_total > 0
                    THEN ROUND(((pb.total_spending - py.prev_year_total) / py.prev_year_total * 100)::NUMERIC, 2)
                    ELSE NULL
                END as yoy_percent_change,
                
                pb.avg_quality_score,
                pb.snapshot_version_source,
                CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP
                
            FROM person_base pb
            LEFT JOIN category_breakdown cb ON 
                cb.year = pb.year AND cb.month = pb.month AND cb.person_name = pb.person_name
            LEFT JOIN top_category tc ON 
                tc.year = pb.year AND tc.month = pb.month AND tc.person_name = pb.person_name
            LEFT JOIN diversity_metrics dm ON 
                dm.year = pb.year AND dm.month = pb.month AND dm.person_name = pb.person_name
            LEFT JOIN weekday_weekend ww ON 
                ww.year = pb.year AND ww.month = pb.month AND ww.person_name = pb.person_name
            LEFT JOIN transaction_buckets tb ON 
                tb.year = pb.year AND tb.month = pb.month AND tb.person_name = pb.person_name
            LEFT JOIN spending_frequency sf ON 
                sf.year = pb.year AND sf.month = pb.month AND sf.person_name = pb.person_name
            LEFT JOIN prev_month pm ON 
                pb.person_name = pm.person_name
                AND pm.year = pm.prev_year
                AND pm.month = pm.prev_month
            LEFT JOIN prev_year py ON 
                py.year = pb.year - 1
                AND py.month = pb.month
                AND pb.person_name = py.person_name
        """)
        
        result = conn.execute(insert_query)
        conn.commit()
        
        inserted_count = result.rowcount
        print(f"✅ Inserted {inserted_count:,} person analytics records\n")
        
        # ============================================
        # STEP 4: Financial health insights
        # ============================================
        print("🔍 STEP 4: Financial health insights...")
        print("-" * 80)
        
        # Essential vs Discretionary summary
        print("\n💰 Essential vs Discretionary Spending:")
        spending_breakdown = conn.execute(text("""
            SELECT 
                AVG(essential_percent) as avg_essential_pct,
                AVG(discretionary_percent) as avg_discretionary_pct,
                AVG(essential_to_discretionary_ratio) as avg_ratio,
                COUNT(*) FILTER (WHERE discretionary_percent > 40) as high_discretionary_count,
                COUNT(*) as total_persons
            FROM dst_person_analytics
            WHERE snapshot_version_source = :version
        """), {"version": snapshot_version}).fetchone()
        
        print(f"   Avg Essential:      {spending_breakdown[0]:.1f}%")
        print(f"   Avg Discretionary:  {spending_breakdown[1]:.1f}%")
        print(f"   Avg E/D Ratio:      {spending_breakdown[2]:.2f}")
        print(f"   High Discretionary: {spending_breakdown[3]} of {spending_breakdown[4]} persons (>{40}%)")
        
        # Top spenders
        print("\n👥 Top 5 spenders:")
        top_spenders = conn.execute(text("""
            SELECT 
                person_name, total_spending, 
                essential_percent, discretionary_percent,
                top_category
            FROM dst_person_analytics
            WHERE snapshot_version_source = :version
            ORDER BY total_spending DESC
            LIMIT 5
        """), {"version": snapshot_version})
        
        for row in top_spenders:
            print(f"   {row[0]:20} ${row[1]:10,.2f}  E:{row[2]:5.1f}% D:{row[3]:5.1f}%  Top: {row[4]}")
        
        # Financial health flags
        print("\n⚠️  Financial health alerts:")
        alerts = conn.execute(text("""
            SELECT 
                person_name,
                discretionary_percent,
                essential_to_discretionary_ratio,
                total_spending
            FROM dst_person_analytics
            WHERE snapshot_version_source = :version
              AND discretionary_percent > 35
            ORDER BY discretionary_percent DESC
            LIMIT 5
        """), {"version": snapshot_version})
        
        alert_count = 0
        for row in alerts:
            alert_count += 1
            print(f"   {row[0]:20} Discretionary: {row[1]:5.1f}%  Ratio: {row[2]:5.2f}  Total: ${row[3]:,.2f}")
        
        if alert_count == 0:
            print("   ✅ No high-discretionary spending alerts!")

except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()
    exit(1)

print("\n" + "=" * 80)
print("✅ PERSON ANALYTICS POPULATION COMPLETED")
print("=" * 80)
print(f"⏰ Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"📦 Batch ID: {batch_id}")
print("\n🎯 KEY INSIGHT: Essential/discretionary breakdown now available for Stage 5 recommendations!")
print("\n📝 Next step: Run 05_populate_payment_summary.py")
print("=" * 80)

