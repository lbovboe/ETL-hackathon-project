"""
DST Stage - Step 2: Populate Monthly Spending Summary
Purpose: Create pre-aggregated monthly totals by person, category, and location
Source: curated_spending_snapshots (latest version)
Target: dst_monthly_spending_summary
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
print("DST STAGE - POPULATE MONTHLY SPENDING SUMMARY")
print("=" * 80)
print(f"⏰ Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

# Generate batch ID
batch_id = f"monthly_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
print(f"📦 Batch ID: {batch_id}\n")

try:
    with engine.connect() as conn:
        # ============================================
        # STEP 1: Get source snapshot version
        # ============================================
        print("📊 STEP 1: Checking source data...")
        print("-" * 80)
        
        result = conn.execute(text("""
            SELECT 
                snapshot_version,
                COUNT(*) as record_count,
                MIN(spending_date) as min_date,
                MAX(spending_date) as max_date
            FROM curated_spending_snapshots
            WHERE is_latest = 1
            GROUP BY snapshot_version
        """))
        
        snapshot_info = result.fetchone()
        
        if not snapshot_info:
            print("❌ No data found in curated_spending_snapshots with is_latest = 1")
            exit(1)
        
        snapshot_version = snapshot_info[0]
        record_count = snapshot_info[1]
        min_date = snapshot_info[2]
        max_date = snapshot_info[3]
        
        print(f"✅ Source snapshot version: {snapshot_version}")
        print(f"   Records: {record_count:,}")
        print(f"   Date range: {min_date} to {max_date}\n")
        
        # ============================================
        # STEP 2: Clear existing data for this version
        # ============================================
        print("🗑️  STEP 2: Clearing existing monthly summary data...")
        print("-" * 80)
        
        delete_result = conn.execute(text("""
            DELETE FROM dst_monthly_spending_summary
            WHERE snapshot_version_source = :version
        """), {"version": snapshot_version})
        
        deleted_count = delete_result.rowcount
        print(f"✅ Deleted {deleted_count} existing records for version {snapshot_version}\n")
        
        # ============================================
        # STEP 3: Aggregate monthly spending data
        # ============================================
        print("📊 STEP 3: Aggregating monthly spending data...")
        print("-" * 80)
        
        # Main aggregation query with MoM and YoY calculations
        insert_query = text("""
            WITH monthly_base AS (
                -- Base aggregation by year, month, person, category, location
                SELECT 
                    spending_year as year,
                    spending_month as month,
                    spending_quarter as quarter,
                    DATE_TRUNC('month', spending_date)::DATE as month_start_date,
                    (DATE_TRUNC('month', spending_date) + INTERVAL '1 month - 1 day')::DATE as month_end_date,
                    person_name,
                    category_name,
                    category_group,
                    location_name,
                    location_type,
                    SUM(amount_cleaned) as total_spending,
                    COUNT(*) as transaction_count,
                    AVG(amount_cleaned) as avg_transaction_amount,
                    MIN(amount_cleaned) as min_transaction_amount,
                    MAX(amount_cleaned) as max_transaction_amount,
                    AVG(data_quality_score) as avg_quality_score,
                    MAX(snapshot_version) as snapshot_version_source
                FROM curated_spending_snapshots
                WHERE is_latest = 1
                GROUP BY 
                    spending_year, spending_month, spending_quarter,
                    DATE_TRUNC('month', spending_date),
                    person_name, category_name, category_group,
                    location_name, location_type
            ),
            prev_month AS (
                -- Previous month spending for MoM calculation
                SELECT 
                    year,
                    month,
                    person_name,
                    category_name,
                    location_name,
                    total_spending as prev_month_spending,
                    -- Calculate previous month (handle year boundary)
                    CASE 
                        WHEN month = 1 THEN year - 1
                        ELSE year
                    END as prev_year,
                    CASE 
                        WHEN month = 1 THEN 12
                        ELSE month - 1
                    END as prev_month
                FROM monthly_base
            ),
            prev_year AS (
                -- Previous year same month for YoY calculation
                SELECT 
                    year,
                    month,
                    person_name,
                    category_name,
                    location_name,
                    total_spending as prev_year_spending
                FROM monthly_base
            )
            INSERT INTO dst_monthly_spending_summary (
                year, month, quarter, month_start_date, month_end_date,
                person_name, category_name, category_group,
                location_name, location_type,
                total_spending, transaction_count,
                avg_transaction_amount, min_transaction_amount, max_transaction_amount,
                prev_month_spending, mom_absolute_change, mom_percent_change,
                prev_year_spending, yoy_absolute_change, yoy_percent_change,
                avg_quality_score, snapshot_version_source,
                created_at, updated_at
            )
            SELECT 
                mb.year,
                mb.month,
                mb.quarter,
                mb.month_start_date,
                mb.month_end_date,
                mb.person_name,
                mb.category_name,
                mb.category_group,
                mb.location_name,
                mb.location_type,
                mb.total_spending,
                mb.transaction_count,
                mb.avg_transaction_amount,
                mb.min_transaction_amount,
                mb.max_transaction_amount,
                
                -- Previous month data
                pm.prev_month_spending,
                mb.total_spending - COALESCE(pm.prev_month_spending, 0) as mom_absolute_change,
                CASE 
                    WHEN pm.prev_month_spending IS NOT NULL AND pm.prev_month_spending > 0
                    THEN ROUND(((mb.total_spending - pm.prev_month_spending) / pm.prev_month_spending * 100)::NUMERIC, 2)
                    ELSE NULL
                END as mom_percent_change,
                
                -- Previous year data
                py.prev_year_spending,
                mb.total_spending - COALESCE(py.prev_year_spending, 0) as yoy_absolute_change,
                CASE 
                    WHEN py.prev_year_spending IS NOT NULL AND py.prev_year_spending > 0
                    THEN ROUND(((mb.total_spending - py.prev_year_spending) / py.prev_year_spending * 100)::NUMERIC, 2)
                    ELSE NULL
                END as yoy_percent_change,
                
                mb.avg_quality_score,
                mb.snapshot_version_source,
                CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP
                
            FROM monthly_base mb
            LEFT JOIN prev_month pm ON 
                mb.person_name = pm.person_name
                AND mb.category_name = pm.category_name
                AND mb.location_name = pm.location_name
                AND pm.year = pm.prev_year
                AND pm.month = pm.prev_month
            LEFT JOIN prev_year py ON 
                py.year = mb.year - 1
                AND py.month = mb.month
                AND mb.person_name = py.person_name
                AND mb.category_name = py.category_name
                AND mb.location_name = py.location_name
        """)
        
        result = conn.execute(insert_query)
        conn.commit()
        
        inserted_count = result.rowcount
        print(f"✅ Inserted {inserted_count:,} monthly summary records\n")
        
        # ============================================
        # STEP 4: Verify results
        # ============================================
        print("🔍 STEP 4: Verifying aggregation results...")
        print("-" * 80)
        
        # Total spending check
        curated_total = conn.execute(text("""
            SELECT SUM(amount_cleaned)
            FROM curated_spending_snapshots
            WHERE is_latest = 1
        """)).scalar()
        
        dst_total = conn.execute(text("""
            SELECT SUM(total_spending)
            FROM dst_monthly_spending_summary
            WHERE snapshot_version_source = :version
        """), {"version": snapshot_version}).scalar()
        
        print(f"✅ Total spending verification:")
        print(f"   Curated total: ${curated_total:,.2f}")
        print(f"   DST total:     ${dst_total:,.2f}")
        
        if abs(curated_total - dst_total) < 0.01:
            print(f"   ✅ Match! Difference: ${abs(curated_total - dst_total):.2f}\n")
        else:
            print(f"   ⚠️  Mismatch! Difference: ${abs(curated_total - dst_total):.2f}\n")
        
        # Summary statistics
        stats = conn.execute(text("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT person_name) as unique_persons,
                COUNT(DISTINCT category_name) as unique_categories,
                COUNT(DISTINCT location_name) as unique_locations,
                COUNT(DISTINCT year || '-' || LPAD(month::TEXT, 2, '0')) as unique_months,
                SUM(transaction_count) as total_transactions,
                AVG(total_spending) as avg_monthly_spending,
                MIN(total_spending) as min_spending,
                MAX(total_spending) as max_spending
            FROM dst_monthly_spending_summary
            WHERE snapshot_version_source = :version
        """), {"version": snapshot_version}).fetchone()
        
        print(f"📊 Summary statistics:")
        print(f"   Total records: {stats[0]:,}")
        print(f"   Unique persons: {stats[1]:,}")
        print(f"   Unique categories: {stats[2]:,}")
        print(f"   Unique locations: {stats[3]:,}")
        print(f"   Unique months: {stats[4]:,}")
        print(f"   Total transactions: {stats[5]:,}")
        print(f"   Avg monthly spending: ${stats[6]:,.2f}")
        print(f"   Min spending: ${stats[7]:,.2f}")
        print(f"   Max spending: ${stats[8]:,.2f}\n")
        
        # Show sample records with trends
        print("📋 Sample records (with MoM trends):")
        print("-" * 80)
        
        samples = conn.execute(text("""
            SELECT 
                year, month, person_name, category_name, location_name,
                total_spending, transaction_count,
                mom_percent_change, yoy_percent_change
            FROM dst_monthly_spending_summary
            WHERE snapshot_version_source = :version
            ORDER BY total_spending DESC
            LIMIT 5
        """), {"version": snapshot_version})
        
        for row in samples:
            mom_trend = f"+{row[7]:.1f}%" if row[7] and row[7] > 0 else f"{row[7]:.1f}%" if row[7] else "N/A"
            yoy_trend = f"+{row[8]:.1f}%" if row[8] and row[8] > 0 else f"{row[8]:.1f}%" if row[8] else "N/A"
            print(f"   {row[0]}-{row[1]:02d} | {row[2][:15]:15} | {row[3][:15]:15} | ${row[5]:8,.2f} | {row[6]:3} txns | MoM: {mom_trend:>7} | YoY: {yoy_trend:>7}")

except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()
    exit(1)

print("\n" + "=" * 80)
print("✅ MONTHLY SPENDING SUMMARY POPULATION COMPLETED")
print("=" * 80)
print(f"⏰ Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"📦 Batch ID: {batch_id}")
print("\n📝 Next step: Run 03_populate_category_trends.py")
print("=" * 80)

