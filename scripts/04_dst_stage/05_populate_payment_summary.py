"""
DST Stage - Step 5: Populate Payment Method Summary
Purpose: Analyze payment method usage, preferences, and market share
Source: curated_spending_snapshots (latest version)
Target: dst_payment_method_summary
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
print("DST STAGE - POPULATE PAYMENT METHOD SUMMARY")
print("=" * 80)
print(f"⏰ Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

# Generate batch ID
batch_id = f"payment_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
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
        print("🗑️  STEP 2: Clearing existing payment summary data...")
        print("-" * 80)
        
        delete_result = conn.execute(text("""
            DELETE FROM dst_payment_method_summary
            WHERE snapshot_version_source = :version
        """), {"version": snapshot_version})
        
        print(f"✅ Deleted {delete_result.rowcount} existing records\n")
        
        # ============================================
        # STEP 3: Aggregate payment method data
        # ============================================
        print("📊 STEP 3: Aggregating payment method usage...")
        print("-" * 80)
        
        insert_query = text("""
            WITH payment_base AS (
                -- Base aggregation by year, month, payment method
                SELECT 
                    spending_year as year,
                    spending_month as month,
                    spending_quarter as quarter,
                    DATE_TRUNC('month', spending_date)::DATE as month_start_date,
                    payment_method_name,
                    payment_type,
                    COUNT(*) as transaction_count,
                    COUNT(DISTINCT person_name) as unique_persons_count,
                    SUM(amount_cleaned) as total_amount,
                    AVG(amount_cleaned) as avg_transaction_amount,
                    MIN(amount_cleaned) as min_transaction_amount,
                    MAX(amount_cleaned) as max_transaction_amount,
                    MAX(snapshot_version) as snapshot_version_source
                FROM curated_spending_snapshots
                WHERE is_latest = 1
                GROUP BY spending_year, spending_month, spending_quarter,
                         DATE_TRUNC('month', spending_date),
                         payment_method_name, payment_type
            ),
            monthly_totals AS (
                -- Total transactions and spending per month for market share
                SELECT 
                    year, month,
                    SUM(transaction_count) as month_total_transactions,
                    SUM(total_amount) as month_total_spending
                FROM payment_base
                GROUP BY year, month
            ),
            top_categories AS (
                -- Top 3 categories per payment method per month
                SELECT 
                    spending_year as year,
                    spending_month as month,
                    payment_method_name,
                    category_name,
                    SUM(amount_cleaned) as category_amount,
                    ROW_NUMBER() OVER (
                        PARTITION BY spending_year, spending_month, payment_method_name 
                        ORDER BY SUM(amount_cleaned) DESC
                    ) as category_rank
                FROM curated_spending_snapshots
                WHERE is_latest = 1
                GROUP BY spending_year, spending_month, payment_method_name, category_name
            ),
            top_cat_1 AS (
                SELECT year, month, payment_method_name, category_name as cat1, category_amount as amt1
                FROM top_categories WHERE category_rank = 1
            ),
            top_cat_2 AS (
                SELECT year, month, payment_method_name, category_name as cat2, category_amount as amt2
                FROM top_categories WHERE category_rank = 2
            ),
            top_cat_3 AS (
                SELECT year, month, payment_method_name, category_name as cat3, category_amount as amt3
                FROM top_categories WHERE category_rank = 3
            ),
            prev_month AS (
                -- Previous month for MoM trends
                SELECT 
                    year, month, payment_method_name,
                    transaction_count as prev_month_transaction_count,
                    total_amount as prev_month_amount,
                    CASE WHEN month = 1 THEN year - 1 ELSE year END as prev_year,
                    CASE WHEN month = 1 THEN 12 ELSE month - 1 END as prev_month
                FROM payment_base
            ),
            payment_ranks AS (
                -- Rank payment methods by total amount
                SELECT 
                    year, month, payment_method_name,
                    ROW_NUMBER() OVER (PARTITION BY year, month ORDER BY total_amount DESC) as payment_method_rank
                FROM payment_base
            )
            INSERT INTO dst_payment_method_summary (
                year, month, quarter, month_start_date,
                payment_method_name, payment_type,
                transaction_count, unique_persons_count,
                total_amount, avg_transaction_amount, 
                min_transaction_amount, max_transaction_amount,
                percent_of_transactions, percent_of_spending,
                top_category_1, top_category_1_amount,
                top_category_2, top_category_2_amount,
                top_category_3, top_category_3_amount,
                prev_month_transaction_count, mom_transaction_change_percent,
                prev_month_amount, mom_amount_change_percent,
                payment_method_rank,
                snapshot_version_source, created_at, updated_at
            )
            SELECT 
                pb.year, pb.month, pb.quarter, pb.month_start_date,
                pb.payment_method_name, pb.payment_type,
                pb.transaction_count, pb.unique_persons_count,
                pb.total_amount, pb.avg_transaction_amount,
                pb.min_transaction_amount, pb.max_transaction_amount,
                
                -- Market share
                ROUND((pb.transaction_count::NUMERIC / NULLIF(mt.month_total_transactions, 0) * 100), 2) as percent_of_transactions,
                ROUND((pb.total_amount / NULLIF(mt.month_total_spending, 0) * 100), 2) as percent_of_spending,
                
                -- Top categories
                tc1.cat1, tc1.amt1,
                tc2.cat2, tc2.amt2,
                tc3.cat3, tc3.amt3,
                
                -- MoM trends
                pm.prev_month_transaction_count,
                CASE 
                    WHEN pm.prev_month_transaction_count IS NOT NULL AND pm.prev_month_transaction_count > 0
                    THEN ROUND(((pb.transaction_count - pm.prev_month_transaction_count)::NUMERIC / pm.prev_month_transaction_count * 100), 2)
                    ELSE NULL
                END as mom_transaction_change_percent,
                pm.prev_month_amount,
                CASE 
                    WHEN pm.prev_month_amount IS NOT NULL AND pm.prev_month_amount > 0
                    THEN ROUND(((pb.total_amount - pm.prev_month_amount) / pm.prev_month_amount * 100), 2)
                    ELSE NULL
                END as mom_amount_change_percent,
                
                -- Rank
                pr.payment_method_rank,
                
                pb.snapshot_version_source,
                CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP
                
            FROM payment_base pb
            LEFT JOIN monthly_totals mt ON 
                mt.year = pb.year AND mt.month = pb.month
            LEFT JOIN top_cat_1 tc1 ON 
                tc1.year = pb.year AND tc1.month = pb.month AND tc1.payment_method_name = pb.payment_method_name
            LEFT JOIN top_cat_2 tc2 ON 
                tc2.year = pb.year AND tc2.month = pb.month AND tc2.payment_method_name = pb.payment_method_name
            LEFT JOIN top_cat_3 tc3 ON 
                tc3.year = pb.year AND tc3.month = pb.month AND tc3.payment_method_name = pb.payment_method_name
            LEFT JOIN prev_month pm ON 
                pb.payment_method_name = pm.payment_method_name
                AND pm.year = pm.prev_year
                AND pm.month = pm.prev_month
            LEFT JOIN payment_ranks pr ON 
                pr.year = pb.year AND pr.month = pb.month AND pr.payment_method_name = pb.payment_method_name
        """)
        
        result = conn.execute(insert_query)
        conn.commit()
        
        inserted_count = result.rowcount
        print(f"✅ Inserted {inserted_count:,} payment method summary records\n")
        
        # ============================================
        # STEP 4: Payment insights
        # ============================================
        print("🔍 STEP 4: Payment method insights...")
        print("-" * 80)
        
        # Market share
        print("\n💳 Payment method market share:")
        market_share = conn.execute(text("""
            SELECT 
                payment_method_name,
                payment_type,
                SUM(transaction_count) as total_txns,
                SUM(total_amount) as total_amt,
                AVG(percent_of_transactions) as avg_txn_share,
                AVG(percent_of_spending) as avg_spending_share
            FROM dst_payment_method_summary
            WHERE snapshot_version_source = :version
            GROUP BY payment_method_name, payment_type
            ORDER BY total_amt DESC
        """), {"version": snapshot_version})
        
        for row in market_share:
            print(f"   {row[0]:25} ({row[1]:15})  {row[2]:6} txns  ${row[3]:10,.2f}  Share: {row[4]:5.1f}% txns / {row[5]:5.1f}% amt")
        
        # Usage trends
        print("\n📈 Payment method trends (MoM):")
        trends = conn.execute(text("""
            SELECT 
                payment_method_name,
                mom_transaction_change_percent,
                mom_amount_change_percent,
                total_amount
            FROM dst_payment_method_summary
            WHERE snapshot_version_source = :version
              AND mom_amount_change_percent IS NOT NULL
            ORDER BY mom_amount_change_percent DESC
            LIMIT 5
        """), {"version": snapshot_version})
        
        for row in trends:
            txn_trend = f"+{row[1]:.1f}%" if row[1] > 0 else f"{row[1]:.1f}%"
            amt_trend = f"+{row[2]:.1f}%" if row[2] > 0 else f"{row[2]:.1f}%"
            print(f"   {row[0]:25} Txns: {txn_trend:>7}  Amount: {amt_trend:>7}  (${row[3]:,.2f})")
        
        # Category preferences
        print("\n🛍️  Payment method category preferences:")
        preferences = conn.execute(text("""
            SELECT 
                payment_method_name,
                top_category_1,
                top_category_1_amount,
                top_category_2,
                top_category_3
            FROM dst_payment_method_summary
            WHERE snapshot_version_source = :version
            ORDER BY total_amount DESC
            LIMIT 5
        """), {"version": snapshot_version})
        
        for row in preferences:
            print(f"   {row[0]:25} Top: {row[1]:15} (${row[2]:,.2f})  #{2}: {row[3] or 'N/A':15}  #{3}: {row[4] or 'N/A'}")

except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()
    exit(1)

print("\n" + "=" * 80)
print("✅ PAYMENT METHOD SUMMARY POPULATION COMPLETED")
print("=" * 80)
print(f"⏰ Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"📦 Batch ID: {batch_id}")
print("\n📝 Next step: Run 06_run_validation.py")
print("=" * 80)

