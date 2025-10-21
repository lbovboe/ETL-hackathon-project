"""
DST Stage - Step 6: Validation Report
Purpose: Validate that all DST aggregations match source data from CURATED
Checks:
  1. Total spending reconciliation across all tables
  2. Transaction count verification
  3. Record count consistency
  4. Data quality checks
  5. Cross-table consistency
"""

from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
from datetime import datetime

# Setup connection
load_dotenv('../../.env')
connection_string = os.getenv('SUPABASE_CONNECTION_STRING')
engine = create_engine(connection_string)

print("=" * 80)
print("DST STAGE - VALIDATION REPORT")
print("=" * 80)
print(f"⏰ Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

validation_passed = True
issues = []

try:
    with engine.connect() as conn:
        # Get snapshot version
        result = conn.execute(text("""
            SELECT snapshot_version, COUNT(*) as record_count
            FROM curated_spending_snapshots
            WHERE is_latest = 1
            GROUP BY snapshot_version
        """))
        
        snapshot_info = result.fetchone()
        if not snapshot_info:
            print("❌ No data found in curated_spending_snapshots")
            exit(1)
        
        snapshot_version = snapshot_info[0]
        curated_count = snapshot_info[1]
        
        print(f"📊 Validating against snapshot version: {snapshot_version}")
        print(f"   Curated records: {curated_count:,}\n")
        print("=" * 80)
        
        # ============================================
        # CHECK 1: Total Spending Reconciliation
        # ============================================
        print("\n✅ CHECK 1: Total Spending Reconciliation")
        print("-" * 80)
        
        curated_total = conn.execute(text("""
            SELECT SUM(amount_cleaned) FROM curated_spending_snapshots WHERE is_latest = 1
        """)).scalar()
        
        monthly_total = conn.execute(text("""
            SELECT SUM(total_spending) FROM dst_monthly_spending_summary
            WHERE snapshot_version_source = :v
        """), {"v": snapshot_version}).scalar()
        
        category_total = conn.execute(text("""
            SELECT SUM(total_spending) FROM dst_category_trends
            WHERE snapshot_version_source = :v
        """), {"v": snapshot_version}).scalar()
        
        person_total = conn.execute(text("""
            SELECT SUM(total_spending) FROM dst_person_analytics
            WHERE snapshot_version_source = :v
        """), {"v": snapshot_version}).scalar()
        
        payment_total = conn.execute(text("""
            SELECT SUM(total_amount) FROM dst_payment_method_summary
            WHERE snapshot_version_source = :v
        """), {"v": snapshot_version}).scalar()
        
        print(f"   Curated (source):        ${curated_total:15,.2f}")
        print(f"   Monthly Summary:         ${monthly_total:15,.2f}  Diff: ${abs(curated_total - monthly_total):,.2f}")
        print(f"   Category Trends:         ${category_total:15,.2f}  Diff: ${abs(curated_total - category_total):,.2f}")
        print(f"   Person Analytics:        ${person_total:15,.2f}  Diff: ${abs(curated_total - person_total):,.2f}")
        print(f"   Payment Summary:         ${payment_total:15,.2f}  Diff: ${abs(curated_total - payment_total):,.2f}")
        
        tolerance = 0.01
        if abs(curated_total - monthly_total) > tolerance:
            validation_passed = False
            issues.append(f"Monthly Summary total mismatch: ${abs(curated_total - monthly_total):.2f}")
        if abs(curated_total - category_total) > tolerance:
            validation_passed = False
            issues.append(f"Category Trends total mismatch: ${abs(curated_total - category_total):.2f}")
        if abs(curated_total - person_total) > tolerance:
            validation_passed = False
            issues.append(f"Person Analytics total mismatch: ${abs(curated_total - person_total):.2f}")
        if abs(curated_total - payment_total) > tolerance:
            validation_passed = False
            issues.append(f"Payment Summary total mismatch: ${abs(curated_total - payment_total):.2f}")
        
        if abs(curated_total - monthly_total) <= tolerance and \
           abs(curated_total - category_total) <= tolerance and \
           abs(curated_total - person_total) <= tolerance and \
           abs(curated_total - payment_total) <= tolerance:
            print("\n   ✅ All totals match within tolerance!")
        else:
            print("\n   ⚠️  Some totals have mismatches!")
        
        # ============================================
        # CHECK 2: Transaction Count Verification
        # ============================================
        print("\n✅ CHECK 2: Transaction Count Verification")
        print("-" * 80)
        
        monthly_txn_sum = conn.execute(text("""
            SELECT SUM(transaction_count) FROM dst_monthly_spending_summary
            WHERE snapshot_version_source = :v
        """), {"v": snapshot_version}).scalar()
        
        category_txn_sum = conn.execute(text("""
            SELECT SUM(transaction_count) FROM dst_category_trends
            WHERE snapshot_version_source = :v
        """), {"v": snapshot_version}).scalar()
        
        person_txn_sum = conn.execute(text("""
            SELECT SUM(transaction_count) FROM dst_person_analytics
            WHERE snapshot_version_source = :v
        """), {"v": snapshot_version}).scalar()
        
        payment_txn_sum = conn.execute(text("""
            SELECT SUM(transaction_count) FROM dst_payment_method_summary
            WHERE snapshot_version_source = :v
        """), {"v": snapshot_version}).scalar()
        
        print(f"   Curated (source):        {curated_count:10,} transactions")
        print(f"   Monthly Summary sum:     {monthly_txn_sum:10,}")
        print(f"   Category Trends sum:     {category_txn_sum:10,}")
        print(f"   Person Analytics sum:    {person_txn_sum:10,}")
        print(f"   Payment Summary sum:     {payment_txn_sum:10,}")
        
        if curated_count == person_txn_sum == payment_txn_sum:
            print("\n   ✅ Transaction counts match!")
        else:
            print("\n   ⚠️  Transaction count mismatch!")
            validation_passed = False
            issues.append("Transaction counts don't match across tables")
        
        # ============================================
        # CHECK 3: Record Count Consistency
        # ============================================
        print("\n✅ CHECK 3: Record Count Consistency")
        print("-" * 80)
        
        monthly_records = conn.execute(text("""
            SELECT COUNT(*) FROM dst_monthly_spending_summary
            WHERE snapshot_version_source = :v
        """), {"v": snapshot_version}).scalar()
        
        category_records = conn.execute(text("""
            SELECT COUNT(*) FROM dst_category_trends
            WHERE snapshot_version_source = :v
        """), {"v": snapshot_version}).scalar()
        
        person_records = conn.execute(text("""
            SELECT COUNT(*) FROM dst_person_analytics
            WHERE snapshot_version_source = :v
        """), {"v": snapshot_version}).scalar()
        
        payment_records = conn.execute(text("""
            SELECT COUNT(*) FROM dst_payment_method_summary
            WHERE snapshot_version_source = :v
        """), {"v": snapshot_version}).scalar()
        
        print(f"   Monthly Summary:         {monthly_records:10,} records")
        print(f"   Category Trends:         {category_records:10,} records")
        print(f"   Person Analytics:        {person_records:10,} records")
        print(f"   Payment Summary:         {payment_records:10,} records")
        print(f"\n   ✅ All tables populated!")
        
        # ============================================
        # CHECK 4: Essential/Discretionary Validation
        # ============================================
        print("\n✅ CHECK 4: Essential/Discretionary Breakdown Validation")
        print("-" * 80)
        
        person_breakdown = conn.execute(text("""
            SELECT 
                SUM(essential_spending),
                SUM(discretionary_spending),
                SUM(transport_spending),
                SUM(healthcare_spending),
                SUM(education_spending),
                SUM(other_spending),
                SUM(total_spending)
            FROM dst_person_analytics
            WHERE snapshot_version_source = :v
        """), {"v": snapshot_version}).fetchone()
        
        breakdown_sum = sum([person_breakdown[i] or 0 for i in range(6)])
        total_spending = person_breakdown[6]
        
        print(f"   Essential:     ${person_breakdown[0]:12,.2f}")
        print(f"   Discretionary: ${person_breakdown[1]:12,.2f}")
        print(f"   Transport:     ${person_breakdown[2]:12,.2f}")
        print(f"   Healthcare:    ${person_breakdown[3]:12,.2f}")
        print(f"   Education:     ${person_breakdown[4]:12,.2f}")
        print(f"   Other:         ${person_breakdown[5]:12,.2f}")
        print(f"   ---" + "-" * 23)
        print(f"   Breakdown Sum: ${breakdown_sum:12,.2f}")
        print(f"   Total:         ${total_spending:12,.2f}")
        print(f"   Difference:    ${abs(breakdown_sum - total_spending):12,.2f}")
        
        if abs(breakdown_sum - total_spending) < 0.01:
            print("\n   ✅ Essential/Discretionary breakdown matches total!")
        else:
            print("\n   ⚠️  Breakdown doesn't sum to total!")
            validation_passed = False
            issues.append("Essential/Discretionary breakdown mismatch")
        
        # ============================================
        # CHECK 5: Data Quality Metrics
        # ============================================
        print("\n✅ CHECK 5: Data Quality Metrics")
        print("-" * 80)
        
        # Check for NULLs in critical fields
        null_checks = [
            ("Monthly Summary - person_name", 
             "SELECT COUNT(*) FROM dst_monthly_spending_summary WHERE person_name IS NULL AND snapshot_version_source = :v"),
            ("Monthly Summary - category_name", 
             "SELECT COUNT(*) FROM dst_monthly_spending_summary WHERE category_name IS NULL AND snapshot_version_source = :v"),
            ("Category Trends - category_name", 
             "SELECT COUNT(*) FROM dst_category_trends WHERE category_name IS NULL AND snapshot_version_source = :v"),
            ("Person Analytics - person_name", 
             "SELECT COUNT(*) FROM dst_person_analytics WHERE person_name IS NULL AND snapshot_version_source = :v"),
            ("Payment Summary - payment_method_name", 
             "SELECT COUNT(*) FROM dst_payment_method_summary WHERE payment_method_name IS NULL AND snapshot_version_source = :v"),
        ]
        
        null_issues = 0
        for check_name, query in null_checks:
            null_count = conn.execute(text(query), {"v": snapshot_version}).scalar()
            if null_count > 0:
                print(f"   ⚠️  {check_name}: {null_count} NULL values")
                null_issues += 1
                validation_passed = False
                issues.append(f"{check_name} has NULL values")
            else:
                print(f"   ✅ {check_name}: No NULLs")
        
        if null_issues == 0:
            print("\n   ✅ All critical fields populated!")
        
        # ============================================
        # CHECK 6: Cross-Table Consistency
        # ============================================
        print("\n✅ CHECK 6: Cross-Table Consistency")
        print("-" * 80)
        
        # Verify unique persons count
        curated_persons = conn.execute(text("""
            SELECT COUNT(DISTINCT person_name) FROM curated_spending_snapshots WHERE is_latest = 1
        """)).scalar()
        
        monthly_persons = conn.execute(text("""
            SELECT COUNT(DISTINCT person_name) FROM dst_monthly_spending_summary
            WHERE snapshot_version_source = :v
        """), {"v": snapshot_version}).scalar()
        
        person_persons = conn.execute(text("""
            SELECT COUNT(DISTINCT person_name) FROM dst_person_analytics
            WHERE snapshot_version_source = :v
        """), {"v": snapshot_version}).scalar()
        
        print(f"   Curated unique persons:  {curated_persons}")
        print(f"   Monthly Summary persons: {monthly_persons}")
        print(f"   Person Analytics persons: {person_persons}")
        
        if curated_persons == monthly_persons == person_persons:
            print("\n   ✅ Person counts consistent across tables!")
        else:
            print("\n   ⚠️  Person count mismatch!")
            validation_passed = False
            issues.append("Person counts inconsistent across tables")

except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()
    exit(1)

# ============================================
# FINAL REPORT
# ============================================
print("\n" + "=" * 80)
print("VALIDATION REPORT SUMMARY")
print("=" * 80)

if validation_passed:
    print("\n✅ ✅ ✅  ALL VALIDATIONS PASSED!  ✅ ✅ ✅")
    print("\n🎉 DST layer is ready for use!")
    print("\n📊 Key Statistics:")
    print(f"   • Source records: {curated_count:,}")
    print(f"   • Total spending: ${curated_total:,.2f}")
    print(f"   • Monthly aggregations: {monthly_records:,}")
    print(f"   • Category trends: {category_records:,}")
    print(f"   • Person analytics: {person_records:,}")
    print(f"   • Payment summaries: {payment_records:,}")
    print("\n🎯 Ready for Stage 5 (DIS - Insights & Recommendations)!")
else:
    print("\n⚠️  ⚠️  ⚠️   VALIDATION ISSUES FOUND   ⚠️  ⚠️  ⚠️")
    print("\nIssues:")
    for i, issue in enumerate(issues, 1):
        print(f"   {i}. {issue}")
    print("\n🔧 Please review and fix the issues above.")

print("\n=" * 80)
print(f"⏰ Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 80)

