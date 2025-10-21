"""
CURATED Stage - Step 3: Validation and Quality Report
Purpose: Validate snapshot data quality and version consistency

Checks:
1. Version integrity (is_latest flag correctness)
2. Data completeness (no missing required fields)
3. Data consistency (match STG counts)
4. Version growth tracking
5. Date range validation
"""

from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
from datetime import datetime

# Setup connection
env_paths = ['.env', '../.env', '../../.env']
for env_path in env_paths:
    if os.path.exists(env_path):
        load_dotenv(env_path)
        break

connection_string = os.getenv('SUPABASE_CONNECTION_STRING')

if not connection_string:
    print("❌ Error: SUPABASE_CONNECTION_STRING not found in .env file")
    exit(1)

engine = create_engine(connection_string)

print("=" * 80)
print("CURATED STAGE - VALIDATION AND QUALITY REPORT")
print("=" * 80)
print(f"⏰ Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

# Track validation results
validation_passed = True
issues_found = []

# ============================================================================
# CHECK 1: VERSION INTEGRITY
# ============================================================================

print("-" * 80)
print("CHECK 1: Version Integrity (is_latest Flag)")
print("-" * 80)

try:
    with engine.connect() as conn:
        # Check how many versions have is_latest = 1
        result = conn.execute(text("""
            SELECT COUNT(DISTINCT snapshot_version) as latest_versions
            FROM curated_spending_snapshots
            WHERE is_latest = 1
        """))
        
        latest_count = result.fetchone()[0]
        
        if latest_count == 0:
            print("⚠️  WARNING: No versions marked as is_latest = 1")
            print("   This is OK if the table is empty")
        elif latest_count == 1:
            print("✅ PASS: Exactly 1 version marked as is_latest = 1")
            
            # Get the latest version details
            result = conn.execute(text("""
                SELECT 
                    snapshot_version,
                    snapshot_date,
                    COUNT(*) as record_count
                FROM curated_spending_snapshots
                WHERE is_latest = 1
                GROUP BY snapshot_version, snapshot_date
            """))
            
            latest = result.fetchone()
            print(f"   Latest Version: {latest[0]}")
            print(f"   Snapshot Date: {latest[1]}")
            print(f"   Record Count: {latest[2]:,}")
        else:
            print(f"❌ FAIL: {latest_count} versions marked as is_latest = 1 (should be exactly 1)")
            validation_passed = False
            issues_found.append(f"Multiple versions ({latest_count}) have is_latest = 1")
        
        # Check for orphaned records (is_latest not 0 or 1)
        result = conn.execute(text("""
            SELECT COUNT(*) 
            FROM curated_spending_snapshots
            WHERE is_latest NOT IN (0, 1)
        """))
        
        invalid_count = result.fetchone()[0]
        if invalid_count > 0:
            print(f"❌ FAIL: {invalid_count} records have invalid is_latest values")
            validation_passed = False
            issues_found.append(f"{invalid_count} records with invalid is_latest values")
        else:
            print("✅ PASS: All records have valid is_latest values (0 or 1)")
            
except Exception as e:
    print(f"❌ ERROR: {e}")
    validation_passed = False
    issues_found.append(f"Version integrity check failed: {e}")

# ============================================================================
# CHECK 2: DATA COMPLETENESS
# ============================================================================

print("\n" + "-" * 80)
print("CHECK 2: Data Completeness (Required Fields)")
print("-" * 80)

try:
    with engine.connect() as conn:
        # Check for NULL values in required fields
        result = conn.execute(text("""
            SELECT 
                COUNT(*) as total_records,
                SUM(CASE WHEN snapshot_version IS NULL THEN 1 ELSE 0 END) as null_version,
                SUM(CASE WHEN snapshot_date IS NULL THEN 1 ELSE 0 END) as null_date,
                SUM(CASE WHEN stg_spending_id IS NULL THEN 1 ELSE 0 END) as null_stg_id,
                SUM(CASE WHEN person_id IS NULL THEN 1 ELSE 0 END) as null_person,
                SUM(CASE WHEN category_id IS NULL THEN 1 ELSE 0 END) as null_category,
                SUM(CASE WHEN spending_date IS NULL THEN 1 ELSE 0 END) as null_spending_date,
                SUM(CASE WHEN amount_cleaned IS NULL THEN 1 ELSE 0 END) as null_amount
            FROM curated_spending_snapshots
        """))
        
        stats = result.fetchone()
        total = stats[0]
        
        if total == 0:
            print("⚠️  WARNING: No records found in curated_spending_snapshots")
        else:
            null_checks = [
                ('snapshot_version', stats[1]),
                ('snapshot_date', stats[2]),
                ('stg_spending_id', stats[3]),
                ('person_id', stats[4]),
                ('category_id', stats[5]),
                ('spending_date', stats[6]),
                ('amount_cleaned', stats[7])
            ]
            
            has_nulls = False
            for field, null_count in null_checks:
                if null_count > 0:
                    print(f"❌ FAIL: {null_count:,} records have NULL {field}")
                    validation_passed = False
                    issues_found.append(f"{null_count} NULL values in {field}")
                    has_nulls = True
            
            if not has_nulls:
                print(f"✅ PASS: All {total:,} records have complete required fields")
        
        # Check denormalized fields (should mostly be populated)
        result = conn.execute(text("""
            SELECT 
                SUM(CASE WHEN person_name IS NULL THEN 1 ELSE 0 END) as null_person_name,
                SUM(CASE WHEN category_name IS NULL THEN 1 ELSE 0 END) as null_category_name,
                SUM(CASE WHEN location_name IS NULL THEN 1 ELSE 0 END) as null_location_name,
                SUM(CASE WHEN payment_method_name IS NULL THEN 1 ELSE 0 END) as null_payment_name
            FROM curated_spending_snapshots
        """))
        
        denorm = result.fetchone()
        denorm_issues = []
        
        if denorm[0] > 0: denorm_issues.append(f"{denorm[0]:,} missing person_name")
        if denorm[1] > 0: denorm_issues.append(f"{denorm[1]:,} missing category_name")
        if denorm[2] > 0: denorm_issues.append(f"{denorm[2]:,} missing location_name")
        if denorm[3] > 0: denorm_issues.append(f"{denorm[3]:,} missing payment_method_name")
        
        if denorm_issues:
            print(f"⚠️  WARNING: Denormalized fields have NULL values:")
            for issue in denorm_issues:
                print(f"   - {issue}")
        else:
            print("✅ PASS: All denormalized dimension fields populated")
            
except Exception as e:
    print(f"❌ ERROR: {e}")
    validation_passed = False
    issues_found.append(f"Data completeness check failed: {e}")

# ============================================================================
# CHECK 3: DATA CONSISTENCY WITH STG
# ============================================================================

print("\n" + "-" * 80)
print("CHECK 3: Data Consistency with STG Layer")
print("-" * 80)

try:
    with engine.connect() as conn:
        # Get STG record count
        result = conn.execute(text("""
            SELECT COUNT(*) FROM stg_fact_spending
        """))
        stg_count = result.fetchone()[0]
        
        # Get latest CURATED record count
        result = conn.execute(text("""
            SELECT COUNT(*) 
            FROM curated_spending_snapshots
            WHERE is_latest = 1
        """))
        curated_latest_count = result.fetchone()[0]
        
        print(f"STG Layer Records: {stg_count:,}")
        print(f"CURATED Latest Records: {curated_latest_count:,}")
        
        if stg_count == curated_latest_count:
            print("✅ PASS: Latest CURATED snapshot matches STG count")
        else:
            diff = abs(stg_count - curated_latest_count)
            print(f"❌ FAIL: Count mismatch (difference: {diff:,})")
            validation_passed = False
            issues_found.append(f"STG/CURATED count mismatch: {diff} records")
        
        # Check if all STG spending_ids are in latest CURATED
        result = conn.execute(text("""
            SELECT COUNT(*) as missing_count
            FROM stg_fact_spending s
            WHERE NOT EXISTS (
                SELECT 1 
                FROM curated_spending_snapshots c
                WHERE c.stg_spending_id = s.spending_id
                  AND c.is_latest = 1
            )
        """))
        
        missing = result.fetchone()[0]
        if missing > 0:
            print(f"❌ FAIL: {missing:,} STG records missing from latest CURATED")
            validation_passed = False
            issues_found.append(f"{missing} STG records not in CURATED")
        else:
            print("✅ PASS: All STG records present in latest CURATED snapshot")
            
except Exception as e:
    print(f"❌ ERROR: {e}")
    validation_passed = False
    issues_found.append(f"STG consistency check failed: {e}")

# ============================================================================
# CHECK 4: VERSION GROWTH TRACKING
# ============================================================================

print("\n" + "-" * 80)
print("CHECK 4: Version Growth Tracking")
print("-" * 80)

try:
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT 
                COUNT(DISTINCT snapshot_version) as total_versions,
                MIN(snapshot_version) as first_version,
                MAX(snapshot_version) as latest_version,
                COUNT(*) as total_records
            FROM curated_spending_snapshots
        """))
        
        stats = result.fetchone()
        
        if stats[0] == 0:
            print("⚠️  No versions found")
        else:
            print(f"Total Versions: {stats[0]}")
            print(f"Version Range: {stats[1]} to {stats[2]}")
            print(f"Total Records: {stats[3]:,}")
            print(f"Average Records per Version: {stats[3] // stats[0]:,}")
            
            # Show version history
            print("\n📊 Version History:")
            result = conn.execute(text("""
                SELECT 
                    snapshot_version,
                    snapshot_date,
                    is_latest,
                    COUNT(*) as record_count,
                    MIN(spending_date) as earliest_transaction,
                    MAX(spending_date) as latest_transaction
                FROM curated_spending_snapshots
                GROUP BY snapshot_version, snapshot_date, is_latest
                ORDER BY snapshot_version DESC
                LIMIT 10
            """))
            
            print(f"{'Ver':<5} {'Date':<12} {'Latest':<7} {'Records':<12} {'Transaction Range':<30}")
            print("-" * 80)
            
            for row in result:
                ver = row[0]
                date = row[1]
                latest = "✓" if row[2] == 1 else ""
                count = f"{row[3]:,}"
                trans_range = f"{row[4]} to {row[5]}"
                print(f"{ver:<5} {date!s:<12} {latest:<7} {count:<12} {trans_range:<30}")
            
            # Growth analysis
            if stats[0] > 1:
                print("\n📈 Version-over-Version Growth:")
                result = conn.execute(text("""
                    WITH version_counts AS (
                        SELECT 
                            snapshot_version,
                            COUNT(*) as record_count,
                            LAG(COUNT(*)) OVER (ORDER BY snapshot_version) as prev_count
                        FROM curated_spending_snapshots
                        GROUP BY snapshot_version
                    )
                    SELECT 
                        snapshot_version,
                        record_count,
                        CASE 
                            WHEN prev_count IS NULL THEN record_count
                            ELSE record_count - prev_count
                        END as growth,
                        CASE 
                            WHEN prev_count IS NULL THEN 0
                            ELSE ROUND(((record_count - prev_count)::NUMERIC / prev_count * 100), 2)
                        END as growth_pct
                    FROM version_counts
                    ORDER BY snapshot_version DESC
                    LIMIT 5
                """))
                
                print(f"{'Version':<10} {'Records':<12} {'Growth':<12} {'Growth %':<10}")
                print("-" * 80)
                
                for row in result:
                    ver = f"V{row[0]}"
                    records = f"{row[1]:,}"
                    growth = f"+{row[2]:,}" if row[2] >= 0 else f"{row[2]:,}"
                    growth_pct = f"{row[3]:+.2f}%" if row[3] != 0 else "Initial"
                    print(f"{ver:<10} {records:<12} {growth:<12} {growth_pct:<10}")
            
            print("\n✅ PASS: Version tracking functional")
            
except Exception as e:
    print(f"❌ ERROR: {e}")
    validation_passed = False
    issues_found.append(f"Version tracking check failed: {e}")

# ============================================================================
# CHECK 5: DATE RANGE VALIDATION
# ============================================================================

print("\n" + "-" * 80)
print("CHECK 5: Date Range Validation")
print("-" * 80)

try:
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT 
                MIN(spending_date) as earliest_transaction,
                MAX(spending_date) as latest_transaction,
                MIN(snapshot_date) as first_snapshot,
                MAX(snapshot_date) as latest_snapshot,
                MAX(snapshot_date) - MIN(snapshot_date) as snapshot_span_days
            FROM curated_spending_snapshots
        """))
        
        dates = result.fetchone()
        
        if dates[0] is None:
            print("⚠️  No date data available")
        else:
            print(f"Transaction Date Range: {dates[0]} to {dates[1]}")
            print(f"Snapshot Date Range: {dates[2]} to {dates[3]}")
            print(f"Snapshot Span: {dates[4]} days")
            
            # Check for future dates
            result = conn.execute(text("""
                SELECT COUNT(*) 
                FROM curated_spending_snapshots
                WHERE spending_date > CURRENT_DATE
            """))
            
            future_count = result.fetchone()[0]
            if future_count > 0:
                print(f"⚠️  WARNING: {future_count:,} records have future spending dates")
                issues_found.append(f"{future_count} records with future dates")
            else:
                print("✅ PASS: No future-dated transactions")
            
            # Check for very old dates (potential data issues)
            result = conn.execute(text("""
                SELECT COUNT(*) 
                FROM curated_spending_snapshots
                WHERE spending_date < '2020-01-01'
            """))
            
            old_count = result.fetchone()[0]
            if old_count > 0:
                print(f"⚠️  INFO: {old_count:,} records dated before 2020")
            
            print("✅ PASS: Date ranges are reasonable")
            
except Exception as e:
    print(f"❌ ERROR: {e}")
    validation_passed = False
    issues_found.append(f"Date validation check failed: {e}")

# ============================================================================
# SUMMARY
# ============================================================================

print("\n" + "=" * 80)
print("VALIDATION SUMMARY")
print("=" * 80)

if validation_passed and len(issues_found) == 0:
    print("\n🎉 ALL VALIDATIONS PASSED!")
    print("   CURATED layer is healthy and ready for analysis")
elif len(issues_found) == 0:
    print("\n✅ VALIDATIONS PASSED (with warnings)")
    print("   Check warnings above for potential improvements")
else:
    print("\n❌ VALIDATION FAILED")
    print(f"   Found {len(issues_found)} issue(s):\n")
    for i, issue in enumerate(issues_found, 1):
        print(f"   {i}. {issue}")

print("\n" + "=" * 80)
print(f"Report completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 80)

# Exit with appropriate code
if validation_passed:
    exit(0)
else:
    exit(1)

