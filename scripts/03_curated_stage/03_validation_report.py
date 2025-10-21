"""
CURATED Stage - Step 3: Validation and Quality Report
Purpose: Validate snapshot data quality and version consistency

Checks:
1. Version integrity (is_latest flag correctness)
2. Data completeness (no missing required fields)
3. Data consistency (match STG counts)
4. Version growth tracking
5. Date range validation
6. Data quality score analysis
7. Storage size report
8. Duplicate stg_spending_ids check
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
# CHECK 6: DATA QUALITY SCORE ANALYSIS
# ============================================================================

print("\n" + "-" * 80)
print("CHECK 6: Data Quality Score Analysis")
print("-" * 80)

try:
    with engine.connect() as conn:
        # Get quality score statistics
        result = conn.execute(text("""
            SELECT 
                COUNT(*) as total_records,
                ROUND(AVG(data_quality_score), 2) as avg_score,
                MIN(data_quality_score) as min_score,
                MAX(data_quality_score) as max_score,
                ROUND(STDDEV(data_quality_score), 2) as std_dev
            FROM curated_spending_snapshots
            WHERE is_latest = 1
        """))
        
        stats = result.fetchone()
        
        if stats[0] == 0:
            print("⚠️  No records to analyze")
        else:
            print(f"Total Records: {stats[0]:,}")
            print(f"Average Quality Score: {stats[1]}/100")
            print(f"Score Range: {stats[2]} - {stats[3]}")
            print(f"Standard Deviation: {stats[4]}")
            
            # Quality score distribution
            print("\n📊 Quality Score Distribution:")
            result = conn.execute(text("""
                SELECT 
                    CASE 
                        WHEN data_quality_score >= 90 THEN 'A+ (90-100)'
                        WHEN data_quality_score >= 80 THEN 'A  (80-89)'
                        WHEN data_quality_score >= 70 THEN 'B  (70-79)'
                        WHEN data_quality_score >= 60 THEN 'C  (60-69)'
                        WHEN data_quality_score >= 50 THEN 'D  (50-59)'
                        ELSE 'F  (<50)'
                    END as quality_grade,
                    COUNT(*) as record_count,
                    ROUND(COUNT(*)::NUMERIC / SUM(COUNT(*)) OVER () * 100, 2) as percentage
                FROM curated_spending_snapshots
                WHERE is_latest = 1
                GROUP BY quality_grade
                ORDER BY MIN(data_quality_score) DESC
            """))
            
            print(f"{'Grade':<15} {'Count':<12} {'Percentage':<10}")
            print("-" * 80)
            
            total_checked = 0
            for row in result:
                grade = row[0]
                count = row[1]
                pct = row[2]
                print(f"{grade:<15} {count:<12,} {pct:>6.2f}%")
                total_checked += count
            
            # Check for low quality records
            result = conn.execute(text("""
                SELECT COUNT(*) 
                FROM curated_spending_snapshots
                WHERE is_latest = 1 AND data_quality_score < 70
            """))
            
            low_quality_count = result.fetchone()[0]
            if low_quality_count > 0:
                pct = (low_quality_count / stats[0]) * 100
                print(f"\n⚠️  WARNING: {low_quality_count:,} records ({pct:.2f}%) have quality score < 70")
                issues_found.append(f"{low_quality_count} records with low quality scores")
            
            # Quality score by version comparison
            print("\n📈 Quality Score by Version:")
            result = conn.execute(text("""
                SELECT 
                    snapshot_version,
                    COUNT(*) as records,
                    ROUND(AVG(data_quality_score), 2) as avg_score,
                    MIN(data_quality_score) as min_score,
                    MAX(data_quality_score) as max_score
                FROM curated_spending_snapshots
                GROUP BY snapshot_version
                ORDER BY snapshot_version DESC
                LIMIT 5
            """))
            
            print(f"{'Version':<10} {'Records':<12} {'Avg Score':<12} {'Min':<8} {'Max':<8}")
            print("-" * 80)
            
            for row in result:
                ver = f"V{row[0]}"
                records = f"{row[1]:,}"
                avg = f"{row[2]}/100"
                min_s = row[3]
                max_s = row[4]
                print(f"{ver:<10} {records:<12} {avg:<12} {min_s:<8} {max_s:<8}")
            
            if stats[1] >= 80:
                print(f"\n✅ PASS: Average quality score is good ({stats[1]}/100)")
            elif stats[1] >= 70:
                print(f"\n⚠️  WARNING: Average quality score is acceptable ({stats[1]}/100)")
            else:
                print(f"\n❌ FAIL: Average quality score is low ({stats[1]}/100)")
                validation_passed = False
                issues_found.append(f"Low average quality score: {stats[1]}/100")
            
except Exception as e:
    print(f"❌ ERROR: {e}")
    validation_passed = False
    issues_found.append(f"Quality score check failed: {e}")

# ============================================================================
# CHECK 7: STORAGE SIZE REPORT
# ============================================================================

print("\n" + "-" * 80)
print("CHECK 7: Storage Size Report")
print("-" * 80)

try:
    with engine.connect() as conn:
        # Get table size
        result = conn.execute(text("""
            SELECT 
                pg_size_pretty(pg_total_relation_size('curated_spending_snapshots')) as total_size,
                pg_size_pretty(pg_relation_size('curated_spending_snapshots')) as table_size,
                pg_size_pretty(pg_indexes_size('curated_spending_snapshots')) as indexes_size
        """))
        
        sizes = result.fetchone()
        print(f"Total Size (Table + Indexes): {sizes[0]}")
        print(f"Table Size: {sizes[1]}")
        print(f"Indexes Size: {sizes[2]}")
        
        # Get row count and calculate per-row size
        result = conn.execute(text("""
            SELECT 
                COUNT(*) as total_rows,
                pg_total_relation_size('curated_spending_snapshots') as total_bytes
            FROM curated_spending_snapshots
        """))
        
        stats = result.fetchone()
        if stats[0] > 0:
            bytes_per_row = stats[1] / stats[0]
            print(f"\nTotal Records: {stats[0]:,}")
            print(f"Average Size per Record: {bytes_per_row:,.0f} bytes ({bytes_per_row/1024:.2f} KB)")
        
        # Size by version
        print("\n📊 Storage by Version:")
        result = conn.execute(text("""
            SELECT 
                snapshot_version,
                COUNT(*) as record_count,
                pg_size_pretty(
                    COUNT(*) * (
                        SELECT pg_total_relation_size('curated_spending_snapshots')::NUMERIC / 
                               NULLIF(COUNT(*), 0)
                        FROM curated_spending_snapshots
                    )::BIGINT
                ) as estimated_size
            FROM curated_spending_snapshots
            GROUP BY snapshot_version
            ORDER BY snapshot_version DESC
            LIMIT 10
        """))
        
        print(f"{'Version':<10} {'Records':<12} {'Est. Size':<15}")
        print("-" * 80)
        
        for row in result:
            ver = f"V{row[0]}"
            records = f"{row[1]:,}"
            size = row[2]
            print(f"{ver:<10} {records:<12} {size:<15}")
        
        # Storage recommendations
        print("\n💡 Storage Recommendations:")
        
        version_count = conn.execute(text("""
            SELECT COUNT(DISTINCT snapshot_version) 
            FROM curated_spending_snapshots
        """)).fetchone()[0]
        
        if version_count > 30:
            print(f"   ⚠️  You have {version_count} versions. Consider:")
            print(f"      - Archive old versions to cold storage")
            print(f"      - Delete versions older than 30 days if not needed")
            issues_found.append(f"{version_count} versions consuming storage")
        elif version_count > 10:
            print(f"   ℹ️  You have {version_count} versions - monitor growth")
        else:
            print(f"   ✓ Storage usage is reasonable ({version_count} versions)")
        
        print(f"\n✅ PASS: Storage report generated successfully")
            
except Exception as e:
    print(f"❌ ERROR: {e}")
    validation_passed = False
    issues_found.append(f"Storage size check failed: {e}")

# ============================================================================
# CHECK 8: DUPLICATE STG_SPENDING_IDS WITHIN SAME VERSION
# ============================================================================

print("\n" + "-" * 80)
print("CHECK 8: Duplicate stg_spending_ids Check")
print("-" * 80)

try:
    with engine.connect() as conn:
        # Check for duplicates within same version
        result = conn.execute(text("""
            SELECT 
                snapshot_version,
                stg_spending_id,
                COUNT(*) as duplicate_count
            FROM curated_spending_snapshots
            GROUP BY snapshot_version, stg_spending_id
            HAVING COUNT(*) > 1
            ORDER BY snapshot_version DESC, duplicate_count DESC
            LIMIT 20
        """))
        
        duplicates = result.fetchall()
        
        if len(duplicates) == 0:
            print("✅ PASS: No duplicate stg_spending_ids found within same version")
        else:
            print(f"❌ FAIL: Found {len(duplicates)} cases of duplicate stg_spending_ids!")
            validation_passed = False
            
            # Count total duplicate records
            total_dup_records = sum([row[2] - 1 for row in duplicates])  # -1 because 1 is valid
            issues_found.append(f"{len(duplicates)} duplicate stg_spending_ids found")
            
            print(f"\n⚠️  Showing first 20 duplicates:")
            print(f"{'Version':<10} {'STG ID':<12} {'Count':<10}")
            print("-" * 80)
            
            for row in duplicates:
                ver = f"V{row[0]}"
                stg_id = row[1]
                count = row[2]
                print(f"{ver:<10} {stg_id:<12} {count:<10}")
            
            print(f"\n💡 This indicates a data integrity issue - each stg_spending_id should")
            print(f"   appear exactly once per version. Total duplicate records: {total_dup_records}")
        
        # Check across all versions (should be duplicates by design)
        result = conn.execute(text("""
            SELECT 
                stg_spending_id,
                COUNT(DISTINCT snapshot_version) as version_count
            FROM curated_spending_snapshots
            GROUP BY stg_spending_id
            HAVING COUNT(DISTINCT snapshot_version) > 1
            LIMIT 5
        """))
        
        cross_version = result.fetchall()
        
        if len(cross_version) > 0:
            print(f"\nℹ️  Info: {len(cross_version)} stg_spending_ids appear in multiple versions")
            print("   (This is EXPECTED behavior - same IDs across versions)")
            
            # Show example
            example = cross_version[0]
            print(f"   Example: stg_spending_id {example[0]} appears in {example[1]} versions")
            
except Exception as e:
    print(f"❌ ERROR: {e}")
    validation_passed = False
    issues_found.append(f"Duplicate check failed: {e}")

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

