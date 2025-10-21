"""
CURATED Stage - Step 2: Create Versioned Snapshot
Purpose: Create a new version snapshot of ALL historical spending data from STG

How it works:
1. Get next version number (MAX + 1)
2. Update all existing snapshots: is_latest = 0
3. Copy ALL data from STG with new version number
4. Set new snapshot: is_latest = 1

Example:
- Oct 20: Run script → Version 1 with 1000 records (is_latest=1)
- Oct 21: Run script → Version 1 (is_latest=0), Version 2 with 1050 records (is_latest=1)
- Oct 22: Run script → Version 1&2 (is_latest=0), Version 3 with 1080 records (is_latest=1)
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
    print("   Please ensure .env file exists with SUPABASE_CONNECTION_STRING")
    exit(1)

engine = create_engine(connection_string)

# Generate batch ID for this snapshot
SNAPSHOT_DATE = datetime.now().date()
BATCH_ID = f"CURATED_SNAPSHOT_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

print("=" * 80)
print("CURATED STAGE - CREATE VERSIONED SNAPSHOT")
print("=" * 80)
print(f"⏰ Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"📅 Snapshot Date: {SNAPSHOT_DATE}")
print(f"📦 Batch ID: {BATCH_ID}\n")

# ============================================================================
# STEP 1: GET NEXT VERSION NUMBER
# ============================================================================

print("-" * 80)
print("STEP 1: Determine Next Version Number")
print("-" * 80)

try:
    with engine.connect() as conn:
        # Get current max version
        result = conn.execute(text("""
            SELECT COALESCE(MAX(snapshot_version), 0) as max_version
            FROM curated_spending_snapshots
        """))
        
        current_max_version = result.fetchone()[0]
        next_version = current_max_version + 1
        
        print(f"✓ Current max version: {current_max_version}")
        print(f"✓ Next version will be: {next_version}")
        
        # Check how many versions exist
        result = conn.execute(text("""
            SELECT COUNT(DISTINCT snapshot_version) as version_count
            FROM curated_spending_snapshots
        """))
        version_count = result.fetchone()[0]
        print(f"✓ Total existing versions: {version_count}")
        
        # Check current latest version
        if version_count > 0:
            result = conn.execute(text("""
                SELECT snapshot_version, snapshot_date, COUNT(*) as record_count
                FROM curated_spending_snapshots
                WHERE is_latest = 1
                GROUP BY snapshot_version, snapshot_date
            """))
            latest = result.fetchone()
            if latest:
                print(f"✓ Current latest version: {latest[0]} (Date: {latest[1]}, Records: {latest[2]})")
        
except Exception as e:
    print(f"❌ Error determining version number: {e}")
    exit(1)

# ============================================================================
# STEP 2: CHECK STG DATA AVAILABILITY
# ============================================================================

print("\n" + "-" * 80)
print("STEP 2: Check STG Data Availability")
print("-" * 80)

try:
    with engine.connect() as conn:
        # Check if STG has data
        result = conn.execute(text("""
            SELECT COUNT(*) as total_records
            FROM stg_fact_spending
        """))
        stg_count = result.fetchone()[0]
        
        if stg_count == 0:
            print("❌ ERROR: No data found in stg_fact_spending table!")
            print("   Please run STG transformation first: 02_transform_and_load_stg.py")
            exit(1)
        
        print(f"✓ STG has {stg_count:,} records to snapshot")
        
        # Check if vw_stg_spending_complete exists
        result = conn.execute(text("""
            SELECT COUNT(*) FROM information_schema.views
            WHERE table_name = 'vw_stg_spending_complete'
        """))
        view_exists = result.fetchone()[0]
        
        if view_exists == 0:
            print("❌ ERROR: View 'vw_stg_spending_complete' not found!")
            print("   Please ensure STG stage is properly set up")
            exit(1)
        
        print("✓ View 'vw_stg_spending_complete' is available")
        
        # Preview STG data structure
        result = conn.execute(text("""
            SELECT 
                MIN(spending_date) as earliest_date,
                MAX(spending_date) as latest_date,
                COUNT(DISTINCT person_name) as unique_persons,
                COUNT(DISTINCT category_name) as unique_categories
            FROM vw_stg_spending_complete
        """))
        stats = result.fetchone()
        print(f"✓ Date range: {stats[0]} to {stats[1]}")
        print(f"✓ Unique persons: {stats[2]}")
        print(f"✓ Unique categories: {stats[3]}")
        
except Exception as e:
    print(f"❌ Error checking STG data: {e}")
    exit(1)

# ============================================================================
# STEP 3: UPDATE EXISTING SNAPSHOTS (is_latest = 0)
# ============================================================================

print("\n" + "-" * 80)
print("STEP 3: Update Existing Snapshots to Historical (is_latest = 0)")
print("-" * 80)

try:
    with engine.connect() as conn:
        # Start transaction
        trans = conn.begin()
        
        try:
            if version_count > 0:
                # Update all existing snapshots to is_latest = 0
                result = conn.execute(text("""
                    UPDATE curated_spending_snapshots
                    SET is_latest = 0
                    WHERE is_latest = 1
                """))
                
                updated_count = result.rowcount
                print(f"✓ Updated {updated_count} records to is_latest = 0")
                
                # Verify update
                result = conn.execute(text("""
                    SELECT COUNT(*) FROM curated_spending_snapshots
                    WHERE is_latest = 1
                """))
                latest_count = result.fetchone()[0]
                
                if latest_count != 0:
                    raise Exception(f"ERROR: Still have {latest_count} records with is_latest = 1!")
                
                print("✓ Verified: No records have is_latest = 1 now")
            else:
                print("✓ No existing snapshots to update (this is the first version)")
            
            # Commit the update
            trans.commit()
            print("✓ Transaction committed")
            
        except Exception as e:
            trans.rollback()
            print(f"❌ Error during update, rolling back: {e}")
            raise
            
except Exception as e:
    print(f"❌ Error updating existing snapshots: {e}")
    exit(1)

# ============================================================================
# STEP 4: CREATE NEW SNAPSHOT VERSION
# ============================================================================

print("\n" + "-" * 80)
print(f"STEP 4: Create New Snapshot (Version {next_version})")
print("-" * 80)

try:
    with engine.connect() as conn:
        # Start transaction
        trans = conn.begin()
        
        try:
            print(f"🚀 Inserting ALL STG data as Version {next_version}...")
            
            # Insert ALL data from STG as new version
            insert_query = text("""
                INSERT INTO curated_spending_snapshots (
                    snapshot_version,
                    snapshot_date,
                    snapshot_batch_id,
                    is_latest,
                    src_id,
                    stg_spending_id,
                    person_id,
                    category_id,
                    location_id,
                    payment_method_id,
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
                )
                SELECT 
                    :version_number as snapshot_version,
                    :snapshot_date as snapshot_date,
                    :batch_id as snapshot_batch_id,
                    1 as is_latest,
                    f.src_id,
                    f.spending_id as stg_spending_id,
                    f.person_id,
                    f.category_id,
                    f.location_id,
                    f.payment_method_id,
                    p.person_name,
                    c.category_name,
                    c.category_group,
                    l.location_name,
                    l.location_type,
                    pm.payment_method_name,
                    pm.payment_type,
                    f.spending_date,
                    f.spending_year,
                    f.spending_month,
                    f.spending_quarter,
                    CASE f.spending_day_of_week
                        WHEN 'Monday' THEN 1
                        WHEN 'Tuesday' THEN 2
                        WHEN 'Wednesday' THEN 3
                        WHEN 'Thursday' THEN 4
                        WHEN 'Friday' THEN 5
                        WHEN 'Saturday' THEN 6
                        WHEN 'Sunday' THEN 7
                        ELSE 0
                    END as spending_day_of_week,
                    f.amount_cleaned,
                    f.currency_code,
                    f.description,
                    f.data_quality_score
                FROM stg_fact_spending f
                JOIN stg_dim_person p ON f.person_id = p.person_id
                JOIN stg_dim_category c ON f.category_id = c.category_id
                JOIN stg_dim_location l ON f.location_id = l.location_id
                JOIN stg_dim_payment_method pm ON f.payment_method_id = pm.payment_method_id
            """)
            
            result = conn.execute(
                insert_query,
                {
                    'version_number': next_version,
                    'snapshot_date': SNAPSHOT_DATE,
                    'batch_id': BATCH_ID
                }
            )
            
            inserted_count = result.rowcount
            print(f"✓ Inserted {inserted_count:,} records as Version {next_version}")
            
            # Verify the insert
            result = conn.execute(text("""
                SELECT COUNT(*) 
                FROM curated_spending_snapshots
                WHERE snapshot_version = :version
                  AND is_latest = 1
            """), {'version': next_version})
            
            verify_count = result.fetchone()[0]
            
            if verify_count != inserted_count:
                raise Exception(f"Verification failed! Expected {inserted_count}, got {verify_count}")
            
            print(f"✓ Verified: Version {next_version} has {verify_count:,} records with is_latest = 1")
            
            # Commit the transaction
            trans.commit()
            print("✓ Transaction committed")
            
        except Exception as e:
            trans.rollback()
            print(f"❌ Error during snapshot creation, rolling back: {e}")
            raise
            
except Exception as e:
    print(f"❌ Error creating new snapshot: {e}")
    exit(1)

# ============================================================================
# STEP 5: VALIDATION AND STATISTICS
# ============================================================================

print("\n" + "=" * 80)
print("STEP 5: Validation and Statistics")
print("=" * 80)

try:
    with engine.connect() as conn:
        # Check total snapshots
        result = conn.execute(text("""
            SELECT 
                COUNT(DISTINCT snapshot_version) as total_versions,
                COUNT(*) as total_records,
                SUM(CASE WHEN is_latest = 1 THEN 1 ELSE 0 END) as latest_records,
                SUM(CASE WHEN is_latest = 0 THEN 1 ELSE 0 END) as historical_records
            FROM curated_spending_snapshots
        """))
        
        stats = result.fetchone()
        print(f"\n📊 Overall Statistics:")
        print(f"   Total versions: {stats[0]}")
        print(f"   Total records: {stats[1]:,}")
        print(f"   Latest records (is_latest=1): {stats[2]:,}")
        print(f"   Historical records (is_latest=0): {stats[3]:,}")
        
        # Verify only ONE version has is_latest = 1
        result = conn.execute(text("""
            SELECT COUNT(DISTINCT snapshot_version)
            FROM curated_spending_snapshots
            WHERE is_latest = 1
        """))
        
        latest_versions = result.fetchone()[0]
        if latest_versions != 1:
            print(f"\n❌ ERROR: {latest_versions} versions have is_latest = 1 (should be exactly 1)!")
        else:
            print(f"\n✅ Validation passed: Exactly 1 version has is_latest = 1")
        
        # Show version summary
        print(f"\n📋 Version Summary:")
        print("-" * 80)
        result = conn.execute(text("""
            SELECT 
                snapshot_version,
                snapshot_date,
                is_latest,
                COUNT(*) as record_count,
                MIN(spending_date) as earliest_transaction,
                MAX(spending_date) as latest_transaction,
                SUM(amount_cleaned) as total_amount
            FROM curated_spending_snapshots
            GROUP BY snapshot_version, snapshot_date, is_latest
            ORDER BY snapshot_version DESC
            LIMIT 10
        """))
        
        print(f"{'Ver':<5} {'Date':<12} {'Latest':<7} {'Records':<10} {'Date Range':<30} {'Total Amount':<15}")
        print("-" * 80)
        
        for row in result:
            version = row[0]
            snap_date = row[1]
            is_latest = "✓" if row[2] == 1 else ""
            count = row[3]
            date_range = f"{row[4]} to {row[5]}"
            total = f"${row[6]:,.2f}"
            print(f"{version:<5} {snap_date!s:<12} {is_latest:<7} {count:<10,} {date_range:<30} {total:<15}")
        
        # Growth analysis (if multiple versions exist)
        if stats[0] > 1:
            print(f"\n📈 Version Growth Analysis:")
            print("-" * 80)
            result = conn.execute(text("""
                WITH version_stats AS (
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
                    prev_count,
                    CASE 
                        WHEN prev_count IS NULL THEN record_count
                        ELSE record_count - prev_count
                    END as growth
                FROM version_stats
                ORDER BY snapshot_version DESC
                LIMIT 5
            """))
            
            print(f"{'Version':<10} {'Records':<12} {'Growth':<10}")
            print("-" * 80)
            for row in result:
                ver = f"V{row[0]}"
                records = f"{row[1]:,}"
                growth = f"+{row[3]:,}" if row[3] >= 0 else f"{row[3]:,}"
                print(f"{ver:<10} {records:<12} {growth:<10}")

except Exception as e:
    print(f"❌ Error during validation: {e}")

# ============================================================================
# COMPLETION
# ============================================================================

print("\n" + "=" * 80)
print("✅ SNAPSHOT CREATION COMPLETED SUCCESSFULLY!")
print("=" * 80)
print(f"⏰ Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"\n📸 Created: Version {next_version}")
print(f"📅 Snapshot Date: {SNAPSHOT_DATE}")
print(f"📦 Batch ID: {BATCH_ID}")
print(f"📊 Records: {inserted_count:,}")
print("\n💡 Query Examples:")
print(f"   -- Get latest snapshot:")
print(f"   SELECT * FROM curated_spending_snapshots WHERE is_latest = 1;")
print(f"\n   -- Get this specific version:")
print(f"   SELECT * FROM curated_spending_snapshots WHERE snapshot_version = {next_version};")
print(f"\n   -- Compare versions:")
print(f"   SELECT snapshot_version, COUNT(*) FROM curated_spending_snapshots GROUP BY snapshot_version;")
print("=" * 80)

