"""
CURATED Stage - Step 1: Create Curated Snapshot Table
Purpose: Create curated_spending_snapshots table for versioned historical snapshots
"""

from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
from datetime import datetime

# Setup connection
load_dotenv('../../.env')
connection_string = os.getenv('SUPABASE_CONNECTION_STRING')
engine = create_engine(connection_string)

print("=" * 70)
print("CURATED STAGE - CREATING SNAPSHOT TABLE")
print("=" * 70)
print(f"⏰ Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

print("📝 Reading SQL file...")

# Read SQL file
sql_file_path = '../../sql/03_curated_stage/cur_01_create_table.sql'
try:
    with open(sql_file_path, 'r') as f:
        sql_content = f.read()
    print(f"✅ SQL file loaded: {sql_file_path}\n")
except FileNotFoundError:
    print(f"❌ Error: SQL file not found at {sql_file_path}")
    exit(1)

print("🚀 Executing SQL to create curated snapshot table...")
print("-" * 70)

# Execute SQL
try:
    with engine.connect() as conn:
        # Execute the entire SQL file
        conn.execute(text(sql_content))
        conn.commit()
        print("✅ Curated snapshot table created successfully!\n")
        
        # Verify table exists
        print("📊 Verifying created table:")
        print("=" * 70)
        
        result = conn.execute(text("""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_name = 'curated_spending_snapshots'
            ORDER BY ordinal_position
        """))
        
        columns = result.fetchall()
        
        if columns:
            print(f"\n✅ TABLE: CURATED_SPENDING_SNAPSHOTS")
            print("-" * 70)
            print(f"{'Column Name':<30} {'Type':<20} {'Nullable':<10}")
            print("-" * 70)
            
            for col in columns:
                col_name = col[0]
                data_type = col[1]
                nullable = "NULL" if col[2] == "YES" else "NOT NULL"
                print(f"{col_name:<30} {data_type:<20} {nullable:<10}")
            
            # Get row count
            count_result = conn.execute(text("SELECT COUNT(*) FROM curated_spending_snapshots"))
            row_count = count_result.fetchone()[0]
            print(f"\nRow count: {row_count}")
            
            # Get index count
            index_result = conn.execute(text("""
                SELECT COUNT(*) 
                FROM pg_indexes 
                WHERE tablename = 'curated_spending_snapshots'
            """))
            index_count = index_result.fetchone()[0]
            print(f"Indexes: {index_count}")
            
            # Show indexes
            print("\n" + "-" * 70)
            print("INDEXES:")
            print("-" * 70)
            
            indexes = conn.execute(text("""
                SELECT indexname
                FROM pg_indexes
                WHERE tablename = 'curated_spending_snapshots'
                ORDER BY indexname
            """))
            
            for idx in indexes:
                print(f"  ✓ {idx[0]}")
                
        else:
            print("❌ TABLE: curated_spending_snapshots - NOT FOUND")
        
except Exception as e:
    print(f"❌ Error: {e}")
    exit(1)

print("\n" + "=" * 70)
print("✅ CURATED STAGE TABLE CREATION COMPLETED SUCCESSFULLY")
print("=" * 70)
print(f"⏰ Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("\n📝 Next step: Run the snapshot creation ETL script")
print("   File: 02_create_snapshot.py")
print("=" * 70)
