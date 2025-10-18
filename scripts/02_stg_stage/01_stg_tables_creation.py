"""
STG Stage - Step 1: Create Staging Tables
Purpose: Create normalized 3NF staging tables with proper data types
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
print("STG STAGE - CREATING NORMALIZED TABLES (3NF)")
print("=" * 70)
print(f"⏰ Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

print("📝 Reading SQL file...")

# Read SQL file
sql_file_path = '../../sql/02_stg_stage/stg_01_create_tables.sql'
try:
    with open(sql_file_path, 'r') as f:
        sql_content = f.read()
    print(f"✅ SQL file loaded: {sql_file_path}\n")
except FileNotFoundError:
    print(f"❌ Error: SQL file not found at {sql_file_path}")
    exit(1)

print("🚀 Executing SQL to create tables...")
print("-" * 70)

# Execute SQL
try:
    with engine.connect() as conn:
        # Execute the entire SQL file
        conn.execute(text(sql_content))
        conn.commit()
        print("✅ All staging tables created successfully!\n")
        
        # Verify tables exist
        tables_to_check = [
            'stg_dim_person',
            'stg_dim_location',
            'stg_dim_category',
            'stg_dim_payment_method',
            'stg_fact_spending'
        ]
        
        print("📊 Verifying created tables:")
        print("=" * 70)
        
        for table_name in tables_to_check:
            result = conn.execute(text(f"""
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_name = '{table_name}'
                ORDER BY ordinal_position
            """))
            
            columns = result.fetchall()
            
            if columns:
                print(f"\n✅ TABLE: {table_name.upper()}")
                print("-" * 70)
                print(f"{'Column Name':<30} {'Type':<20} {'Nullable':<10}")
                print("-" * 70)
                
                for col in columns:
                    col_name = col[0]
                    data_type = col[1]
                    nullable = "NULL" if col[2] == "YES" else "NOT NULL"
                    print(f"{col_name:<30} {data_type:<20} {nullable:<10}")
                
                # Get row count
                count_result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                row_count = count_result.fetchone()[0]
                print(f"\nRow count: {row_count}")
            else:
                print(f"❌ TABLE: {table_name.upper()} - NOT FOUND")
        
        # Check if view was created
        print("\n" + "=" * 70)
        print("📊 Verifying created view:")
        print("=" * 70)
        
        view_check = conn.execute(text("""
            SELECT table_name 
            FROM information_schema.views 
            WHERE table_schema = 'public' 
            AND table_name = 'vw_stg_spending_complete'
        """))
        
        if view_check.fetchone():
            print("✅ VIEW: vw_stg_spending_complete - Created successfully")
        else:
            print("❌ VIEW: vw_stg_spending_complete - NOT FOUND")
        
except Exception as e:
    print(f"❌ Error: {e}")
    exit(1)

print("\n" + "=" * 70)
print("✅ STG STAGE TABLES CREATION COMPLETED SUCCESSFULLY")
print("=" * 70)
print(f"⏰ Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("\n📝 Next step: Run the ETL transformation script to populate these tables")

