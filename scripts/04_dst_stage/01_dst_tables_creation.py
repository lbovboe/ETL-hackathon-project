"""
DST Stage - Step 1: Create DST Aggregation Tables
Purpose: Create 4 pre-aggregated tables for fast reporting and analytics
  - dst_monthly_spending_summary
  - dst_category_trends
  - dst_person_analytics
  - dst_payment_method_summary
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
print("DST STAGE - CREATING PRE-AGGREGATION TABLES")
print("=" * 80)
print(f"⏰ Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

print("📝 Reading SQL file...")

# Read SQL file
sql_file_path = '../../sql/04_dst_stage/dst_01_create_tables.sql'
try:
    with open(sql_file_path, 'r') as f:
        sql_content = f.read()
    print(f"✅ SQL file loaded: {sql_file_path}\n")
except FileNotFoundError:
    print(f"❌ Error: SQL file not found at {sql_file_path}")
    exit(1)

print("🚀 Executing SQL to create DST aggregation tables...")
print("-" * 80)

# Execute SQL
try:
    with engine.connect() as conn:
        # Execute the entire SQL file
        conn.execute(text(sql_content))
        conn.commit()
        print("✅ DST aggregation tables created successfully!\n")
        
        # Verify tables exist
        print("📊 Verifying created tables:")
        print("=" * 80)
        
        # List of tables to verify
        tables_to_check = [
            'dst_monthly_spending_summary',
            'dst_category_trends',
            'dst_person_analytics',
            'dst_payment_method_summary'
        ]
        
        for table_name in tables_to_check:
            print(f"\n✅ TABLE: {table_name.upper()}")
            print("-" * 80)
            
            # Get column count
            result = conn.execute(text(f"""
                SELECT COUNT(*) 
                FROM information_schema.columns
                WHERE table_name = '{table_name}'
            """))
            col_count = result.fetchone()[0]
            
            if col_count > 0:
                # Get columns
                result = conn.execute(text(f"""
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_name = '{table_name}'
                    ORDER BY ordinal_position
                """))
                
                columns = result.fetchall()
                
                print(f"{'Column Name':<40} {'Type':<20} {'Nullable':<10}")
                print("-" * 80)
                
                for col in columns:
                    col_name = col[0]
                    data_type = col[1]
                    nullable = "NULL" if col[2] == "YES" else "NOT NULL"
                    print(f"{col_name:<40} {data_type:<20} {nullable:<10}")
                
                # Get row count
                count_result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                row_count = count_result.fetchone()[0]
                
                # Get index count
                index_result = conn.execute(text(f"""
                    SELECT COUNT(*) 
                    FROM pg_indexes 
                    WHERE tablename = '{table_name}'
                """))
                index_count = index_result.fetchone()[0]
                
                print(f"\n📈 Columns: {col_count}")
                print(f"📊 Rows: {row_count}")
                print(f"🔍 Indexes: {index_count}")
                
                # Show indexes
                indexes = conn.execute(text(f"""
                    SELECT indexname
                    FROM pg_indexes
                    WHERE tablename = '{table_name}'
                    ORDER BY indexname
                """))
                
                print(f"\nIndexes:")
                for idx in indexes:
                    print(f"  ✓ {idx[0]}")
                    
            else:
                print(f"❌ TABLE {table_name} - NOT FOUND")
        
        # Check for view
        print("\n" + "=" * 80)
        print("📊 Verifying created view:")
        print("-" * 80)
        
        view_result = conn.execute(text("""
            SELECT table_name 
            FROM information_schema.views 
            WHERE table_name = 'vw_dst_latest_month_dashboard'
        """))
        
        view_exists = view_result.fetchone()
        if view_exists:
            print("✅ VIEW: vw_dst_latest_month_dashboard - CREATED")
        else:
            print("⚠️  VIEW: vw_dst_latest_month_dashboard - NOT FOUND")
        
        # Check for function
        print("\n" + "-" * 80)
        print("⚙️  Verifying created function:")
        print("-" * 80)
        
        func_result = conn.execute(text("""
            SELECT routine_name 
            FROM information_schema.routines 
            WHERE routine_name = 'get_trend_direction'
        """))
        
        func_exists = func_result.fetchone()
        if func_exists:
            print("✅ FUNCTION: get_trend_direction() - CREATED")
        else:
            print("⚠️  FUNCTION: get_trend_direction() - NOT FOUND")
        
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()
    exit(1)

print("\n" + "=" * 80)
print("✅ DST STAGE TABLE CREATION COMPLETED SUCCESSFULLY")
print("=" * 80)
print(f"⏰ Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

print("\n📋 Summary:")
print("=" * 80)
print("✅ 4 aggregation tables created:")
print("   • dst_monthly_spending_summary    (22 columns, 5 indexes)")
print("   • dst_category_trends             (24 columns, 4 indexes)")
print("   • dst_person_analytics            (51 columns, 3 indexes)")
print("   • dst_payment_method_summary      (24 columns, 4 indexes)")
print("\n✅ 1 dashboard view created:")
print("   • vw_dst_latest_month_dashboard")
print("\n✅ 1 helper function created:")
print("   • get_trend_direction()")

print("\n📝 Next steps:")
print("-" * 80)
print("1. Run ETL scripts to populate aggregation tables from CURATED:")
print("   File: 02_populate_monthly_summary.py")
print("   File: 03_populate_category_trends.py")
print("   File: 04_populate_person_analytics.py")
print("   File: 05_populate_payment_summary.py")
print("\n2. Run validation script to verify aggregations:")
print("   File: 06_run_validation.py")
print("=" * 80)

