from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

# Setup connection
load_dotenv('../../.env')
connection_string = os.getenv('SUPABASE_CONNECTION_STRING')
engine = create_engine(connection_string)

print("📝 Reading SQL file...")

# Read SQL file
with open('../../sql/01_src_stage/src_01_create_tables.sql', 'r') as f:
    sql_content = f.read()

print("✅ SQL file loaded")
print("\n🚀 Executing SQL...")

# Execute SQL
try:
    with engine.connect() as conn:
        conn.execute(text(sql_content))
        conn.commit()
        print("✅ Table 'src_daily_spending' created successfully!")
        
        # Verify table exists
        result = conn.execute(text("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = 'src_daily_spending'
            ORDER BY ordinal_position
        """))
        
        print("\n📊 Table structure:")
        print("-" * 60)
        for row in result:
            nullable = "NULL" if row[2] == "YES" else "NOT NULL"
            print(f"  {row[0]:<25} {row[1]:<20} {nullable}")
        print("-" * 60)
        
except Exception as e:
    print(f"❌ Error: {e}")