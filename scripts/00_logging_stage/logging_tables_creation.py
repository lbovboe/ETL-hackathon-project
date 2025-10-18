from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

# Setup connection
load_dotenv('../../.env')
connection_string = os.getenv('SUPABASE_CONNECTION_STRING')
engine = create_engine(connection_string)

print("📝 Creating logging table...")

# Read SQL file
with open('../../sql/00_logging_stage/log_01_create_table.sql', 'r') as f:
    sql_content = f.read()

# Execute SQL
try:
    with engine.connect() as conn:
        conn.execute(text(sql_content))
        conn.commit()
        print("✅ Logging table created successfully!")
        
        # Verify table exists
        result = conn.execute(text("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'log_validation_results'
            ORDER BY ordinal_position
        """))
        
        print("\n📊 Table structure:")
        print("-" * 60)
        for row in result:
            print(f"  {row[0]:<30} {row[1]}")
        print("-" * 60)
        
except Exception as e:
    print(f"❌ Error: {e}")