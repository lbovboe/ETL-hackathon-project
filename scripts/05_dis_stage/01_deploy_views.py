"""
DIS Stage - Step 1: Deploy Analytical Views
Purpose: Create all 5 business insight views that answer:
         "What lifestyle choices can improve my financial burden?"
Views Created:
  1. vw_financial_health_scorecard  - Financial health assessment
  2. vw_spending_recommendations    - Personalized recommendations
  3. vw_budget_alerts               - Overspending alerts
  4. vw_category_insights           - Category opportunities
  5. vw_lifestyle_improvement_plan  - Comprehensive action plans
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
print("DIS STAGE - DEPLOYING ANALYTICAL VIEWS")
print("=" * 80)
print(f"⏰ Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

print("📝 Reading SQL file...")

# Read SQL file
sql_file_path = '../../sql/05_dis_stage/dis_01_create_views.sql'
try:
    with open(sql_file_path, 'r') as f:
        sql_content = f.read()
    print(f"✅ SQL file loaded: {sql_file_path}\n")
except FileNotFoundError:
    print(f"❌ Error: SQL file not found at {sql_file_path}")
    exit(1)

print("🚀 Executing SQL to create analytical views...")
print("-" * 80)

# Execute SQL
try:
    with engine.connect() as conn:
        # Execute the entire SQL file
        conn.execute(text(sql_content))
        conn.commit()
        print("✅ DIS analytical views created successfully!\n")
        
        # Verify views exist
        print("📊 Verifying created views:")
        print("=" * 80)
        
        # List of views to verify
        views_to_check = [
            'vw_financial_health_scorecard',
            'vw_spending_recommendations',
            'vw_budget_alerts',
            'vw_category_insights',
            'vw_lifestyle_improvement_plan'
        ]
        
        for view_name in views_to_check:
            print(f"\n✅ VIEW: {view_name.upper()}")
            print("-" * 80)
            
            # Get column count
            result = conn.execute(text(f"""
                SELECT COUNT(*) 
                FROM information_schema.columns
                WHERE table_name = '{view_name}'
            """))
            col_count = result.fetchone()[0]
            
            if col_count > 0:
                # Get columns
                result = conn.execute(text(f"""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_name = '{view_name}'
                    ORDER BY ordinal_position
                """))
                
                columns = result.fetchall()
                
                print(f"{'Column Name':<40} {'Type':<20}")
                print("-" * 80)
                
                for col in columns[:10]:  # Show first 10 columns
                    col_name = col[0]
                    data_type = col[1]
                    print(f"{col_name:<40} {data_type:<20}")
                
                if len(columns) > 10:
                    print(f"... and {len(columns) - 10} more columns")
                
                # Get row count
                try:
                    count_result = conn.execute(text(f"SELECT COUNT(*) FROM {view_name}"))
                    row_count = count_result.fetchone()[0]
                    print(f"\n📊 Columns: {col_count}")
                    print(f"📈 Rows: {row_count}")
                except Exception as e:
                    print(f"\n📊 Columns: {col_count}")
                    print(f"⚠️  Could not count rows: {e}")
                    
            else:
                print(f"❌ VIEW {view_name} - NOT FOUND")
        
        # Show sample insights
        print("\n" + "=" * 80)
        print("📊 SAMPLE INSIGHTS")
        print("=" * 80)
        
        # Sample from each view
        print("\n1️⃣ Financial Health Scorecard (Top 3):")
        print("-" * 80)
        health = conn.execute(text("""
            SELECT person_name, health_grade, health_score, 
                   discretionary_percent, potential_monthly_savings
            FROM vw_financial_health_scorecard
            ORDER BY health_score ASC
            LIMIT 3
        """))
        for row in health:
            print(f"   {row[0]:20} {row[1]:20} Score:{row[2]:3}  Disc:{row[3]:5.1f}%  Savings:${row[4]:,.2f}")
        
        print("\n2️⃣ Top Recommendations:")
        print("-" * 80)
        recommendations = conn.execute(text("""
            SELECT person_name, recommendation_title, potential_monthly_savings
            FROM vw_spending_recommendations
            WHERE priority = 1
            LIMIT 3
        """))
        for row in recommendations:
            print(f"   {row[0]:20} {row[1]:50} Save:${row[2]:,.2f}")
        
        print("\n3️⃣ High Priority Alerts:")
        print("-" * 80)
        alerts = conn.execute(text("""
            SELECT person_name, alert_title
            FROM vw_budget_alerts
            WHERE alert_severity = 'HIGH'
            LIMIT 3
        """))
        alert_count = 0
        for row in alerts:
            alert_count += 1
            print(f"   {row[0]:20} {row[1]}")
        if alert_count == 0:
            print("   ✅ No high-priority alerts!")
        
        print("\n4️⃣ Top Category Opportunities:")
        print("-" * 80)
        categories = conn.execute(text("""
            SELECT category_name, total_spending, opportunity_score
            FROM vw_category_insights
            ORDER BY opportunity_score DESC
            LIMIT 3
        """))
        for row in categories:
            print(f"   {row[0]:20} ${row[1]:10,.2f}  Opportunity:{row[2]:3}")
        
        print("\n5️⃣ Lifestyle Improvement Plans:")
        print("-" * 80)
        plans = conn.execute(text("""
            SELECT person_name, monthly_savings_potential, action_1_priority
            FROM vw_lifestyle_improvement_plan
            ORDER BY monthly_savings_potential DESC
            LIMIT 3
        """))
        for row in plans:
            print(f"   {row[0]:20} Potential:${row[1]:,.2f}/mo  {row[2][:50]}")

except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()
    exit(1)

print("\n" + "=" * 80)
print("✅ DIS STAGE VIEW DEPLOYMENT COMPLETED SUCCESSFULLY")
print("=" * 80)
print(f"⏰ Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

print("\n📋 Summary:")
print("=" * 80)
print("✅ 5 analytical views created:")
print("   • vw_financial_health_scorecard  - Health assessment with scores")
print("   • vw_spending_recommendations    - Personalized cost-saving tips")
print("   • vw_budget_alerts               - Proactive overspending warnings")
print("   • vw_category_insights           - Category optimization opportunities")
print("   • vw_lifestyle_improvement_plan  - Comprehensive action plans")

print("\n🎯 ULTIMATE GOAL ACHIEVED!")
print("=" * 80)
print("You can now answer: 'What lifestyle choices can improve my financial burden?'")
print("\n📊 Try these queries:")
print("   SELECT * FROM vw_financial_health_scorecard;")
print("   SELECT * FROM vw_spending_recommendations WHERE person_name = 'John Tan';")
print("   SELECT * FROM vw_budget_alerts WHERE alert_severity = 'HIGH';")
print("   SELECT * FROM vw_lifestyle_improvement_plan;")
print("\n🎉 Stage 5 (DIS) - COMPLETE!")
print("=" * 80)

