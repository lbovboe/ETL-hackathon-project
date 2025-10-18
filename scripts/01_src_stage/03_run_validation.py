#!/usr/bin/env python3
"""
Run Data Quality Validation
Purpose: Validate data quality in src_daily_spending table
Usage: python scripts/run_validation.py
"""

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime
import os
import sys


def main():
    """Main validation process"""
    
    # Setup
    load_dotenv('../../.env')
    connection_string = os.getenv('SUPABASE_CONNECTION_STRING')
    
    if not connection_string:
        print("❌ Error: SUPABASE_CONNECTION_STRING not found in .env file")
        sys.exit(1)
    
    engine = create_engine(connection_string)
    
    print("="*80)
    print("DATA QUALITY VALIDATION")
    print("="*80)
    
    # ============================================
    # STEP 1: Check Data Exists
    # ============================================
    
    print("\n📊 Checking for data...")
    
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM src_daily_spending"))
        count = result.fetchone()[0]
        
        if count == 0:
            print("❌ No data in src_daily_spending table!")
            print("   Run load script first:")
            print("   python scripts/load_parquet_to_src.py")
            sys.exit(1)
        else:
            print(f"✅ Found {count:,} records to validate")
    
    # ============================================
    # STEP 2: Read SQL File
    # ============================================
    
    validation_run_id = f"VAL_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    print(f"🏷️  Validation ID: {validation_run_id}")
    
    sql_file = '../../sql/01_src_stage/src_02_error_validation.sql'
    
    if not os.path.exists(sql_file):
        print(f"❌ SQL file not found: {sql_file}")
        print(f"   Current directory: {os.getcwd()}")
        sys.exit(1)
    
    print(f"\n📄 Reading: {sql_file}")
    
    with open(sql_file, 'r') as f:
        sql_content = f.read()
    
    # Replace placeholder
    sql_content = sql_content.replace(':validation_run_id', f"'{validation_run_id}'")
    
    # Parse statements
    statements = []
    for stmt in sql_content.split(';'):
        cleaned = stmt.strip()
        if cleaned and not cleaned.startswith('--'):
            statements.append(cleaned)
    
    print(f"✅ Found {len(statements)} validation checks")
    
    # ============================================
    # STEP 3: Run Validation Checks
    # ============================================
    
    print("\n🔍 Running validation checks...")
    print("="*80)
    
    with engine.connect() as conn:
        check_num = 0
        for stmt in statements:
            if 'TRUNCATE' in stmt.upper():
                conn.execute(text(stmt))
                print("  ✓ Cleared previous results")
            elif 'INSERT' in stmt.upper():
                conn.execute(text(stmt))
                check_num += 1
                print(f"  ✓ Check {check_num} completed")
        
        conn.commit()
    
    print("="*80)
    print("✅ All checks completed!")
    
    # ============================================
    # STEP 4: View Results
    # ============================================
    
    print("\n" + "="*80)
    print("VALIDATION SUMMARY")
    print("="*80)
    
    summary = pd.read_sql_query(
        "SELECT * FROM v_latest_validation_summary", 
        engine
    )
    print(summary.to_string(index=False))
    
    # ============================================
    # STEP 5: Show Failed Checks
    # ============================================
    
    failed = pd.read_sql_query(f"""
        SELECT 
            check_name,
            check_type,
            records_checked,
            records_failed,
            failure_percentage,
            error_message,
            sample_failed_ids
        FROM log_validation_results
        WHERE validation_run_id = '{validation_run_id}'
        AND check_status = 'FAILED'
        ORDER BY 
            CASE WHEN check_type = 'ERROR' THEN 1 ELSE 2 END,
            failure_percentage DESC
    """, engine)
    
    if len(failed) > 0:
        print("\n" + "="*80)
        print("FAILED CHECKS")
        print("="*80)
        for _, row in failed.iterrows():
            print(f"\n❌ {row['check_type']}: {row['check_name']}")
            print(f"   Failed: {row['records_failed']}/{row['records_checked']} ({row['failure_percentage']}%)")
            print(f"   Issue: {row['error_message']}")
            if row['sample_failed_ids']:
                print(f"   Sample IDs: {row['sample_failed_ids'][:100]}")
    else:
        print("\n✅ ALL CHECKS PASSED!")
    
    # ============================================
    # STEP 6: Decision
    # ============================================
    
    print("\n" + "="*80)
    print("DECISION")
    print("="*80)
    
    errors = summary['errors'].sum() if len(summary) > 0 else 0
    warnings = summary['warnings'].sum() if len(summary) > 0 else 0
    
    if errors > 0:
        print(f"❌ CANNOT PROCEED - {errors} ERROR(S) FOUND")
        print("\n   Actions:")
        print("   1. Review failed checks above")
        print("   2. Fix source data")
        print("   3. Delete bad batch:")
        print("      Query: SELECT load_batch_id FROM src_daily_spending ORDER BY loaded_at DESC LIMIT 1;")
        print("      Delete: DELETE FROM src_daily_spending WHERE load_batch_id = 'BATCH_XXX';")
        print("   4. Re-run load script")
        sys.exit(1)
    elif warnings > 0:
        print(f"⚠️  CAN PROCEED WITH CAUTION - {warnings} WARNING(S)")
        print("   Data quality issues found but not critical")
        print("   Review warnings and proceed to Stage 2")
        sys.exit(0)
    else:
        print("✅ ALL CLEAR - READY FOR STAGE 2!")
        print("   Data quality is excellent")
        print("   Proceed to STG transformation")
        sys.exit(0)


if __name__ == "__main__":
    main()