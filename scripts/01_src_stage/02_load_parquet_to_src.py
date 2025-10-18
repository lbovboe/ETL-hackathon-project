#!/usr/bin/env python3
"""
Load Parquet to SRC Table
Purpose: Load data from .parquet files to src_daily_spending table
Usage: python scripts/load_parquet_to_src.py [parquet_file]
"""

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime
import os
import sys
import argparse

def log_load_event(engine, load_run_id, event_type, status, message, row_count=0):
    """Log load events to log_validation_results table"""
    try:
        with engine.connect() as conn:
            conn.execute(text(f"""
                INSERT INTO log_validation_results (
                    validation_run_id, validation_timestamp, stage, table_name,
                    check_name, check_type, check_status, records_checked, error_message
                ) VALUES (
                    '{load_run_id}',
                    CURRENT_TIMESTAMP,
                    'SRC',
                    'src_daily_spending',
                    '{event_type}',
                    'ERROR',
                    '{status}',
                    {row_count},
                    '{message.replace("'", "''")[:500]}'
                )
            """))
            conn.commit()
    except Exception as e:
        print(f"⚠️  Warning: Could not log to database: {e}")


def main():
    """Main load process"""
    
    # Parse arguments
    parser = argparse.ArgumentParser(description='Load parquet file to SRC table')
    parser.add_argument('parquet_file', nargs='?', 
                       default='../../data/daily_spending_sample.parquet',
                       help='Path to parquet file')
    parser.add_argument('--chunk-size', type=int, default=1000,
                       help='Number of rows per chunk (default: 1000)')
    args = parser.parse_args()
    
    # Configuration
    CHUNK_SIZE = args.chunk_size
    parquet_file = args.parquet_file
    
    # Setup connection
    load_dotenv('../../.env')
    connection_string = os.getenv('SUPABASE_CONNECTION_STRING')
    
    if not connection_string:
        print("❌ Error: SUPABASE_CONNECTION_STRING not found in .env file")
        sys.exit(1)
    
    engine = create_engine(connection_string)
    
    # Generate IDs
    batch_id = f"BATCH_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    load_run_id = f"LOAD_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    print("="*80)
    print("LOAD PARQUET TO SRC")
    print("="*80)
    print(f"🏷️  Batch ID: {batch_id}")
    print(f"🏷️  Load Run ID: {load_run_id}")
    print(f"📦 Chunk Size: {CHUNK_SIZE}")
    
    # ============================================
    # STEP 1: Check File Exists
    # ============================================
    
    print("\n📂 Checking file...")
    
    if not os.path.exists(parquet_file):
        error_msg = f"File not found: {parquet_file}"
        print(f"❌ {error_msg}")
        log_load_event(engine, load_run_id, 'FILE_NOT_FOUND', 'FAILED', error_msg)
        sys.exit(1)
    
    print(f"✅ File found: {os.path.basename(parquet_file)}")
    
    # ============================================
    # STEP 2: Read Parquet Metadata
    # ============================================
    
    print("\n📊 Reading file info...")
    
    try:
        import pyarrow.parquet as pq
        metadata = pq.read_metadata(parquet_file)
        total_rows = metadata.num_rows
        file_size_mb = os.path.getsize(parquet_file) / (1024 * 1024)
        num_chunks = (total_rows // CHUNK_SIZE) + (1 if total_rows % CHUNK_SIZE > 0 else 0)
        
        print(f"✅ Total rows: {total_rows:,}")
        print(f"💾 File size: {file_size_mb:.2f} MB")
        print(f"✅ Will load in {num_chunks} chunk(s)")
        
    except Exception as e:
        error_msg = f"Failed to read parquet: {str(e)}"
        print(f"❌ {error_msg}")
        log_load_event(engine, load_run_id, 'FILE_READ_ERROR', 'FAILED', error_msg)
        sys.exit(1)
    
    # ============================================
    # STEP 3: Load Data in Chunks (Memory-Efficient)
    # ============================================
    
    print("\n🚀 Loading data...")
    print("💡 Using streaming mode - never loads entire file into memory")
    
    total_loaded = 0
    connection = engine.raw_connection()
    
    try:
        # Use PyArrow to stream the parquet file in batches (memory-efficient!)
        parquet_file_reader = pq.ParquetFile(parquet_file)
        
        chunk_num = 0
        for batch in parquet_file_reader.iter_batches(batch_size=CHUNK_SIZE):
            chunk_num += 1
            
            # Convert PyArrow batch to pandas DataFrame
            df_chunk = batch.to_pandas()
            
            # Add metadata
            df_chunk['source_file'] = os.path.basename(parquet_file)
            df_chunk['load_batch_id'] = batch_id
            df_chunk['loaded_at'] = datetime.now()
            
            # Insert chunk
            df_chunk.to_sql('src_daily_spending', engine, if_exists='append', index=False)
            
            # Verify
            cursor = connection.cursor()
            cursor.execute(
                "SELECT COUNT(*) FROM src_daily_spending WHERE load_batch_id = %s", 
                (batch_id,)
            )
            current_count = cursor.fetchone()[0]
            
            if current_count == total_loaded + len(df_chunk):
                connection.commit()
                total_loaded += len(df_chunk)
                progress = 100 * total_loaded / total_rows
                print(f"  ✅ Chunk {chunk_num}/{num_chunks}: {len(df_chunk)} rows ({progress:.1f}% complete)")
            else:
                connection.rollback()
                raise Exception(f"Chunk {chunk_num}: Row count mismatch")
            
            cursor.close()
        
        # Success!
        print(f"\n✅ Load complete: {total_loaded:,} rows")
        
        # Log success
        log_load_event(
            engine, load_run_id, 
            'DATA_LOAD_SUCCESS', 
            'PASSED',
            f'Successfully loaded {total_loaded} rows in {num_chunks} chunks',
            total_loaded
        )
        
        # Show sample
        print("\n📋 Sample data:")
        cursor = connection.cursor()
        cursor.execute("""
            SELECT src_id, person_name, spending_date, category, amount
            FROM src_daily_spending
            WHERE load_batch_id = %s
            ORDER BY src_id
            LIMIT 5
        """, (batch_id,))
        
        print("="*80)
        for row in cursor.fetchall():
            print(f"{row[0]:<5} | {row[1]:<15} | {row[2]:<15} | {row[3]:<15} | {row[4]}")
        print("="*80)
        cursor.close()
    
    except Exception as e:
        # Rollback on error
        connection.rollback()
        
        error_msg = f"Load failed: {str(e)}"
        print(f"\n❌ {error_msg}")
        print(f"🔄 All changes rolled back")
        
        # Log failure
        log_load_event(
            engine, load_run_id,
            'DATA_LOAD_FAILED',
            'FAILED',
            error_msg,
            total_loaded
        )
        
        sys.exit(1)
    
    finally:
        connection.close()
    
    # ============================================
    # SUMMARY
    # ============================================
    
    print("\n" + "="*80)
    print("✅ DATA LOAD COMPLETE")
    print("="*80)
    print("📊 Summary:")
    print(f"   - Loaded: {total_loaded:,} rows")
    print(f"   - Batch ID: {batch_id}")
    print(f"   - Source: {os.path.basename(parquet_file)}")
    print()
    print("➡️  NEXT STEP: Run validation")
    print("   python scripts/run_validation.py")
    print("="*80)


if __name__ == "__main__":
    main()