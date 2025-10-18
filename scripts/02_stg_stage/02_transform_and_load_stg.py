"""
STG Stage - Step 3: IMPROVED Transform and Load Data
Purpose: Enhanced version with robust date parsing for multiple formats

Improvements:
- Handles multiple date formats (DD-MMM-YYYY, DD/MM/YYYY, YYYY-MM-DD, DD/MM/YY, etc.)
- Better error handling and logging
- Clears existing staging data before reload
- Provides detailed statistics
"""

from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import pandas as pd
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from dateutil import parser as date_parser

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

# Generate batch ID for this ETL run
BATCH_ID = f"STG_TRANSFORM_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

print("=" * 80)
print("STG STAGE - IMPROVED TRANSFORM AND LOAD DATA")
print("=" * 80)
print(f"⏰ Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"📦 Batch ID: {BATCH_ID}\n")

# ============================================
# HELPER FUNCTIONS FOR DATA CLEANING
# ============================================

def clean_amount(amount_str):
    """
    Clean amount string and extract numeric value and currency
    Examples:
        '155.66' -> (155.66, 'SGD')
        '$40.10' -> (40.10, 'SGD')
        '333.95 SGD' -> (333.95, 'SGD')
        'SGD 17.51' -> (17.51, 'SGD')
    
    Returns: (cleaned_amount, currency_code, success_flag)
    """
    if not amount_str or pd.isna(amount_str):
        return None, 'SGD', False
    
    try:
        # Convert to string and clean
        amount_str = str(amount_str).strip()
        
        # Extract currency if present
        currency = 'SGD'  # Default
        if 'SGD' in amount_str.upper():
            currency = 'SGD'
            amount_str = amount_str.upper().replace('SGD', '').strip()
        
        # Remove currency symbols
        amount_str = amount_str.replace('$', '').replace(',', '').strip()
        
        # Convert to Decimal for precision
        cleaned_amount = Decimal(amount_str)
        
        return float(cleaned_amount), currency, True
        
    except (ValueError, InvalidOperation, Exception):
        return None, 'SGD', False


def clean_date_improved(date_str):
    """
    IMPROVED: Parse date string with support for multiple formats
    
    Supported formats:
        - '01-Apr-2022' (DD-MMM-YYYY)
        - '14/05/2024' (DD/MM/YYYY)
        - '2023-09-28' (YYYY-MM-DD)
        - '21/10/24' (DD/MM/YY)
        - '20-Sep-2022' (DD-MMM-YYYY)
        - And more...
    
    Returns: (date_object, success_flag)
    """
    if not date_str or pd.isna(date_str):
        return None, False
    
    try:
        date_str = str(date_str).strip()
        
        # Strategy 1: Try common explicit formats first
        common_formats = [
            '%d-%b-%Y',      # 01-Apr-2022
            '%d/%m/%Y',      # 14/05/2024
            '%Y-%m-%d',      # 2023-09-28
            '%d/%m/%y',      # 21/10/24
            '%d-%m-%Y',      # 01-04-2022
            '%d-%m-%y',      # 01-04-22
            '%Y/%m/%d',      # 2023/09/28
            '%d-%b-%y',      # 01-Apr-22
        ]
        
        for fmt in common_formats:
            try:
                date_obj = datetime.strptime(date_str, fmt)
                return date_obj.date(), True
            except ValueError:
                continue
        
        # Strategy 2: Use dateutil parser as fallback (intelligent parsing)
        # dayfirst=True assumes DD/MM/YYYY rather than MM/DD/YYYY (common in Asia/Europe)
        try:
            date_obj = date_parser.parse(date_str, dayfirst=True)
            return date_obj.date(), True
        except:
            pass
        
        # If all strategies fail
        return None, False
        
    except Exception:
        return None, False


def classify_location_type(location_name):
    """
    Classify location into type based on name
    Returns: 'Online', 'Transport', 'Physical', or 'Unknown'
    """
    if not location_name:
        return 'Unknown'
    
    location_lower = location_name.lower()
    
    # Online platforms
    online_keywords = ['shopee', 'lazada', 'zalora', 'amazon', 'grab', 'foodpanda', 
                       'udemy', 'netflix', 'spotify', 'online', 'taobao', '.com', 'web']
    if any(keyword in location_lower for keyword in online_keywords):
        return 'Online'
    
    # Transport locations
    transport_keywords = ['mrt', 'bus', 'taxi', 'grab', 'gojek', 'station', 'interchange']
    if any(keyword in location_lower for keyword in transport_keywords):
        return 'Transport'
    
    # Physical locations
    physical_keywords = ['mall', 'restaurant', 'cafe', 'clinic', 'hospital', 'court', 'market']
    if any(keyword in location_lower for keyword in physical_keywords):
        return 'Physical'
    
    # Default
    return 'Physical'


def classify_payment_type(payment_method):
    """
    Classify payment method into type
    Returns: 'Card', 'Digital Wallet', 'Transit Card', 'Bank Transfer', or 'Other'
    """
    if not payment_method:
        return 'Other'
    
    payment_lower = payment_method.lower()
    
    # Card payments
    if any(keyword in payment_lower for keyword in ['card', 'visa', 'mastercard', 'amex']):
        return 'Card'
    
    # Digital wallets
    if any(keyword in payment_lower for keyword in ['pay', 'wallet', 'apple', 'google', 'grab']):
        return 'Digital Wallet'
    
    # Transit cards
    if any(keyword in payment_lower for keyword in ['ez-link', 'nets', 'flashpay']):
        return 'Transit Card'
    
    # Bank transfers
    if any(keyword in payment_lower for keyword in ['bank', 'transfer', 'giro']):
        return 'Bank Transfer'
    
    return 'Other'


def classify_category_group(category):
    """
    Classify spending category into higher-level group
    Returns: 'Essential', 'Discretionary', 'Transport', 'Healthcare', 'Education'
    """
    if not category:
        return 'Other'
    
    category_lower = category.lower()
    
    # Essential spending
    if category_lower in ['groceries', 'food', 'utilities']:
        return 'Essential'
    
    # Discretionary spending
    if category_lower in ['shopping', 'entertainment', 'dining']:
        return 'Discretionary'
    
    # Transport
    if category_lower in ['transport', 'transportation']:
        return 'Transport'
    
    # Healthcare
    if category_lower in ['healthcare', 'medical', 'health']:
        return 'Healthcare'
    
    # Education
    if category_lower in ['education', 'learning', 'books']:
        return 'Education'
    
    return 'Other'


def calculate_data_quality_score(row):
    """
    Calculate data quality score (0-100) based on completeness and validity
    """
    score = 100
    
    # Deduct points for missing or invalid data
    if not row['is_amount_parsed_successfully']:
        score -= 30
    if not row['is_date_parsed_successfully']:
        score -= 30
    if pd.isna(row['description']) or not row['description']:
        score -= 10
    if pd.isna(row['person_name']) or not row['person_name']:
        score -= 20
    if pd.isna(row['location_name']) or not row['location_name']:
        score -= 5
    if pd.isna(row['category_name']) or not row['category_name']:
        score -= 5
    
    return max(0, score)


# ============================================
# STEP 0: CLEAR EXISTING STAGING DATA
# ============================================

print("🗑️  STEP 0: Clearing existing staging data...")
print("-" * 80)

try:
    with engine.connect() as conn:
        # Delete in proper order (fact table first due to foreign keys)
        tables_to_clear = [
            'stg_fact_spending',
            'stg_dim_payment_method',
            'stg_dim_category',
            'stg_dim_location',
            'stg_dim_person'
        ]
        
        for table in tables_to_clear:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
            old_count = result.scalar()
            
            conn.execute(text(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE"))
            print(f"   ✅ Cleared {table}: {old_count} records removed")
        
        conn.commit()
        print("\n✅ All staging tables cleared successfully!\n")
        
except Exception as e:
    print(f"❌ Error clearing tables: {e}")
    exit(1)

# ============================================
# STEP 1: EXTRACT DATA FROM SOURCE
# ============================================

print("📤 STEP 1: Extracting data from src_daily_spending...")
print("-" * 80)

try:
    with engine.connect() as conn:
        # Extract all data from source
        query = """
            SELECT 
                src_id,
                person_name,
                spending_date,
                category,
                amount,
                location,
                description,
                payment_method,
                source_file,
                load_batch_id
            FROM src_daily_spending
            ORDER BY src_id
        """
        
        df_source = pd.read_sql(query, conn)
        
        print(f"✅ Extracted {len(df_source)} records from source table")
        print(f"   Source columns: {list(df_source.columns)}")
        
except Exception as e:
    print(f"❌ Error extracting data: {e}")
    exit(1)

# ============================================
# STEP 2: TRANSFORM DATA
# ============================================

print("\n🔄 STEP 2: Transforming and cleaning data...")
print("-" * 80)

# Create a working dataframe
df_transformed = df_source.copy()

# Clean and parse amounts
print("   💰 Cleaning amounts...")
amount_results = df_transformed['amount'].apply(clean_amount)
df_transformed['amount_cleaned'] = amount_results.apply(lambda x: x[0])
df_transformed['currency_code'] = amount_results.apply(lambda x: x[1])
df_transformed['is_amount_parsed_successfully'] = amount_results.apply(lambda x: x[2])

amount_success_rate = df_transformed['is_amount_parsed_successfully'].sum() / len(df_transformed) * 100
print(f"      Success rate: {amount_success_rate:.1f}% ({df_transformed['is_amount_parsed_successfully'].sum()}/{len(df_transformed)})")

# Clean and parse dates with IMPROVED parser
print("   📅 Parsing dates (with improved multi-format support)...")
date_results = df_transformed['spending_date'].apply(clean_date_improved)
df_transformed['spending_date_parsed'] = date_results.apply(lambda x: x[0])
df_transformed['is_date_parsed_successfully'] = date_results.apply(lambda x: x[1])

date_success_rate = df_transformed['is_date_parsed_successfully'].sum() / len(df_transformed) * 100
print(f"      Success rate: {date_success_rate:.1f}% ({df_transformed['is_date_parsed_successfully'].sum()}/{len(df_transformed)})")

# Extract date components for successfully parsed dates
df_transformed['spending_year'] = df_transformed['spending_date_parsed'].apply(
    lambda x: x.year if x else None
)
df_transformed['spending_month'] = df_transformed['spending_date_parsed'].apply(
    lambda x: x.month if x else None
)
df_transformed['spending_day'] = df_transformed['spending_date_parsed'].apply(
    lambda x: x.day if x else None
)
df_transformed['spending_quarter'] = df_transformed['spending_date_parsed'].apply(
    lambda x: (x.month - 1) // 3 + 1 if x else None
)
df_transformed['spending_day_of_week'] = df_transformed['spending_date_parsed'].apply(
    lambda x: x.strftime('%A') if x else None
)

# Classify locations
print("   📍 Classifying locations...")
df_transformed['location_type'] = df_transformed['location'].apply(classify_location_type)

# Classify payment methods
print("   💳 Classifying payment methods...")
df_transformed['payment_type'] = df_transformed['payment_method'].apply(classify_payment_type)

# Classify categories
print("   📊 Classifying categories...")
df_transformed['category_group'] = df_transformed['category'].apply(classify_category_group)

# Clean text fields
print("   🧹 Cleaning text fields...")
df_transformed['person_name'] = df_transformed['person_name'].str.strip()
df_transformed['location_name'] = df_transformed['location'].str.strip()
df_transformed['category_name'] = df_transformed['category'].str.strip()
df_transformed['payment_method_name'] = df_transformed['payment_method'].str.strip()

# Calculate data quality scores
print("   📈 Calculating data quality scores...")
df_transformed['data_quality_score'] = df_transformed.apply(calculate_data_quality_score, axis=1)

# Add batch metadata
df_transformed['transform_batch_id'] = BATCH_ID

# Filter out records with critical errors (no date or no amount)
df_valid = df_transformed[
    df_transformed['is_amount_parsed_successfully'] & 
    df_transformed['is_date_parsed_successfully']
].copy()

# Also keep track of rejected records for logging
df_rejected = df_transformed[
    ~(df_transformed['is_amount_parsed_successfully'] & 
      df_transformed['is_date_parsed_successfully'])
].copy()

print(f"\n✅ Transformation complete!")
print(f"   Total records processed: {len(df_transformed)}")
print(f"   Valid records: {len(df_valid)} ({len(df_valid)/len(df_transformed)*100:.1f}%)")
print(f"   Rejected records: {len(df_rejected)} ({len(df_rejected)/len(df_transformed)*100:.1f}%)")

if len(df_rejected) > 0:
    print(f"\n   📋 Rejection reasons:")
    failed_amounts = (~df_transformed['is_amount_parsed_successfully']).sum()
    failed_dates = (~df_transformed['is_date_parsed_successfully']).sum()
    print(f"      - Failed amount parsing: {failed_amounts}")
    print(f"      - Failed date parsing: {failed_dates}")

if len(df_valid) > 0:
    print(f"\n   📊 Valid record statistics:")
    print(f"      - Average data quality score: {df_valid['data_quality_score'].mean():.1f}/100")
    print(f"      - Date range: {df_valid['spending_date_parsed'].min()} to {df_valid['spending_date_parsed'].max()}")

# ============================================
# STEP 3: LOAD DIMENSION TABLES
# ============================================

print("\n📥 STEP 3: Loading dimension tables...")
print("-" * 80)

try:
    with engine.connect() as conn:
        
        # 3.1: Load Person Dimension
        print("   👤 Loading stg_dim_person...")
        persons = df_valid[['person_name']].drop_duplicates()
        
        for _, row in persons.iterrows():
            conn.execute(text("""
                INSERT INTO stg_dim_person (person_name)
                VALUES (:person_name)
                ON CONFLICT (person_name) DO NOTHING
            """), {"person_name": row['person_name']})
        
        conn.commit()
        
        # Get person IDs
        person_map = pd.read_sql("""
            SELECT person_id, person_name FROM stg_dim_person
        """, conn)
        print(f"      ✅ Loaded {len(person_map)} unique persons")
        
        # 3.2: Load Location Dimension
        print("   📍 Loading stg_dim_location...")
        locations = df_valid[['location_name', 'location_type']].drop_duplicates()
        
        for _, row in locations.iterrows():
            conn.execute(text("""
                INSERT INTO stg_dim_location (location_name, location_type)
                VALUES (:location_name, :location_type)
                ON CONFLICT (location_name) 
                DO UPDATE SET location_type = EXCLUDED.location_type
            """), {
                "location_name": row['location_name'],
                "location_type": row['location_type']
            })
        
        conn.commit()
        
        # Get location IDs
        location_map = pd.read_sql("""
            SELECT location_id, location_name FROM stg_dim_location
        """, conn)
        print(f"      ✅ Loaded {len(location_map)} unique locations")
        
        # 3.3: Load Category Dimension
        print("   📊 Loading stg_dim_category...")
        categories = df_valid[['category_name', 'category_group']].drop_duplicates()
        
        for _, row in categories.iterrows():
            conn.execute(text("""
                INSERT INTO stg_dim_category (category_name, category_group)
                VALUES (:category_name, :category_group)
                ON CONFLICT (category_name) 
                DO UPDATE SET category_group = EXCLUDED.category_group
            """), {
                "category_name": row['category_name'],
                "category_group": row['category_group']
            })
        
        conn.commit()
        
        # Get category IDs
        category_map = pd.read_sql("""
            SELECT category_id, category_name FROM stg_dim_category
        """, conn)
        print(f"      ✅ Loaded {len(category_map)} unique categories")
        
        # 3.4: Load Payment Method Dimension
        print("   💳 Loading stg_dim_payment_method...")
        payment_methods = df_valid[['payment_method_name', 'payment_type']].drop_duplicates()
        
        for _, row in payment_methods.iterrows():
            conn.execute(text("""
                INSERT INTO stg_dim_payment_method (payment_method_name, payment_type)
                VALUES (:payment_method_name, :payment_type)
                ON CONFLICT (payment_method_name) 
                DO UPDATE SET payment_type = EXCLUDED.payment_type
            """), {
                "payment_method_name": row['payment_method_name'],
                "payment_type": row['payment_type']
            })
        
        conn.commit()
        
        # Get payment method IDs
        payment_map = pd.read_sql("""
            SELECT payment_method_id, payment_method_name FROM stg_dim_payment_method
        """, conn)
        print(f"      ✅ Loaded {len(payment_map)} unique payment methods")

except Exception as e:
    print(f"❌ Error loading dimension tables: {e}")
    exit(1)

# ============================================
# STEP 4: LOAD FACT TABLE
# ============================================

print("\n📥 STEP 4: Loading fact table...")
print("-" * 80)

# Merge dimension IDs into fact dataframe
df_fact = df_valid.copy()
df_fact = df_fact.merge(person_map, on='person_name', how='left')
df_fact = df_fact.merge(location_map, left_on='location_name', right_on='location_name', how='left')
df_fact = df_fact.merge(category_map, left_on='category_name', right_on='category_name', how='left')
df_fact = df_fact.merge(payment_map, left_on='payment_method_name', right_on='payment_method_name', how='left')

# Check for any missing dimension keys
missing_persons = df_fact['person_id'].isna().sum()
missing_locations = df_fact['location_id'].isna().sum()
missing_categories = df_fact['category_id'].isna().sum()
missing_payments = df_fact['payment_method_id'].isna().sum()

if any([missing_persons, missing_locations, missing_categories, missing_payments]):
    print(f"⚠️  Warning: Found missing dimension keys:")
    print(f"   - Missing person_id: {missing_persons}")
    print(f"   - Missing location_id: {missing_locations}")
    print(f"   - Missing category_id: {missing_categories}")
    print(f"   - Missing payment_method_id: {missing_payments}")
    print("   Filtering out records with missing keys...")
    df_fact = df_fact.dropna(subset=['person_id', 'location_id', 'category_id', 'payment_method_id'])
    print(f"   Records remaining: {len(df_fact)}")

try:
    with engine.connect() as conn:
        
        print(f"   Loading {len(df_fact)} records into stg_fact_spending...")
        
        # Use bulk insert for better performance
        records_to_insert = []
        for _, row in df_fact.iterrows():
            records_to_insert.append({
                'person_id': int(row['person_id']),
                'location_id': int(row['location_id']),
                'category_id': int(row['category_id']),
                'payment_method_id': int(row['payment_method_id']),
                'spending_date': row['spending_date_parsed'],
                'spending_year': int(row['spending_year']),
                'spending_month': int(row['spending_month']),
                'spending_day': int(row['spending_day']),
                'spending_quarter': int(row['spending_quarter']),
                'spending_day_of_week': row['spending_day_of_week'],
                'amount_raw': row['amount'],
                'amount_cleaned': float(row['amount_cleaned']),
                'currency_code': row['currency_code'],
                'description': row['description'],
                'is_amount_parsed_successfully': row['is_amount_parsed_successfully'],
                'is_date_parsed_successfully': row['is_date_parsed_successfully'],
                'data_quality_score': int(row['data_quality_score']),
                'src_id': int(row['src_id']),
                'transform_batch_id': row['transform_batch_id']
            })
        
        # Batch insert
        for i, record in enumerate(records_to_insert, 1):
            conn.execute(text("""
                INSERT INTO stg_fact_spending (
                    person_id, location_id, category_id, payment_method_id,
                    spending_date, spending_year, spending_month, spending_day,
                    spending_quarter, spending_day_of_week,
                    amount_raw, amount_cleaned, currency_code, description,
                    is_amount_parsed_successfully, is_date_parsed_successfully,
                    data_quality_score, src_id, transform_batch_id
                ) VALUES (
                    :person_id, :location_id, :category_id, :payment_method_id,
                    :spending_date, :spending_year, :spending_month, :spending_day,
                    :spending_quarter, :spending_day_of_week,
                    :amount_raw, :amount_cleaned, :currency_code, :description,
                    :is_amount_parsed_successfully, :is_date_parsed_successfully,
                    :data_quality_score, :src_id, :transform_batch_id
                )
            """), record)
            
            # Progress indicator every 500 records
            if i % 500 == 0:
                print(f"      Progress: {i}/{len(records_to_insert)} records ({i/len(records_to_insert)*100:.1f}%)")
        
        conn.commit()
        print(f"      ✅ Successfully loaded {len(records_to_insert)} records")

except Exception as e:
    print(f"❌ Error loading fact table: {e}")
    import traceback
    traceback.print_exc()
    exit(1)

# ============================================
# STEP 5: VERIFICATION & SUMMARY
# ============================================

print("\n📊 STEP 5: Verification and Summary")
print("=" * 80)

try:
    with engine.connect() as conn:
        
        # Get row counts
        person_count = conn.execute(text("SELECT COUNT(*) FROM stg_dim_person")).scalar()
        location_count = conn.execute(text("SELECT COUNT(*) FROM stg_dim_location")).scalar()
        category_count = conn.execute(text("SELECT COUNT(*) FROM stg_dim_category")).scalar()
        payment_count = conn.execute(text("SELECT COUNT(*) FROM stg_dim_payment_method")).scalar()
        fact_count = conn.execute(text("SELECT COUNT(*) FROM stg_fact_spending")).scalar()
        
        print("\n📈 Table Row Counts:")
        print(f"   • stg_dim_person:         {person_count:,} records")
        print(f"   • stg_dim_location:       {location_count:,} records")
        print(f"   • stg_dim_category:       {category_count:,} records")
        print(f"   • stg_dim_payment_method: {payment_count:,} records")
        print(f"   • stg_fact_spending:      {fact_count:,} records")
        
        # Calculate load success rate
        source_count = len(df_source)
        success_rate = (fact_count / source_count * 100) if source_count > 0 else 0
        print(f"\n   📊 Load Success Rate: {success_rate:.1f}% ({fact_count}/{source_count})")
        
        # Sample data quality statistics
        quality_stats = conn.execute(text("""
            SELECT 
                AVG(data_quality_score) as avg_score,
                MIN(data_quality_score) as min_score,
                MAX(data_quality_score) as max_score,
                AVG(amount_cleaned) as avg_amount,
                SUM(amount_cleaned) as total_amount,
                MIN(spending_date) as min_date,
                MAX(spending_date) as max_date
            FROM stg_fact_spending
        """)).fetchone()
        
        print("\n💯 Data Quality Statistics:")
        print(f"   • Average Quality Score: {quality_stats[0]:.1f}/100")
        print(f"   • Min Quality Score: {quality_stats[1]}")
        print(f"   • Max Quality Score: {quality_stats[2]}")
        
        print("\n💰 Financial Statistics:")
        print(f"   • Average Transaction: SGD {quality_stats[3]:.2f}")
        print(f"   • Total Amount: SGD {quality_stats[4]:,.2f}")
        
        print("\n📅 Date Range:")
        print(f"   • From: {quality_stats[5]}")
        print(f"   • To: {quality_stats[6]}")
        
        # Category breakdown
        print("\n📊 Spending by Category:")
        category_stats = pd.read_sql("""
            SELECT 
                c.category_name,
                COUNT(*) as transactions,
                SUM(f.amount_cleaned) as total_amount,
                AVG(f.amount_cleaned) as avg_amount
            FROM stg_fact_spending f
            JOIN stg_dim_category c ON f.category_id = c.category_id
            GROUP BY c.category_name
            ORDER BY total_amount DESC
            LIMIT 5
        """, conn)
        
        for _, row in category_stats.iterrows():
            print(f"   • {row['category_name']:<15} {row['transactions']:>4} txns  SGD {row['total_amount']:>10,.2f}")
        
        # Sample records from view
        print("\n📋 Sample Records (from denormalized view):")
        print("-" * 80)
        sample = pd.read_sql("""
            SELECT 
                person_name,
                spending_date,
                category_name,
                amount_cleaned,
                location_name,
                payment_method_name
            FROM vw_stg_spending_complete
            ORDER BY spending_date DESC
            LIMIT 10
        """, conn)
        
        print(sample.to_string(index=False))

except Exception as e:
    print(f"⚠️  Warning during verification: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 80)
print("✅ STG STAGE IMPROVED TRANSFORMATION COMPLETED!")
print("=" * 80)
print(f"⏰ Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"📦 Batch ID: {BATCH_ID}")
print(f"\n📈 Final Statistics:")
print(f"   • Records Processed: {len(df_source):,}")
print(f"   • Records Loaded: {fact_count:,}")
print(f"   • Success Rate: {success_rate:.1f}%")
print("\n📝 Data is now ready for analysis in the staging layer!")
print("   Query the denormalized view: vw_stg_spending_complete")
print("=" * 80)

