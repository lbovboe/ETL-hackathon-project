# STG (Staging) Stage - Data Normalization and Transformation

## Overview
The STG (Staging) stage transforms raw data from the source layer into a clean, normalized, and standardized format following Third Normal Form (3NF) principles. This layer serves as the foundation for analytics and business intelligence.

## Architecture

### Data Model
The staging layer implements a **star schema** with:
- **4 Dimension Tables** (normalized entities)
- **1 Fact Table** (transaction data with foreign keys)
- **1 Denormalized View** (for easy querying)

```
┌─────────────────────┐
│  stg_dim_person     │
│  - person_id (PK)   │───┐
│  - person_name      │   │
└─────────────────────┘   │
                          │
┌─────────────────────┐   │
│  stg_dim_location   │   │
│  - location_id (PK) │───┤
│  - location_name    │   │
│  - location_type    │   │
└─────────────────────┘   │
                          │    ┌──────────────────────┐
┌─────────────────────┐   │    │  stg_fact_spending   │
│  stg_dim_category   │   ├───→│  - spending_id (PK)  │
│  - category_id (PK) │   │    │  - person_id (FK)    │
│  - category_name    │   │    │  - location_id (FK)  │
│  - category_group   │   │    │  - category_id (FK)  │
└─────────────────────┘   │    │  - payment_method_id │
                          │    │  - spending_date     │
┌─────────────────────┐   │    │  - amount_cleaned    │
│ stg_dim_payment_    │   │    │  - currency_code     │
│     method          │   │    │  - description       │
│ - payment_method_id │───┘    │  - data_quality_score│
│ - payment_method_   │        │  - src_id (lineage)  │
│   name              │        │  - transform_batch_id│
│ - payment_type      │        └──────────────────────┘
└─────────────────────┘
```

## Tables

### Dimension Tables

#### `stg_dim_person`
Stores unique persons/customers.
- **person_id**: Surrogate key (SERIAL PRIMARY KEY)
- **person_name**: Unique person name (VARCHAR, UNIQUE)
- **created_at**, **updated_at**: Audit timestamps

#### `stg_dim_location`
Stores unique spending locations with classification.
- **location_id**: Surrogate key (SERIAL PRIMARY KEY)
- **location_name**: Unique location name (VARCHAR, UNIQUE)
- **location_type**: Classification ('Online', 'Physical', 'Transport', 'Unknown')
- **created_at**, **updated_at**: Audit timestamps

#### `stg_dim_category`
Stores spending categories with grouping.
- **category_id**: Surrogate key (SERIAL PRIMARY KEY)
- **category_name**: Unique category name (VARCHAR, UNIQUE)
- **category_group**: Higher-level grouping ('Essential', 'Discretionary', 'Transport', 'Healthcare', 'Education', 'Other')
- **created_at**, **updated_at**: Audit timestamps

#### `stg_dim_payment_method`
Stores payment methods with type classification.
- **payment_method_id**: Surrogate key (SERIAL PRIMARY KEY)
- **payment_method_name**: Unique payment method name (VARCHAR, UNIQUE)
- **payment_type**: Type classification ('Card', 'Digital Wallet', 'Transit Card', 'Bank Transfer', 'Other')
- **created_at**, **updated_at**: Audit timestamps

### Fact Table

#### `stg_fact_spending`
Main transaction fact table with cleaned, normalized data.

**Foreign Keys:**
- person_id → stg_dim_person(person_id)
- location_id → stg_dim_location(location_id)
- category_id → stg_dim_category(category_id)
- payment_method_id → stg_dim_payment_method(payment_method_id)

**Date Dimensions:**
- spending_date (DATE)
- spending_year, spending_month, spending_day (INTEGER)
- spending_quarter (INTEGER, 1-4)
- spending_day_of_week (VARCHAR, 'Monday'-'Sunday')

**Financial Measures:**
- amount_raw (VARCHAR) - Original amount string
- amount_cleaned (NUMERIC(12,2)) - Cleaned numeric amount
- currency_code (VARCHAR(3)) - ISO currency code (default: 'SGD')

**Descriptive:**
- description (TEXT)

**Data Quality:**
- is_amount_parsed_successfully (BOOLEAN)
- is_date_parsed_successfully (BOOLEAN)
- data_quality_score (INTEGER, 0-100)

**Lineage:**
- src_id (BIGINT) - Reference back to src_daily_spending
- transform_batch_id (VARCHAR) - ETL batch identifier
- transformed_at (TIMESTAMP)

### View

#### `vw_stg_spending_complete`
Denormalized view joining fact and dimension tables for easy analysis.

## Scripts

### 1. `01_stg_tables_creation.py`
**Purpose:** Create staging tables schema

**What it does:**
- Executes SQL DDL from `sql/02_stg_stage/stg_01_create_tables.sql`
- Creates all dimension tables with proper constraints
- Creates fact table with foreign key relationships
- Creates indexes for performance
- Creates denormalized view
- Verifies table structure

**Usage:**
```bash
cd scripts/02_stg_stage
python 01_stg_tables_creation.py
```

### 2. `02_transform_and_load_stg.py` ⭐ **PRODUCTION SCRIPT**
**Purpose:** Production-ready transformation with robust parsing

**What it does:**
1. **Clears existing staging data** (TRUNCATE with CASCADE)
2. **Extracts** all data from src_daily_spending
3. **Transforms & Cleans:**
   - **Amounts:** Handles multiple formats (`$40.10`, `SGD 17.51`, `155.66`)
   - **Dates:** Handles multiple formats (`DD-MMM-YYYY`, `DD/MM/YYYY`, `YYYY-MM-DD`, `DD/MM/YY`)
   - **Locations:** Classifies into Online, Physical, Transport
   - **Payment Methods:** Classifies into Card, Digital Wallet, etc.
   - **Categories:** Groups into Essential, Discretionary, etc.
4. **Loads Dimensions:**
   - Inserts unique values with ON CONFLICT handling
   - Retrieves surrogate keys
5. **Loads Facts:**
   - Links to dimension tables via foreign keys
   - Includes data quality scoring
   - Maintains lineage tracking
6. **Validates:**
   - Checks load completeness
   - Calculates statistics
   - Shows sample data

**Success Rate:** 100% (6,000/6,000 records loaded)

**Usage:**
```bash
cd scripts/02_stg_stage
python 02_transform_and_load_stg.py
```

**Expected Output:**
- Load success: 100%
- Average data quality score: 100/100
- Date range: 2022-01-01 to 2024-12-31
- Total amount: ~SGD 517,218

### 3. `03_data_quality_report.py`
**Purpose:** Comprehensive data quality validation and reporting

**What it does:**
1. **Data Completeness Checks:**
   - Table record counts
   - Load completeness percentage
   - NULL value detection in critical fields

2. **Data Accuracy Checks:**
   - Data quality score distribution
   - Amount validity (negative, zero, outliers)
   - Date validity (future dates, range)

3. **Referential Integrity:**
   - Foreign key validation
   - Orphaned record detection
   - Dimension usage analysis

4. **Business Analytics:**
   - Spending by person
   - Spending by category
   - Payment method usage
   - Monthly spending trends
   - Location type distribution

5. **Summary & Recommendations:**
   - Overall grade (A+, A, B, C)
   - Production readiness assessment
   - Improvement recommendations

**Usage:**
```bash
cd scripts/02_stg_stage
python 03_data_quality_report.py
```

**Current Grade:** A+ (Excellent) - Production Ready ✅

## Data Transformations

### Amount Cleaning
**Input Examples:**
- `"155.66"` → 155.66 SGD
- `"$40.10"` → 40.10 SGD
- `"333.95 SGD"` → 333.95 SGD
- `"SGD 17.51"` → 17.51 SGD

**Logic:**
1. Detect and extract currency code
2. Remove currency symbols (`$`, commas)
3. Parse to NUMERIC with 2 decimal precision
4. Flag parsing success/failure

### Date Parsing (Enhanced)
**Input Examples:**
- `"01-Apr-2022"` → 2022-04-01
- `"14/05/2024"` → 2024-05-14
- `"2023-09-28"` → 2023-09-28
- `"21/10/24"` → 2024-10-21

**Strategy:**
1. Try common formats with explicit parsing
2. Fallback to intelligent dateutil parser (dayfirst=True for Asian/European formats)
3. Extract year, month, day, quarter, day of week
4. Flag parsing success/failure

### Location Classification
**Rules:**
- **Online:** Contains keywords (shopee, lazada, zalora, amazon, .com)
- **Transport:** Contains keywords (mrt, bus, taxi, station, interchange)
- **Physical:** Contains keywords (mall, restaurant, clinic) or default

### Payment Type Classification
**Rules:**
- **Card:** Contains keywords (card, visa, mastercard)
- **Digital Wallet:** Contains keywords (pay, wallet, apple, google, grab)
- **Transit Card:** Contains keywords (ez-link, nets)
- **Bank Transfer:** Contains keywords (bank, transfer, giro)
- **Other:** Default

### Category Grouping
**Mapping:**
- **Essential:** Groceries, Food, Utilities
- **Discretionary:** Shopping, Entertainment, Dining
- **Transport:** Transport, Transportation
- **Healthcare:** Healthcare, Medical, Health
- **Education:** Education, Learning, Books
- **Other:** Default

### Data Quality Scoring
**Calculation:** Starts at 100, deducts points for:
- Failed amount parsing: -30 points
- Failed date parsing: -30 points
- Missing description: -10 points
- Missing person name: -20 points
- Missing location: -5 points
- Missing category: -5 points

**Result:** Score between 0-100

## Data Quality Results

### Current Metrics (As of Latest Run)
- ✅ **Load Completeness:** 100% (6,000/6,000 records)
- ✅ **Data Quality Score:** 100/100 average
- ✅ **NULL Values:** 0 in critical fields
- ✅ **Foreign Key Integrity:** 100% valid
- ✅ **Amount Validity:** All positive, no outliers
- ✅ **Date Validity:** All valid, no future dates
- ✅ **Grade:** A+ (Excellent) - Production Ready

### Statistics
- **Total Transactions:** 6,000
- **Total Amount:** SGD 517,218.30
- **Average Transaction:** SGD 86.20
- **Date Range:** 2022-01-01 to 2024-12-31
- **Unique Dates:** 1,089
- **Persons:** 3 (David Wong, Mary Lim, John Tan)
- **Locations:** 48
- **Categories:** 9
- **Payment Methods:** 10

### Top Categories by Spending
1. **Shopping:** SGD 198,527 (38.4%)
2. **Food:** SGD 76,516 (14.8%)
3. **Groceries:** SGD 67,150 (13.0%)
4. **Healthcare:** SGD 46,427 (9.0%)
5. **Education:** SGD 36,897 (7.1%)

## Performance Optimizations

### Indexes Created
**Dimension Tables:**
- Unique indexes on natural keys (person_name, location_name, etc.)
- Indexed on classification fields (location_type, payment_type, category_group)

**Fact Table:**
- Single-column indexes on all foreign keys
- Index on spending_date
- Composite index on (spending_year, spending_month)
- Composite indexes for common query patterns:
  - (person_id, spending_date)
  - (category_id, spending_date)

### Processing Strategy
- Batch processing with progress indicators
- ON CONFLICT handling for dimension upserts (SCD Type 1)
- Bulk inserts for fact table
- Transaction isolation for consistency

## Query Examples

### Get All Spending for a Person
```sql
SELECT * 
FROM vw_stg_spending_complete
WHERE person_name = 'David Wong'
ORDER BY spending_date DESC;
```

### Monthly Spending by Category
```sql
SELECT 
    spending_year,
    spending_month,
    category_name,
    COUNT(*) as transactions,
    SUM(amount_cleaned) as total_amount
FROM stg_fact_spending f
JOIN stg_dim_category c ON f.category_id = c.category_id
GROUP BY spending_year, spending_month, category_name
ORDER BY spending_year DESC, spending_month DESC;
```

### Top Locations by Spending
```sql
SELECT 
    l.location_name,
    l.location_type,
    COUNT(*) as visits,
    SUM(f.amount_cleaned) as total_spent
FROM stg_fact_spending f
JOIN stg_dim_location l ON f.location_id = l.location_id
GROUP BY l.location_name, l.location_type
ORDER BY total_spent DESC
LIMIT 10;
```

### Payment Method Analysis
```sql
SELECT 
    pm.payment_method_name,
    pm.payment_type,
    COUNT(*) as usage_count,
    AVG(f.amount_cleaned) as avg_transaction
FROM stg_fact_spending f
JOIN stg_dim_payment_method pm ON f.payment_method_id = pm.payment_method_id
GROUP BY pm.payment_method_name, pm.payment_type
ORDER BY usage_count DESC;
```

## Maintenance

### Re-running Transformation
The improved transformation script (`03_improved_transform_and_load.py`) safely handles re-runs:
1. Clears all staging tables with TRUNCATE CASCADE
2. Resets surrogate key sequences
3. Re-transforms all source data
4. Validates completeness

### Incremental Updates
For incremental updates (not currently implemented):
1. Track last processed `src_id` or `load_batch_id`
2. Process only new records from source
3. Use ON CONFLICT for dimension updates (SCD Type 1)
4. Append to fact table

## Next Steps

### Immediate
- ✅ STG layer is complete and production-ready
- ✅ All 6,000 records successfully loaded with 100% quality score
- ✅ Data model follows 3NF principles

### Future Enhancements
1. **Slowly Changing Dimensions (SCD Type 2)**
   - Track historical changes in dimensions
   - Add effective_from, effective_to, is_current columns

2. **Data Mart Layer**
   - Create aggregated tables for specific analytics
   - Pre-calculate common metrics

3. **Anomaly Detection**
   - Identify unusual spending patterns
   - Flag outliers and suspicious transactions

4. **Advanced Analytics**
   - Time series forecasting
   - Customer segmentation
   - Spending pattern analysis

5. **Automation**
   - Schedule periodic ETL runs
   - Automated data quality alerts
   - Email notifications for failures

## Troubleshooting

### Issue: Date Parsing Failures
**Solution:** Use `03_improved_transform_and_load.py` which handles multiple date formats.

### Issue: Foreign Key Violations
**Cause:** Dimension tables not populated before fact table
**Solution:** Scripts process dimensions first, then facts.

### Issue: Duplicate Dimension Values
**Solution:** ON CONFLICT clauses ensure uniqueness without errors.

### Issue: Performance Degradation
**Solution:** 
- Check index usage with EXPLAIN ANALYZE
- Consider partitioning fact table by date
- Implement data archival strategy

## Contact & Support

For issues or questions about the STG stage:
1. Check this README
2. Review data quality report output
3. Examine SQL schema in `sql/02_stg_stage/`
4. Review transformation logic in Python scripts

---

**Last Updated:** 2025-10-18  
**Current Status:** ✅ Production Ready  
**Data Quality Grade:** A+ (Excellent)

