# CURATED Stage - Versioned Snapshot Layer

## 📋 Overview

The **CURATED stage** creates versioned snapshots of **ALL historical** spending data. Each version captures the complete data state at a specific point in time, enabling powerful time-travel analytics and historical comparisons.

---

## 🎯 Purpose

- **Version Management**: Track how your data evolves over time
- **Historical Preservation**: Keep complete snapshots for audit and compliance
- **Time-Travel Queries**: View data exactly as it existed on any past date
- **Growth Analysis**: Compare versions to understand data changes
- **Self-Contained**: Each snapshot includes denormalized dimensions for fast queries

---

## 🏗️ Architecture

### How CURATED Works

```
Oct 20, 2025: Run snapshot script
  → Version 1 created with 1000 records (ALL historical data)
  → is_latest = 1

Oct 21, 2025: Run snapshot script (50 new transactions in STG)
  → Version 1: is_latest changed to 0 (now historical)
  → Version 2 created with 1050 records (ALL data: 1000 + 50)
  → is_latest = 1

Oct 22, 2025: Run snapshot script (30 more transactions)
  → Version 1 & 2: is_latest = 0
  → Version 3 created with 1080 records (ALL data: 1050 + 30)
  → is_latest = 1
```

### Key Features

1. **Full Historical Snapshot**: Each version contains ALL data, not just changes
2. **Latest Flag**: `is_latest = 1` for current version, `0` for historical
3. **Denormalized**: Includes dimension values for fast queries without joins
4. **Data Lineage**: Links back to both SRC (`src_id`) and STG (`stg_spending_id`)
5. **Version Tracking**: Incremental version numbers (1, 2, 3, ...)

---

## 📊 Table Schema

### `curated_spending_snapshots` (28 columns)

| Category | Columns | Description |
|----------|---------|-------------|
| **Primary Key** | `snapshot_id` | Unique identifier (auto-increment) |
| **Version Control** | `snapshot_version`, `snapshot_date`, `snapshot_batch_id`, `is_latest` | Version management |
| **Data Lineage** | `src_id`, `stg_spending_id` | Track data origin |
| **Foreign Keys** | `person_id`, `category_id`, `location_id`, `payment_method_id` | Optional joins |
| **Denormalized Dimensions** | `person_name`, `category_name`, `category_group`, `location_name`, `location_type`, `payment_method_name`, `payment_type` | Fast queries without joins |
| **Time Dimensions** | `spending_date`, `spending_year`, `spending_month`, `spending_quarter`, `spending_day_of_week` | Time-based analysis |
| **Transaction Data** | `amount_cleaned`, `currency_code`, `description` | Spending details |
| **Quality** | `data_quality_score` | Quality tracking (0-100) |
| **Audit** | `created_at` | Timestamp |

---

## 🚀 Getting Started

### Prerequisites

- ✅ SRC stage completed (raw data loaded)
- ✅ STG stage completed (normalized data available)
- ✅ Environment variables configured (`.env` file)

### File Structure

```
/scripts/03_curated_stage/
├── 01_cur_tables_creation.py    # Step 1: Create snapshot table
├── 02_create_snapshot.py         # Step 2: Create version snapshots
├── 03_validation_report.py       # Step 3: Validate data quality
└── README.md                      # Documentation (this file)

/sql/03_curated_stage/
├── cur_01_create_table.sql       # DDL for snapshot table
└── cur_02_helper_queries.sql     # Common analysis queries
```

---

## 📝 Usage Guide

### Step 1: Create Snapshot Table (One-Time)

```bash
cd scripts/03_curated_stage
python 01_cur_tables_creation.py
```

**What it does:**
- Creates `curated_spending_snapshots` table
- Adds 8 performance indexes
- Sets up constraints and comments

**Expected output:**
```
================================================================================
CURATED STAGE - CREATING SNAPSHOT TABLE
================================================================================
✅ Curated snapshot table created successfully!
📊 Columns: 28
📊 Indexes: 8
```

---

### Step 2: Create Snapshot (Run Daily/Periodically)

```bash
python 02_create_snapshot.py
```

**What it does:**
1. Gets next version number (MAX + 1)
2. Updates all existing snapshots: `is_latest = 0`
3. Copies **ALL** data from STG as new version
4. Sets new snapshot: `is_latest = 1`
5. Shows validation statistics

**Expected output:**
```
================================================================================
CURATED STAGE - CREATE VERSIONED SNAPSHOT
================================================================================
STEP 1: Determine Next Version Number
✓ Current max version: 1
✓ Next version will be: 2

STEP 2: Check STG Data Availability
✓ STG has 1,050 records to snapshot

STEP 3: Update Existing Snapshots to Historical (is_latest = 0)
✓ Updated 1,000 records to is_latest = 0

STEP 4: Create New Snapshot (Version 2)
✓ Inserted 1,050 records as Version 2

STEP 5: Validation and Statistics
📊 Overall Statistics:
   Total versions: 2
   Total records: 2,050 (1000 + 1050)
   Latest records (is_latest=1): 1,050
   Historical records (is_latest=0): 1,000

✅ SNAPSHOT CREATION COMPLETED SUCCESSFULLY!
```

---

### Step 3: Run Validation

```bash
python 03_validation_report.py
```

**What it checks:**
1. **Version Integrity**: Only 1 version has `is_latest = 1`
2. **Data Completeness**: No missing required fields
3. **STG Consistency**: Latest snapshot matches STG count
4. **Version Growth**: Growth tracking works correctly
5. **Date Ranges**: Dates are valid and reasonable

**Expected output:**
```
================================================================================
CURATED STAGE - VALIDATION AND QUALITY REPORT
================================================================================
CHECK 1: Version Integrity (is_latest Flag)
✅ PASS: Exactly 1 version marked as is_latest = 1

CHECK 2: Data Completeness (Required Fields)
✅ PASS: All 1,050 records have complete required fields

CHECK 3: Data Consistency with STG Layer
✅ PASS: Latest CURATED snapshot matches STG count

CHECK 4: Version Growth Tracking
✅ PASS: Version tracking functional

CHECK 5: Date Range Validation
✅ PASS: Date ranges are reasonable

================================================================================
VALIDATION SUMMARY
================================================================================
🎉 ALL VALIDATIONS PASSED!
   CURATED layer is healthy and ready for analysis
```

---

## 📊 Query Examples

### Get Latest Snapshot (Most Common)

```sql
-- Get all current data
SELECT 
    person_name,
    category_name,
    spending_date,
    amount_cleaned,
    description
FROM curated_spending_snapshots
WHERE is_latest = 1
ORDER BY spending_date DESC;
```

### Time-Travel Query

```sql
-- What did data look like on Oct 20?
SELECT 
    person_name,
    category_name,
    SUM(amount_cleaned) as total_spent
FROM curated_spending_snapshots
WHERE snapshot_version = 1  -- Oct 20 snapshot
GROUP BY person_name, category_name;
```

### Compare Versions

```sql
-- How much did spending grow between versions?
SELECT 
    snapshot_version,
    COUNT(*) as record_count,
    SUM(amount_cleaned) as total_spending
FROM curated_spending_snapshots
GROUP BY snapshot_version
ORDER BY snapshot_version;
```

### Category Analysis (Fast - No Joins!)

```sql
-- Top spending categories (denormalized query)
SELECT 
    category_name,
    category_group,
    COUNT(*) as transactions,
    SUM(amount_cleaned) as total_spent
FROM curated_spending_snapshots
WHERE is_latest = 1
GROUP BY category_name, category_group
ORDER BY total_spent DESC
LIMIT 10;
```

**More queries**: See `sql/03_curated_stage/cur_02_helper_queries.sql` for 22+ analytical queries!

---

## 🔄 Workflow

### Daily/Periodic Snapshot Workflow

```bash
# 1. Load new raw data (if available)
cd scripts/01_src_stage
python 02_load_parquet_to_src.py

# 2. Transform to STG (processes new data)
cd ../02_stg_stage
python 02_transform_and_load_stg.py

# 3. Create new snapshot (captures growth)
cd ../03_curated_stage
python 02_create_snapshot.py

# 4. Validate (optional but recommended)
python 03_validation_report.py
```

### Growth Tracking Example

```
Day 1 (Oct 20):
  SRC: Load 500 records
  STG: Transform → 500 records
  CURATED: Snapshot V1 → 500 records (is_latest=1)

Day 2 (Oct 21):
  SRC: Load 50 new records → total 550
  STG: Transform → 550 records
  CURATED: Snapshot V2 → 550 records (is_latest=1)
           Version V1 → 500 records (is_latest=0)
  Growth: +50 records

Day 3 (Oct 22):
  SRC: Load 30 new records → total 580
  STG: Transform → 580 records
  CURATED: Snapshot V3 → 580 records (is_latest=1)
           Version V1 → 500 records (is_latest=0)
           Version V2 → 550 records (is_latest=0)
  Growth: +30 records
```

---

## 💡 Best Practices

### When to Run Snapshots

1. **Daily**: Most common - captures daily data growth
2. **After Major Updates**: When significant new data is added
3. **Before Analysis**: Ensure you have current snapshot for reporting
4. **Weekly/Monthly**: If data doesn't change frequently

### Storage Considerations

**Storage grows with versions:**
- 1 version with 1000 records = 1,000 total
- 2 versions with 1000 records each = 2,000 total
- 10 versions with ~1000 records each = ~10,000 total

**Cleanup strategy:**
```sql
-- Delete old versions (keep last 30 days)
DELETE FROM curated_spending_snapshots
WHERE snapshot_date < CURRENT_DATE - INTERVAL '30 days'
  AND is_latest = 0;
```

### Performance Tips

1. **Use `is_latest = 1` filter**: Queries are fast with the partial index
2. **Denormalized queries**: No joins needed for common analysis
3. **Batch operations**: Create snapshots during off-peak hours
4. **Archive old versions**: Move to cold storage after X days

---

## 🐛 Troubleshooting

### Issue: "No data found in stg_fact_spending"

**Solution:**
```bash
# Run STG transformation first
cd scripts/02_stg_stage
python 02_transform_and_load_stg.py
```

### Issue: Multiple versions have `is_latest = 1`

**Solution:**
```sql
-- Manually fix: Set all to 0, then set latest to 1
UPDATE curated_spending_snapshots SET is_latest = 0;

UPDATE curated_spending_snapshots 
SET is_latest = 1
WHERE snapshot_version = (
    SELECT MAX(snapshot_version) FROM curated_spending_snapshots
);
```

### Issue: Version count doesn't match STG

**Run validation:**
```bash
python 03_validation_report.py
```

Check the "Data Consistency" section for mismatches.

---

## 📈 Analytics Use Cases

### 1. Historical Trend Analysis
Track how spending evolved over time across versions.

### 2. Point-in-Time Reporting
Generate reports showing data state at specific dates (compliance, audit).

### 3. Growth Tracking
Monitor transaction volume growth and spending increases.

### 4. Data Quality Evolution
Track how `data_quality_score` improves over versions.

### 5. A/B Testing Snapshots
Compare different data processing approaches across versions.

---

## 🔗 Related Documentation

- **SRC Stage**: See `scripts/01_src_stage/` for raw data loading
- **STG Stage**: See `scripts/02_stg_stage/README.md` for transformation details
- **DST Stage** (Coming Next): Pre-aggregated analytics
- **DIS Stage** (Future): Financial recommendations and insights

---

## 🎓 Key Concepts

### Snapshot vs Incremental

| Snapshot (CURATED) | Incremental |
|-------------------|-------------|
| Stores ALL historical data in each version | Stores only changes (delta) |
| Larger storage | Smaller storage |
| Simple queries (no joins across versions) | Complex queries (union across deltas) |
| Fast point-in-time queries | Slower historical reconstruction |
| **Our choice** ✓ | Not used |

### Why Denormalize?

**CURATED is a consumption layer** for analytics:
- ✅ Fast queries (no joins to dimension tables)
- ✅ Self-contained (survives dimension changes)
- ✅ Historical accuracy (preserves exact values at snapshot time)
- ✅ Analyst-friendly (simple SQL)

**Trade-off**: More storage space (worth it for performance!)

---

## 📊 Success Metrics

After running CURATED stage successfully:

✅ **Snapshot table created** with 28 columns and 8 indexes  
✅ **First snapshot created** (Version 1) with all STG data  
✅ **Validation passed** with no errors  
✅ **Queries run fast** using `is_latest = 1` filter  
✅ **Ready for DST stage** (next: pre-aggregations)

---

## 🆘 Need Help?

1. **Check validation report**: `python 03_validation_report.py`
2. **Review helper queries**: `sql/03_curated_stage/cur_02_helper_queries.sql`
3. **Inspect table structure**: Check column names and types
4. **Test with sample queries**: Verify `is_latest` logic works

---

**Next Stage**: DST (Dissemination Staging) - Pre-aggregated tables for fast reporting

---

**Last Updated**: October 21, 2025  
**Stage Progress**: CURATED - 100% Complete ✅

