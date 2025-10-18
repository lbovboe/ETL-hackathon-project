-- ============================================
-- STAGE 2: STG (STAGING) - CREATE NORMALIZED TABLES
-- Purpose: Transform raw data into 3NF normalized, clean structure
-- ============================================

-- ============================================
-- DIMENSION TABLE: stg_dim_person
-- Purpose: Stores unique persons/customers
-- ============================================
CREATE TABLE IF NOT EXISTS stg_dim_person (
    person_id SERIAL PRIMARY KEY,
    person_name VARCHAR(255) NOT NULL UNIQUE,
    
    -- ETL metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_stg_person_name ON stg_dim_person(person_name);

COMMENT ON TABLE stg_dim_person IS 'Dimension table for persons/customers in 3NF';
COMMENT ON COLUMN stg_dim_person.person_id IS 'Surrogate key for person dimension';

-- ============================================
-- DIMENSION TABLE: stg_dim_location
-- Purpose: Stores unique spending locations
-- ============================================
CREATE TABLE IF NOT EXISTS stg_dim_location (
    location_id SERIAL PRIMARY KEY,
    location_name VARCHAR(255) NOT NULL UNIQUE,
    location_type VARCHAR(50), -- e.g., 'Online', 'Physical', 'Transport'
    
    -- ETL metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_stg_location_name ON stg_dim_location(location_name);
CREATE INDEX IF NOT EXISTS idx_stg_location_type ON stg_dim_location(location_type);

COMMENT ON TABLE stg_dim_location IS 'Dimension table for spending locations in 3NF';
COMMENT ON COLUMN stg_dim_location.location_id IS 'Surrogate key for location dimension';
COMMENT ON COLUMN stg_dim_location.location_type IS 'Categorization of location (Online, Physical, Transport)';

-- ============================================
-- DIMENSION TABLE: stg_dim_category
-- Purpose: Stores spending categories
-- ============================================
CREATE TABLE IF NOT EXISTS stg_dim_category (
    category_id SERIAL PRIMARY KEY,
    category_name VARCHAR(100) NOT NULL UNIQUE,
    category_group VARCHAR(50), -- e.g., 'Essential', 'Discretionary', 'Transport'
    
    -- ETL metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_stg_category_name ON stg_dim_category(category_name);
CREATE INDEX IF NOT EXISTS idx_stg_category_group ON stg_dim_category(category_group);

COMMENT ON TABLE stg_dim_category IS 'Dimension table for spending categories in 3NF';
COMMENT ON COLUMN stg_dim_category.category_id IS 'Surrogate key for category dimension';
COMMENT ON COLUMN stg_dim_category.category_group IS 'Higher-level grouping of categories';

-- ============================================
-- DIMENSION TABLE: stg_dim_payment_method
-- Purpose: Stores payment methods
-- ============================================
CREATE TABLE IF NOT EXISTS stg_dim_payment_method (
    payment_method_id SERIAL PRIMARY KEY,
    payment_method_name VARCHAR(100) NOT NULL UNIQUE,
    payment_type VARCHAR(50), -- e.g., 'Card', 'Digital Wallet', 'Cash'
    
    -- ETL metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_stg_payment_name ON stg_dim_payment_method(payment_method_name);
CREATE INDEX IF NOT EXISTS idx_stg_payment_type ON stg_dim_payment_method(payment_type);

COMMENT ON TABLE stg_dim_payment_method IS 'Dimension table for payment methods in 3NF';
COMMENT ON COLUMN stg_dim_payment_method.payment_method_id IS 'Surrogate key for payment method dimension';
COMMENT ON COLUMN stg_dim_payment_method.payment_type IS 'Categorization of payment method';

-- ============================================
-- FACT TABLE: stg_fact_spending
-- Purpose: Main fact table with cleaned, normalized spending transactions
-- ============================================
CREATE TABLE IF NOT EXISTS stg_fact_spending (
    spending_id BIGSERIAL PRIMARY KEY,
    
    -- Foreign keys to dimension tables
    person_id INTEGER NOT NULL REFERENCES stg_dim_person(person_id),
    location_id INTEGER NOT NULL REFERENCES stg_dim_location(location_id),
    category_id INTEGER NOT NULL REFERENCES stg_dim_category(category_id),
    payment_method_id INTEGER NOT NULL REFERENCES stg_dim_payment_method(payment_method_id),
    
    -- Date dimension (properly typed)
    spending_date DATE NOT NULL,
    spending_year INTEGER NOT NULL,
    spending_month INTEGER NOT NULL,
    spending_day INTEGER NOT NULL,
    spending_quarter INTEGER NOT NULL,
    spending_day_of_week VARCHAR(10) NOT NULL, -- Monday, Tuesday, etc.
    
    -- Financial measures (using NUMERIC for precision)
    amount_raw VARCHAR(50), -- Original amount string for reference
    amount_cleaned NUMERIC(12, 2) NOT NULL, -- Cleaned numeric amount
    currency_code VARCHAR(3) DEFAULT 'SGD', -- ISO currency code
    
    -- Transaction details
    description TEXT,
    
    -- Data quality flags
    is_amount_parsed_successfully BOOLEAN DEFAULT TRUE,
    is_date_parsed_successfully BOOLEAN DEFAULT TRUE,
    data_quality_score INTEGER, -- 0-100 score based on completeness
    
    -- ETL metadata - link back to source
    src_id BIGINT NOT NULL, -- Reference to src_daily_spending
    transform_batch_id VARCHAR(100) NOT NULL,
    transformed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_stg_spending_person ON stg_fact_spending(person_id);
CREATE INDEX IF NOT EXISTS idx_stg_spending_location ON stg_fact_spending(location_id);
CREATE INDEX IF NOT EXISTS idx_stg_spending_category ON stg_fact_spending(category_id);
CREATE INDEX IF NOT EXISTS idx_stg_spending_payment ON stg_fact_spending(payment_method_id);
CREATE INDEX IF NOT EXISTS idx_stg_spending_date ON stg_fact_spending(spending_date);
CREATE INDEX IF NOT EXISTS idx_stg_spending_year_month ON stg_fact_spending(spending_year, spending_month);
CREATE INDEX IF NOT EXISTS idx_stg_spending_src ON stg_fact_spending(src_id);
CREATE INDEX IF NOT EXISTS idx_stg_spending_batch ON stg_fact_spending(transform_batch_id);

-- Composite index for common queries
CREATE INDEX IF NOT EXISTS idx_stg_spending_person_date 
    ON stg_fact_spending(person_id, spending_date);
CREATE INDEX IF NOT EXISTS idx_stg_spending_category_date 
    ON stg_fact_spending(category_id, spending_date);

COMMENT ON TABLE stg_fact_spending IS 'Normalized fact table for spending transactions with cleaned, typed data in 3NF';
COMMENT ON COLUMN stg_fact_spending.spending_id IS 'Surrogate key for spending fact';
COMMENT ON COLUMN stg_fact_spending.amount_cleaned IS 'Cleaned numeric amount in standard decimal format';
COMMENT ON COLUMN stg_fact_spending.currency_code IS 'ISO 4217 currency code (default: SGD)';
COMMENT ON COLUMN stg_fact_spending.data_quality_score IS 'Quality score 0-100 based on data completeness and validity';
COMMENT ON COLUMN stg_fact_spending.src_id IS 'Foreign key back to src_daily_spending for lineage';

-- ============================================
-- VIEW: vw_stg_spending_complete
-- Purpose: Denormalized view for easy querying
-- ============================================
CREATE OR REPLACE VIEW vw_stg_spending_complete AS
SELECT 
    f.spending_id,
    p.person_name,
    f.spending_date,
    f.spending_year,
    f.spending_month,
    f.spending_quarter,
    f.spending_day_of_week,
    c.category_name,
    c.category_group,
    f.amount_cleaned,
    f.currency_code,
    l.location_name,
    l.location_type,
    f.description,
    pm.payment_method_name,
    pm.payment_type,
    f.data_quality_score,
    f.src_id,
    f.transformed_at
FROM stg_fact_spending f
JOIN stg_dim_person p ON f.person_id = p.person_id
JOIN stg_dim_location l ON f.location_id = l.location_id
JOIN stg_dim_category c ON f.category_id = c.category_id
JOIN stg_dim_payment_method pm ON f.payment_method_id = pm.payment_method_id;

COMMENT ON VIEW vw_stg_spending_complete IS 'Denormalized view of spending data for easy analysis';

