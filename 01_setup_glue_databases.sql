-- =============================================================================
-- GLUE DATABASES SETUP
-- =============================================================================
-- This script creates the three Glue databases for the Thera data pipeline:
-- - thera_bronze: Raw data from external sources (CSV, JSON)
-- - thera_silver: Cleaned and normalized data (Parquet)
-- - thera_gold: Business-ready aggregated data (Parquet)

-- =============================================================================
-- DATABASE: thera_bronze
-- =============================================================================
-- Purpose: Store raw external data from various sources
-- Data formats: CSV, JSON Lines
-- Storage: s3://thera-raw/
CREATE DATABASE IF NOT EXISTS thera_bronze
COMMENT 'Raw data from external sources - CSV dumps and JSON lines'
LOCATION 's3://thera-raw/'
TBLPROPERTIES (
    'classification' = 'raw',
    'data_quality' = 'bronze',
    'retention_days' = '90'
);

-- =============================================================================
-- DATABASE: thera_silver
-- =============================================================================
-- Purpose: Cleaned and normalized data ready for analysis
-- Data formats: Parquet (partitioned by dt)
-- Storage: s3://thera-curated/silver/
CREATE DATABASE IF NOT EXISTS thera_silver
COMMENT 'Cleaned and normalized data - Parquet format with partitioning'
LOCATION 's3://thera-curated/silver/'
TBLPROPERTIES (
    'classification' = 'curated',
    'data_quality' = 'silver',
    'retention_days' = '365',
    'partitioning' = 'dt'
);

-- =============================================================================
-- DATABASE: thera_gold
-- =============================================================================
-- Purpose: Business-ready aggregated data for analytics and ML
-- Data formats: Parquet (partitioned by dt)
-- Storage: s3://thera-curated/gold/
CREATE DATABASE IF NOT EXISTS thera_gold
COMMENT 'Business-ready aggregated data for analytics and ML'
LOCATION 's3://thera-curated/gold/'
TBLPROPERTIES (
    'classification' = 'aggregated',
    'data_quality' = 'gold',
    'retention_days' = '730',
    'partitioning' = 'dt'
);

-- =============================================================================
-- VERIFICATION QUERIES
-- =============================================================================
-- Uncomment to verify database creation:
-- SHOW DATABASES LIKE 'thera_*';

-- =============================================================================
-- NOTES FOR CONFIGURATION
-- =============================================================================
-- 1. Adjust S3 bucket names in LOCATION properties if using different buckets
-- 2. Modify retention_days based on your data retention policy
-- 3. Add additional TBLPROPERTIES as needed for your organization
-- 4. Ensure proper IAM permissions for Glue to access S3 buckets
-- 5. Consider adding encryption properties if required by compliance
