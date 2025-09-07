-- =============================================================================
-- S3 PARTITIONING STRUCTURE SETUP
-- =============================================================================
-- This script sets up proper S3 partitioning structure for silver/ and gold/ tables
-- under s3://thera-curated/ with time-based partitioning by dt

-- =============================================================================
-- S3 PARTITIONING CONFIGURATION
-- =============================================================================

-- Update table properties to enable partitioning
ALTER TABLE thera_silver.silver_companies SET TBLPROPERTIES (
    'partitioning' = 'dt',
    'partitioning.type' = 'time',
    'partitioning.format' = 'yyyy-MM-dd',
    'partitioning.retention.days' = '365',
    'partitioning.compression' = 'SNAPPY'
);

ALTER TABLE thera_silver.silver_apollo_companies SET TBLPROPERTIES (
    'partitioning' = 'dt',
    'partitioning.type' = 'time',
    'partitioning.format' = 'yyyy-MM-dd',
    'partitioning.retention.days' = '365',
    'partitioning.compression' = 'SNAPPY'
);

ALTER TABLE thera_silver.silver_apollo_contacts SET TBLPROPERTIES (
    'partitioning' = 'dt',
    'partitioning.type' = 'time',
    'partitioning.format' = 'yyyy-MM-dd',
    'partitioning.retention.days' = '365',
    'partitioning.compression' = 'SNAPPY'
);

ALTER TABLE thera_silver.silver_domain_health SET TBLPROPERTIES (
    'partitioning' = 'dt',
    'partitioning.type' = 'time',
    'partitioning.format' = 'yyyy-MM-dd',
    'partitioning.retention.days' = '365',
    'partitioning.compression' = 'SNAPPY'
);

ALTER TABLE thera_silver.silver_web_extracts SET TBLPROPERTIES (
    'partitioning' = 'dt',
    'partitioning.type' = 'time',
    'partitioning.format' = 'yyyy-MM-dd',
    'partitioning.retention.days' = '365',
    'partitioning.compression' = 'SNAPPY'
);

ALTER TABLE thera_gold.gold_startup_profiles SET TBLPROPERTIES (
    'partitioning' = 'dt',
    'partitioning.type' = 'time',
    'partitioning.format' = 'yyyy-MM-dd',
    'partitioning.retention.days' = '730',
    'partitioning.compression' = 'SNAPPY'
);

-- =============================================================================
-- S3 LIFECYCLE POLICIES
-- =============================================================================

-- Create S3 lifecycle policies for data retention
-- Note: These would be implemented in AWS S3 console or via CloudFormation

-- Bronze layer lifecycle policy (90 days retention)
-- s3://thera-raw/
-- - Transition to IA after 30 days
-- - Transition to Glacier after 60 days
-- - Delete after 90 days

-- Silver layer lifecycle policy (365 days retention)
-- s3://thera-curated/silver/
-- - Transition to IA after 30 days
-- - Transition to Glacier after 90 days
-- - Delete after 365 days

-- Gold layer lifecycle policy (730 days retention)
-- s3://thera-curated/gold/
-- - Transition to IA after 30 days
-- - Transition to Glacier after 180 days
-- - Delete after 730 days

-- =============================================================================
-- PARTITION MANAGEMENT QUERIES
-- =============================================================================

-- Query to check partition status
CREATE OR REPLACE VIEW partition_status AS
SELECT 
    'silver_companies' as table_name,
    dt,
    COUNT(*) as record_count,
    MIN(created_at) as earliest_record,
    MAX(created_at) as latest_record
FROM thera_silver.silver_companies
GROUP BY dt
UNION ALL
SELECT 
    'silver_apollo_companies',
    dt,
    COUNT(*),
    MIN(created_at),
    MAX(created_at)
FROM thera_silver.silver_apollo_companies
GROUP BY dt
UNION ALL
SELECT 
    'silver_apollo_contacts',
    dt,
    COUNT(*),
    MIN(created_at),
    MAX(created_at)
FROM thera_silver.silver_apollo_contacts
GROUP BY dt
UNION ALL
SELECT 
    'silver_domain_health',
    dt,
    COUNT(*),
    MIN(created_at),
    MAX(created_at)
FROM thera_silver.silver_domain_health
GROUP BY dt
UNION ALL
SELECT 
    'silver_web_extracts',
    dt,
    COUNT(*),
    MIN(created_at),
    MAX(created_at)
FROM thera_silver.silver_web_extracts
GROUP BY dt
UNION ALL
SELECT 
    'gold_startup_profiles',
    dt,
    COUNT(*),
    MIN(created_at),
    MAX(created_at)
FROM thera_gold.gold_startup_profiles
GROUP BY dt
ORDER BY table_name, dt DESC;

-- Query to identify missing partitions
CREATE OR REPLACE VIEW missing_partitions AS
WITH date_range AS (
    SELECT date_add('day', -30, current_date) as start_date,
           current_date as end_date
),
expected_dates AS (
    SELECT date_format(date_add('day', pos, start_date), 'yyyy-MM-dd') as dt
    FROM date_range
    CROSS JOIN UNNEST(sequence(0, date_diff('day', start_date, end_date))) as t(pos)
),
existing_partitions AS (
    SELECT 'silver_companies' as table_name, dt FROM thera_silver.silver_companies GROUP BY dt
    UNION ALL
    SELECT 'silver_apollo_companies', dt FROM thera_silver.silver_apollo_companies GROUP BY dt
    UNION ALL
    SELECT 'silver_apollo_contacts', dt FROM thera_silver.silver_apollo_contacts GROUP BY dt
    UNION ALL
    SELECT 'silver_domain_health', dt FROM thera_silver.silver_domain_health GROUP BY dt
    UNION ALL
    SELECT 'silver_web_extracts', dt FROM thera_silver.silver_web_extracts GROUP BY dt
    UNION ALL
    SELECT 'gold_startup_profiles', dt FROM thera_gold.gold_startup_profiles GROUP BY dt
)
SELECT 
    ed.dt,
    'silver_companies' as missing_table
FROM expected_dates ed
LEFT JOIN existing_partitions ep ON ed.dt = ep.dt AND ep.table_name = 'silver_companies'
WHERE ep.dt IS NULL
UNION ALL
SELECT 
    ed.dt,
    'silver_apollo_companies'
FROM expected_dates ed
LEFT JOIN existing_partitions ep ON ed.dt = ep.dt AND ep.table_name = 'silver_apollo_companies'
WHERE ep.dt IS NULL
UNION ALL
SELECT 
    ed.dt,
    'silver_apollo_contacts'
FROM expected_dates ed
LEFT JOIN existing_partitions ep ON ed.dt = ep.dt AND ep.table_name = 'silver_apollo_contacts'
WHERE ep.dt IS NULL
UNION ALL
SELECT 
    ed.dt,
    'silver_domain_health'
FROM expected_dates ed
LEFT JOIN existing_partitions ep ON ed.dt = ep.dt AND ep.table_name = 'silver_domain_health'
WHERE ep.dt IS NULL
UNION ALL
SELECT 
    ed.dt,
    'silver_web_extracts'
FROM expected_dates ed
LEFT JOIN existing_partitions ep ON ed.dt = ep.dt AND ep.table_name = 'silver_web_extracts'
WHERE ep.dt IS NULL
UNION ALL
SELECT 
    ed.dt,
    'gold_startup_profiles'
FROM expected_dates ed
LEFT JOIN existing_partitions ep ON ed.dt = ep.dt AND ep.table_name = 'gold_startup_profiles'
WHERE ep.dt IS NULL
ORDER BY dt DESC, missing_table;

-- =============================================================================
-- PARTITION OPTIMIZATION QUERIES
-- =============================================================================

-- Query to analyze partition sizes
CREATE OR REPLACE VIEW partition_size_analysis AS
SELECT 
    'silver_companies' as table_name,
    dt,
    COUNT(*) as record_count,
    COUNT(DISTINCT company_key) as unique_companies,
    COUNT(DISTINCT domain) as unique_domains,
    ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT company_key), 2) as avg_records_per_company
FROM thera_silver.silver_companies
GROUP BY dt
UNION ALL
SELECT 
    'silver_apollo_companies',
    dt,
    COUNT(*),
    COUNT(DISTINCT apollo_id),
    COUNT(DISTINCT domain),
    ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT apollo_id), 2)
FROM thera_silver.silver_apollo_companies
GROUP BY dt
UNION ALL
SELECT 
    'silver_apollo_contacts',
    dt,
    COUNT(*),
    COUNT(DISTINCT apollo_id),
    COUNT(DISTINCT company_key),
    ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT company_key), 2)
FROM thera_silver.silver_apollo_contacts
GROUP BY dt
UNION ALL
SELECT 
    'silver_domain_health',
    dt,
    COUNT(*),
    COUNT(DISTINCT domain),
    COUNT(DISTINCT company_key),
    ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT domain), 2)
FROM thera_silver.silver_domain_health
GROUP BY dt
UNION ALL
SELECT 
    'silver_web_extracts',
    dt,
    COUNT(*),
    COUNT(DISTINCT company_key),
    COUNT(DISTINCT domain),
    ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT company_key), 2)
FROM thera_silver.silver_web_extracts
GROUP BY dt
UNION ALL
SELECT 
    'gold_startup_profiles',
    dt,
    COUNT(*),
    COUNT(DISTINCT company_key),
    COUNT(DISTINCT domain),
    ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT company_key), 2)
FROM thera_gold.gold_startup_profiles
GROUP BY dt
ORDER BY table_name, dt DESC;

-- =============================================================================
-- PARTITION MAINTENANCE PROCEDURES
-- =============================================================================

-- Procedure to add new partition
CREATE OR REPLACE PROCEDURE add_partition(
    table_name STRING,
    partition_date STRING
)
LANGUAGE SQL
AS $$
    -- Add partition for the specified table and date
    -- This would be implemented based on the specific table
    CASE table_name
        WHEN 'silver_companies' THEN
            ALTER TABLE thera_silver.silver_companies ADD IF NOT EXISTS PARTITION (dt = partition_date)
        WHEN 'silver_apollo_companies' THEN
            ALTER TABLE thera_silver.silver_apollo_companies ADD IF NOT EXISTS PARTITION (dt = partition_date)
        WHEN 'silver_apollo_contacts' THEN
            ALTER TABLE thera_silver.silver_apollo_contacts ADD IF NOT EXISTS PARTITION (dt = partition_date)
        WHEN 'silver_domain_health' THEN
            ALTER TABLE thera_silver.silver_domain_health ADD IF NOT EXISTS PARTITION (dt = partition_date)
        WHEN 'silver_web_extracts' THEN
            ALTER TABLE thera_silver.silver_web_extracts ADD IF NOT EXISTS PARTITION (dt = partition_date)
        WHEN 'gold_startup_profiles' THEN
            ALTER TABLE thera_gold.gold_startup_profiles ADD IF NOT EXISTS PARTITION (dt = partition_date)
        ELSE
            SELECT 'Unknown table: ' || table_name
    END
$$;

-- Procedure to drop old partitions
CREATE OR REPLACE PROCEDURE drop_old_partitions(
    table_name STRING,
    retention_days INT
)
LANGUAGE SQL
AS $$
    -- Drop partitions older than retention_days
    -- This would be implemented based on the specific table
    CASE table_name
        WHEN 'silver_companies' THEN
            ALTER TABLE thera_silver.silver_companies DROP IF EXISTS PARTITION (dt < date_format(date_add('day', -retention_days, current_date), 'yyyy-MM-dd'))
        WHEN 'silver_apollo_companies' THEN
            ALTER TABLE thera_silver.silver_apollo_companies DROP IF EXISTS PARTITION (dt < date_format(date_add('day', -retention_days, current_date), 'yyyy-MM-dd'))
        WHEN 'silver_apollo_contacts' THEN
            ALTER TABLE thera_silver.silver_apollo_contacts DROP IF EXISTS PARTITION (dt < date_format(date_add('day', -retention_days, current_date), 'yyyy-MM-dd'))
        WHEN 'silver_domain_health' THEN
            ALTER TABLE thera_silver.silver_domain_health DROP IF EXISTS PARTITION (dt < date_format(date_add('day', -retention_days, current_date), 'yyyy-MM-dd'))
        WHEN 'silver_web_extracts' THEN
            ALTER TABLE thera_silver.silver_web_extracts DROP IF EXISTS PARTITION (dt < date_format(date_add('day', -retention_days, current_date), 'yyyy-MM-dd'))
        WHEN 'gold_startup_profiles' THEN
            ALTER TABLE thera_gold.gold_startup_profiles DROP IF EXISTS PARTITION (dt < date_format(date_add('day', -retention_days, current_date), 'yyyy-MM-dd'))
        ELSE
            SELECT 'Unknown table: ' || table_name
    END
$$;

-- =============================================================================
-- S3 STORAGE OPTIMIZATION
-- =============================================================================

-- Query to analyze storage usage by partition
CREATE OR REPLACE VIEW storage_usage_analysis AS
SELECT 
    'silver_companies' as table_name,
    dt,
    COUNT(*) as record_count,
    -- Estimate storage size (rough calculation)
    ROUND(COUNT(*) * 2.5, 2) as estimated_size_mb
FROM thera_silver.silver_companies
GROUP BY dt
UNION ALL
SELECT 
    'silver_apollo_companies',
    dt,
    COUNT(*),
    ROUND(COUNT(*) * 3.0, 2)
FROM thera_silver.silver_apollo_companies
GROUP BY dt
UNION ALL
SELECT 
    'silver_apollo_contacts',
    dt,
    COUNT(*),
    ROUND(COUNT(*) * 2.0, 2)
FROM thera_silver.silver_apollo_contacts
GROUP BY dt
UNION ALL
SELECT 
    'silver_domain_health',
    dt,
    COUNT(*),
    ROUND(COUNT(*) * 1.5, 2)
FROM thera_silver.silver_domain_health
GROUP BY dt
UNION ALL
SELECT 
    'silver_web_extracts',
    dt,
    COUNT(*),
    ROUND(COUNT(*) * 5.0, 2)
FROM thera_silver.silver_web_extracts
GROUP BY dt
UNION ALL
SELECT 
    'gold_startup_profiles',
    dt,
    COUNT(*),
    ROUND(COUNT(*) * 4.0, 2)
FROM thera_gold.gold_startup_profiles
GROUP BY dt
ORDER BY table_name, dt DESC;

-- =============================================================================
-- PARTITION MONITORING QUERIES
-- =============================================================================

-- Query to monitor partition health
CREATE OR REPLACE VIEW partition_health_monitor AS
SELECT 
    table_name,
    dt,
    record_count,
    CASE 
        WHEN record_count = 0 THEN 'EMPTY'
        WHEN record_count < 100 THEN 'LOW'
        WHEN record_count < 1000 THEN 'NORMAL'
        WHEN record_count < 10000 THEN 'HIGH'
        ELSE 'VERY_HIGH'
    END as partition_size_category,
    CASE 
        WHEN dt = date_format(current_date, 'yyyy-MM-dd') THEN 'CURRENT'
        WHEN dt = date_format(date_add('day', -1, current_date), 'yyyy-MM-dd') THEN 'YESTERDAY'
        WHEN dt >= date_format(date_add('day', -7, current_date), 'yyyy-MM-dd') THEN 'RECENT'
        WHEN dt >= date_format(date_add('day', -30, current_date), 'yyyy-MM-dd') THEN 'OLD'
        ELSE 'VERY_OLD'
    END as partition_age_category
FROM partition_status
ORDER BY table_name, dt DESC;

-- =============================================================================
-- NOTES FOR CONFIGURATION
-- =============================================================================
-- 1. Adjust retention policies based on business requirements
-- 2. Monitor partition sizes and optimize if needed
-- 3. Set up automated partition management
-- 4. Consider adding additional partitioning dimensions if needed
-- 5. Monitor S3 costs and adjust lifecycle policies accordingly
-- 6. Add partition repair procedures for corrupted partitions
