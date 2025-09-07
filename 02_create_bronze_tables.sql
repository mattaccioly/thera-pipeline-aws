-- =============================================================================
-- BRONZE LAYER EXTERNAL TABLES
-- =============================================================================
-- This script creates external tables for raw data ingestion from various sources
-- All tables are external and point to S3 raw data locations

USE thera_bronze;

-- =============================================================================
-- TABLE: bronze_crunchbase_ext
-- =============================================================================
-- Purpose: External table for Crunchbase CSV dumps
-- Source: s3://thera-raw/crunchbase/
-- Format: CSV with header
CREATE EXTERNAL TABLE IF NOT EXISTS bronze_crunchbase_ext (
    -- Basic company information
    company_name STRING,
    domain STRING,
    linkedin_url STRING,
    website_url STRING,
    
    -- Location and industry
    country STRING,
    city STRING,
    state STRING,
    industry STRING,
    sub_industry STRING,
    
    -- Company details
    description STRING,
    founded_year INT,
    employee_count INT,
    employee_range STRING,
    
    -- Financial information
    total_funding DECIMAL(15,2),
    last_funding_date STRING,
    last_funding_type STRING,
    last_funding_amount DECIMAL(15,2),
    
    -- Additional fields
    crunchbase_url STRING,
    facebook_url STRING,
    twitter_url STRING,
    instagram_url STRING,
    
    -- Metadata
    raw_data STRING,
    ingestion_timestamp TIMESTAMP
)
COMMENT 'External table for Crunchbase CSV data dumps'
STORED AS TEXTFILE
LOCATION 's3://thera-raw/crunchbase/'
TBLPROPERTIES (
    'skip.header.line.count' = '1',
    'serialization.format' = ',',
    'field.delim' = ',',
    'quote.delim' = '"',
    'escape.delim' = '\\'
);

-- =============================================================================
-- TABLE: bronze_apollo_companies
-- =============================================================================
-- Purpose: External table for Apollo companies JSON data
-- Source: s3://thera-raw/apollo/companies/
-- Format: JSON Lines
CREATE EXTERNAL TABLE IF NOT EXISTS bronze_apollo_companies (
    -- Raw JSON data
    raw_json STRING
)
COMMENT 'External table for Apollo companies JSON Lines data'
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION 's3://thera-raw/apollo/companies/'
TBLPROPERTIES (
    'serialization.format' = '1'
);

-- =============================================================================
-- TABLE: bronze_apollo_contacts
-- =============================================================================
-- Purpose: External table for Apollo contacts JSON data
-- Source: s3://thera-raw/apollo/contacts/
-- Format: JSON Lines
CREATE EXTERNAL TABLE IF NOT EXISTS bronze_apollo_contacts (
    -- Raw JSON data
    raw_json STRING
)
COMMENT 'External table for Apollo contacts JSON Lines data'
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION 's3://thera-raw/apollo/contacts/'
TBLPROPERTIES (
    'serialization.format' = '1'
);

-- =============================================================================
-- TABLE: bronze_firecrawl_pages
-- =============================================================================
-- Purpose: External table for Firecrawl scraped pages JSON data
-- Source: s3://thera-raw/firecrawl/pages/
-- Format: JSON Lines
CREATE EXTERNAL TABLE IF NOT EXISTS bronze_firecrawl_pages (
    -- Raw JSON data from Firecrawl
    raw_json STRING
)
COMMENT 'External table for Firecrawl scraped pages JSON Lines data'
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION 's3://thera-raw/firecrawl/pages/'
TBLPROPERTIES (
    'serialization.format' = '1'
);

-- =============================================================================
-- VERIFICATION QUERIES
-- =============================================================================
-- Uncomment to verify table creation and sample data:
-- SHOW TABLES IN thera_bronze;
-- SELECT COUNT(*) FROM bronze_crunchbase_ext LIMIT 10;
-- SELECT raw_json FROM bronze_apollo_companies LIMIT 5;
-- SELECT raw_json FROM bronze_apollo_contacts LIMIT 5;
-- SELECT raw_json FROM bronze_firecrawl_pages LIMIT 5;

-- =============================================================================
-- NOTES FOR CONFIGURATION
-- =============================================================================
-- 1. Adjust S3 paths in LOCATION properties based on your actual bucket structure
-- 2. Modify column names and types based on actual CSV headers from Crunchbase
-- 3. For JSON tables, consider using OpenX JSON SerDe for better performance:
--    ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
-- 4. Add additional TBLPROPERTIES for compression if files are compressed
-- 5. Consider adding partitioning for time-based data if applicable
-- 6. Ensure proper IAM permissions for Glue to access S3 locations
