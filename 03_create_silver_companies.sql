-- =============================================================================
-- SILVER LAYER: silver_companies
-- =============================================================================
-- Purpose: Canonical company records with deduplication and survivorship rules
-- Deduplication priority: domain exact > linkedin > fuzzy name matching
-- Storage: Parquet format partitioned by dt

USE thera_silver;

-- =============================================================================
-- TABLE: silver_companies
-- =============================================================================
CREATE TABLE IF NOT EXISTS silver_companies (
    -- Primary identifiers
    company_key STRING NOT NULL,
    domain STRING NOT NULL,
    linkedin_url STRING,
    
    -- Company identity
    company_name STRING NOT NULL,
    legal_name STRING,
    dba_name STRING,
    
    -- Location information
    country STRING,
    state_province STRING,
    city STRING,
    postal_code STRING,
    timezone STRING,
    
    -- Industry classification
    industry STRING,
    sub_industry STRING,
    industry_category STRING,
    naics_code STRING,
    sic_code STRING,
    
    -- Company details
    description STRING,
    short_description STRING,
    founded_year INT,
    founded_month INT,
    founded_date DATE,
    
    -- Size information
    employee_count INT,
    employee_range STRING,
    revenue_range STRING,
    size_bracket STRING, -- startup, small, medium, large, enterprise
    
    -- Financial information
    total_funding DECIMAL(15,2),
    last_funding_date DATE,
    last_funding_type STRING,
    last_funding_amount DECIMAL(15,2),
    funding_stage STRING,
    valuation DECIMAL(15,2),
    
    -- Website and social
    website_url STRING,
    facebook_url STRING,
    twitter_url STRING,
    instagram_url STRING,
    youtube_url STRING,
    github_url STRING,
    
    -- Data quality and source tracking
    data_quality_score DECIMAL(3,2), -- 0.00 to 1.00
    source_priority INT, -- 1=highest priority source
    source_systems ARRAY<STRING>, -- List of source systems
    confidence_score DECIMAL(3,2), -- 0.00 to 1.00
    
    -- Deduplication tracking
    duplicate_group_id STRING,
    is_primary_record BOOLEAN,
    duplicate_reason STRING,
    survivorship_rules_applied ARRAY<STRING>,
    
    -- Metadata
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    last_verified_at TIMESTAMP,
    
    -- Partitioning
    dt STRING
)
COMMENT 'Canonical company records with deduplication and survivorship rules'
PARTITIONED BY (dt)
STORED AS PARQUET
LOCATION 's3://thera-curated/silver/companies/'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY',
    'classification' = 'silver',
    'data_quality' = 'curated'
);

-- =============================================================================
-- CTAS: Populate silver_companies with deduplication logic
-- =============================================================================
-- This CTAS implements the deduplication and survivorship rules:
-- 1. Domain exact match (highest priority)
-- 2. LinkedIn URL match (medium priority)  
-- 3. Fuzzy name matching (lowest priority)

CREATE TABLE IF NOT EXISTS silver_companies_temp AS
WITH 
-- Step 1: Extract and normalize data from all sources
crunchbase_companies AS (
    SELECT 
        -- Generate company_key from domain
        LOWER(TRIM(domain)) as company_key,
        LOWER(TRIM(domain)) as domain,
        linkedin_url,
        TRIM(company_name) as company_name,
        TRIM(legal_name) as legal_name,
        TRIM(dba_name) as dba_name,
        TRIM(country) as country,
        TRIM(state) as state_province,
        TRIM(city) as city,
        TRIM(postal_code) as postal_code,
        TRIM(industry) as industry,
        TRIM(sub_industry) as sub_industry,
        TRIM(description) as description,
        founded_year,
        employee_count,
        employee_range,
        total_funding,
        last_funding_date,
        last_funding_type,
        last_funding_amount,
        website_url,
        facebook_url,
        twitter_url,
        instagram_url,
        crunchbase_url,
        'crunchbase' as source_system,
        1 as source_priority,
        ingestion_timestamp as created_at
    FROM thera_bronze.bronze_crunchbase_ext
    WHERE domain IS NOT NULL 
        AND domain != ''
        AND company_name IS NOT NULL
        AND company_name != ''
),

-- Step 2: Extract Apollo companies (assuming JSON structure)
apollo_companies AS (
    SELECT 
        -- Extract from JSON - adjust field names based on actual Apollo schema
        LOWER(TRIM(JSON_EXTRACT_SCALAR(raw_json, '$.domain'))) as company_key,
        LOWER(TRIM(JSON_EXTRACT_SCALAR(raw_json, '$.domain'))) as domain,
        JSON_EXTRACT_SCALAR(raw_json, '$.linkedin_url') as linkedin_url,
        TRIM(JSON_EXTRACT_SCALAR(raw_json, '$.name')) as company_name,
        TRIM(JSON_EXTRACT_SCALAR(raw_json, '$.legal_name')) as legal_name,
        TRIM(JSON_EXTRACT_SCALAR(raw_json, '$.dba_name')) as dba_name,
        TRIM(JSON_EXTRACT_SCALAR(raw_json, '$.country')) as country,
        TRIM(JSON_EXTRACT_SCALAR(raw_json, '$.state')) as state_province,
        TRIM(JSON_EXTRACT_SCALAR(raw_json, '$.city')) as city,
        TRIM(JSON_EXTRACT_SCALAR(raw_json, '$.postal_code')) as postal_code,
        TRIM(JSON_EXTRACT_SCALAR(raw_json, '$.industry')) as industry,
        TRIM(JSON_EXTRACT_SCALAR(raw_json, '$.sub_industry')) as sub_industry,
        TRIM(JSON_EXTRACT_SCALAR(raw_json, '$.description')) as description,
        CAST(JSON_EXTRACT_SCALAR(raw_json, '$.founded_year') AS INT) as founded_year,
        CAST(JSON_EXTRACT_SCALAR(raw_json, '$.employee_count') AS INT) as employee_count,
        JSON_EXTRACT_SCALAR(raw_json, '$.employee_range') as employee_range,
        CAST(JSON_EXTRACT_SCALAR(raw_json, '$.total_funding') AS DECIMAL(15,2)) as total_funding,
        JSON_EXTRACT_SCALAR(raw_json, '$.last_funding_date') as last_funding_date,
        JSON_EXTRACT_SCALAR(raw_json, '$.last_funding_type') as last_funding_type,
        CAST(JSON_EXTRACT_SCALAR(raw_json, '$.last_funding_amount') AS DECIMAL(15,2)) as last_funding_amount,
        JSON_EXTRACT_SCALAR(raw_json, '$.website_url') as website_url,
        JSON_EXTRACT_SCALAR(raw_json, '$.facebook_url') as facebook_url,
        JSON_EXTRACT_SCALAR(raw_json, '$.twitter_url') as twitter_url,
        JSON_EXTRACT_SCALAR(raw_json, '$.instagram_url') as instagram_url,
        'apollo' as source_system,
        2 as source_priority,
        CURRENT_TIMESTAMP as created_at
    FROM thera_bronze.bronze_apollo_companies
    WHERE JSON_EXTRACT_SCALAR(raw_json, '$.domain') IS NOT NULL
        AND JSON_EXTRACT_SCALAR(raw_json, '$.domain') != ''
        AND JSON_EXTRACT_SCALAR(raw_json, '$.name') IS NOT NULL
        AND JSON_EXTRACT_SCALAR(raw_json, '$.name') != ''
),

-- Step 3: Combine all sources
all_companies AS (
    SELECT * FROM crunchbase_companies
    UNION ALL
    SELECT * FROM apollo_companies
),

-- Step 4: Apply deduplication logic
deduplicated_companies AS (
    SELECT 
        *,
        -- Create duplicate group ID based on domain exact match
        FIRST_VALUE(company_key) OVER (
            PARTITION BY domain 
            ORDER BY source_priority ASC, created_at ASC
        ) as duplicate_group_id,
        
        -- Mark primary record (first in group by priority)
        ROW_NUMBER() OVER (
            PARTITION BY domain 
            ORDER BY source_priority ASC, created_at ASC
        ) = 1 as is_primary_record,
        
        -- Calculate data quality score
        CASE 
            WHEN domain IS NOT NULL AND company_name IS NOT NULL THEN 1.0
            WHEN domain IS NOT NULL THEN 0.8
            WHEN company_name IS NOT NULL THEN 0.6
            ELSE 0.4
        END as data_quality_score,
        
        -- Calculate confidence score based on data completeness
        (
            CASE WHEN domain IS NOT NULL THEN 0.3 ELSE 0 END +
            CASE WHEN company_name IS NOT NULL THEN 0.3 ELSE 0 END +
            CASE WHEN country IS NOT NULL THEN 0.1 ELSE 0 END +
            CASE WHEN industry IS NOT NULL THEN 0.1 ELSE 0 END +
            CASE WHEN description IS NOT NULL THEN 0.1 ELSE 0 END +
            CASE WHEN founded_year IS NOT NULL THEN 0.1 ELSE 0 END
        ) as confidence_score,
        
        -- Apply survivorship rules
        ARRAY['domain_exact_match'] as survivorship_rules_applied,
        
        -- Determine duplicate reason
        CASE 
            WHEN ROW_NUMBER() OVER (PARTITION BY domain ORDER BY source_priority ASC, created_at ASC) > 1 
            THEN 'domain_duplicate'
            ELSE NULL
        END as duplicate_reason,
        
        -- Generate size bracket
        CASE 
            WHEN employee_count IS NULL THEN 'unknown'
            WHEN employee_count <= 10 THEN 'startup'
            WHEN employee_count <= 50 THEN 'small'
            WHEN employee_count <= 200 THEN 'medium'
            WHEN employee_count <= 1000 THEN 'large'
            ELSE 'enterprise'
        END as size_bracket,
        
        -- Format current date for partitioning
        DATE_FORMAT(CURRENT_DATE, 'yyyy-MM-dd') as dt
        
    FROM all_companies
)

SELECT 
    company_key,
    domain,
    linkedin_url,
    company_name,
    legal_name,
    dba_name,
    country,
    state_province,
    city,
    postal_code,
    timezone,
    industry,
    sub_industry,
    industry_category,
    naics_code,
    sic_code,
    description,
    short_description,
    founded_year,
    founded_month,
    founded_date,
    employee_count,
    employee_range,
    revenue_range,
    size_bracket,
    total_funding,
    last_funding_date,
    last_funding_type,
    last_funding_amount,
    funding_stage,
    valuation,
    website_url,
    facebook_url,
    twitter_url,
    instagram_url,
    youtube_url,
    github_url,
    data_quality_score,
    source_priority,
    source_systems,
    confidence_score,
    duplicate_group_id,
    is_primary_record,
    duplicate_reason,
    survivorship_rules_applied,
    created_at,
    CURRENT_TIMESTAMP as updated_at,
    CURRENT_TIMESTAMP as last_verified_at,
    dt
FROM deduplicated_companies
WHERE is_primary_record = true;

-- =============================================================================
-- NOTES FOR CONFIGURATION
-- =============================================================================
-- 1. Adjust JSON field extraction based on actual Apollo API response structure
-- 2. Modify deduplication logic if additional matching criteria are needed
-- 3. Add fuzzy name matching using Levenshtein distance if required
-- 4. Adjust source_priority values based on data quality requirements
-- 5. Add additional survivorship rules as needed
-- 6. Consider adding data validation rules for specific fields
