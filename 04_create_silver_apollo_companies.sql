-- =============================================================================
-- SILVER LAYER: silver_apollo_companies
-- =============================================================================
-- Purpose: Normalized Apollo companies data with data quality checks
-- Source: bronze_apollo_companies (JSON)
-- Storage: Parquet format partitioned by dt

USE thera_silver;

-- =============================================================================
-- TABLE: silver_apollo_companies
-- =============================================================================
CREATE TABLE IF NOT EXISTS silver_apollo_companies (
    -- Apollo-specific identifiers
    apollo_id STRING NOT NULL,
    apollo_url STRING,
    company_key STRING NOT NULL, -- Links to silver_companies
    
    -- Company information
    name STRING NOT NULL,
    legal_name STRING,
    dba_name STRING,
    domain STRING NOT NULL,
    website_url STRING,
    
    -- Location details
    country STRING,
    state_province STRING,
    city STRING,
    postal_code STRING,
    address_line1 STRING,
    address_line2 STRING,
    timezone STRING,
    latitude DECIMAL(10,8),
    longitude DECIMAL(11,8),
    
    -- Industry classification
    industry STRING,
    sub_industry STRING,
    industry_category STRING,
    naics_code STRING,
    sic_code STRING,
    industry_tags ARRAY<STRING>,
    
    -- Company details
    description STRING,
    short_description STRING,
    tagline STRING,
    founded_year INT,
    founded_month INT,
    founded_date DATE,
    
    -- Size and financial information
    employee_count INT,
    employee_range STRING,
    revenue_range STRING,
    annual_revenue DECIMAL(15,2),
    total_funding DECIMAL(15,2),
    last_funding_date DATE,
    last_funding_type STRING,
    last_funding_amount DECIMAL(15,2),
    funding_stage STRING,
    valuation DECIMAL(15,2),
    
    -- Social media and online presence
    linkedin_url STRING,
    facebook_url STRING,
    twitter_url STRING,
    instagram_url STRING,
    youtube_url STRING,
    github_url STRING,
    crunchbase_url STRING,
    
    -- Apollo-specific fields
    apollo_organization_id STRING,
    apollo_organization_type STRING,
    apollo_organization_status STRING,
    apollo_organization_size STRING,
    apollo_organization_industry STRING,
    apollo_organization_website STRING,
    apollo_organization_phone STRING,
    apollo_organization_email STRING,
    
    -- Technology stack (if available)
    technology_stack ARRAY<STRING>,
    programming_languages ARRAY<STRING>,
    frameworks ARRAY<STRING>,
    databases ARRAY<STRING>,
    cloud_providers ARRAY<STRING>,
    
    -- Data quality metrics
    data_quality_score DECIMAL(3,2), -- 0.00 to 1.00
    completeness_score DECIMAL(3,2), -- 0.00 to 1.00
    accuracy_score DECIMAL(3,2), -- 0.00 to 1.00
    freshness_score DECIMAL(3,2), -- 0.00 to 1.00
    
    -- Validation flags
    is_valid_domain BOOLEAN,
    is_valid_email BOOLEAN,
    is_valid_phone BOOLEAN,
    is_valid_website BOOLEAN,
    has_complete_address BOOLEAN,
    has_industry_classification BOOLEAN,
    
    -- Data lineage and metadata
    source_system STRING,
    source_record_id STRING,
    raw_data_hash STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    last_verified_at TIMESTAMP,
    
    -- Partitioning
    dt STRING
)
COMMENT 'Normalized Apollo companies data with data quality checks'
PARTITIONED BY (dt)
STORED AS PARQUET
LOCATION 's3://thera-curated/silver/apollo_companies/'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY',
    'classification' = 'silver',
    'data_quality' = 'curated'
);

-- =============================================================================
-- CTAS: Populate silver_apollo_companies with normalization and quality checks
-- =============================================================================
CREATE TABLE IF NOT EXISTS silver_apollo_companies_temp AS
WITH 
-- Step 1: Extract and normalize data from Apollo JSON
apollo_raw AS (
    SELECT 
        raw_json,
        -- Extract basic identifiers
        JSON_EXTRACT_SCALAR(raw_json, '$.id') as apollo_id,
        JSON_EXTRACT_SCALAR(raw_json, '$.url') as apollo_url,
        JSON_EXTRACT_SCALAR(raw_json, '$.organization_id') as apollo_organization_id,
        
        -- Extract company information
        JSON_EXTRACT_SCALAR(raw_json, '$.name') as name,
        JSON_EXTRACT_SCALAR(raw_json, '$.legal_name') as legal_name,
        JSON_EXTRACT_SCALAR(raw_json, '$.dba_name') as dba_name,
        JSON_EXTRACT_SCALAR(raw_json, '$.domain') as domain,
        JSON_EXTRACT_SCALAR(raw_json, '$.website') as website_url,
        
        -- Extract location data
        JSON_EXTRACT_SCALAR(raw_json, '$.country') as country,
        JSON_EXTRACT_SCALAR(raw_json, '$.state') as state_province,
        JSON_EXTRACT_SCALAR(raw_json, '$.city') as city,
        JSON_EXTRACT_SCALAR(raw_json, '$.postal_code') as postal_code,
        JSON_EXTRACT_SCALAR(raw_json, '$.address_line1') as address_line1,
        JSON_EXTRACT_SCALAR(raw_json, '$.address_line2') as address_line2,
        JSON_EXTRACT_SCALAR(raw_json, '$.timezone') as timezone,
        JSON_EXTRACT_SCALAR(raw_json, '$.latitude') as latitude,
        JSON_EXTRACT_SCALAR(raw_json, '$.longitude') as longitude,
        
        -- Extract industry data
        JSON_EXTRACT_SCALAR(raw_json, '$.industry') as industry,
        JSON_EXTRACT_SCALAR(raw_json, '$.sub_industry') as sub_industry,
        JSON_EXTRACT_SCALAR(raw_json, '$.industry_category') as industry_category,
        JSON_EXTRACT_SCALAR(raw_json, '$.naics_code') as naics_code,
        JSON_EXTRACT_SCALAR(raw_json, '$.sic_code') as sic_code,
        
        -- Extract company details
        JSON_EXTRACT_SCALAR(raw_json, '$.description') as description,
        JSON_EXTRACT_SCALAR(raw_json, '$.short_description') as short_description,
        JSON_EXTRACT_SCALAR(raw_json, '$.tagline') as tagline,
        JSON_EXTRACT_SCALAR(raw_json, '$.founded_year') as founded_year,
        JSON_EXTRACT_SCALAR(raw_json, '$.founded_month') as founded_month,
        JSON_EXTRACT_SCALAR(raw_json, '$.founded_date') as founded_date,
        
        -- Extract size and financial data
        JSON_EXTRACT_SCALAR(raw_json, '$.employee_count') as employee_count,
        JSON_EXTRACT_SCALAR(raw_json, '$.employee_range') as employee_range,
        JSON_EXTRACT_SCALAR(raw_json, '$.revenue_range') as revenue_range,
        JSON_EXTRACT_SCALAR(raw_json, '$.annual_revenue') as annual_revenue,
        JSON_EXTRACT_SCALAR(raw_json, '$.total_funding') as total_funding,
        JSON_EXTRACT_SCALAR(raw_json, '$.last_funding_date') as last_funding_date,
        JSON_EXTRACT_SCALAR(raw_json, '$.last_funding_type') as last_funding_type,
        JSON_EXTRACT_SCALAR(raw_json, '$.last_funding_amount') as last_funding_amount,
        JSON_EXTRACT_SCALAR(raw_json, '$.funding_stage') as funding_stage,
        JSON_EXTRACT_SCALAR(raw_json, '$.valuation') as valuation,
        
        -- Extract social media URLs
        JSON_EXTRACT_SCALAR(raw_json, '$.linkedin_url') as linkedin_url,
        JSON_EXTRACT_SCALAR(raw_json, '$.facebook_url') as facebook_url,
        JSON_EXTRACT_SCALAR(raw_json, '$.twitter_url') as twitter_url,
        JSON_EXTRACT_SCALAR(raw_json, '$.instagram_url') as instagram_url,
        JSON_EXTRACT_SCALAR(raw_json, '$.youtube_url') as youtube_url,
        JSON_EXTRACT_SCALAR(raw_json, '$.github_url') as github_url,
        JSON_EXTRACT_SCALAR(raw_json, '$.crunchbase_url') as crunchbase_url,
        
        -- Extract Apollo-specific fields
        JSON_EXTRACT_SCALAR(raw_json, '$.organization_type') as apollo_organization_type,
        JSON_EXTRACT_SCALAR(raw_json, '$.organization_status') as apollo_organization_status,
        JSON_EXTRACT_SCALAR(raw_json, '$.organization_size') as apollo_organization_size,
        JSON_EXTRACT_SCALAR(raw_json, '$.organization_industry') as apollo_organization_industry,
        JSON_EXTRACT_SCALAR(raw_json, '$.organization_website') as apollo_organization_website,
        JSON_EXTRACT_SCALAR(raw_json, '$.organization_phone') as apollo_organization_phone,
        JSON_EXTRACT_SCALAR(raw_json, '$.organization_email') as apollo_organization_email,
        
        -- Extract technology stack arrays
        JSON_EXTRACT(raw_json, '$.technology_stack') as technology_stack_json,
        JSON_EXTRACT(raw_json, '$.programming_languages') as programming_languages_json,
        JSON_EXTRACT(raw_json, '$.frameworks') as frameworks_json,
        JSON_EXTRACT(raw_json, '$.databases') as databases_json,
        JSON_EXTRACT(raw_json, '$.cloud_providers') as cloud_providers_json,
        JSON_EXTRACT(raw_json, '$.industry_tags') as industry_tags_json
        
    FROM thera_bronze.bronze_apollo_companies
    WHERE raw_json IS NOT NULL
        AND JSON_EXTRACT_SCALAR(raw_json, '$.id') IS NOT NULL
        AND JSON_EXTRACT_SCALAR(raw_json, '$.domain') IS NOT NULL
),

-- Step 2: Apply data quality checks and normalization
normalized_apollo AS (
    SELECT 
        -- Identifiers
        apollo_id,
        apollo_url,
        LOWER(TRIM(domain)) as company_key,
        LOWER(TRIM(domain)) as domain,
        apollo_organization_id,
        
        -- Company information
        TRIM(name) as name,
        TRIM(legal_name) as legal_name,
        TRIM(dba_name) as dba_name,
        TRIM(website_url) as website_url,
        
        -- Location data
        TRIM(country) as country,
        TRIM(state_province) as state_province,
        TRIM(city) as city,
        TRIM(postal_code) as postal_code,
        TRIM(address_line1) as address_line1,
        TRIM(address_line2) as address_line2,
        TRIM(timezone) as timezone,
        CAST(latitude AS DECIMAL(10,8)) as latitude,
        CAST(longitude AS DECIMAL(11,8)) as longitude,
        
        -- Industry classification
        TRIM(industry) as industry,
        TRIM(sub_industry) as sub_industry,
        TRIM(industry_category) as industry_category,
        TRIM(naics_code) as naics_code,
        TRIM(sic_code) as sic_code,
        
        -- Company details
        TRIM(description) as description,
        TRIM(short_description) as short_description,
        TRIM(tagline) as tagline,
        CAST(founded_year AS INT) as founded_year,
        CAST(founded_month AS INT) as founded_month,
        CAST(founded_date AS DATE) as founded_date,
        
        -- Size and financial data
        CAST(employee_count AS INT) as employee_count,
        TRIM(employee_range) as employee_range,
        TRIM(revenue_range) as revenue_range,
        CAST(annual_revenue AS DECIMAL(15,2)) as annual_revenue,
        CAST(total_funding AS DECIMAL(15,2)) as total_funding,
        CAST(last_funding_date AS DATE) as last_funding_date,
        TRIM(last_funding_type) as last_funding_type,
        CAST(last_funding_amount AS DECIMAL(15,2)) as last_funding_amount,
        TRIM(funding_stage) as funding_stage,
        CAST(valuation AS DECIMAL(15,2)) as valuation,
        
        -- Social media URLs
        TRIM(linkedin_url) as linkedin_url,
        TRIM(facebook_url) as facebook_url,
        TRIM(twitter_url) as twitter_url,
        TRIM(instagram_url) as instagram_url,
        TRIM(youtube_url) as youtube_url,
        TRIM(github_url) as github_url,
        TRIM(crunchbase_url) as crunchbase_url,
        
        -- Apollo-specific fields
        TRIM(apollo_organization_type) as apollo_organization_type,
        TRIM(apollo_organization_status) as apollo_organization_status,
        TRIM(apollo_organization_size) as apollo_organization_size,
        TRIM(apollo_organization_industry) as apollo_organization_industry,
        TRIM(apollo_organization_website) as apollo_organization_website,
        TRIM(apollo_organization_phone) as apollo_organization_phone,
        TRIM(apollo_organization_email) as apollo_organization_email,
        
        -- Technology stack arrays (convert JSON arrays to string arrays)
        CAST(technology_stack_json AS ARRAY<STRING>) as technology_stack,
        CAST(programming_languages_json AS ARRAY<STRING>) as programming_languages,
        CAST(frameworks_json AS ARRAY<STRING>) as frameworks,
        CAST(databases_json AS ARRAY<STRING>) as databases,
        CAST(cloud_providers_json AS ARRAY<STRING>) as cloud_providers,
        CAST(industry_tags_json AS ARRAY<STRING>) as industry_tags,
        
        -- Data quality validation flags
        CASE 
            WHEN domain IS NOT NULL AND domain != '' AND domain LIKE '%.%' THEN true
            ELSE false
        END as is_valid_domain,
        
        CASE 
            WHEN apollo_organization_email IS NOT NULL 
                AND apollo_organization_email LIKE '%@%.%' THEN true
            ELSE false
        END as is_valid_email,
        
        CASE 
            WHEN apollo_organization_phone IS NOT NULL 
                AND LENGTH(REGEXP_REPLACE(apollo_organization_phone, '[^0-9]', '')) >= 10 THEN true
            ELSE false
        END as is_valid_phone,
        
        CASE 
            WHEN website_url IS NOT NULL 
                AND (website_url LIKE 'http%' OR website_url LIKE 'www.%') THEN true
            ELSE false
        END as is_valid_website,
        
        CASE 
            WHEN address_line1 IS NOT NULL 
                AND city IS NOT NULL 
                AND state_province IS NOT NULL 
                AND country IS NOT NULL THEN true
            ELSE false
        END as has_complete_address,
        
        CASE 
            WHEN industry IS NOT NULL 
                AND industry != '' THEN true
            ELSE false
        END as has_industry_classification,
        
        -- Calculate data quality scores
        (
            CASE WHEN domain IS NOT NULL AND domain != '' THEN 0.2 ELSE 0 END +
            CASE WHEN name IS NOT NULL AND name != '' THEN 0.2 ELSE 0 END +
            CASE WHEN country IS NOT NULL AND country != '' THEN 0.1 ELSE 0 END +
            CASE WHEN industry IS NOT NULL AND industry != '' THEN 0.1 ELSE 0 END +
            CASE WHEN description IS NOT NULL AND description != '' THEN 0.1 ELSE 0 END +
            CASE WHEN founded_year IS NOT NULL THEN 0.1 ELSE 0 END +
            CASE WHEN employee_count IS NOT NULL THEN 0.1 ELSE 0 END +
            CASE WHEN linkedin_url IS NOT NULL AND linkedin_url != '' THEN 0.1 ELSE 0 END
        ) as data_quality_score,
        
        -- Calculate completeness score
        (
            CASE WHEN domain IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN name IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN country IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN industry IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN description IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN founded_year IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN employee_count IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN linkedin_url IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN website_url IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN city IS NOT NULL THEN 1 ELSE 0 END
        ) / 10.0 as completeness_score,
        
        -- Calculate accuracy score based on validation flags
        (
            CASE WHEN is_valid_domain THEN 1 ELSE 0 END +
            CASE WHEN is_valid_email THEN 1 ELSE 0 END +
            CASE WHEN is_valid_phone THEN 1 ELSE 0 END +
            CASE WHEN is_valid_website THEN 1 ELSE 0 END +
            CASE WHEN has_complete_address THEN 1 ELSE 0 END +
            CASE WHEN has_industry_classification THEN 1 ELSE 0 END
        ) / 6.0 as accuracy_score,
        
        -- Calculate freshness score (assuming recent data is fresher)
        1.0 as freshness_score, -- Placeholder - would need actual data timestamps
        
        -- Metadata
        'apollo' as source_system,
        apollo_id as source_record_id,
        MD5(raw_json) as raw_data_hash,
        CURRENT_TIMESTAMP as created_at,
        CURRENT_TIMESTAMP as updated_at,
        CURRENT_TIMESTAMP as last_verified_at,
        
        -- Partitioning
        DATE_FORMAT(CURRENT_DATE, 'yyyy-MM-dd') as dt
        
    FROM apollo_raw
    WHERE apollo_id IS NOT NULL 
        AND domain IS NOT NULL 
        AND domain != ''
        AND name IS NOT NULL 
        AND name != ''
)

SELECT 
    apollo_id,
    apollo_url,
    company_key,
    domain,
    apollo_organization_id,
    name,
    legal_name,
    dba_name,
    website_url,
    country,
    state_province,
    city,
    postal_code,
    address_line1,
    address_line2,
    timezone,
    latitude,
    longitude,
    industry,
    sub_industry,
    industry_category,
    naics_code,
    sic_code,
    industry_tags,
    description,
    short_description,
    tagline,
    founded_year,
    founded_month,
    founded_date,
    employee_count,
    employee_range,
    revenue_range,
    annual_revenue,
    total_funding,
    last_funding_date,
    last_funding_type,
    last_funding_amount,
    funding_stage,
    valuation,
    linkedin_url,
    facebook_url,
    twitter_url,
    instagram_url,
    youtube_url,
    github_url,
    crunchbase_url,
    apollo_organization_type,
    apollo_organization_status,
    apollo_organization_size,
    apollo_organization_industry,
    apollo_organization_website,
    apollo_organization_phone,
    apollo_organization_email,
    technology_stack,
    programming_languages,
    frameworks,
    databases,
    cloud_providers,
    data_quality_score,
    completeness_score,
    accuracy_score,
    freshness_score,
    is_valid_domain,
    is_valid_email,
    is_valid_phone,
    is_valid_website,
    has_complete_address,
    has_industry_classification,
    source_system,
    source_record_id,
    raw_data_hash,
    created_at,
    updated_at,
    last_verified_at,
    dt
FROM normalized_apollo;

-- =============================================================================
-- NOTES FOR CONFIGURATION
-- =============================================================================
-- 1. Adjust JSON field extraction based on actual Apollo API response structure
-- 2. Modify data quality scoring weights based on business requirements
-- 3. Add additional validation rules as needed
-- 4. Consider adding data lineage tracking for audit purposes
-- 5. Adjust partitioning strategy if different time-based partitioning is needed
-- 6. Add indexes on frequently queried fields if performance is critical
