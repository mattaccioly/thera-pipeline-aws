-- =============================================================================
-- SILVER LAYER: silver_apollo_contacts
-- =============================================================================
-- Purpose: Normalized Apollo contacts data with PII masking/minimization
-- Source: bronze_apollo_contacts (JSON)
-- Storage: Parquet format partitioned by dt
-- Security: PII fields are masked or hashed for privacy compliance

USE thera_silver;

-- =============================================================================
-- TABLE: silver_apollo_contacts
-- =============================================================================
CREATE TABLE IF NOT EXISTS silver_apollo_contacts (
    -- Apollo-specific identifiers
    apollo_id STRING NOT NULL,
    apollo_url STRING,
    contact_key STRING NOT NULL, -- Generated unique key
    company_key STRING NOT NULL, -- Links to silver_companies
    
    -- Contact information (PII masked)
    first_name_masked STRING, -- Masked version of first name
    last_name_masked STRING, -- Masked version of last name
    full_name_masked STRING, -- Masked version of full name
    first_name_hash STRING, -- Hash for deduplication
    last_name_hash STRING, -- Hash for deduplication
    full_name_hash STRING, -- Hash for deduplication
    
    -- Professional information
    title STRING,
    department STRING,
    seniority_level STRING, -- junior, mid, senior, executive, c-level
    job_function STRING,
    job_title_normalized STRING,
    
    -- Contact details (PII masked)
    email_masked STRING, -- Masked email (e.g., j***@company.com)
    email_hash STRING, -- Hash for deduplication
    phone_masked STRING, -- Masked phone number
    phone_hash STRING, -- Hash for deduplication
    linkedin_url STRING,
    twitter_url STRING,
    
    -- Company association
    company_name STRING,
    company_domain STRING,
    company_industry STRING,
    company_size_range STRING,
    company_location STRING,
    
    -- Professional details
    years_experience INT,
    education_level STRING,
    education_institution STRING,
    skills ARRAY<STRING>,
    certifications ARRAY<STRING>,
    languages ARRAY<STRING>,
    
    -- Contact preferences and status
    contact_status STRING, -- active, inactive, bounced, unsubscribed
    contact_source STRING, -- apollo, manual, import
    contact_quality_score DECIMAL(3,2), -- 0.00 to 1.00
    is_verified BOOLEAN,
    is_senior_level BOOLEAN,
    is_decision_maker BOOLEAN,
    
    -- Data quality metrics
    data_quality_score DECIMAL(3,2), -- 0.00 to 1.00
    completeness_score DECIMAL(3,2), -- 0.00 to 1.00
    accuracy_score DECIMAL(3,2), -- 0.00 to 1.00
    freshness_score DECIMAL(3,2), -- 0.00 to 1.00
    
    -- Validation flags
    is_valid_email BOOLEAN,
    is_valid_phone BOOLEAN,
    is_valid_linkedin BOOLEAN,
    has_complete_name BOOLEAN,
    has_professional_info BOOLEAN,
    has_company_association BOOLEAN,
    
    -- Data lineage and metadata
    source_system STRING,
    source_record_id STRING,
    raw_data_hash STRING,
    pii_masking_applied ARRAY<STRING>,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    last_verified_at TIMESTAMP,
    
    -- Partitioning
    dt STRING
)
COMMENT 'Normalized Apollo contacts data with PII masking and privacy compliance'
PARTITIONED BY (dt)
STORED AS PARQUET
LOCATION 's3://thera-curated/silver/apollo_contacts/'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY',
    'classification' = 'silver',
    'data_quality' = 'curated',
    'pii_handling' = 'masked'
);

-- =============================================================================
-- CTAS: Populate silver_apollo_contacts with PII masking and normalization
-- =============================================================================
CREATE TABLE IF NOT EXISTS silver_apollo_contacts_temp AS
WITH 
-- Step 1: Extract and normalize data from Apollo contacts JSON
apollo_contacts_raw AS (
    SELECT 
        raw_json,
        -- Extract basic identifiers
        JSON_EXTRACT_SCALAR(raw_json, '$.id') as apollo_id,
        JSON_EXTRACT_SCALAR(raw_json, '$.url') as apollo_url,
        JSON_EXTRACT_SCALAR(raw_json, '$.organization_id') as apollo_organization_id,
        
        -- Extract contact information
        JSON_EXTRACT_SCALAR(raw_json, '$.first_name') as first_name,
        JSON_EXTRACT_SCALAR(raw_json, '$.last_name') as last_name,
        JSON_EXTRACT_SCALAR(raw_json, '$.full_name') as full_name,
        JSON_EXTRACT_SCALAR(raw_json, '$.title') as title,
        JSON_EXTRACT_SCALAR(raw_json, '$.department') as department,
        JSON_EXTRACT_SCALAR(raw_json, '$.seniority_level') as seniority_level,
        JSON_EXTRACT_SCALAR(raw_json, '$.job_function') as job_function,
        
        -- Extract contact details
        JSON_EXTRACT_SCALAR(raw_json, '$.email') as email,
        JSON_EXTRACT_SCALAR(raw_json, '$.phone') as phone,
        JSON_EXTRACT_SCALAR(raw_json, '$.linkedin_url') as linkedin_url,
        JSON_EXTRACT_SCALAR(raw_json, '$.twitter_url') as twitter_url,
        
        -- Extract company information
        JSON_EXTRACT_SCALAR(raw_json, '$.organization_name') as company_name,
        JSON_EXTRACT_SCALAR(raw_json, '$.organization_domain') as company_domain,
        JSON_EXTRACT_SCALAR(raw_json, '$.organization_industry') as company_industry,
        JSON_EXTRACT_SCALAR(raw_json, '$.organization_size') as company_size_range,
        JSON_EXTRACT_SCALAR(raw_json, '$.organization_location') as company_location,
        
        -- Extract professional details
        JSON_EXTRACT_SCALAR(raw_json, '$.years_experience') as years_experience,
        JSON_EXTRACT_SCALAR(raw_json, '$.education_level') as education_level,
        JSON_EXTRACT_SCALAR(raw_json, '$.education_institution') as education_institution,
        
        -- Extract arrays
        JSON_EXTRACT(raw_json, '$.skills') as skills_json,
        JSON_EXTRACT(raw_json, '$.certifications') as certifications_json,
        JSON_EXTRACT(raw_json, '$.languages') as languages_json,
        
        -- Extract contact status
        JSON_EXTRACT_SCALAR(raw_json, '$.contact_status') as contact_status,
        JSON_EXTRACT_SCALAR(raw_json, '$.contact_source') as contact_source,
        JSON_EXTRACT_SCALAR(raw_json, '$.is_verified') as is_verified,
        
        -- Extract additional fields
        JSON_EXTRACT_SCALAR(raw_json, '$.created_at') as apollo_created_at,
        JSON_EXTRACT_SCALAR(raw_json, '$.updated_at') as apollo_updated_at
        
    FROM thera_bronze.bronze_apollo_contacts
    WHERE raw_json IS NOT NULL
        AND JSON_EXTRACT_SCALAR(raw_json, '$.id') IS NOT NULL
),

-- Step 2: Apply PII masking and normalization
masked_contacts AS (
    SELECT 
        -- Identifiers
        apollo_id,
        apollo_url,
        CONCAT('contact_', apollo_id) as contact_key,
        LOWER(TRIM(company_domain)) as company_key,
        apollo_organization_id,
        
        -- PII masking functions
        CASE 
            WHEN first_name IS NOT NULL AND first_name != '' THEN
                CONCAT(SUBSTRING(first_name, 1, 1), REPEAT('*', GREATEST(1, LENGTH(first_name) - 1)))
            ELSE NULL
        END as first_name_masked,
        
        CASE 
            WHEN last_name IS NOT NULL AND last_name != '' THEN
                CONCAT(SUBSTRING(last_name, 1, 1), REPEAT('*', GREATEST(1, LENGTH(last_name) - 1)))
            ELSE NULL
        END as last_name_masked,
        
        CASE 
            WHEN full_name IS NOT NULL AND full_name != '' THEN
                CONCAT(SUBSTRING(full_name, 1, 1), REPEAT('*', GREATEST(1, LENGTH(full_name) - 1)))
            ELSE NULL
        END as full_name_masked,
        
        -- Hash functions for deduplication (using MD5 for simplicity)
        MD5(LOWER(TRIM(first_name))) as first_name_hash,
        MD5(LOWER(TRIM(last_name))) as last_name_hash,
        MD5(LOWER(TRIM(full_name))) as full_name_hash,
        
        -- Professional information
        TRIM(title) as title,
        TRIM(department) as department,
        TRIM(seniority_level) as seniority_level,
        TRIM(job_function) as job_function,
        
        -- Normalize job title
        CASE 
            WHEN LOWER(title) LIKE '%ceo%' OR LOWER(title) LIKE '%chief executive%' THEN 'CEO'
            WHEN LOWER(title) LIKE '%cto%' OR LOWER(title) LIKE '%chief technology%' THEN 'CTO'
            WHEN LOWER(title) LIKE '%cfo%' OR LOWER(title) LIKE '%chief financial%' THEN 'CFO'
            WHEN LOWER(title) LIKE '%cmo%' OR LOWER(title) LIKE '%chief marketing%' THEN 'CMO'
            WHEN LOWER(title) LIKE '%coo%' OR LOWER(title) LIKE '%chief operating%' THEN 'COO'
            WHEN LOWER(title) LIKE '%vp%' OR LOWER(title) LIKE '%vice president%' THEN 'VP'
            WHEN LOWER(title) LIKE '%director%' THEN 'Director'
            WHEN LOWER(title) LIKE '%manager%' THEN 'Manager'
            WHEN LOWER(title) LIKE '%senior%' THEN 'Senior'
            WHEN LOWER(title) LIKE '%junior%' OR LOWER(title) LIKE '%jr%' THEN 'Junior'
            ELSE TRIM(title)
        END as job_title_normalized,
        
        -- Contact details (PII masked)
        CASE 
            WHEN email IS NOT NULL AND email != '' THEN
                CONCAT(
                    SUBSTRING(SPLIT(email, '@')[0], 1, 1),
                    REPEAT('*', GREATEST(1, LENGTH(SPLIT(email, '@')[0]) - 1)),
                    '@',
                    SPLIT(email, '@')[1]
                )
            ELSE NULL
        END as email_masked,
        
        MD5(LOWER(TRIM(email))) as email_hash,
        
        CASE 
            WHEN phone IS NOT NULL AND phone != '' THEN
                CONCAT(
                    SUBSTRING(phone, 1, 3),
                    REPEAT('*', GREATEST(1, LENGTH(phone) - 6)),
                    SUBSTRING(phone, LENGTH(phone) - 2, 3)
                )
            ELSE NULL
        END as phone_masked,
        
        MD5(REGEXP_REPLACE(phone, '[^0-9]', '')) as phone_hash,
        
        TRIM(linkedin_url) as linkedin_url,
        TRIM(twitter_url) as twitter_url,
        
        -- Company association
        TRIM(company_name) as company_name,
        LOWER(TRIM(company_domain)) as company_domain,
        TRIM(company_industry) as company_industry,
        TRIM(company_size_range) as company_size_range,
        TRIM(company_location) as company_location,
        
        -- Professional details
        CAST(years_experience AS INT) as years_experience,
        TRIM(education_level) as education_level,
        TRIM(education_institution) as education_institution,
        
        -- Convert JSON arrays to string arrays
        CAST(skills_json AS ARRAY<STRING>) as skills,
        CAST(certifications_json AS ARRAY<STRING>) as certifications,
        CAST(languages_json AS ARRAY<STRING>) as languages,
        
        -- Contact preferences and status
        TRIM(contact_status) as contact_status,
        TRIM(contact_source) as contact_source,
        CAST(is_verified AS BOOLEAN) as is_verified,
        
        -- Determine seniority flags
        CASE 
            WHEN LOWER(seniority_level) IN ('executive', 'c-level', 'senior executive') THEN true
            WHEN LOWER(title) LIKE '%ceo%' OR LOWER(title) LIKE '%cto%' OR LOWER(title) LIKE '%cfo%' THEN true
            WHEN LOWER(title) LIKE '%chief%' OR LOWER(title) LIKE '%president%' THEN true
            ELSE false
        END as is_senior_level,
        
        CASE 
            WHEN LOWER(title) LIKE '%ceo%' OR LOWER(title) LIKE '%cto%' OR LOWER(title) LIKE '%cfo%' THEN true
            WHEN LOWER(title) LIKE '%chief%' OR LOWER(title) LIKE '%president%' THEN true
            WHEN LOWER(title) LIKE '%director%' OR LOWER(title) LIKE '%vp%' THEN true
            ELSE false
        END as is_decision_maker,
        
        -- Data quality validation flags
        CASE 
            WHEN email IS NOT NULL 
                AND email != '' 
                AND email LIKE '%@%.%' THEN true
            ELSE false
        END as is_valid_email,
        
        CASE 
            WHEN phone IS NOT NULL 
                AND LENGTH(REGEXP_REPLACE(phone, '[^0-9]', '')) >= 10 THEN true
            ELSE false
        END as is_valid_phone,
        
        CASE 
            WHEN linkedin_url IS NOT NULL 
                AND linkedin_url LIKE '%linkedin.com%' THEN true
            ELSE false
        END as is_valid_linkedin,
        
        CASE 
            WHEN (first_name IS NOT NULL AND first_name != '') 
                AND (last_name IS NOT NULL AND last_name != '') THEN true
            ELSE false
        END as has_complete_name,
        
        CASE 
            WHEN title IS NOT NULL 
                AND title != '' 
                AND department IS NOT NULL 
                AND department != '' THEN true
            ELSE false
        END as has_professional_info,
        
        CASE 
            WHEN company_name IS NOT NULL 
                AND company_name != '' 
                AND company_domain IS NOT NULL 
                AND company_domain != '' THEN true
            ELSE false
        END as has_company_association,
        
        -- Calculate data quality scores
        (
            CASE WHEN first_name IS NOT NULL AND first_name != '' THEN 0.15 ELSE 0 END +
            CASE WHEN last_name IS NOT NULL AND last_name != '' THEN 0.15 ELSE 0 END +
            CASE WHEN email IS NOT NULL AND email != '' THEN 0.2 ELSE 0 END +
            CASE WHEN title IS NOT NULL AND title != '' THEN 0.15 ELSE 0 END +
            CASE WHEN company_name IS NOT NULL AND company_name != '' THEN 0.15 ELSE 0 END +
            CASE WHEN linkedin_url IS NOT NULL AND linkedin_url != '' THEN 0.1 ELSE 0 END +
            CASE WHEN phone IS NOT NULL AND phone != '' THEN 0.1 ELSE 0 END
        ) as data_quality_score,
        
        -- Calculate completeness score
        (
            CASE WHEN first_name IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN last_name IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN email IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN title IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN company_name IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN linkedin_url IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN phone IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN department IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN seniority_level IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN years_experience IS NOT NULL THEN 1 ELSE 0 END
        ) / 10.0 as completeness_score,
        
        -- Calculate accuracy score based on validation flags
        (
            CASE WHEN is_valid_email THEN 1 ELSE 0 END +
            CASE WHEN is_valid_phone THEN 1 ELSE 0 END +
            CASE WHEN is_valid_linkedin THEN 1 ELSE 0 END +
            CASE WHEN has_complete_name THEN 1 ELSE 0 END +
            CASE WHEN has_professional_info THEN 1 ELSE 0 END +
            CASE WHEN has_company_association THEN 1 ELSE 0 END
        ) / 6.0 as accuracy_score,
        
        -- Calculate freshness score (assuming recent data is fresher)
        1.0 as freshness_score, -- Placeholder - would need actual data timestamps
        
        -- Calculate contact quality score
        (
            CASE WHEN is_verified THEN 0.3 ELSE 0 END +
            CASE WHEN is_senior_level THEN 0.3 ELSE 0 END +
            CASE WHEN is_decision_maker THEN 0.2 ELSE 0 END +
            CASE WHEN is_valid_email THEN 0.1 ELSE 0 END +
            CASE WHEN is_valid_linkedin THEN 0.1 ELSE 0 END
        ) as contact_quality_score,
        
        -- Metadata
        'apollo' as source_system,
        apollo_id as source_record_id,
        MD5(raw_json) as raw_data_hash,
        ARRAY['first_name', 'last_name', 'full_name', 'email', 'phone'] as pii_masking_applied,
        CURRENT_TIMESTAMP as created_at,
        CURRENT_TIMESTAMP as updated_at,
        CURRENT_TIMESTAMP as last_verified_at,
        
        -- Partitioning
        DATE_FORMAT(CURRENT_DATE, 'yyyy-MM-dd') as dt
        
    FROM apollo_contacts_raw
    WHERE apollo_id IS NOT NULL 
        AND (first_name IS NOT NULL OR last_name IS NOT NULL OR full_name IS NOT NULL)
)

SELECT 
    apollo_id,
    apollo_url,
    contact_key,
    company_key,
    apollo_organization_id,
    first_name_masked,
    last_name_masked,
    full_name_masked,
    first_name_hash,
    last_name_hash,
    full_name_hash,
    title,
    department,
    seniority_level,
    job_function,
    job_title_normalized,
    email_masked,
    email_hash,
    phone_masked,
    phone_hash,
    linkedin_url,
    twitter_url,
    company_name,
    company_domain,
    company_industry,
    company_size_range,
    company_location,
    years_experience,
    education_level,
    education_institution,
    skills,
    certifications,
    languages,
    contact_status,
    contact_source,
    contact_quality_score,
    is_verified,
    is_senior_level,
    is_decision_maker,
    data_quality_score,
    completeness_score,
    accuracy_score,
    freshness_score,
    is_valid_email,
    is_valid_phone,
    is_valid_linkedin,
    has_complete_name,
    has_professional_info,
    has_company_association,
    source_system,
    source_record_id,
    raw_data_hash,
    pii_masking_applied,
    created_at,
    updated_at,
    last_verified_at,
    dt
FROM masked_contacts;

-- =============================================================================
-- NOTES FOR CONFIGURATION
-- =============================================================================
-- 1. Adjust JSON field extraction based on actual Apollo contacts API response structure
-- 2. Modify PII masking patterns based on compliance requirements (GDPR, CCPA, etc.)
-- 3. Consider using stronger hashing algorithms (SHA-256) for production
-- 4. Add additional validation rules for contact data quality
-- 5. Adjust seniority level detection logic based on industry standards
-- 6. Consider adding data retention policies for PII data
-- 7. Add audit logging for PII access and modifications
