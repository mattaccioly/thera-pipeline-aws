-- =============================================================================
-- DATA QUALITY CHECKS AND VALIDATION RULES
-- =============================================================================
-- This script implements comprehensive data quality checks and validation rules
-- for all tables in the Thera data pipeline

-- =============================================================================
-- DATA QUALITY VALIDATION FUNCTIONS
-- =============================================================================

-- Function to validate email format
CREATE OR REPLACE FUNCTION validate_email(email STRING)
RETURNS BOOLEAN
LANGUAGE SQL
AS $$
    email IS NOT NULL 
    AND email != '' 
    AND email LIKE '%@%.%'
    AND LENGTH(email) <= 255
    AND email NOT LIKE '%..%'
    AND email NOT LIKE '.%'
    AND email NOT LIKE '%.'
$$;

-- Function to validate phone number format
CREATE OR REPLACE FUNCTION validate_phone(phone STRING)
RETURNS BOOLEAN
LANGUAGE SQL
AS $$
    phone IS NOT NULL 
    AND LENGTH(REGEXP_REPLACE(phone, '[^0-9]', '')) >= 10
    AND LENGTH(REGEXP_REPLACE(phone, '[^0-9]', '')) <= 15
$$;

-- Function to validate domain format
CREATE OR REPLACE FUNCTION validate_domain(domain STRING)
RETURNS BOOLEAN
LANGUAGE SQL
AS $$
    domain IS NOT NULL 
    AND domain != '' 
    AND domain LIKE '%.%'
    AND domain NOT LIKE '%.'
    AND domain NOT LIKE '.%'
    AND LENGTH(domain) <= 253
$$;

-- Function to validate URL format
CREATE OR REPLACE FUNCTION validate_url(url STRING)
RETURNS BOOLEAN
LANGUAGE SQL
AS $$
    url IS NOT NULL 
    AND url != '' 
    AND (url LIKE 'http://%' OR url LIKE 'https://%')
    AND LENGTH(url) <= 2048
$$;

-- =============================================================================
-- SILVER LAYER DATA QUALITY CHECKS
-- =============================================================================

-- 1. silver_companies data quality checks
CREATE OR REPLACE VIEW silver_companies_data_quality AS
SELECT 
    company_key,
    domain,
    company_name,
    
    -- Required field checks
    CASE WHEN company_key IS NULL OR company_key = '' THEN 'FAIL' ELSE 'PASS' END as company_key_check,
    CASE WHEN domain IS NULL OR domain = '' THEN 'FAIL' ELSE 'PASS' END as domain_check,
    CASE WHEN company_name IS NULL OR company_name = '' THEN 'FAIL' ELSE 'PASS' END as company_name_check,
    
    -- Domain validation
    CASE WHEN validate_domain(domain) THEN 'PASS' ELSE 'FAIL' END as domain_format_check,
    
    -- Data completeness
    CASE 
        WHEN country IS NOT NULL AND industry IS NOT NULL AND description IS NOT NULL THEN 'COMPLETE'
        WHEN country IS NOT NULL OR industry IS NOT NULL OR description IS NOT NULL THEN 'PARTIAL'
        ELSE 'INCOMPLETE'
    END as data_completeness,
    
    -- Data quality score validation
    CASE 
        WHEN data_quality_score >= 0.8 THEN 'HIGH'
        WHEN data_quality_score >= 0.6 THEN 'MEDIUM'
        WHEN data_quality_score >= 0.4 THEN 'LOW'
        ELSE 'VERY_LOW'
    END as quality_level,
    
    -- Duplicate checks
    CASE 
        WHEN is_primary_record = true THEN 'PRIMARY'
        ELSE 'DUPLICATE'
    END as record_type,
    
    -- Validation flags
    CASE WHEN linkedin_url IS NOT NULL AND validate_url(linkedin_url) THEN 'VALID' ELSE 'INVALID' END as linkedin_url_check,
    CASE WHEN website_url IS NOT NULL AND validate_url(website_url) THEN 'VALID' ELSE 'INVALID' END as website_url_check,
    
    -- Business logic validation
    CASE 
        WHEN founded_year IS NOT NULL AND founded_year > YEAR(CURRENT_DATE) THEN 'INVALID'
        WHEN founded_year IS NOT NULL AND founded_year < 1800 THEN 'INVALID'
        ELSE 'VALID'
    END as founded_year_check,
    
    CASE 
        WHEN employee_count IS NOT NULL AND employee_count < 0 THEN 'INVALID'
        WHEN employee_count IS NOT NULL AND employee_count > 1000000 THEN 'INVALID'
        ELSE 'VALID'
    END as employee_count_check,
    
    CASE 
        WHEN total_funding IS NOT NULL AND total_funding < 0 THEN 'INVALID'
        WHEN total_funding IS NOT NULL AND total_funding > 1000000000000 THEN 'INVALID'
        ELSE 'VALID'
    END as total_funding_check,
    
    -- Overall validation status
    CASE 
        WHEN (company_key IS NULL OR company_key = '') OR 
             (domain IS NULL OR domain = '') OR 
             (company_name IS NULL OR company_name = '') OR
             NOT validate_domain(domain) THEN 'FAIL'
        ELSE 'PASS'
    END as overall_validation_status
    
FROM thera_silver.silver_companies;

-- 2. silver_apollo_contacts data quality checks
CREATE OR REPLACE VIEW silver_apollo_contacts_data_quality AS
SELECT 
    apollo_id,
    contact_key,
    company_key,
    
    -- Required field checks
    CASE WHEN apollo_id IS NULL OR apollo_id = '' THEN 'FAIL' ELSE 'PASS' END as apollo_id_check,
    CASE WHEN contact_key IS NULL OR contact_key = '' THEN 'FAIL' ELSE 'PASS' END as contact_key_check,
    CASE WHEN company_key IS NULL OR company_key = '' THEN 'FAIL' ELSE 'PASS' END as company_key_check,
    
    -- PII masking validation
    CASE 
        WHEN first_name_masked IS NOT NULL AND first_name_masked LIKE '%*%' THEN 'MASKED'
        WHEN first_name_masked IS NOT NULL THEN 'NOT_MASKED'
        ELSE 'NO_DATA'
    END as first_name_masking_check,
    
    CASE 
        WHEN last_name_masked IS NOT NULL AND last_name_masked LIKE '%*%' THEN 'MASKED'
        WHEN last_name_masked IS NOT NULL THEN 'NOT_MASKED'
        ELSE 'NO_DATA'
    END as last_name_masking_check,
    
    CASE 
        WHEN email_masked IS NOT NULL AND email_masked LIKE '%*%' THEN 'MASKED'
        WHEN email_masked IS NOT NULL THEN 'NOT_MASKED'
        ELSE 'NO_DATA'
    END as email_masking_check,
    
    -- Contact validation
    CASE WHEN email_masked IS NOT NULL AND validate_email(email_masked) THEN 'VALID' ELSE 'INVALID' END as email_format_check,
    CASE WHEN phone_masked IS NOT NULL AND validate_phone(phone_masked) THEN 'VALID' ELSE 'INVALID' END as phone_format_check,
    CASE WHEN linkedin_url IS NOT NULL AND validate_url(linkedin_url) THEN 'VALID' ELSE 'INVALID' END as linkedin_url_check,
    
    -- Professional information validation
    CASE 
        WHEN title IS NOT NULL AND title != '' THEN 'COMPLETE'
        WHEN department IS NOT NULL AND department != '' THEN 'PARTIAL'
        ELSE 'INCOMPLETE'
    END as professional_info_check,
    
    -- Data quality score validation
    CASE 
        WHEN data_quality_score >= 0.8 THEN 'HIGH'
        WHEN data_quality_score >= 0.6 THEN 'MEDIUM'
        WHEN data_quality_score >= 0.4 THEN 'LOW'
        ELSE 'VERY_LOW'
    END as quality_level,
    
    -- Overall validation status
    CASE 
        WHEN (apollo_id IS NULL OR apollo_id = '') OR 
             (contact_key IS NULL OR contact_key = '') OR 
             (company_key IS NULL OR company_key = '') THEN 'FAIL'
        ELSE 'PASS'
    END as overall_validation_status
    
FROM thera_silver.silver_apollo_contacts;

-- 3. silver_domain_health data quality checks
CREATE OR REPLACE VIEW silver_domain_health_data_quality AS
SELECT 
    domain,
    company_key,
    
    -- Required field checks
    CASE WHEN domain IS NULL OR domain = '' THEN 'FAIL' ELSE 'PASS' END as domain_check,
    CASE WHEN company_key IS NULL OR company_key = '' THEN 'FAIL' ELSE 'PASS' END as company_key_check,
    
    -- Domain validation
    CASE WHEN validate_domain(domain) THEN 'PASS' ELSE 'FAIL' END as domain_format_check,
    
    -- Health score validation
    CASE 
        WHEN health_score IS NULL THEN 'NO_DATA'
        WHEN health_score < 0 OR health_score > 1 THEN 'INVALID'
        WHEN health_score >= 0.8 THEN 'EXCELLENT'
        WHEN health_score >= 0.6 THEN 'GOOD'
        WHEN health_score >= 0.4 THEN 'FAIR'
        ELSE 'POOR'
    END as health_score_validation,
    
    -- Grade validation
    CASE 
        WHEN overall_grade IS NULL THEN 'NO_DATA'
        WHEN overall_grade IN ('A+', 'A', 'B', 'C', 'D', 'F') THEN 'VALID'
        ELSE 'INVALID'
    END as grade_validation,
    
    -- Risk level validation
    CASE 
        WHEN risk_level IS NULL THEN 'NO_DATA'
        WHEN risk_level IN ('low', 'medium', 'high', 'critical') THEN 'VALID'
        ELSE 'INVALID'
    END as risk_level_validation,
    
    -- Critical issues validation
    CASE 
        WHEN SIZE(critical_issues) = 0 THEN 'CLEAN'
        WHEN SIZE(critical_issues) <= 2 THEN 'MINOR_ISSUES'
        ELSE 'MAJOR_ISSUES'
    END as critical_issues_validation,
    
    -- Overall validation status
    CASE 
        WHEN (domain IS NULL OR domain = '') OR 
             (company_key IS NULL OR company_key = '') OR
             NOT validate_domain(domain) OR
             health_score IS NULL OR
             health_score < 0 OR health_score > 1 THEN 'FAIL'
        ELSE 'PASS'
    END as overall_validation_status
    
FROM thera_silver.silver_domain_health;

-- 4. silver_web_extracts data quality checks
CREATE OR REPLACE VIEW silver_web_extracts_data_quality AS
SELECT 
    company_key,
    domain,
    page_url,
    
    -- Required field checks
    CASE WHEN company_key IS NULL OR company_key = '' THEN 'FAIL' ELSE 'PASS' END as company_key_check,
    CASE WHEN domain IS NULL OR domain = '' THEN 'FAIL' ELSE 'PASS' END as domain_check,
    CASE WHEN page_url IS NULL OR page_url = '' THEN 'FAIL' ELSE 'PASS' END as page_url_check,
    
    -- URL validation
    CASE WHEN validate_url(page_url) THEN 'PASS' ELSE 'FAIL' END as page_url_format_check,
    CASE WHEN validate_domain(domain) THEN 'PASS' ELSE 'FAIL' END as domain_format_check,
    
    -- Content validation
    CASE 
        WHEN title IS NOT NULL AND title != '' THEN 'COMPLETE'
        WHEN meta_description IS NOT NULL AND meta_description != '' THEN 'PARTIAL'
        ELSE 'INCOMPLETE'
    END as content_completeness,
    
    -- Content quality validation
    CASE 
        WHEN content_richness_score >= 0.8 THEN 'HIGH'
        WHEN content_richness_score >= 0.6 THEN 'MEDIUM'
        WHEN content_richness_score >= 0.4 THEN 'LOW'
        ELSE 'VERY_LOW'
    END as content_quality_level,
    
    -- Content hash validation
    CASE 
        WHEN content_hash IS NOT NULL AND LENGTH(content_hash) = 32 THEN 'VALID'
        WHEN content_hash IS NOT NULL THEN 'INVALID_LENGTH'
        ELSE 'NO_HASH'
    END as content_hash_validation,
    
    -- Overall validation status
    CASE 
        WHEN (company_key IS NULL OR company_key = '') OR 
             (domain IS NULL OR domain = '') OR 
             (page_url IS NULL OR page_url = '') OR
             NOT validate_url(page_url) OR
             NOT validate_domain(domain) THEN 'FAIL'
        ELSE 'PASS'
    END as overall_validation_status
    
FROM thera_silver.silver_web_extracts;

-- =============================================================================
-- GOLD LAYER DATA QUALITY CHECKS
-- =============================================================================

-- 5. gold_startup_profiles data quality checks
CREATE OR REPLACE VIEW gold_startup_profiles_data_quality AS
SELECT 
    company_key,
    domain,
    company_name,
    
    -- Required field checks
    CASE WHEN company_key IS NULL OR company_key = '' THEN 'FAIL' ELSE 'PASS' END as company_key_check,
    CASE WHEN domain IS NULL OR domain = '' THEN 'FAIL' ELSE 'PASS' END as domain_check,
    CASE WHEN company_name IS NULL OR company_name = '' THEN 'FAIL' ELSE 'PASS' END as company_name_check,
    
    -- Domain validation
    CASE WHEN validate_domain(domain) THEN 'PASS' ELSE 'FAIL' END as domain_format_check,
    
    -- Profile text validation
    CASE 
        WHEN profile_text IS NOT NULL AND LENGTH(profile_text) > 500 THEN 'COMPLETE'
        WHEN profile_text IS NOT NULL AND LENGTH(profile_text) > 100 THEN 'PARTIAL'
        WHEN profile_text IS NOT NULL THEN 'INCOMPLETE'
        ELSE 'NO_DATA'
    END as profile_text_completeness,
    
    -- Profile text hash validation
    CASE 
        WHEN profile_text_hash IS NOT NULL AND LENGTH(profile_text_hash) = 32 THEN 'VALID'
        WHEN profile_text_hash IS NOT NULL THEN 'INVALID_LENGTH'
        ELSE 'NO_HASH'
    END as profile_text_hash_validation,
    
    -- Data quality score validation
    CASE 
        WHEN overall_data_quality_score >= 0.8 THEN 'HIGH'
        WHEN overall_data_quality_score >= 0.6 THEN 'MEDIUM'
        WHEN overall_data_quality_score >= 0.4 THEN 'LOW'
        ELSE 'VERY_LOW'
    END as quality_level,
    
    -- Confidence level validation
    CASE 
        WHEN confidence_level IN ('high', 'medium', 'low') THEN 'VALID'
        ELSE 'INVALID'
    END as confidence_level_validation,
    
    -- Investment readiness validation
    CASE 
        WHEN investment_readiness_score IS NULL THEN 'NO_DATA'
        WHEN investment_readiness_score < 0 OR investment_readiness_score > 1 THEN 'INVALID'
        WHEN investment_readiness_score >= 0.8 THEN 'READY'
        WHEN investment_readiness_score >= 0.6 THEN 'NEARLY_READY'
        WHEN investment_readiness_score >= 0.4 THEN 'NEEDS_WORK'
        ELSE 'NOT_READY'
    END as investment_readiness_validation,
    
    -- Overall validation status
    CASE 
        WHEN (company_key IS NULL OR company_key = '') OR 
             (domain IS NULL OR domain = '') OR 
             (company_name IS NULL OR company_name = '') OR
             NOT validate_domain(domain) OR
             profile_text IS NULL OR
             profile_text_hash IS NULL THEN 'FAIL'
        ELSE 'PASS'
    END as overall_validation_status
    
FROM thera_gold.gold_startup_profiles;

-- =============================================================================
-- DATA QUALITY SUMMARY REPORTS
-- =============================================================================

-- Overall data quality summary
CREATE OR REPLACE VIEW data_quality_summary AS
SELECT 
    'silver_companies' as table_name,
    COUNT(*) as total_records,
    SUM(CASE WHEN overall_validation_status = 'PASS' THEN 1 ELSE 0 END) as valid_records,
    SUM(CASE WHEN overall_validation_status = 'FAIL' THEN 1 ELSE 0 END) as invalid_records,
    ROUND(SUM(CASE WHEN overall_validation_status = 'PASS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as validation_percentage
FROM silver_companies_data_quality
UNION ALL
SELECT 
    'silver_apollo_contacts',
    COUNT(*),
    SUM(CASE WHEN overall_validation_status = 'PASS' THEN 1 ELSE 0 END),
    SUM(CASE WHEN overall_validation_status = 'FAIL' THEN 1 ELSE 0 END),
    ROUND(SUM(CASE WHEN overall_validation_status = 'PASS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
FROM silver_apollo_contacts_data_quality
UNION ALL
SELECT 
    'silver_domain_health',
    COUNT(*),
    SUM(CASE WHEN overall_validation_status = 'PASS' THEN 1 ELSE 0 END),
    SUM(CASE WHEN overall_validation_status = 'FAIL' THEN 1 ELSE 0 END),
    ROUND(SUM(CASE WHEN overall_validation_status = 'PASS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
FROM silver_domain_health_data_quality
UNION ALL
SELECT 
    'silver_web_extracts',
    COUNT(*),
    SUM(CASE WHEN overall_validation_status = 'PASS' THEN 1 ELSE 0 END),
    SUM(CASE WHEN overall_validation_status = 'FAIL' THEN 1 ELSE 0 END),
    ROUND(SUM(CASE WHEN overall_validation_status = 'PASS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
FROM silver_web_extracts_data_quality
UNION ALL
SELECT 
    'gold_startup_profiles',
    COUNT(*),
    SUM(CASE WHEN overall_validation_status = 'PASS' THEN 1 ELSE 0 END),
    SUM(CASE WHEN overall_validation_status = 'FAIL' THEN 1 ELSE 0 END),
    ROUND(SUM(CASE WHEN overall_validation_status = 'PASS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
FROM gold_startup_profiles_data_quality;

-- =============================================================================
-- DATA QUALITY MONITORING QUERIES
-- =============================================================================

-- Query to identify records that need attention
CREATE OR REPLACE VIEW data_quality_issues AS
SELECT 
    'silver_companies' as table_name,
    company_key,
    domain,
    company_name,
    overall_validation_status,
    CASE 
        WHEN company_key IS NULL OR company_key = '' THEN 'Missing company_key'
        WHEN domain IS NULL OR domain = '' THEN 'Missing domain'
        WHEN company_name IS NULL OR company_name = '' THEN 'Missing company_name'
        WHEN NOT validate_domain(domain) THEN 'Invalid domain format'
        ELSE 'Other validation issue'
    END as issue_description
FROM silver_companies_data_quality
WHERE overall_validation_status = 'FAIL'
UNION ALL
SELECT 
    'silver_apollo_contacts',
    contact_key,
    company_key,
    CONCAT(first_name_masked, ' ', last_name_masked),
    overall_validation_status,
    CASE 
        WHEN apollo_id IS NULL OR apollo_id = '' THEN 'Missing apollo_id'
        WHEN contact_key IS NULL OR contact_key = '' THEN 'Missing contact_key'
        WHEN company_key IS NULL OR company_key = '' THEN 'Missing company_key'
        ELSE 'Other validation issue'
    END as issue_description
FROM silver_apollo_contacts_data_quality
WHERE overall_validation_status = 'FAIL'
UNION ALL
SELECT 
    'silver_domain_health',
    domain,
    company_key,
    domain,
    overall_validation_status,
    CASE 
        WHEN domain IS NULL OR domain = '' THEN 'Missing domain'
        WHEN company_key IS NULL OR company_key = '' THEN 'Missing company_key'
        WHEN NOT validate_domain(domain) THEN 'Invalid domain format'
        WHEN health_score IS NULL THEN 'Missing health_score'
        WHEN health_score < 0 OR health_score > 1 THEN 'Invalid health_score'
        ELSE 'Other validation issue'
    END as issue_description
FROM silver_domain_health_data_quality
WHERE overall_validation_status = 'FAIL'
UNION ALL
SELECT 
    'silver_web_extracts',
    company_key,
    domain,
    page_url,
    overall_validation_status,
    CASE 
        WHEN company_key IS NULL OR company_key = '' THEN 'Missing company_key'
        WHEN domain IS NULL OR domain = '' THEN 'Missing domain'
        WHEN page_url IS NULL OR page_url = '' THEN 'Missing page_url'
        WHEN NOT validate_url(page_url) THEN 'Invalid page_url format'
        WHEN NOT validate_domain(domain) THEN 'Invalid domain format'
        ELSE 'Other validation issue'
    END as issue_description
FROM silver_web_extracts_data_quality
WHERE overall_validation_status = 'FAIL'
UNION ALL
SELECT 
    'gold_startup_profiles',
    company_key,
    domain,
    company_name,
    overall_validation_status,
    CASE 
        WHEN company_key IS NULL OR company_key = '' THEN 'Missing company_key'
        WHEN domain IS NULL OR domain = '' THEN 'Missing domain'
        WHEN company_name IS NULL OR company_name = '' THEN 'Missing company_name'
        WHEN NOT validate_domain(domain) THEN 'Invalid domain format'
        WHEN profile_text IS NULL THEN 'Missing profile_text'
        WHEN profile_text_hash IS NULL THEN 'Missing profile_text_hash'
        ELSE 'Other validation issue'
    END as issue_description
FROM gold_startup_profiles_data_quality
WHERE overall_validation_status = 'FAIL';

-- =============================================================================
-- NOTES FOR CONFIGURATION
-- =============================================================================
-- 1. Run these data quality checks regularly to monitor data health
-- 2. Set up alerts for validation failures above threshold
-- 3. Consider adding automated data quality fixes for common issues
-- 4. Add additional validation rules based on business requirements
-- 5. Monitor data quality trends over time
-- 6. Consider adding data lineage tracking for quality issues
