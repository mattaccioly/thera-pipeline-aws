-- =============================================================================
-- SILVER LAYER: silver_domain_health
-- =============================================================================
-- Purpose: Domain health scoring and flags from gate job processing
-- Source: Domain health analysis job (gate job)
-- Storage: Parquet format partitioned by dt
-- Usage: Provides domain health metrics for startup profiling

USE thera_silver;

-- =============================================================================
-- TABLE: silver_domain_health
-- =============================================================================
CREATE TABLE IF NOT EXISTS silver_domain_health (
    -- Primary identifiers
    domain STRING NOT NULL,
    company_key STRING NOT NULL, -- Links to silver_companies
    
    -- Domain health metrics
    health_score DECIMAL(3,2) NOT NULL, -- 0.00 to 1.00
    overall_grade STRING, -- A, B, C, D, F
    risk_level STRING, -- low, medium, high, critical
    
    -- Technical health indicators
    is_https_enabled BOOLEAN,
    ssl_grade STRING, -- A+, A, B, C, D, F
    ssl_expiry_date DATE,
    is_ssl_valid BOOLEAN,
    
    -- Performance metrics
    page_load_time_ms INT,
    performance_grade STRING, -- A, B, C, D, F
    mobile_performance_score DECIMAL(3,2), -- 0.00 to 1.00
    desktop_performance_score DECIMAL(3,2), -- 0.00 to 1.00
    
    -- SEO and content health
    seo_score DECIMAL(3,2), -- 0.00 to 1.00
    seo_grade STRING, -- A, B, C, D, F
    has_meta_description BOOLEAN,
    has_meta_keywords BOOLEAN,
    has_structured_data BOOLEAN,
    title_tag_length INT,
    meta_description_length INT,
    
    -- Security indicators
    security_score DECIMAL(3,2), -- 0.00 to 1.00
    security_grade STRING, -- A, B, C, D, F
    has_security_headers BOOLEAN,
    has_hsts_header BOOLEAN,
    has_csp_header BOOLEAN,
    has_xss_protection BOOLEAN,
    is_malware_detected BOOLEAN,
    is_phishing_detected BOOLEAN,
    
    -- Accessibility metrics
    accessibility_score DECIMAL(3,2), -- 0.00 to 1.00
    accessibility_grade STRING, -- A, B, C, D, F
    has_alt_text BOOLEAN,
    has_heading_structure BOOLEAN,
    has_aria_labels BOOLEAN,
    color_contrast_score DECIMAL(3,2), -- 0.00 to 1.00
    
    -- Domain reputation
    domain_age_days INT,
    domain_authority_score INT, -- 0 to 100
    backlink_count INT,
    referring_domains_count INT,
    spam_score DECIMAL(3,2), -- 0.00 to 1.00
    
    -- Social media presence
    social_media_score DECIMAL(3,2), -- 0.00 to 1.00
    has_linkedin BOOLEAN,
    has_facebook BOOLEAN,
    has_twitter BOOLEAN,
    has_instagram BOOLEAN,
    has_youtube BOOLEAN,
    has_github BOOLEAN,
    
    -- Content quality indicators
    content_quality_score DECIMAL(3,2), -- 0.00 to 1.00
    content_freshness_score DECIMAL(3,2), -- 0.00 to 1.00
    has_blog BOOLEAN,
    has_news_section BOOLEAN,
    has_about_page BOOLEAN,
    has_contact_page BOOLEAN,
    has_privacy_policy BOOLEAN,
    has_terms_of_service BOOLEAN,
    
    -- Business indicators
    business_credibility_score DECIMAL(3,2), -- 0.00 to 1.00
    has_contact_info BOOLEAN,
    has_physical_address BOOLEAN,
    has_phone_number BOOLEAN,
    has_email_address BOOLEAN,
    has_team_page BOOLEAN,
    has_careers_page BOOLEAN,
    has_pricing_page BOOLEAN,
    
    -- Technology stack indicators
    technology_score DECIMAL(3,2), -- 0.00 to 1.00
    uses_modern_framework BOOLEAN,
    uses_cdn BOOLEAN,
    uses_analytics BOOLEAN,
    uses_heatmaps BOOLEAN,
    uses_chat_widget BOOLEAN,
    uses_live_chat BOOLEAN,
    
    -- Flags and alerts
    health_flags ARRAY<STRING>, -- List of health issues
    critical_issues ARRAY<STRING>, -- List of critical issues
    warnings ARRAY<STRING>, -- List of warnings
    recommendations ARRAY<STRING>, -- List of recommendations
    
    -- Data quality metrics
    data_quality_score DECIMAL(3,2), -- 0.00 to 1.00
    completeness_score DECIMAL(3,2), -- 0.00 to 1.00
    accuracy_score DECIMAL(3,2), -- 0.00 to 1.00
    freshness_score DECIMAL(3,2), -- 0.00 to 1.00
    
    -- Validation flags
    is_valid_domain BOOLEAN,
    is_domain_resolvable BOOLEAN,
    is_website_accessible BOOLEAN,
    is_analysis_complete BOOLEAN,
    
    -- Data lineage and metadata
    source_system STRING,
    analysis_job_id STRING,
    analysis_timestamp TIMESTAMP,
    analysis_duration_seconds INT,
    raw_analysis_data STRING, -- JSON with full analysis results
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    last_verified_at TIMESTAMP,
    
    -- Partitioning
    dt STRING
)
COMMENT 'Domain health scoring and flags from gate job processing'
PARTITIONED BY (dt)
STORED AS PARQUET
LOCATION 's3://thera-curated/silver/domain_health/'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY',
    'classification' = 'silver',
    'data_quality' = 'curated'
);

-- =============================================================================
-- CTAS: Populate silver_domain_health with health scoring logic
-- =============================================================================
-- This CTAS simulates domain health analysis results
-- In production, this would be populated by the actual gate job

CREATE TABLE IF NOT EXISTS silver_domain_health_temp AS
WITH 
-- Step 1: Get domains from silver_companies
company_domains AS (
    SELECT DISTINCT
        domain,
        company_key,
        company_name,
        industry,
        country
    FROM thera_silver.silver_companies
    WHERE domain IS NOT NULL 
        AND domain != ''
),

-- Step 2: Simulate domain health analysis results
-- In production, this would come from the actual gate job
domain_health_analysis AS (
    SELECT 
        domain,
        company_key,
        company_name,
        industry,
        country,
        
        -- Simulate health score calculation (0.00 to 1.00)
        -- In production, this would be calculated by the gate job
        CASE 
            WHEN RAND() < 0.1 THEN 0.9 + (RAND() * 0.1) -- 10% get A grade
            WHEN RAND() < 0.3 THEN 0.8 + (RAND() * 0.1) -- 20% get B grade
            WHEN RAND() < 0.6 THEN 0.6 + (RAND() * 0.2) -- 30% get C grade
            WHEN RAND() < 0.8 THEN 0.4 + (RAND() * 0.2) -- 20% get D grade
            ELSE 0.2 + (RAND() * 0.2) -- 20% get F grade
        END as health_score,
        
        -- Simulate technical health indicators
        CASE WHEN RAND() < 0.85 THEN true ELSE false END as is_https_enabled,
        CASE 
            WHEN RAND() < 0.2 THEN 'A+'
            WHEN RAND() < 0.4 THEN 'A'
            WHEN RAND() < 0.6 THEN 'B'
            WHEN RAND() < 0.8 THEN 'C'
            ELSE 'D'
        END as ssl_grade,
        
        -- Simulate performance metrics
        CAST(500 + (RAND() * 3000) AS INT) as page_load_time_ms,
        CASE 
            WHEN RAND() < 0.2 THEN 'A'
            WHEN RAND() < 0.4 THEN 'B'
            WHEN RAND() < 0.6 THEN 'C'
            WHEN RAND() < 0.8 THEN 'D'
            ELSE 'F'
        END as performance_grade,
        
        0.5 + (RAND() * 0.5) as mobile_performance_score,
        0.5 + (RAND() * 0.5) as desktop_performance_score,
        
        -- Simulate SEO metrics
        0.3 + (RAND() * 0.7) as seo_score,
        CASE 
            WHEN RAND() < 0.2 THEN 'A'
            WHEN RAND() < 0.4 THEN 'B'
            WHEN RAND() < 0.6 THEN 'C'
            WHEN RAND() < 0.8 THEN 'D'
            ELSE 'F'
        END as seo_grade,
        
        CASE WHEN RAND() < 0.7 THEN true ELSE false END as has_meta_description,
        CASE WHEN RAND() < 0.5 THEN true ELSE false END as has_meta_keywords,
        CASE WHEN RAND() < 0.6 THEN true ELSE false END as has_structured_data,
        CAST(30 + (RAND() * 40) AS INT) as title_tag_length,
        CAST(100 + (RAND() * 100) AS INT) as meta_description_length,
        
        -- Simulate security metrics
        0.4 + (RAND() * 0.6) as security_score,
        CASE 
            WHEN RAND() < 0.3 THEN 'A'
            WHEN RAND() < 0.5 THEN 'B'
            WHEN RAND() < 0.7 THEN 'C'
            WHEN RAND() < 0.9 THEN 'D'
            ELSE 'F'
        END as security_grade,
        
        CASE WHEN RAND() < 0.6 THEN true ELSE false END as has_security_headers,
        CASE WHEN RAND() < 0.4 THEN true ELSE false END as has_hsts_header,
        CASE WHEN RAND() < 0.3 THEN true ELSE false END as has_csp_header,
        CASE WHEN RAND() < 0.5 THEN true ELSE false END as has_xss_protection,
        CASE WHEN RAND() < 0.05 THEN true ELSE false END as is_malware_detected,
        CASE WHEN RAND() < 0.02 THEN true ELSE false END as is_phishing_detected,
        
        -- Simulate accessibility metrics
        0.3 + (RAND() * 0.7) as accessibility_score,
        CASE 
            WHEN RAND() < 0.2 THEN 'A'
            WHEN RAND() < 0.4 THEN 'B'
            WHEN RAND() < 0.6 THEN 'C'
            WHEN RAND() < 0.8 THEN 'D'
            ELSE 'F'
        END as accessibility_grade,
        
        CASE WHEN RAND() < 0.6 THEN true ELSE false END as has_alt_text,
        CASE WHEN RAND() < 0.7 THEN true ELSE false END as has_heading_structure,
        CASE WHEN RAND() < 0.4 THEN true ELSE false END as has_aria_labels,
        0.4 + (RAND() * 0.6) as color_contrast_score,
        
        -- Simulate domain reputation
        CAST(365 + (RAND() * 3650) AS INT) as domain_age_days,
        CAST(20 + (RAND() * 80) AS INT) as domain_authority_score,
        CAST(RAND() * 1000 AS INT) as backlink_count,
        CAST(RAND() * 100 AS INT) as referring_domains_count,
        RAND() * 0.3 as spam_score,
        
        -- Simulate social media presence
        0.2 + (RAND() * 0.8) as social_media_score,
        CASE WHEN RAND() < 0.7 THEN true ELSE false END as has_linkedin,
        CASE WHEN RAND() < 0.6 THEN true ELSE false END as has_facebook,
        CASE WHEN RAND() < 0.5 THEN true ELSE false END as has_twitter,
        CASE WHEN RAND() < 0.4 THEN true ELSE false END as has_instagram,
        CASE WHEN RAND() < 0.3 THEN true ELSE false END as has_youtube,
        CASE WHEN RAND() < 0.2 THEN true ELSE false END as has_github,
        
        -- Simulate content quality
        0.3 + (RAND() * 0.7) as content_quality_score,
        0.4 + (RAND() * 0.6) as content_freshness_score,
        CASE WHEN RAND() < 0.6 THEN true ELSE false END as has_blog,
        CASE WHEN RAND() < 0.4 THEN true ELSE false END as has_news_section,
        CASE WHEN RAND() < 0.8 THEN true ELSE false END as has_about_page,
        CASE WHEN RAND() < 0.7 THEN true ELSE false END as has_contact_page,
        CASE WHEN RAND() < 0.5 THEN true ELSE false END as has_privacy_policy,
        CASE WHEN RAND() < 0.4 THEN true ELSE false END as has_terms_of_service,
        
        -- Simulate business credibility
        0.4 + (RAND() * 0.6) as business_credibility_score,
        CASE WHEN RAND() < 0.8 THEN true ELSE false END as has_contact_info,
        CASE WHEN RAND() < 0.6 THEN true ELSE false END as has_physical_address,
        CASE WHEN RAND() < 0.7 THEN true ELSE false END as has_phone_number,
        CASE WHEN RAND() < 0.8 THEN true ELSE false END as has_email_address,
        CASE WHEN RAND() < 0.5 THEN true ELSE false END as has_team_page,
        CASE WHEN RAND() < 0.4 THEN true ELSE false END as has_careers_page,
        CASE WHEN RAND() < 0.6 THEN true ELSE false END as has_pricing_page,
        
        -- Simulate technology stack
        0.3 + (RAND() * 0.7) as technology_score,
        CASE WHEN RAND() < 0.6 THEN true ELSE false END as uses_modern_framework,
        CASE WHEN RAND() < 0.5 THEN true ELSE false END as uses_cdn,
        CASE WHEN RAND() < 0.8 THEN true ELSE false END as uses_analytics,
        CASE WHEN RAND() < 0.3 THEN true ELSE false END as uses_heatmaps,
        CASE WHEN RAND() < 0.4 THEN true ELSE false END as uses_chat_widget,
        CASE WHEN RAND() < 0.3 THEN true ELSE false END as uses_live_chat
        
    FROM company_domains
),

-- Step 3: Calculate derived metrics and flags
calculated_metrics AS (
    SELECT 
        *,
        
        -- Calculate overall grade based on health score
        CASE 
            WHEN health_score >= 0.9 THEN 'A'
            WHEN health_score >= 0.8 THEN 'B'
            WHEN health_score >= 0.7 THEN 'C'
            WHEN health_score >= 0.6 THEN 'D'
            ELSE 'F'
        END as overall_grade,
        
        -- Calculate risk level
        CASE 
            WHEN health_score >= 0.8 THEN 'low'
            WHEN health_score >= 0.6 THEN 'medium'
            WHEN health_score >= 0.4 THEN 'high'
            ELSE 'critical'
        END as risk_level,
        
        -- Calculate SSL validity
        CASE 
            WHEN ssl_grade IN ('A+', 'A', 'B') THEN true
            ELSE false
        END as is_ssl_valid,
        
        -- Calculate SSL expiry date (simulate future date)
        DATE_ADD(CURRENT_DATE, CAST(30 + (RAND() * 365) AS INT)) as ssl_expiry_date,
        
        -- Generate health flags based on issues
        CASE 
            WHEN is_https_enabled = false THEN ARRAY['no_https']
            WHEN ssl_grade IN ('D', 'F') THEN ARRAY['poor_ssl']
            WHEN page_load_time_ms > 3000 THEN ARRAY['slow_loading']
            WHEN seo_score < 0.5 THEN ARRAY['poor_seo']
            WHEN security_score < 0.5 THEN ARRAY['security_issues']
            WHEN accessibility_score < 0.5 THEN ARRAY['accessibility_issues']
            WHEN is_malware_detected = true THEN ARRAY['malware_detected']
            WHEN is_phishing_detected = true THEN ARRAY['phishing_detected']
            ELSE ARRAY[]
        END as health_flags,
        
        -- Generate critical issues
        CASE 
            WHEN is_malware_detected = true THEN ARRAY['malware_detected']
            WHEN is_phishing_detected = true THEN ARRAY['phishing_detected']
            WHEN is_https_enabled = false AND industry IN ('finance', 'healthcare', 'ecommerce') THEN ARRAY['no_https_critical']
            ELSE ARRAY[]
        END as critical_issues,
        
        -- Generate warnings
        CASE 
            WHEN ssl_grade IN ('C', 'D', 'F') THEN ARRAY['ssl_warning']
            WHEN page_load_time_ms > 2000 THEN ARRAY['performance_warning']
            WHEN seo_score < 0.6 THEN ARRAY['seo_warning']
            WHEN security_score < 0.6 THEN ARRAY['security_warning']
            WHEN accessibility_score < 0.6 THEN ARRAY['accessibility_warning']
            ELSE ARRAY[]
        END as warnings,
        
        -- Generate recommendations
        CASE 
            WHEN is_https_enabled = false THEN ARRAY['enable_https']
            WHEN ssl_grade IN ('C', 'D', 'F') THEN ARRAY['improve_ssl']
            WHEN page_load_time_ms > 2000 THEN ARRAY['optimize_performance']
            WHEN seo_score < 0.6 THEN ARRAY['improve_seo']
            WHEN security_score < 0.6 THEN ARRAY['improve_security']
            WHEN accessibility_score < 0.6 THEN ARRAY['improve_accessibility']
            WHEN has_meta_description = false THEN ARRAY['add_meta_description']
            WHEN has_contact_page = false THEN ARRAY['add_contact_page']
            ELSE ARRAY[]
        END as recommendations,
        
        -- Calculate data quality scores
        (
            CASE WHEN domain IS NOT NULL THEN 0.2 ELSE 0 END +
            CASE WHEN health_score IS NOT NULL THEN 0.2 ELSE 0 END +
            CASE WHEN is_https_enabled IS NOT NULL THEN 0.1 ELSE 0 END +
            CASE WHEN ssl_grade IS NOT NULL THEN 0.1 ELSE 0 END +
            CASE WHEN page_load_time_ms IS NOT NULL THEN 0.1 ELSE 0 END +
            CASE WHEN seo_score IS NOT NULL THEN 0.1 ELSE 0 END +
            CASE WHEN security_score IS NOT NULL THEN 0.1 ELSE 0 END +
            CASE WHEN accessibility_score IS NOT NULL THEN 0.1 ELSE 0 END
        ) as data_quality_score,
        
        -- Calculate completeness score
        (
            CASE WHEN health_score IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN is_https_enabled IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN ssl_grade IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN page_load_time_ms IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN seo_score IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN security_score IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN accessibility_score IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN domain_age_days IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN domain_authority_score IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN backlink_count IS NOT NULL THEN 1 ELSE 0 END
        ) / 10.0 as completeness_score,
        
        -- Calculate accuracy score (simulate based on validation)
        0.8 + (RAND() * 0.2) as accuracy_score,
        
        -- Calculate freshness score
        1.0 as freshness_score, -- Assuming recent analysis
        
        -- Validation flags
        CASE 
            WHEN domain IS NOT NULL AND domain != '' AND domain LIKE '%.%' THEN true
            ELSE false
        END as is_valid_domain,
        
        CASE 
            WHEN domain IS NOT NULL AND domain != '' THEN true
            ELSE false
        END as is_domain_resolvable,
        
        CASE 
            WHEN health_score > 0.3 THEN true
            ELSE false
        END as is_website_accessible,
        
        true as is_analysis_complete,
        
        -- Metadata
        'gate_job' as source_system,
        CONCAT('analysis_', CAST(UNIX_TIMESTAMP() AS STRING)) as analysis_job_id,
        CURRENT_TIMESTAMP as analysis_timestamp,
        CAST(30 + (RAND() * 120) AS INT) as analysis_duration_seconds,
        CONCAT('{"domain":"', domain, '","analysis_timestamp":"', CURRENT_TIMESTAMP, '"}') as raw_analysis_data,
        CURRENT_TIMESTAMP as created_at,
        CURRENT_TIMESTAMP as updated_at,
        CURRENT_TIMESTAMP as last_verified_at,
        
        -- Partitioning
        DATE_FORMAT(CURRENT_DATE, 'yyyy-MM-dd') as dt
        
    FROM domain_health_analysis
)

SELECT 
    domain,
    company_key,
    health_score,
    overall_grade,
    risk_level,
    is_https_enabled,
    ssl_grade,
    ssl_expiry_date,
    is_ssl_valid,
    page_load_time_ms,
    performance_grade,
    mobile_performance_score,
    desktop_performance_score,
    seo_score,
    seo_grade,
    has_meta_description,
    has_meta_keywords,
    has_structured_data,
    title_tag_length,
    meta_description_length,
    security_score,
    security_grade,
    has_security_headers,
    has_hsts_header,
    has_csp_header,
    has_xss_protection,
    is_malware_detected,
    is_phishing_detected,
    accessibility_score,
    accessibility_grade,
    has_alt_text,
    has_heading_structure,
    has_aria_labels,
    color_contrast_score,
    domain_age_days,
    domain_authority_score,
    backlink_count,
    referring_domains_count,
    spam_score,
    social_media_score,
    has_linkedin,
    has_facebook,
    has_twitter,
    has_instagram,
    has_youtube,
    has_github,
    content_quality_score,
    content_freshness_score,
    has_blog,
    has_news_section,
    has_about_page,
    has_contact_page,
    has_privacy_policy,
    has_terms_of_service,
    business_credibility_score,
    has_contact_info,
    has_physical_address,
    has_phone_number,
    has_email_address,
    has_team_page,
    has_careers_page,
    has_pricing_page,
    technology_score,
    uses_modern_framework,
    uses_cdn,
    uses_analytics,
    uses_heatmaps,
    uses_chat_widget,
    uses_live_chat,
    health_flags,
    critical_issues,
    warnings,
    recommendations,
    data_quality_score,
    completeness_score,
    accuracy_score,
    freshness_score,
    is_valid_domain,
    is_domain_resolvable,
    is_website_accessible,
    is_analysis_complete,
    source_system,
    analysis_job_id,
    analysis_timestamp,
    analysis_duration_seconds,
    raw_analysis_data,
    created_at,
    updated_at,
    last_verified_at,
    dt
FROM calculated_metrics;

-- =============================================================================
-- NOTES FOR CONFIGURATION
-- =============================================================================
-- 1. Replace simulation logic with actual gate job results
-- 2. Adjust scoring algorithms based on business requirements
-- 3. Add additional health indicators as needed
-- 4. Modify flag generation logic based on industry standards
-- 5. Consider adding real-time monitoring for critical issues
-- 6. Add alerting mechanisms for high-risk domains
-- 7. Consider adding historical tracking for health score trends
