-- =============================================================================
-- GOLD LAYER: gold_startup_profiles
-- =============================================================================
-- Purpose: Comprehensive startup profiles combining all data sources
-- Source: All silver layer tables
-- Storage: Parquet format partitioned by dt
-- Features: LLM-ready profile_text generation, comprehensive startup insights

USE thera_gold;

-- =============================================================================
-- TABLE: gold_startup_profiles
-- =============================================================================
CREATE TABLE IF NOT EXISTS gold_startup_profiles (
    -- Primary identifiers
    company_key STRING NOT NULL,
    domain STRING NOT NULL,
    company_name STRING NOT NULL,
    
    -- Identity information
    legal_name STRING,
    dba_name STRING,
    linkedin_url STRING,
    website_url STRING,
    
    -- Location and industry
    country STRING,
    state_province STRING,
    city STRING,
    industry STRING,
    sub_industry STRING,
    industry_category STRING,
    size_bracket STRING, -- startup, small, medium, large, enterprise
    
    -- Company details
    description STRING,
    short_description STRING,
    tagline STRING,
    founded_year INT,
    founded_date DATE,
    employee_count INT,
    employee_range STRING,
    revenue_range STRING,
    
    -- Financial information
    total_funding DECIMAL(15,2),
    last_funding_date DATE,
    last_funding_type STRING,
    last_funding_amount DECIMAL(15,2),
    funding_stage STRING,
    valuation DECIMAL(15,2),
    
    -- Domain health metrics
    domain_health_score DECIMAL(3,2), -- 0.00 to 1.00
    domain_grade STRING, -- A, B, C, D, F
    risk_level STRING, -- low, medium, high, critical
    health_flags ARRAY<STRING>,
    critical_issues ARRAY<STRING>,
    
    -- Apollo aggregates
    apollo_contact_count INT,
    apollo_has_verified_contact BOOLEAN,
    apollo_seniority_mix STRUCT<
        c_level: INT,
        vp_director: INT,
        manager: INT,
        senior: INT,
        mid_level: INT,
        junior: INT
    >,
    apollo_contact_quality_score DECIMAL(3,2),
    
    -- Web content fields
    web_title STRING,
    web_meta_description STRING,
    web_about_snippet STRING,
    web_content_categories ARRAY<STRING>,
    web_topics ARRAY<STRING>,
    web_keywords ARRAY<STRING>,
    
    -- Social media presence
    social_media_presence STRUCT<
        linkedin: STRING,
        facebook: STRING,
        twitter: STRING,
        instagram: STRING,
        youtube: STRING,
        github: STRING,
        tiktok: STRING,
        discord: STRING,
        slack: STRING
    >,
    social_media_score DECIMAL(3,2),
    
    -- Technology stack
    technology_stack ARRAY<STRING>,
    programming_languages ARRAY<STRING>,
    frameworks ARRAY<STRING>,
    databases ARRAY<STRING>,
    cloud_providers ARRAY<STRING>,
    cms_platforms ARRAY<STRING>,
    analytics_tools ARRAY<STRING>,
    technology_score DECIMAL(3,2),
    
    -- Content quality metrics
    content_richness_score DECIMAL(3,2),
    content_quality_grade STRING, -- A, B, C, D, F
    readability_score DECIMAL(3,2),
    technical_depth_score DECIMAL(3,2),
    business_info_score DECIMAL(3,2),
    
    -- Business credibility
    business_credibility_score DECIMAL(3,2),
    has_contact_info BOOLEAN,
    has_physical_address BOOLEAN,
    has_team_page BOOLEAN,
    has_careers_page BOOLEAN,
    has_pricing_page BOOLEAN,
    has_privacy_policy BOOLEAN,
    has_terms_of_service BOOLEAN,
    
    -- Client and testimonial information
    clients_mentions ARRAY<STRING>,
    testimonials ARRAY<STRING>,
    case_studies ARRAY<STRING>,
    client_credibility_score DECIMAL(3,2),
    
    -- Data quality and confidence
    overall_data_quality_score DECIMAL(3,2),
    data_completeness_score DECIMAL(3,2),
    data_accuracy_score DECIMAL(3,2),
    data_freshness_score DECIMAL(3,2),
    confidence_level STRING, -- high, medium, low
    
    -- LLM-ready profile text
    profile_text STRING, -- Comprehensive profile for LLM processing
    profile_text_hash STRING, -- Hash for change detection
    profile_summary STRING, -- Short summary for quick reference
    profile_tags ARRAY<STRING>, -- Tags for categorization and search
    
    -- Startup-specific insights
    startup_stage STRING, -- idea, mvp, growth, scale, mature
    growth_indicators ARRAY<STRING>,
    innovation_indicators ARRAY<STRING>,
    market_presence_score DECIMAL(3,2),
    competitive_advantage_score DECIMAL(3,2),
    
    -- Investment readiness
    investment_readiness_score DECIMAL(3,2),
    investment_readiness_grade STRING, -- A, B, C, D, F
    funding_readiness_indicators ARRAY<STRING>,
    due_diligence_red_flags ARRAY<STRING>,
    
    -- Market analysis
    market_size_estimate STRING, -- small, medium, large, enterprise
    target_market ARRAY<STRING>,
    competitive_landscape ARRAY<STRING>,
    market_opportunity_score DECIMAL(3,2),
    
    -- Data lineage and metadata
    source_systems ARRAY<STRING>,
    last_updated_source STRING,
    profile_generation_method STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    last_verified_at TIMESTAMP,
    
    -- Partitioning
    dt STRING
)
COMMENT 'Comprehensive startup profiles combining all data sources with LLM-ready profile text'
PARTITIONED BY (dt)
STORED AS PARQUET
LOCATION 's3://thera-curated/gold/startup_profiles/'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY',
    'classification' = 'gold',
    'data_quality' = 'aggregated'
);

-- =============================================================================
-- CTAS: Populate gold_startup_profiles with comprehensive data aggregation
-- =============================================================================
CREATE TABLE IF NOT EXISTS gold_startup_profiles_temp AS
WITH 
-- Step 1: Get base company information from silver_companies
base_companies AS (
    SELECT 
        company_key,
        domain,
        company_name,
        legal_name,
        dba_name,
        linkedin_url,
        website_url,
        country,
        state_province,
        city,
        industry,
        sub_industry,
        industry_category,
        size_bracket,
        description,
        short_description,
        tagline,
        founded_year,
        founded_date,
        employee_count,
        employee_range,
        revenue_range,
        total_funding,
        last_funding_date,
        last_funding_type,
        last_funding_amount,
        funding_stage,
        valuation,
        data_quality_score,
        confidence_score,
        created_at,
        updated_at
    FROM thera_silver.silver_companies
    WHERE is_primary_record = true
),

-- Step 2: Get domain health information
domain_health AS (
    SELECT 
        domain,
        health_score as domain_health_score,
        overall_grade as domain_grade,
        risk_level,
        health_flags,
        critical_issues,
        security_score,
        performance_grade,
        seo_grade,
        accessibility_grade
    FROM thera_silver.silver_domain_health
),

-- Step 3: Get Apollo contact aggregates
apollo_aggregates AS (
    SELECT 
        company_key,
        COUNT(*) as apollo_contact_count,
        MAX(CASE WHEN is_verified = true THEN 1 ELSE 0 END) as apollo_has_verified_contact,
        AVG(contact_quality_score) as apollo_contact_quality_score,
        
        -- Calculate seniority mix
        SUM(CASE WHEN seniority_level = 'c-level' OR seniority_level = 'executive' THEN 1 ELSE 0 END) as c_level_count,
        SUM(CASE WHEN seniority_level = 'vp' OR seniority_level = 'director' THEN 1 ELSE 0 END) as vp_director_count,
        SUM(CASE WHEN seniority_level = 'manager' THEN 1 ELSE 0 END) as manager_count,
        SUM(CASE WHEN seniority_level = 'senior' THEN 1 ELSE 0 END) as senior_count,
        SUM(CASE WHEN seniority_level = 'mid' OR seniority_level = 'mid-level' THEN 1 ELSE 0 END) as mid_level_count,
        SUM(CASE WHEN seniority_level = 'junior' THEN 1 ELSE 0 END) as junior_count
        
    FROM thera_silver.silver_apollo_contacts
    GROUP BY company_key
),

-- Step 4: Get web content aggregates
web_content AS (
    SELECT 
        company_key,
        -- Get the most recent and comprehensive web content
        FIRST_VALUE(title) OVER (
            PARTITION BY company_key 
            ORDER BY content_richness_score DESC, crawl_ts DESC
        ) as web_title,
        
        FIRST_VALUE(meta_description) OVER (
            PARTITION BY company_key 
            ORDER BY content_richness_score DESC, crawl_ts DESC
        ) as web_meta_description,
        
        FIRST_VALUE(about_snippet) OVER (
            PARTITION BY company_key 
            ORDER BY content_richness_score DESC, crawl_ts DESC
        ) as web_about_snippet,
        
        -- Aggregate arrays
        COLLECT_SET(content_categories) as all_content_categories,
        COLLECT_SET(topics) as all_topics,
        COLLECT_SET(keywords) as all_keywords,
        
        -- Social media presence
        FIRST_VALUE(social_links) OVER (
            PARTITION BY company_key 
            ORDER BY content_richness_score DESC, crawl_ts DESC
        ) as social_links,
        
        -- Technology stack
        COLLECT_SET(tech_hints) as all_tech_hints,
        COLLECT_SET(programming_languages) as all_programming_languages,
        COLLECT_SET(frameworks) as all_frameworks,
        COLLECT_SET(databases) as all_databases,
        COLLECT_SET(cloud_providers) as all_cloud_providers,
        COLLECT_SET(cms_platforms) as all_cms_platforms,
        COLLECT_SET(analytics_tools) as all_analytics_tools,
        
        -- Content quality metrics
        AVG(content_richness_score) as avg_content_richness_score,
        AVG(readability_score) as avg_readability_score,
        AVG(technical_depth_score) as avg_technical_depth_score,
        AVG(business_info_score) as avg_business_info_score,
        
        -- Client information
        COLLECT_SET(clients_mentions) as all_clients_mentions,
        COLLECT_SET(testimonials) as all_testimonials,
        COLLECT_SET(case_studies) as all_case_studies
        
    FROM thera_silver.silver_web_extracts
    WHERE is_company_website = true
    GROUP BY company_key
),

-- Step 5: Combine all data sources
combined_data AS (
    SELECT 
        bc.*,
        
        -- Domain health
        COALESCE(dh.domain_health_score, 0.5) as domain_health_score,
        COALESCE(dh.domain_grade, 'C') as domain_grade,
        COALESCE(dh.risk_level, 'medium') as risk_level,
        COALESCE(dh.health_flags, ARRAY[]) as health_flags,
        COALESCE(dh.critical_issues, ARRAY[]) as critical_issues,
        
        -- Apollo aggregates
        COALESCE(aa.apollo_contact_count, 0) as apollo_contact_count,
        COALESCE(aa.apollo_has_verified_contact, false) as apollo_has_verified_contact,
        COALESCE(aa.apollo_contact_quality_score, 0.0) as apollo_contact_quality_score,
        
        -- Apollo seniority mix
        STRUCT(
            COALESCE(aa.c_level_count, 0) as c_level,
            COALESCE(aa.vp_director_count, 0) as vp_director,
            COALESCE(aa.manager_count, 0) as manager,
            COALESCE(aa.senior_count, 0) as senior,
            COALESCE(aa.mid_level_count, 0) as mid_level,
            COALESCE(aa.junior_count, 0) as junior
        ) as apollo_seniority_mix,
        
        -- Web content
        wc.web_title,
        wc.web_meta_description,
        wc.web_about_snippet,
        wc.all_content_categories,
        wc.all_topics,
        wc.all_keywords,
        wc.social_links,
        wc.all_tech_hints,
        wc.all_programming_languages,
        wc.all_frameworks,
        wc.all_databases,
        wc.all_cloud_providers,
        wc.all_cms_platforms,
        wc.all_analytics_tools,
        wc.avg_content_richness_score,
        wc.avg_readability_score,
        wc.avg_technical_depth_score,
        wc.avg_business_info_score,
        wc.all_clients_mentions,
        wc.all_testimonials,
        wc.all_case_studies
        
    FROM base_companies bc
    LEFT JOIN domain_health dh ON bc.domain = dh.domain
    LEFT JOIN apollo_aggregates aa ON bc.company_key = aa.company_key
    LEFT JOIN web_content wc ON bc.company_key = wc.company_key
),

-- Step 6: Calculate derived metrics and generate profile text
final_profiles AS (
    SELECT 
        *,
        
        -- Calculate social media score
        (
            CASE WHEN social_links.linkedin IS NOT NULL THEN 0.2 ELSE 0 END +
            CASE WHEN social_links.facebook IS NOT NULL THEN 0.15 ELSE 0 END +
            CASE WHEN social_links.twitter IS NOT NULL THEN 0.15 ELSE 0 END +
            CASE WHEN social_links.instagram IS NOT NULL THEN 0.1 ELSE 0 END +
            CASE WHEN social_links.youtube IS NOT NULL THEN 0.1 ELSE 0 END +
            CASE WHEN social_links.github IS NOT NULL THEN 0.2 ELSE 0 END +
            CASE WHEN social_links.tiktok IS NOT NULL THEN 0.05 ELSE 0 END +
            CASE WHEN social_links.discord IS NOT NULL THEN 0.05 ELSE 0 END +
            CASE WHEN social_links.slack IS NOT NULL THEN 0.05 ELSE 0 END
        ) as social_media_score,
        
        -- Calculate technology score
        (
            CASE WHEN SIZE(all_tech_hints) > 0 THEN 0.3 ELSE 0 END +
            CASE WHEN SIZE(all_programming_languages) > 0 THEN 0.2 ELSE 0 END +
            CASE WHEN SIZE(all_frameworks) > 0 THEN 0.2 ELSE 0 END +
            CASE WHEN SIZE(all_databases) > 0 THEN 0.1 ELSE 0 END +
            CASE WHEN SIZE(all_cloud_providers) > 0 THEN 0.1 ELSE 0 END +
            CASE WHEN SIZE(all_analytics_tools) > 0 THEN 0.1 ELSE 0 END
        ) as technology_score,
        
        -- Calculate content quality grade
        CASE 
            WHEN avg_content_richness_score >= 0.8 THEN 'A'
            WHEN avg_content_richness_score >= 0.6 THEN 'B'
            WHEN avg_content_richness_score >= 0.4 THEN 'C'
            WHEN avg_content_richness_score >= 0.2 THEN 'D'
            ELSE 'F'
        END as content_quality_grade,
        
        -- Calculate business credibility score
        (
            CASE WHEN website_url IS NOT NULL THEN 0.2 ELSE 0 END +
            CASE WHEN linkedin_url IS NOT NULL THEN 0.2 ELSE 0 END +
            CASE WHEN description IS NOT NULL AND LENGTH(description) > 100 THEN 0.2 ELSE 0 END +
            CASE WHEN founded_year IS NOT NULL THEN 0.1 ELSE 0 END +
            CASE WHEN employee_count IS NOT NULL THEN 0.1 ELSE 0 END +
            CASE WHEN total_funding IS NOT NULL AND total_funding > 0 THEN 0.1 ELSE 0 END +
            CASE WHEN SIZE(all_testimonials) > 0 THEN 0.1 ELSE 0 END
        ) as business_credibility_score,
        
        -- Calculate client credibility score
        (
            CASE WHEN SIZE(all_clients_mentions) > 0 THEN 0.4 ELSE 0 END +
            CASE WHEN SIZE(all_testimonials) > 0 THEN 0.3 ELSE 0 END +
            CASE WHEN SIZE(all_case_studies) > 0 THEN 0.3 ELSE 0 END
        ) as client_credibility_score,
        
        -- Calculate overall data quality score
        (
            data_quality_score * 0.4 +
            COALESCE(domain_health_score, 0.5) * 0.2 +
            COALESCE(apollo_contact_quality_score, 0.5) * 0.2 +
            COALESCE(avg_content_richness_score, 0.5) * 0.2
        ) as overall_data_quality_score,
        
        -- Calculate data completeness score
        (
            CASE WHEN company_name IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN domain IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN country IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN industry IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN description IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN founded_year IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN employee_count IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN linkedin_url IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN website_url IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN web_title IS NOT NULL THEN 1 ELSE 0 END
        ) / 10.0 as data_completeness_score,
        
        -- Calculate confidence level
        CASE 
            WHEN overall_data_quality_score >= 0.8 THEN 'high'
            WHEN overall_data_quality_score >= 0.6 THEN 'medium'
            ELSE 'low'
        END as confidence_level,
        
        -- Determine startup stage
        CASE 
            WHEN founded_year IS NULL OR founded_year > YEAR(CURRENT_DATE) - 1 THEN 'idea'
            WHEN employee_count IS NULL OR employee_count <= 10 THEN 'mvp'
            WHEN employee_count <= 50 THEN 'growth'
            WHEN employee_count <= 200 THEN 'scale'
            ELSE 'mature'
        END as startup_stage,
        
        -- Calculate investment readiness score
        (
            CASE WHEN total_funding IS NOT NULL AND total_funding > 0 THEN 0.2 ELSE 0 END +
            CASE WHEN apollo_contact_count > 0 THEN 0.2 ELSE 0 END +
            CASE WHEN domain_health_score >= 0.7 THEN 0.2 ELSE 0 END +
            CASE WHEN business_credibility_score >= 0.7 THEN 0.2 ELSE 0 END +
            CASE WHEN client_credibility_score >= 0.5 THEN 0.2 ELSE 0 END
        ) as investment_readiness_score,
        
        -- Calculate market opportunity score
        (
            CASE WHEN industry IS NOT NULL THEN 0.3 ELSE 0 END +
            CASE WHEN SIZE(all_content_categories) > 0 THEN 0.2 ELSE 0 END +
            CASE WHEN SIZE(all_topics) > 0 THEN 0.2 ELSE 0 END +
            CASE WHEN social_media_score >= 0.5 THEN 0.1 ELSE 0 END +
            CASE WHEN technology_score >= 0.5 THEN 0.1 ELSE 0 END +
            CASE WHEN apollo_contact_count > 5 THEN 0.1 ELSE 0 END
        ) as market_opportunity_score,
        
        -- Generate profile tags
        ARRAY_CONCAT(
            CASE WHEN industry IS NOT NULL THEN ARRAY[industry] ELSE ARRAY[] END,
            CASE WHEN sub_industry IS NOT NULL THEN ARRAY[sub_industry] ELSE ARRAY[] END,
            CASE WHEN size_bracket IS NOT NULL THEN ARRAY[size_bracket] ELSE ARRAY[] END,
            CASE WHEN startup_stage IS NOT NULL THEN ARRAY[startup_stage] ELSE ARRAY[] END,
            CASE WHEN funding_stage IS NOT NULL THEN ARRAY[funding_stage] ELSE ARRAY[] END,
            CASE WHEN country IS NOT NULL THEN ARRAY[country] ELSE ARRAY[] END
        ) as profile_tags,
        
        -- Generate comprehensive profile text
        CONCAT(
            'COMPANY PROFILE\n',
            '===============\n\n',
            'Company: ', COALESCE(company_name, 'Unknown'), '\n',
            'Domain: ', COALESCE(domain, 'Unknown'), '\n',
            'Industry: ', COALESCE(industry, 'Unknown'), '\n',
            'Location: ', COALESCE(city, ''), ', ', COALESCE(country, 'Unknown'), '\n',
            'Founded: ', COALESCE(CAST(founded_year AS STRING), 'Unknown'), '\n',
            'Size: ', COALESCE(employee_range, 'Unknown'), ' (', COALESCE(CAST(employee_count AS STRING), 'Unknown'), ' employees)\n',
            'Stage: ', COALESCE(startup_stage, 'Unknown'), '\n\n',
            
            'DESCRIPTION\n',
            '===========\n',
            COALESCE(description, 'No description available'), '\n\n',
            
            'WEBSITE CONTENT\n',
            '===============\n',
            'Title: ', COALESCE(web_title, 'No title available'), '\n',
            'Meta Description: ', COALESCE(web_meta_description, 'No meta description available'), '\n',
            'About: ', COALESCE(web_about_snippet, 'No about information available'), '\n\n',
            
            'TECHNOLOGY STACK\n',
            '================\n',
            'Technologies: ', COALESCE(ARRAY_JOIN(all_tech_hints, ', '), 'Unknown'), '\n',
            'Programming Languages: ', COALESCE(ARRAY_JOIN(all_programming_languages, ', '), 'Unknown'), '\n',
            'Frameworks: ', COALESCE(ARRAY_JOIN(all_frameworks, ', '), 'Unknown'), '\n',
            'Cloud Providers: ', COALESCE(ARRAY_JOIN(all_cloud_providers, ', '), 'Unknown'), '\n\n',
            
            'SOCIAL MEDIA\n',
            '============\n',
            'LinkedIn: ', COALESCE(social_links.linkedin, 'Not available'), '\n',
            'Twitter: ', COALESCE(social_links.twitter, 'Not available'), '\n',
            'GitHub: ', COALESCE(social_links.github, 'Not available'), '\n\n',
            
            'FUNDING INFORMATION\n',
            '===================\n',
            'Total Funding: $', COALESCE(CAST(total_funding AS STRING), 'Unknown'), '\n',
            'Last Funding: ', COALESCE(CAST(last_funding_date AS STRING), 'Unknown'), '\n',
            'Funding Stage: ', COALESCE(funding_stage, 'Unknown'), '\n\n',
            
            'DOMAIN HEALTH\n',
            '=============\n',
            'Health Score: ', COALESCE(CAST(domain_health_score AS STRING), 'Unknown'), '\n',
            'Grade: ', COALESCE(domain_grade, 'Unknown'), '\n',
            'Risk Level: ', COALESCE(risk_level, 'Unknown'), '\n\n',
            
            'CONTACT INFORMATION\n',
            '===================\n',
            'Apollo Contacts: ', COALESCE(CAST(apollo_contact_count AS STRING), '0'), '\n',
            'Verified Contacts: ', COALESCE(CAST(apollo_has_verified_contact AS STRING), 'false'), '\n',
            'Contact Quality: ', COALESCE(CAST(apollo_contact_quality_score AS STRING), 'Unknown'), '\n\n',
            
            'QUALITY METRICS\n',
            '===============\n',
            'Data Quality: ', COALESCE(CAST(overall_data_quality_score AS STRING), 'Unknown'), '\n',
            'Content Richness: ', COALESCE(CAST(avg_content_richness_score AS STRING), 'Unknown'), '\n',
            'Business Credibility: ', COALESCE(CAST(business_credibility_score AS STRING), 'Unknown'), '\n',
            'Confidence Level: ', COALESCE(confidence_level, 'Unknown'), '\n'
        ) as profile_text,
        
        -- Generate profile summary
        CONCAT(
            company_name, ' is a ', COALESCE(industry, 'technology'), ' company ',
            CASE WHEN founded_year IS NOT NULL THEN 'founded in ' || CAST(founded_year AS STRING) ELSE '' END,
            CASE WHEN country IS NOT NULL THEN ' based in ' || country ELSE '' END,
            CASE WHEN employee_count IS NOT NULL THEN ' with ' || CAST(employee_count AS STRING) || ' employees' ELSE '' END,
            CASE WHEN total_funding IS NOT NULL AND total_funding > 0 THEN ' and has raised $' || CAST(total_funding AS STRING) || ' in funding' ELSE '' END,
            '.'
        ) as profile_summary,
        
        -- Generate profile text hash
        MD5(CONCAT(company_key, domain, company_name, description, web_title)) as profile_text_hash,
        
        -- Metadata
        ARRAY['silver_companies', 'silver_domain_health', 'silver_apollo_contacts', 'silver_web_extracts'] as source_systems,
        'gold_aggregation' as last_updated_source,
        'comprehensive_aggregation' as profile_generation_method,
        CURRENT_TIMESTAMP as created_at,
        CURRENT_TIMESTAMP as updated_at,
        CURRENT_TIMESTAMP as last_verified_at,
        
        -- Partitioning
        DATE_FORMAT(CURRENT_DATE, 'yyyy-MM-dd') as dt
        
    FROM combined_data
)

SELECT 
    company_key,
    domain,
    company_name,
    legal_name,
    dba_name,
    linkedin_url,
    website_url,
    country,
    state_province,
    city,
    industry,
    sub_industry,
    industry_category,
    size_bracket,
    description,
    short_description,
    tagline,
    founded_year,
    founded_date,
    employee_count,
    employee_range,
    revenue_range,
    total_funding,
    last_funding_date,
    last_funding_type,
    last_funding_amount,
    funding_stage,
    valuation,
    domain_health_score,
    domain_grade,
    risk_level,
    health_flags,
    critical_issues,
    apollo_contact_count,
    apollo_has_verified_contact,
    apollo_seniority_mix,
    apollo_contact_quality_score,
    web_title,
    web_meta_description,
    web_about_snippet,
    all_content_categories as web_content_categories,
    all_topics as web_topics,
    all_keywords as web_keywords,
    social_links as social_media_presence,
    social_media_score,
    all_tech_hints as technology_stack,
    all_programming_languages as programming_languages,
    all_frameworks as frameworks,
    all_databases as databases,
    all_cloud_providers as cloud_providers,
    all_cms_platforms as cms_platforms,
    all_analytics_tools as analytics_tools,
    technology_score,
    avg_content_richness_score as content_richness_score,
    content_quality_grade,
    avg_readability_score as readability_score,
    avg_technical_depth_score as technical_depth_score,
    avg_business_info_score as business_info_score,
    business_credibility_score,
    false as has_contact_info, -- Placeholder
    false as has_physical_address, -- Placeholder
    false as has_team_page, -- Placeholder
    false as has_careers_page, -- Placeholder
    false as has_pricing_page, -- Placeholder
    false as has_privacy_policy, -- Placeholder
    false as has_terms_of_service, -- Placeholder
    all_clients_mentions as clients_mentions,
    all_testimonials as testimonials,
    all_case_studies as case_studies,
    client_credibility_score,
    overall_data_quality_score,
    data_completeness_score,
    0.8 as data_accuracy_score, -- Placeholder
    1.0 as data_freshness_score, -- Placeholder
    confidence_level,
    profile_text,
    profile_text_hash,
    profile_summary,
    profile_tags,
    startup_stage,
    ARRAY[] as growth_indicators, -- Placeholder
    ARRAY[] as innovation_indicators, -- Placeholder
    0.5 as market_presence_score, -- Placeholder
    0.5 as competitive_advantage_score, -- Placeholder
    investment_readiness_score,
    CASE 
        WHEN investment_readiness_score >= 0.8 THEN 'A'
        WHEN investment_readiness_score >= 0.6 THEN 'B'
        WHEN investment_readiness_score >= 0.4 THEN 'C'
        WHEN investment_readiness_score >= 0.2 THEN 'D'
        ELSE 'F'
    END as investment_readiness_grade,
    ARRAY[] as funding_readiness_indicators, -- Placeholder
    ARRAY[] as due_diligence_red_flags, -- Placeholder
    'medium' as market_size_estimate, -- Placeholder
    ARRAY[] as target_market, -- Placeholder
    ARRAY[] as competitive_landscape, -- Placeholder
    market_opportunity_score,
    source_systems,
    last_updated_source,
    profile_generation_method,
    created_at,
    updated_at,
    last_verified_at,
    dt
FROM final_profiles;

-- =============================================================================
-- NOTES FOR CONFIGURATION
-- =============================================================================
-- 1. Adjust profile text generation based on specific business requirements
-- 2. Modify scoring algorithms based on industry standards
-- 3. Add additional startup-specific insights as needed
-- 4. Consider adding real-time market data integration
-- 5. Add competitive analysis features
-- 6. Consider adding investment recommendation scoring
-- 7. Add data lineage tracking for audit purposes
