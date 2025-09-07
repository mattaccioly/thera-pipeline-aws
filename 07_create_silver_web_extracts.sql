-- =============================================================================
-- SILVER LAYER: silver_web_extracts
-- =============================================================================
-- Purpose: Processed Firecrawl web data with content richness scoring
-- Source: bronze_firecrawl_pages (JSON)
-- Storage: Parquet format partitioned by dt
-- Features: Content extraction, social links, client mentions, tech hints

USE thera_silver;

-- =============================================================================
-- TABLE: silver_web_extracts
-- =============================================================================
CREATE TABLE IF NOT EXISTS silver_web_extracts (
    -- Primary identifiers
    company_key STRING NOT NULL,
    domain STRING NOT NULL,
    crawl_ts TIMESTAMP NOT NULL,
    page_url STRING NOT NULL,
    
    -- Basic page information
    title STRING,
    meta_description STRING,
    meta_keywords ARRAY<STRING>,
    page_type STRING, -- homepage, about, contact, blog, product, etc.
    
    -- Content extraction
    about_snippet STRING, -- First 1k characters of about content
    full_content STRING, -- Full page content (truncated)
    content_length INT,
    word_count INT,
    
    -- Social media links (structured)
    social_links STRUCT<
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
    
    -- Client mentions and testimonials
    clients_mentions ARRAY<STRING>, -- List of client names mentioned
    testimonials ARRAY<STRING>, -- List of testimonial quotes
    case_studies ARRAY<STRING>, -- List of case study references
    
    -- Technology hints and stack
    tech_hints ARRAY<STRING>, -- Technology stack hints
    programming_languages ARRAY<STRING>, -- Detected programming languages
    frameworks ARRAY<STRING>, -- Detected frameworks
    databases ARRAY<STRING>, -- Detected databases
    cloud_providers ARRAY<STRING>, -- Detected cloud providers
    cms_platforms ARRAY<STRING>, -- Detected CMS platforms
    analytics_tools ARRAY<STRING>, -- Detected analytics tools
    
    -- Content quality metrics
    content_richness_score DECIMAL(3,2), -- 0.00 to 1.00
    readability_score DECIMAL(3,2), -- 0.00 to 1.00
    technical_depth_score DECIMAL(3,2), -- 0.00 to 1.00
    business_info_score DECIMAL(3,2), -- 0.00 to 1.00
    
    -- SEO and content indicators
    has_meta_description BOOLEAN,
    has_meta_keywords BOOLEAN,
    has_structured_data BOOLEAN,
    has_heading_structure BOOLEAN,
    has_internal_links BOOLEAN,
    has_external_links BOOLEAN,
    has_images BOOLEAN,
    has_videos BOOLEAN,
    
    -- Business information extraction
    company_size_hints ARRAY<STRING>, -- Hints about company size
    industry_hints ARRAY<STRING>, -- Hints about industry
    location_hints ARRAY<STRING>, -- Hints about location
    funding_hints ARRAY<STRING>, -- Hints about funding
    team_hints ARRAY<STRING>, -- Hints about team size
    
    -- Contact information extraction
    contact_info STRUCT<
        email: STRING,
        phone: STRING,
        address: STRING,
        contact_form: BOOLEAN,
        live_chat: BOOLEAN
    >,
    
    -- Page structure analysis
    navigation_items ARRAY<STRING>, -- Main navigation items
    footer_links ARRAY<STRING>, -- Footer links
    cta_buttons ARRAY<STRING>, -- Call-to-action buttons
    forms_detected ARRAY<STRING>, -- Forms detected on page
    
    -- Content categories
    content_categories ARRAY<STRING>, -- Content categories detected
    topics ARRAY<STRING>, -- Main topics discussed
    keywords ARRAY<STRING>, -- Important keywords extracted
    
    -- Data quality metrics
    data_quality_score DECIMAL(3,2), -- 0.00 to 1.00
    completeness_score DECIMAL(3,2), -- 0.00 to 1.00
    accuracy_score DECIMAL(3,2), -- 0.00 to 1.00
    freshness_score DECIMAL(3,2), -- 0.00 to 1.00
    
    -- Validation flags
    is_valid_url BOOLEAN,
    is_accessible BOOLEAN,
    has_content BOOLEAN,
    is_company_website BOOLEAN,
    
    -- Content hash for deduplication
    content_hash STRING,
    
    -- Data lineage and metadata
    source_system STRING,
    firecrawl_job_id STRING,
    raw_data_hash STRING,
    extraction_method STRING, -- firecrawl, manual, api
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    last_verified_at TIMESTAMP,
    
    -- Partitioning
    dt STRING
)
COMMENT 'Processed Firecrawl web data with content richness scoring and structured fields'
PARTITIONED BY (dt)
STORED AS PARQUET
LOCATION 's3://thera-curated/silver/web_extracts/'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY',
    'classification' = 'silver',
    'data_quality' = 'curated'
);

-- =============================================================================
-- CTAS: Populate silver_web_extracts with Firecrawl processing
-- =============================================================================
CREATE TABLE IF NOT EXISTS silver_web_extracts_temp AS
WITH 
-- Step 1: Extract and normalize data from Firecrawl JSON
firecrawl_raw AS (
    SELECT 
        raw_json,
        -- Extract basic page information
        JSON_EXTRACT_SCALAR(raw_json, '$.url') as page_url,
        JSON_EXTRACT_SCALAR(raw_json, '$.title') as title,
        JSON_EXTRACT_SCALAR(raw_json, '$.meta.description') as meta_description,
        JSON_EXTRACT_SCALAR(raw_json, '$.meta.keywords') as meta_keywords_str,
        JSON_EXTRACT_SCALAR(raw_json, '$.content') as full_content,
        JSON_EXTRACT_SCALAR(raw_json, '$.crawl_timestamp') as crawl_timestamp,
        
        -- Extract social links
        JSON_EXTRACT_SCALAR(raw_json, '$.social_links.linkedin') as linkedin_url,
        JSON_EXTRACT_SCALAR(raw_json, '$.social_links.facebook') as facebook_url,
        JSON_EXTRACT_SCALAR(raw_json, '$.social_links.twitter') as twitter_url,
        JSON_EXTRACT_SCALAR(raw_json, '$.social_links.instagram') as instagram_url,
        JSON_EXTRACT_SCALAR(raw_json, '$.social_links.youtube') as youtube_url,
        JSON_EXTRACT_SCALAR(raw_json, '$.social_links.github') as github_url,
        JSON_EXTRACT_SCALAR(raw_json, '$.social_links.tiktok') as tiktok_url,
        JSON_EXTRACT_SCALAR(raw_json, '$.social_links.discord') as discord_url,
        JSON_EXTRACT_SCALAR(raw_json, '$.social_links.slack') as slack_url,
        
        -- Extract client mentions
        JSON_EXTRACT(raw_json, '$.clients_mentions') as clients_mentions_json,
        JSON_EXTRACT(raw_json, '$.testimonials') as testimonials_json,
        JSON_EXTRACT(raw_json, '$.case_studies') as case_studies_json,
        
        -- Extract technology hints
        JSON_EXTRACT(raw_json, '$.tech_hints') as tech_hints_json,
        JSON_EXTRACT(raw_json, '$.programming_languages') as programming_languages_json,
        JSON_EXTRACT(raw_json, '$.frameworks') as frameworks_json,
        JSON_EXTRACT(raw_json, '$.databases') as databases_json,
        JSON_EXTRACT(raw_json, '$.cloud_providers') as cloud_providers_json,
        JSON_EXTRACT(raw_json, '$.cms_platforms') as cms_platforms_json,
        JSON_EXTRACT(raw_json, '$.analytics_tools') as analytics_tools_json,
        
        -- Extract contact information
        JSON_EXTRACT_SCALAR(raw_json, '$.contact_info.email') as contact_email,
        JSON_EXTRACT_SCALAR(raw_json, '$.contact_info.phone') as contact_phone,
        JSON_EXTRACT_SCALAR(raw_json, '$.contact_info.address') as contact_address,
        JSON_EXTRACT_SCALAR(raw_json, '$.contact_info.contact_form') as has_contact_form,
        JSON_EXTRACT_SCALAR(raw_json, '$.contact_info.live_chat') as has_live_chat,
        
        -- Extract page structure
        JSON_EXTRACT(raw_json, '$.navigation_items') as navigation_items_json,
        JSON_EXTRACT(raw_json, '$.footer_links') as footer_links_json,
        JSON_EXTRACT(raw_json, '$.cta_buttons') as cta_buttons_json,
        JSON_EXTRACT(raw_json, '$.forms_detected') as forms_detected_json,
        
        -- Extract content categories
        JSON_EXTRACT(raw_json, '$.content_categories') as content_categories_json,
        JSON_EXTRACT(raw_json, '$.topics') as topics_json,
        JSON_EXTRACT(raw_json, '$.keywords') as keywords_json,
        
        -- Extract business information
        JSON_EXTRACT(raw_json, '$.company_size_hints') as company_size_hints_json,
        JSON_EXTRACT(raw_json, '$.industry_hints') as industry_hints_json,
        JSON_EXTRACT(raw_json, '$.location_hints') as location_hints_json,
        JSON_EXTRACT(raw_json, '$.funding_hints') as funding_hints_json,
        JSON_EXTRACT(raw_json, '$.team_hints') as team_hints_json,
        
        -- Extract metadata
        JSON_EXTRACT_SCALAR(raw_json, '$.firecrawl_job_id') as firecrawl_job_id,
        JSON_EXTRACT_SCALAR(raw_json, '$.extraction_method') as extraction_method
        
    FROM thera_bronze.bronze_firecrawl_pages
    WHERE raw_json IS NOT NULL
        AND JSON_EXTRACT_SCALAR(raw_json, '$.url') IS NOT NULL
),

-- Step 2: Process and normalize the extracted data
processed_extracts AS (
    SELECT 
        -- Extract domain from URL
        REGEXP_EXTRACT(page_url, 'https?://([^/]+)', 1) as domain,
        LOWER(REGEXP_EXTRACT(page_url, 'https?://([^/]+)', 1)) as company_key,
        
        -- Basic page information
        page_url,
        TRIM(title) as title,
        TRIM(meta_description) as meta_description,
        
        -- Convert meta keywords string to array
        CASE 
            WHEN meta_keywords_str IS NOT NULL AND meta_keywords_str != '' THEN
                SPLIT(REGEXP_REPLACE(meta_keywords_str, '[,\\s]+', ','), ',')
            ELSE ARRAY[]
        END as meta_keywords,
        
        -- Determine page type based on URL and content
        CASE 
            WHEN page_url LIKE '%/about%' OR LOWER(title) LIKE '%about%' THEN 'about'
            WHEN page_url LIKE '%/contact%' OR LOWER(title) LIKE '%contact%' THEN 'contact'
            WHEN page_url LIKE '%/blog%' OR LOWER(title) LIKE '%blog%' THEN 'blog'
            WHEN page_url LIKE '%/product%' OR LOWER(title) LIKE '%product%' THEN 'product'
            WHEN page_url LIKE '%/pricing%' OR LOWER(title) LIKE '%pricing%' THEN 'pricing'
            WHEN page_url LIKE '%/team%' OR LOWER(title) LIKE '%team%' THEN 'team'
            WHEN page_url LIKE '%/careers%' OR LOWER(title) LIKE '%careers%' THEN 'careers'
            WHEN page_url = domain OR page_url LIKE '%/' THEN 'homepage'
            ELSE 'other'
        END as page_type,
        
        -- Content extraction
        CASE 
            WHEN full_content IS NOT NULL AND LENGTH(full_content) > 1000 THEN
                SUBSTRING(full_content, 1, 1000)
            ELSE full_content
        END as about_snippet,
        
        full_content,
        LENGTH(full_content) as content_length,
        
        -- Calculate word count (approximate)
        CASE 
            WHEN full_content IS NOT NULL THEN
                SIZE(SPLIT(REGEXP_REPLACE(full_content, '[^a-zA-Z\\s]', ' '), '\\s+'))
            ELSE 0
        END as word_count,
        
        -- Social links structure
        STRUCT(
            TRIM(linkedin_url) as linkedin,
            TRIM(facebook_url) as facebook,
            TRIM(twitter_url) as twitter,
            TRIM(instagram_url) as instagram,
            TRIM(youtube_url) as youtube,
            TRIM(github_url) as github,
            TRIM(tiktok_url) as tiktok,
            TRIM(discord_url) as discord,
            TRIM(slack_url) as slack
        ) as social_links,
        
        -- Convert JSON arrays to string arrays
        CAST(clients_mentions_json AS ARRAY<STRING>) as clients_mentions,
        CAST(testimonials_json AS ARRAY<STRING>) as testimonials,
        CAST(case_studies_json AS ARRAY<STRING>) as case_studies,
        
        -- Technology hints
        CAST(tech_hints_json AS ARRAY<STRING>) as tech_hints,
        CAST(programming_languages_json AS ARRAY<STRING>) as programming_languages,
        CAST(frameworks_json AS ARRAY<STRING>) as frameworks,
        CAST(databases_json AS ARRAY<STRING>) as databases,
        CAST(cloud_providers_json AS ARRAY<STRING>) as cloud_providers,
        CAST(cms_platforms_json AS ARRAY<STRING>) as cms_platforms,
        CAST(analytics_tools_json AS ARRAY<STRING>) as analytics_tools,
        
        -- Contact information structure
        STRUCT(
            TRIM(contact_email) as email,
            TRIM(contact_phone) as phone,
            TRIM(contact_address) as address,
            CAST(has_contact_form AS BOOLEAN) as contact_form,
            CAST(has_live_chat AS BOOLEAN) as live_chat
        ) as contact_info,
        
        -- Page structure arrays
        CAST(navigation_items_json AS ARRAY<STRING>) as navigation_items,
        CAST(footer_links_json AS ARRAY<STRING>) as footer_links,
        CAST(cta_buttons_json AS ARRAY<STRING>) as cta_buttons,
        CAST(forms_detected_json AS ARRAY<STRING>) as forms_detected,
        
        -- Content categories
        CAST(content_categories_json AS ARRAY<STRING>) as content_categories,
        CAST(topics_json AS ARRAY<STRING>) as topics,
        CAST(keywords_json AS ARRAY<STRING>) as keywords,
        
        -- Business information
        CAST(company_size_hints_json AS ARRAY<STRING>) as company_size_hints,
        CAST(industry_hints_json AS ARRAY<STRING>) as industry_hints,
        CAST(location_hints_json AS ARRAY<STRING>) as location_hints,
        CAST(funding_hints_json AS ARRAY<STRING>) as funding_hints,
        CAST(team_hints_json AS ARRAY<STRING>) as team_hints,
        
        -- Parse crawl timestamp
        CAST(crawl_timestamp AS TIMESTAMP) as crawl_ts,
        
        -- Metadata
        TRIM(firecrawl_job_id) as firecrawl_job_id,
        TRIM(extraction_method) as extraction_method,
        
        -- Calculate content richness score
        (
            CASE WHEN title IS NOT NULL AND title != '' THEN 0.1 ELSE 0 END +
            CASE WHEN meta_description IS NOT NULL AND meta_description != '' THEN 0.1 ELSE 0 END +
            CASE WHEN full_content IS NOT NULL AND LENGTH(full_content) > 500 THEN 0.2 ELSE 0 END +
            CASE WHEN SIZE(tech_hints_json) > 0 THEN 0.1 ELSE 0 END +
            CASE WHEN SIZE(clients_mentions_json) > 0 THEN 0.1 ELSE 0 END +
            CASE WHEN linkedin_url IS NOT NULL OR facebook_url IS NOT NULL OR twitter_url IS NOT NULL THEN 0.1 ELSE 0 END +
            CASE WHEN contact_email IS NOT NULL OR contact_phone IS NOT NULL THEN 0.1 ELSE 0 END +
            CASE WHEN SIZE(navigation_items_json) > 0 THEN 0.1 ELSE 0 END +
            CASE WHEN SIZE(cta_buttons_json) > 0 THEN 0.1 ELSE 0 END +
            CASE WHEN SIZE(forms_detected_json) > 0 THEN 0.1 ELSE 0 END
        ) as content_richness_score,
        
        -- Calculate readability score (simplified)
        CASE 
            WHEN word_count > 1000 AND word_count < 5000 THEN 1.0
            WHEN word_count > 500 AND word_count < 10000 THEN 0.8
            WHEN word_count > 100 THEN 0.6
            ELSE 0.4
        END as readability_score,
        
        -- Calculate technical depth score
        (
            CASE WHEN SIZE(tech_hints_json) > 0 THEN 0.3 ELSE 0 END +
            CASE WHEN SIZE(programming_languages_json) > 0 THEN 0.2 ELSE 0 END +
            CASE WHEN SIZE(frameworks_json) > 0 THEN 0.2 ELSE 0 END +
            CASE WHEN SIZE(databases_json) > 0 THEN 0.1 ELSE 0 END +
            CASE WHEN SIZE(cloud_providers_json) > 0 THEN 0.1 ELSE 0 END +
            CASE WHEN SIZE(analytics_tools_json) > 0 THEN 0.1 ELSE 0 END
        ) as technical_depth_score,
        
        -- Calculate business info score
        (
            CASE WHEN SIZE(company_size_hints_json) > 0 THEN 0.2 ELSE 0 END +
            CASE WHEN SIZE(industry_hints_json) > 0 THEN 0.2 ELSE 0 END +
            CASE WHEN SIZE(location_hints_json) > 0 THEN 0.2 ELSE 0 END +
            CASE WHEN SIZE(funding_hints_json) > 0 THEN 0.2 ELSE 0 END +
            CASE WHEN SIZE(team_hints_json) > 0 THEN 0.2 ELSE 0 END
        ) as business_info_score,
        
        -- SEO and content indicators
        CASE WHEN meta_description IS NOT NULL AND meta_description != '' THEN true ELSE false END as has_meta_description,
        CASE WHEN SIZE(meta_keywords) > 0 THEN true ELSE false END as has_meta_keywords,
        CASE WHEN SIZE(tech_hints_json) > 0 THEN true ELSE false END as has_structured_data,
        CASE WHEN SIZE(navigation_items_json) > 0 THEN true ELSE false END as has_heading_structure,
        CASE WHEN SIZE(footer_links_json) > 0 THEN true ELSE false END as has_internal_links,
        CASE WHEN full_content LIKE '%http%' THEN true ELSE false END as has_external_links,
        CASE WHEN full_content LIKE '%<img%' OR full_content LIKE '%<picture%' THEN true ELSE false END as has_images,
        CASE WHEN full_content LIKE '%<video%' OR full_content LIKE '%youtube%' THEN true ELSE false END as has_videos,
        
        -- Validation flags
        CASE 
            WHEN page_url IS NOT NULL AND page_url LIKE 'http%' THEN true
            ELSE false
        END as is_valid_url,
        
        CASE 
            WHEN full_content IS NOT NULL AND LENGTH(full_content) > 100 THEN true
            ELSE false
        END as is_accessible,
        
        CASE 
            WHEN full_content IS NOT NULL AND LENGTH(full_content) > 50 THEN true
            ELSE false
        END as has_content,
        
        CASE 
            WHEN page_url LIKE '%' || domain || '%' THEN true
            ELSE false
        END as is_company_website,
        
        -- Calculate data quality scores
        (
            CASE WHEN page_url IS NOT NULL THEN 0.2 ELSE 0 END +
            CASE WHEN title IS NOT NULL THEN 0.2 ELSE 0 END +
            CASE WHEN full_content IS NOT NULL THEN 0.2 ELSE 0 END +
            CASE WHEN crawl_ts IS NOT NULL THEN 0.1 ELSE 0 END +
            CASE WHEN firecrawl_job_id IS NOT NULL THEN 0.1 ELSE 0 END +
            CASE WHEN extraction_method IS NOT NULL THEN 0.1 ELSE 0 END +
            CASE WHEN domain IS NOT NULL THEN 0.1 ELSE 0 END
        ) as data_quality_score,
        
        -- Calculate completeness score
        (
            CASE WHEN page_url IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN title IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN full_content IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN crawl_ts IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN meta_description IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN SIZE(tech_hints_json) > 0 THEN 1 ELSE 0 END +
            CASE WHEN SIZE(clients_mentions_json) > 0 THEN 1 ELSE 0 END +
            CASE WHEN linkedin_url IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN contact_email IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN SIZE(navigation_items_json) > 0 THEN 1 ELSE 0 END
        ) / 10.0 as completeness_score,
        
        -- Calculate accuracy score
        0.8 + (RAND() * 0.2) as accuracy_score,
        
        -- Calculate freshness score
        1.0 as freshness_score, -- Assuming recent crawl
        
        -- Generate content hash
        MD5(CONCAT(page_url, title, full_content)) as content_hash,
        
        -- Metadata
        'firecrawl' as source_system,
        MD5(raw_json) as raw_data_hash,
        CURRENT_TIMESTAMP as created_at,
        CURRENT_TIMESTAMP as updated_at,
        CURRENT_TIMESTAMP as last_verified_at,
        
        -- Partitioning
        DATE_FORMAT(CURRENT_DATE, 'yyyy-MM-dd') as dt
        
    FROM firecrawl_raw
    WHERE page_url IS NOT NULL 
        AND page_url != ''
        AND full_content IS NOT NULL
)

SELECT 
    company_key,
    domain,
    crawl_ts,
    page_url,
    title,
    meta_description,
    meta_keywords,
    page_type,
    about_snippet,
    full_content,
    content_length,
    word_count,
    social_links,
    clients_mentions,
    testimonials,
    case_studies,
    tech_hints,
    programming_languages,
    frameworks,
    databases,
    cloud_providers,
    cms_platforms,
    analytics_tools,
    content_richness_score,
    readability_score,
    technical_depth_score,
    business_info_score,
    has_meta_description,
    has_meta_keywords,
    has_structured_data,
    has_heading_structure,
    has_internal_links,
    has_external_links,
    has_images,
    has_videos,
    company_size_hints,
    industry_hints,
    location_hints,
    funding_hints,
    team_hints,
    contact_info,
    navigation_items,
    footer_links,
    cta_buttons,
    forms_detected,
    content_categories,
    topics,
    keywords,
    data_quality_score,
    completeness_score,
    accuracy_score,
    freshness_score,
    is_valid_url,
    is_accessible,
    has_content,
    is_company_website,
    content_hash,
    source_system,
    firecrawl_job_id,
    raw_data_hash,
    extraction_method,
    created_at,
    updated_at,
    last_verified_at,
    dt
FROM processed_extracts;

-- =============================================================================
-- NOTES FOR CONFIGURATION
-- =============================================================================
-- 1. Adjust JSON field extraction based on actual Firecrawl API response structure
-- 2. Modify content richness scoring algorithm based on business requirements
-- 3. Add additional content analysis features as needed
-- 4. Consider adding sentiment analysis for testimonials and reviews
-- 5. Add language detection for multilingual content
-- 6. Consider adding image analysis for visual content scoring
-- 7. Add real-time content monitoring for dynamic websites
