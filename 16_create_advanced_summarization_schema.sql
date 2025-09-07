-- =============================================================================
-- ADVANCED SUMMARIZATION SCHEMA
-- =============================================================================
-- Purpose: Enhanced database schema for advanced LLM-powered summarization
-- Features: Executive summaries, key insights, business intelligence, competitive analysis
-- Cost Optimization: Structured for efficient batch processing and caching

USE thera_gold;

-- =============================================================================
-- TABLE: gold_advanced_summaries
-- =============================================================================
CREATE TABLE IF NOT EXISTS gold_advanced_summaries (
    -- Primary identifiers
    company_key STRING NOT NULL,
    domain STRING NOT NULL,
    company_name STRING NOT NULL,
    
    -- Executive Summary (2-3 sentences)
    executive_summary STRING,
    executive_summary_hash STRING,
    executive_summary_confidence DECIMAL(3,2),
    
    -- Key Business Insights
    key_insights ARRAY<STRING>, -- Top 5-7 business insights
    key_insights_hash STRING,
    business_model STRING, -- B2B, B2C, B2B2C, Marketplace, SaaS, etc.
    value_proposition STRING,
    target_customers ARRAY<STRING>,
    revenue_model STRING,
    
    -- Technology Stack Analysis
    technology_stack_analysis STRUCT<
        primary_technologies: ARRAY<STRING>,
        programming_languages: ARRAY<STRING>,
        frameworks: ARRAY<STRING>,
        databases: ARRAY<STRING>,
        cloud_providers: ARRAY<STRING>,
        dev_tools: ARRAY<STRING>,
        tech_maturity_score: DECIMAL(3,2),
        innovation_level: STRING -- cutting-edge, modern, standard, legacy
    >,
    tech_stack_hash STRING,
    
    -- Competitive Analysis
    competitive_analysis STRUCT<
        market_position: STRING, -- leader, challenger, follower, niche
        competitive_advantages: ARRAY<STRING>,
        differentiators: ARRAY<STRING>,
        market_share_estimate: STRING, -- small, medium, large, dominant
        competitive_threats: ARRAY<STRING>,
        moat_strength: STRING -- weak, moderate, strong, very-strong
    >,
    competitive_analysis_hash STRING,
    
    -- Risk Assessment
    risk_assessment STRUCT<
        business_risks: ARRAY<STRING>,
        technical_risks: ARRAY<STRING>,
        market_risks: ARRAY<STRING>,
        financial_risks: ARRAY<STRING>,
        regulatory_risks: ARRAY<STRING>,
        overall_risk_level: STRING, -- low, medium, high, critical
        risk_score: DECIMAL(3,2),
        red_flags: ARRAY<STRING>
    >,
    risk_assessment_hash STRING,
    
    -- Business Intelligence
    business_intelligence STRUCT<
        market_opportunity: STRING, -- small, medium, large, massive
        growth_potential: STRING, -- low, moderate, high, exponential
        scalability_indicators: ARRAY<STRING>,
        market_timing: STRING, -- early, optimal, late, saturated
        industry_trends: ARRAY<STRING>,
        market_dynamics: ARRAY<STRING>
    >,
    business_intelligence_hash STRING,
    
    -- Investment Analysis
    investment_analysis STRUCT<
        investment_readiness: STRING, -- not-ready, early-stage, ready, overvalued
        funding_recommendation: STRING, -- avoid, watch, consider, strong-buy
        valuation_indicators: ARRAY<STRING>,
        due_diligence_notes: ARRAY<STRING>,
        exit_potential: STRING, -- low, moderate, high, very-high
        investment_timeline: STRING -- immediate, 6-months, 1-year, 2-years
    >,
    investment_analysis_hash STRING,
    
    -- Market Analysis
    market_analysis STRUCT<
        target_market_size: STRING, -- small, medium, large, enterprise
        market_segments: ARRAY<STRING>,
        geographic_focus: ARRAY<STRING>,
        customer_personas: ARRAY<STRING>,
        market_barriers: ARRAY<STRING>,
        market_opportunities: ARRAY<STRING>
    >,
    market_analysis_hash STRING,
    
    -- Growth Indicators
    growth_indicators ARRAY<STRING>,
    innovation_indicators ARRAY<STRING>,
    traction_indicators ARRAY<STRING>,
    scalability_indicators ARRAY<STRING>,
    
    -- Quality and Confidence Metrics
    summary_quality_score DECIMAL(3,2),
    data_completeness_score DECIMAL(3,2),
    confidence_level STRING, -- high, medium, low
    validation_status STRING, -- pending, validated, needs-review, rejected
    
    -- Cost and Processing Metadata
    llm_model_used STRING, -- claude-3-haiku, claude-3-sonnet, etc.
    processing_cost_usd DECIMAL(10,6),
    processing_time_ms INT,
    tokens_used INT,
    batch_id STRING,
    
    -- Caching and Change Detection
    content_hash STRING, -- Hash of source content for change detection
    last_content_update TIMESTAMP,
    summary_generation_method STRING, -- llm_batch, llm_individual, cached
    
    -- Data lineage
    source_systems ARRAY<STRING>,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    last_verified_at TIMESTAMP,
    
    -- Partitioning
    dt STRING
)
COMMENT 'Advanced LLM-powered summarization and business intelligence analysis'
PARTITIONED BY (dt)
STORED AS PARQUET
LOCATION 's3://thera-curated/gold/advanced_summaries/'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY',
    'classification' = 'gold',
    'data_quality' = 'llm_enhanced'
);

-- =============================================================================
-- TABLE: llm_processing_logs
-- =============================================================================
CREATE TABLE IF NOT EXISTS llm_processing_logs (
    -- Processing identifiers
    batch_id STRING NOT NULL,
    company_key STRING NOT NULL,
    processing_timestamp TIMESTAMP NOT NULL,
    
    -- Request details
    llm_model STRING,
    prompt_type STRING, -- executive_summary, key_insights, tech_analysis, etc.
    input_tokens INT,
    output_tokens INT,
    total_tokens INT,
    
    -- Cost tracking
    cost_per_token DECIMAL(10,8),
    total_cost_usd DECIMAL(10,6),
    
    -- Performance metrics
    processing_time_ms INT,
    success BOOLEAN,
    error_message STRING,
    
    -- Quality metrics
    output_quality_score DECIMAL(3,2),
    confidence_score DECIMAL(3,2),
    
    -- Caching
    was_cached BOOLEAN,
    cache_hit_reason STRING,
    
    -- Partitioning
    dt STRING
)
COMMENT 'Detailed logging of LLM processing for cost tracking and optimization'
PARTITIONED BY (dt)
STORED AS PARQUET
LOCATION 's3://thera-curated/gold/llm_processing_logs/'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY',
    'classification' = 'gold',
    'data_quality' = 'operational'
);

-- =============================================================================
-- TABLE: llm_cost_tracking
-- =============================================================================
CREATE TABLE IF NOT EXISTS llm_cost_tracking (
    -- Time dimensions
    tracking_date DATE NOT NULL,
    hour_of_day INT,
    
    -- Cost aggregation
    total_requests INT,
    total_tokens INT,
    total_cost_usd DECIMAL(10,2),
    avg_cost_per_request DECIMAL(10,6),
    avg_cost_per_token DECIMAL(10,8),
    
    -- Model breakdown
    claude_haiku_requests INT,
    claude_haiku_cost DECIMAL(10,2),
    claude_sonnet_requests INT,
    claude_sonnet_cost DECIMAL(10,2),
    
    -- Processing type breakdown
    executive_summary_requests INT,
    key_insights_requests INT,
    tech_analysis_requests INT,
    competitive_analysis_requests INT,
    risk_assessment_requests INT,
    business_intelligence_requests INT,
    
    -- Efficiency metrics
    cache_hit_rate DECIMAL(3,2),
    avg_processing_time_ms INT,
    error_rate DECIMAL(3,2),
    
    -- Budget tracking
    daily_budget_usd DECIMAL(10,2),
    budget_utilization DECIMAL(3,2),
    remaining_budget_usd DECIMAL(10,2),
    
    -- Partitioning
    dt STRING
)
COMMENT 'Daily cost tracking and budget management for LLM usage'
PARTITIONED BY (dt)
STORED AS PARQUET
LOCATION 's3://thera-curated/gold/llm_cost_tracking/'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY',
    'classification' = 'gold',
    'data_quality' = 'operational'
);

-- =============================================================================
-- INDEXES AND OPTIMIZATIONS
-- =============================================================================

-- Create indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_advanced_summaries_company_key 
ON gold_advanced_summaries (company_key);

CREATE INDEX IF NOT EXISTS idx_advanced_summaries_domain 
ON gold_advanced_summaries (domain);

CREATE INDEX IF NOT EXISTS idx_advanced_summaries_confidence 
ON gold_advanced_summaries (confidence_level);

CREATE INDEX IF NOT EXISTS idx_llm_logs_batch_id 
ON llm_processing_logs (batch_id);

CREATE INDEX IF NOT EXISTS idx_llm_logs_timestamp 
ON llm_processing_logs (processing_timestamp);

-- =============================================================================
-- COST OPTIMIZATION CONFIGURATION
-- =============================================================================

-- Cost thresholds and limits
-- Claude 3 Haiku: $0.25 per 1M input tokens, $1.25 per 1M output tokens
-- Claude 3 Sonnet: $3.00 per 1M input tokens, $15.00 per 1M output tokens

-- Recommended usage:
-- - Executive Summary: Claude 3 Haiku (cost-effective for simple tasks)
-- - Key Insights: Claude 3 Haiku (structured extraction)
-- - Tech Analysis: Claude 3 Haiku (pattern recognition)
-- - Competitive Analysis: Claude 3 Sonnet (complex reasoning)
-- - Risk Assessment: Claude 3 Sonnet (critical analysis)
-- - Business Intelligence: Claude 3 Sonnet (strategic insights)

-- =============================================================================
-- NOTES FOR IMPLEMENTATION
-- =============================================================================
-- 1. Use Claude 3 Haiku for 80% of tasks to minimize costs
-- 2. Reserve Claude 3 Sonnet for complex analysis requiring deep reasoning
-- 3. Implement aggressive caching based on content_hash
-- 4. Use batch processing to reduce API overhead
-- 5. Set daily cost limits and automatic throttling
-- 6. Monitor token usage and optimize prompts
-- 7. Implement fallback strategies for API failures
-- 8. Use structured outputs to reduce token consumption
-- 9. Implement content change detection to avoid re-processing
-- 10. Create cost alerts and budget management
