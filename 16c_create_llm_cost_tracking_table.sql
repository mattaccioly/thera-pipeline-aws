USE thera_gold;

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
LOCATION 's3://thera-curated-805595753342-v3/gold/llm_cost_tracking/'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY',
    'classification' = 'gold',
    'data_quality' = 'operational'
);
