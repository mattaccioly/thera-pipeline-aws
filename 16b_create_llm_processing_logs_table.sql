USE thera_gold;

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
LOCATION 's3://thera-curated-805595753342-v3/gold/llm_processing_logs/'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY',
    'classification' = 'gold',
    'data_quality' = 'operational'
);
