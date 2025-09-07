-- Create Glue tables for ML and AMS components
-- This file contains DDL statements for creating Glue tables used by the ML training and AMS computation

-- Create events_matching table for tracking matching events
CREATE TABLE IF NOT EXISTS thera_analytics.events_matching (
    event_id string,
    challenge_id string,
    company_key string,
    event_type string,
    event_timestamp timestamp,
    event_data map<string, string>,
    final_score double,
    embedding_similarity double,
    industry_geo_score double,
    apollo_score double,
    ml_score double,
    rule_features map<string, double>,
    reason string,
    created_at timestamp,
    updated_at timestamp
)
PARTITIONED BY (dt string)
STORED AS PARQUET
LOCATION 's3://thera-curated/events/matching/'
TBLPROPERTIES (
    'classification' = 'parquet',
    'typeOfData' = 'file',
    'projection.enabled' = 'true',
    'projection.dt.type' = 'date',
    'projection.dt.range' = '2024-01-01,NOW',
    'projection.dt.format' = 'yyyy-MM-dd',
    'projection.dt.interval' = '1',
    'projection.dt.interval.unit' = 'DAYS',
    'storage.location.template' = 's3://thera-curated/events/matching/dt=${dt}/'
);

-- Create metrics_ams table for storing AMS (Average Match Score) metrics
CREATE TABLE IF NOT EXISTS thera_analytics.metrics_ams (
    date string,
    total_challenges int,
    total_shortlists int,
    avg_ams_challenge double,
    avg_embedding_similarity double,
    avg_ml_score double,
    computed_at timestamp
)
PARTITIONED BY (dt string)
STORED AS PARQUET
LOCATION 's3://thera-metrics/metrics/ams/overall/'
TBLPROPERTIES (
    'classification' = 'parquet',
    'typeOfData' = 'file',
    'projection.enabled' = 'true',
    'projection.dt.type' = 'date',
    'projection.dt.range' = '2024-01-01,NOW',
    'projection.dt.format' = 'yyyy-MM-dd',
    'projection.dt.interval' = '1',
    'projection.dt.interval.unit' = 'DAYS',
    'storage.location.template' = 's3://thera-metrics/metrics/ams/overall/dt=${dt}/'
);

-- Create challenge_metrics table for storing per-challenge AMS metrics
CREATE TABLE IF NOT EXISTS thera_analytics.challenge_metrics (
    date string,
    challenge_id string,
    total_shortlists int,
    ams_challenge double,
    avg_embedding_similarity double,
    avg_ml_score double,
    top_score double,
    min_score double,
    score_std double,
    computed_at timestamp
)
PARTITIONED BY (dt string)
STORED AS PARQUET
LOCATION 's3://thera-metrics/metrics/ams/challenges/'
TBLPROPERTIES (
    'classification' = 'parquet',
    'typeOfData' = 'file',
    'projection.enabled' = 'true',
    'projection.dt.type' = 'date',
    'projection.dt.range' = '2024-01-01,NOW',
    'projection.dt.format' = 'yyyy-MM-dd',
    'projection.dt.interval' = '1',
    'projection.dt.interval.unit' = 'DAYS',
    'storage.location.template' = 's3://thera-metrics/metrics/ams/challenges/dt=${dt}/'
);

-- Create model_versions table for tracking ML model versions
CREATE TABLE IF NOT EXISTS thera_analytics.model_versions (
    model_id string,
    model_type string,
    model_version string,
    training_date string,
    auc_score double,
    pr_auc_score double,
    training_samples int,
    test_samples int,
    feature_count int,
    model_path string,
    created_at timestamp
)
PARTITIONED BY (dt string)
STORED AS PARQUET
LOCATION 's3://thera-models/versions/'
TBLPROPERTIES (
    'classification' = 'parquet',
    'typeOfData' = 'file',
    'projection.enabled' = 'true',
    'projection.dt.type' = 'date',
    'projection.dt.range' = '2024-01-01,NOW',
    'projection.dt.format' = 'yyyy-MM-dd',
    'projection.dt.interval' = '1',
    'projection.dt.interval.unit' = 'DAYS',
    'storage.location.template' = 's3://thera-models/versions/dt=${dt}/'
);

-- Create training_data table for storing training data snapshots
CREATE TABLE IF NOT EXISTS thera_analytics.training_data (
    challenge_id string,
    company_key string,
    final_score double,
    embedding_similarity double,
    ml_score double,
    rule_features map<string, double>,
    industry string,
    country string,
    employee_count double,
    annual_revenue double,
    total_funding double,
    domain_health_score double,
    content_richness_score double,
    contacted_within_14_days int,
    created_at timestamp
)
PARTITIONED BY (dt string)
STORED AS PARQUET
LOCATION 's3://thera-curated/training_data/'
TBLPROPERTIES (
    'classification' = 'parquet',
    'typeOfData' = 'file',
    'projection.enabled' = 'true',
    'projection.dt.type' = 'date',
    'projection.dt.range' = '2024-01-01,NOW',
    'projection.dt.format' = 'yyyy-MM-dd',
    'projection.dt.interval' = '1',
    'projection.dt.interval.unit' = 'DAYS',
    'storage.location.template' = 's3://thera-curated/training_data/dt=${dt}/'
);

-- Add comments to tables
ALTER TABLE thera_analytics.events_matching SET TBLPROPERTIES (
    'comment' = 'Table for tracking matching events and their outcomes'
);

ALTER TABLE thera_analytics.metrics_ams SET TBLPROPERTIES (
    'comment' = 'Table for storing overall AMS metrics aggregated across all challenges'
);

ALTER TABLE thera_analytics.challenge_metrics SET TBLPROPERTIES (
    'comment' = 'Table for storing per-challenge AMS metrics and statistics'
);

ALTER TABLE thera_analytics.model_versions SET TBLPROPERTIES (
    'comment' = 'Table for tracking ML model versions and their performance metrics'
);

ALTER TABLE thera_analytics.training_data SET TBLPROPERTIES (
    'comment' = 'Table for storing training data snapshots used for ML model training'
);

-- Example queries for the new tables:

-- 1. Get latest model performance
-- SELECT model_type, model_version, auc_score, pr_auc_score, training_date
-- FROM thera_analytics.model_versions 
-- WHERE dt >= '2024-01-01'
-- ORDER BY training_date DESC, auc_score DESC;

-- 2. Get AMS trends over time
-- SELECT date, avg_ams_challenge, total_challenges, total_shortlists
-- FROM thera_analytics.metrics_ams 
-- WHERE dt >= '2024-01-01'
-- ORDER BY date DESC;

-- 3. Get challenge performance distribution
-- SELECT 
--     CASE 
--         WHEN ams_challenge >= 0.8 THEN 'Excellent (0.8+)'
--         WHEN ams_challenge >= 0.6 THEN 'Good (0.6-0.8)'
--         WHEN ams_challenge >= 0.4 THEN 'Fair (0.4-0.6)'
--         ELSE 'Poor (<0.4)'
--     END as performance_category,
--     COUNT(*) as challenge_count
-- FROM thera_analytics.challenge_metrics 
-- WHERE dt >= '2024-01-01'
-- GROUP BY 1
-- ORDER BY 1;

-- 4. Get training data statistics
-- SELECT 
--     COUNT(*) as total_records,
--     AVG(final_score) as avg_final_score,
--     AVG(embedding_similarity) as avg_embedding_similarity,
--     AVG(ml_score) as avg_ml_score,
--     SUM(contacted_within_14_days) as positive_cases
-- FROM thera_analytics.training_data 
-- WHERE dt >= '2024-01-01';

-- 5. Get model performance comparison
-- SELECT 
--     model_type,
--     AVG(auc_score) as avg_auc,
--     AVG(pr_auc_score) as avg_pr_auc,
--     COUNT(*) as model_count
-- FROM thera_analytics.model_versions 
-- WHERE dt >= '2024-01-01'
-- GROUP BY model_type
-- ORDER BY avg_auc DESC;
