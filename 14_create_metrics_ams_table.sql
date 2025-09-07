-- Create metrics_ams table for storing AMS (Average Match Score) metrics
-- This table stores computed metrics for challenge scoring

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

-- Add comments to metrics_ams table
ALTER TABLE thera_analytics.metrics_ams SET TBLPROPERTIES (
    'comment' = 'Table for storing overall AMS metrics aggregated across all challenges'
);

-- Add comments to challenge_metrics table
ALTER TABLE thera_analytics.challenge_metrics SET TBLPROPERTIES (
    'comment' = 'Table for storing per-challenge AMS metrics and statistics'
);

-- Add comments to metrics_ams columns
ALTER TABLE thera_analytics.metrics_ams CHANGE COLUMN date date string COMMENT 'Date for which metrics were computed (YYYY-MM-DD)';
ALTER TABLE thera_analytics.metrics_ams CHANGE COLUMN total_challenges total_challenges int COMMENT 'Total number of challenges processed';
ALTER TABLE thera_analytics.metrics_ams CHANGE COLUMN total_shortlists total_shortlists int COMMENT 'Total number of shortlists across all challenges';
ALTER TABLE thera_analytics.metrics_ams CHANGE COLUMN avg_ams_challenge avg_ams_challenge double COMMENT 'Average AMS score across all challenges';
ALTER TABLE thera_analytics.metrics_ams CHANGE COLUMN avg_embedding_similarity avg_embedding_similarity double COMMENT 'Average embedding similarity score across all challenges';
ALTER TABLE thera_analytics.metrics_ams CHANGE COLUMN avg_ml_score avg_ml_score double COMMENT 'Average ML score across all challenges';
ALTER TABLE thera_analytics.metrics_ams CHANGE COLUMN computed_at computed_at timestamp COMMENT 'Timestamp when metrics were computed';

-- Add comments to challenge_metrics columns
ALTER TABLE thera_analytics.challenge_metrics CHANGE COLUMN date date string COMMENT 'Date for which metrics were computed (YYYY-MM-DD)';
ALTER TABLE thera_analytics.challenge_metrics CHANGE COLUMN challenge_id challenge_id string COMMENT 'Unique identifier for the challenge';
ALTER TABLE thera_analytics.challenge_metrics CHANGE COLUMN total_shortlists total_shortlists int COMMENT 'Total number of shortlists for this challenge';
ALTER TABLE thera_analytics.challenge_metrics CHANGE COLUMN ams_challenge ams_challenge double COMMENT 'Average Match Score for this challenge (top 10 average)';
ALTER TABLE thera_analytics.challenge_metrics CHANGE COLUMN avg_embedding_similarity avg_embedding_similarity double COMMENT 'Average embedding similarity score for this challenge';
ALTER TABLE thera_analytics.challenge_metrics CHANGE COLUMN avg_ml_score avg_ml_score double COMMENT 'Average ML score for this challenge';
ALTER TABLE thera_analytics.challenge_metrics CHANGE COLUMN top_score top_score double COMMENT 'Highest final score for this challenge';
ALTER TABLE thera_analytics.challenge_metrics CHANGE COLUMN min_score min_score double COMMENT 'Lowest final score for this challenge';
ALTER TABLE thera_analytics.challenge_metrics CHANGE COLUMN score_std score_std double COMMENT 'Standard deviation of final scores for this challenge';
ALTER TABLE thera_analytics.challenge_metrics CHANGE COLUMN computed_at computed_at timestamp COMMENT 'Timestamp when metrics were computed';

-- Example queries for the metrics tables:

-- 1. Get overall AMS metrics for a date range
-- SELECT date, total_challenges, total_shortlists, avg_ams_challenge, avg_embedding_similarity, avg_ml_score
-- FROM thera_analytics.metrics_ams 
-- WHERE dt >= '2024-01-01' 
-- AND dt <= '2024-01-31'
-- ORDER BY date DESC;

-- 2. Get challenge-specific metrics for a date
-- SELECT challenge_id, total_shortlists, ams_challenge, top_score, min_score, score_std
-- FROM thera_analytics.challenge_metrics 
-- WHERE dt = '2024-01-15'
-- ORDER BY ams_challenge DESC;

-- 3. Get average AMS trend over time
-- SELECT date, AVG(avg_ams_challenge) as overall_avg_ams
-- FROM thera_analytics.metrics_ams 
-- WHERE dt >= '2024-01-01'
-- GROUP BY date
-- ORDER BY date;

-- 4. Get top performing challenges by AMS
-- SELECT challenge_id, AVG(ams_challenge) as avg_ams, COUNT(*) as days_measured
-- FROM thera_analytics.challenge_metrics 
-- WHERE dt >= '2024-01-01'
-- GROUP BY challenge_id
-- HAVING COUNT(*) >= 7  -- At least a week of data
-- ORDER BY avg_ams DESC
-- LIMIT 10;

-- 5. Get challenge performance distribution
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

-- 6. Get daily summary with growth metrics
-- SELECT 
--     date,
--     total_challenges,
--     total_shortlists,
--     avg_ams_challenge,
--     LAG(avg_ams_challenge) OVER (ORDER BY date) as prev_avg_ams,
--     avg_ams_challenge - LAG(avg_ams_challenge) OVER (ORDER BY date) as ams_change
-- FROM thera_analytics.metrics_ams 
-- WHERE dt >= '2024-01-01'
-- ORDER BY date;
