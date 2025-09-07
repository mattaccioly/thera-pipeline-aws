-- Create events_matching table for tracking matching events
-- This table stores events related to the matching process

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

-- Add comments to table
ALTER TABLE thera_analytics.events_matching SET TBLPROPERTIES (
    'comment' = 'Table for tracking matching events and their outcomes'
);

-- Add comments to columns
ALTER TABLE thera_analytics.events_matching CHANGE COLUMN event_id event_id string COMMENT 'Unique identifier for the event';
ALTER TABLE thera_analytics.events_matching CHANGE COLUMN challenge_id challenge_id string COMMENT 'Identifier for the challenge';
ALTER TABLE thera_analytics.events_matching CHANGE COLUMN company_key company_key string COMMENT 'Unique identifier for the company';
ALTER TABLE thera_analytics.events_matching CHANGE COLUMN event_type event_type string COMMENT 'Type of event (match_created, match_updated, match_deleted, etc.)';
ALTER TABLE thera_analytics.events_matching CHANGE COLUMN event_timestamp event_timestamp timestamp COMMENT 'Timestamp when the event occurred';
ALTER TABLE thera_analytics.events_matching CHANGE COLUMN event_data event_data map<string, string> COMMENT 'Additional event-specific data';
ALTER TABLE thera_analytics.events_matching CHANGE COLUMN final_score final_score double COMMENT 'Final computed score for the match';
ALTER TABLE thera_analytics.events_matching CHANGE COLUMN embedding_similarity embedding_similarity double COMMENT 'Similarity score from embeddings';
ALTER TABLE thera_analytics.events_matching CHANGE COLUMN industry_geo_score industry_geo_score double COMMENT 'Combined industry and geographic match score';
ALTER TABLE thera_analytics.events_matching CHANGE COLUMN apollo_score apollo_score double COMMENT 'Apollo data quality score';
ALTER TABLE thera_analytics.events_matching CHANGE COLUMN ml_score ml_score double COMMENT 'Machine learning prediction score';
ALTER TABLE thera_analytics.events_matching CHANGE COLUMN rule_features rule_features map<string, double> COMMENT 'Rule-based feature scores';
ALTER TABLE thera_analytics.events_matching CHANGE COLUMN reason reason string COMMENT 'Reason for the match or event';
ALTER TABLE thera_analytics.events_matching CHANGE COLUMN created_at created_at timestamp COMMENT 'Timestamp when the record was created';
ALTER TABLE thera_analytics.events_matching CHANGE COLUMN updated_at updated_at timestamp COMMENT 'Timestamp when the record was last updated';

-- Create index on commonly queried columns
-- Note: Athena doesn't support traditional indexes, but we can optimize with partitioning
-- The table is already partitioned by dt (date) for efficient querying

-- Example queries for the events_matching table:

-- 1. Get all matching events for a specific challenge
-- SELECT * FROM thera_analytics.events_matching 
-- WHERE challenge_id = 'challenge_123' 
-- AND dt >= '2024-01-01' 
-- ORDER BY event_timestamp DESC;

-- 2. Get high-scoring matches
-- SELECT challenge_id, company_key, final_score, event_timestamp
-- FROM thera_analytics.events_matching 
-- WHERE final_score > 0.8 
-- AND dt >= '2024-01-01'
-- ORDER BY final_score DESC;

-- 3. Get matching events by type
-- SELECT event_type, COUNT(*) as event_count
-- FROM thera_analytics.events_matching 
-- WHERE dt >= '2024-01-01'
-- GROUP BY event_type;

-- 4. Get average scores by challenge
-- SELECT challenge_id, 
--        AVG(final_score) as avg_final_score,
--        AVG(embedding_similarity) as avg_embedding_similarity,
--        AVG(ml_score) as avg_ml_score
-- FROM thera_analytics.events_matching 
-- WHERE dt >= '2024-01-01'
-- GROUP BY challenge_id
-- ORDER BY avg_final_score DESC;
