# Thera Data Pipeline - Athena/Glue SQL Implementation

## Overview

This implementation provides a comprehensive data pipeline using AWS Athena and Glue to process startup data from multiple sources through BRONZE → SILVER → GOLD layers. The pipeline includes data ingestion, cleaning, normalization, deduplication, and aggregation for startup profiling and analysis.

## Architecture

### Data Layers

1. **BRONZE Layer** (`thera_bronze`)
   - Raw data from external sources
   - Formats: CSV, JSON Lines
   - Storage: `s3://thera-raw/`

2. **SILVER Layer** (`thera_silver`)
   - Cleaned and normalized data
   - Format: Parquet (partitioned by dt)
   - Storage: `s3://thera-curated/silver/`

3. **GOLD Layer** (`thera_gold`)
   - Business-ready aggregated data
   - Format: Parquet (partitioned by dt)
   - Storage: `s3://thera-curated/gold/`

### Data Sources

- **Crunchbase**: Company information, funding data
- **Apollo**: Contact information, company details
- **Firecrawl**: Web content, technology stack, social media

## File Structure

```
thera-pipeline-aws/
├── 01_setup_glue_databases.sql          # Database setup
├── 02_create_bronze_tables.sql          # External tables for raw data
├── 03_create_silver_companies.sql       # Canonical company records
├── 04_create_silver_apollo_companies.sql # Normalized Apollo companies
├── 05_create_silver_apollo_contacts.sql # PII-masked contacts
├── 06_create_silver_domain_health.sql   # Domain health scoring
├── 07_create_silver_web_extracts.sql    # Web content processing
├── 08_create_gold_startup_profiles.sql  # Comprehensive profiles
├── 09_create_ctas_queries.sql           # Table population queries
├── 10_data_quality_checks.sql           # Data validation and monitoring
├── 11_s3_partitioning_structure.sql     # Partitioning and lifecycle
└── 12_documentation_and_comments.md     # This documentation
```

## Configuration Requirements

### S3 Buckets

Update the following S3 paths in all SQL files:

```sql
-- Current configuration
s3://thera-raw/         # Raw data
s3://thera-curated/     # Processed data

-- Update to your actual buckets
s3://your-raw-bucket/
s3://your-curated-bucket/
```

### Glue Databases

Update database names if needed:

```sql
-- Current configuration
thera_bronze
thera_silver
thera_gold

-- Update to your actual database names
your_bronze_db
your_silver_db
your_gold_db
```

### IAM Permissions

Ensure the following permissions are granted:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::thera-raw/*",
                "arn:aws:s3:::thera-curated/*",
                "arn:aws:s3:::thera-raw",
                "arn:aws:s3:::thera-curated"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "glue:CreateDatabase",
                "glue:GetDatabase",
                "glue:UpdateDatabase",
                "glue:DeleteDatabase",
                "glue:CreateTable",
                "glue:GetTable",
                "glue:UpdateTable",
                "glue:DeleteTable",
                "glue:GetPartitions",
                "glue:CreatePartition",
                "glue:DeletePartition"
            ],
            "Resource": "*"
        }
    ]
}
```

## Implementation Steps

### 1. Database Setup

```bash
# Run database creation
aws athena start-query-execution \
    --query-string "$(cat 01_setup_glue_databases.sql)" \
    --result-configuration OutputLocation=s3://your-query-results/
```

### 2. Bronze Layer Setup

```bash
# Create external tables
aws athena start-query-execution \
    --query-string "$(cat 02_create_bronze_tables.sql)" \
    --result-configuration OutputLocation=s3://your-query-results/
```

### 3. Silver Layer Implementation

```bash
# Create and populate silver tables
aws athena start-query-execution \
    --query-string "$(cat 03_create_silver_companies.sql)" \
    --result-configuration OutputLocation=s3://your-query-results/

aws athena start-query-execution \
    --query-string "$(cat 04_create_silver_apollo_companies.sql)" \
    --result-configuration OutputLocation=s3://your-query-results/

aws athena start-query-execution \
    --query-string "$(cat 05_create_silver_apollo_contacts.sql)" \
    --result-configuration OutputLocation=s3://your-query-results/

aws athena start-query-execution \
    --query-string "$(cat 06_create_silver_domain_health.sql)" \
    --result-configuration OutputLocation=s3://your-query-results/

aws athena start-query-execution \
    --query-string "$(cat 07_create_silver_web_extracts.sql)" \
    --result-configuration OutputLocation=s3://your-query-results/
```

### 4. Gold Layer Implementation

```bash
# Create and populate gold tables
aws athena start-query-execution \
    --query-string "$(cat 08_create_gold_startup_profiles.sql)" \
    --result-configuration OutputLocation=s3://your-query-results/
```

### 5. Data Population

```bash
# Populate all tables
aws athena start-query-execution \
    --query-string "$(cat 09_create_ctas_queries.sql)" \
    --result-configuration OutputLocation=s3://your-query-results/
```

### 6. Data Quality Setup

```bash
# Set up data quality monitoring
aws athena start-query-execution \
    --query-string "$(cat 10_data_quality_checks.sql)" \
    --result-configuration OutputLocation=s3://your-query-results/
```

### 7. Partitioning Setup

```bash
# Set up partitioning and lifecycle
aws athena start-query-execution \
    --query-string "$(cat 11_s3_partitioning_structure.sql)" \
    --result-configuration OutputLocation=s3://your-query-results/
```

## Data Quality Monitoring

### Key Metrics

- **Data Completeness**: Percentage of required fields populated
- **Data Accuracy**: Validation of data formats and business rules
- **Data Freshness**: Age of data and update frequency
- **Data Consistency**: Cross-table validation and referential integrity

### Monitoring Queries

```sql
-- Overall data quality summary
SELECT * FROM data_quality_summary;

-- Data quality issues
SELECT * FROM data_quality_issues;

-- Partition health
SELECT * FROM partition_health_monitor;
```

## Performance Optimization

### Partitioning Strategy

- **Time-based partitioning**: All tables partitioned by `dt` (date)
- **Retention policies**: 
  - Bronze: 90 days
  - Silver: 365 days
  - Gold: 730 days

### Compression

- **Format**: Parquet with SNAPPY compression
- **Benefits**: Reduced storage costs, faster queries

### Query Optimization

- Use partition pruning in WHERE clauses
- Filter on `dt` column for time-based queries
- Use appropriate data types for better compression

## Security Considerations

### PII Handling

- Contact information is masked in `silver_apollo_contacts`
- Sensitive data is hashed for deduplication
- Raw PII is not stored in processed tables

### Access Control

- Implement IAM policies for fine-grained access
- Use Glue security configurations
- Enable S3 bucket encryption

## Maintenance Procedures

### Daily Tasks

1. Monitor data quality metrics
2. Check partition health
3. Verify data freshness
4. Review error logs

### Weekly Tasks

1. Analyze storage usage
2. Optimize slow queries
3. Update data quality rules
4. Review access patterns

### Monthly Tasks

1. Clean up old partitions
2. Review and update retention policies
3. Analyze cost optimization opportunities
4. Update documentation

## Troubleshooting

### Common Issues

1. **Partition not found**: Check if partition exists and is properly formatted
2. **Data quality failures**: Review validation rules and data sources
3. **Performance issues**: Check partitioning and query patterns
4. **Access denied**: Verify IAM permissions

### Debug Queries

```sql
-- Check table structure
DESCRIBE thera_silver.silver_companies;

-- Check partition information
SHOW PARTITIONS thera_silver.silver_companies;

-- Check data quality
SELECT * FROM silver_companies_data_quality WHERE overall_validation_status = 'FAIL';
```

## Cost Optimization

### Storage Costs

- Use appropriate compression (SNAPPY)
- Implement lifecycle policies
- Monitor partition sizes
- Clean up unused data

### Query Costs

- Use partition pruning
- Optimize query patterns
- Monitor query performance
- Use appropriate data types

## Future Enhancements

### Planned Features

1. Real-time data processing
2. Machine learning integration
3. Advanced analytics
4. API endpoints
5. Data lineage tracking

### Scalability Considerations

1. Horizontal scaling with Glue
2. Auto-scaling based on demand
3. Multi-region deployment
4. Data replication strategies

## Support and Maintenance

### Documentation Updates

- Keep this documentation current
- Update configuration examples
- Add new features and procedures
- Maintain troubleshooting guides

### Monitoring and Alerting

- Set up CloudWatch alarms
- Monitor data quality metrics
- Track performance indicators
- Alert on failures

## Contact Information

For questions or issues with this implementation:

- **Documentation**: This file and inline comments
- **Code Repository**: GitHub repository
- **Issue Tracking**: GitHub issues
- **Support**: Contact your data engineering team

---

**Last Updated**: [Current Date]
**Version**: 1.0
**Maintainer**: Data Engineering Team
