# Thera Pipeline AWS Lambda Jobs

A comprehensive AWS-based data pipeline for startup discovery and matching using Apollo API, Firecrawl web scraping, Bedrock embeddings, and machine learning.

## 🏗️ Architecture Overview

The pipeline consists of 8 Lambda functions that work together to process startup data:

1. **Apollo Delta Puller** - Rate-limited API client for Apollo data
2. **Domain Health Gate** - Async domain health checks
3. **Firecrawl Orchestrator** - Web scraping orchestration
4. **Embeddings Batch** - Bedrock Titan embeddings processing
5. **Matcher** - Similarity matching with ML
6. **AMS Computation** - Challenge scoring metrics
7. **DynamoDB Publisher** - Data publishing to public/private tables
8. **ML Trainer** - Weekly model training

## 📁 Project Structure

```
thera-pipeline-aws/
├── lambda-*.py                 # Lambda function code
├── lambda-*.yaml              # CloudFormation templates
├── lambda-ml-training/        # ML training Lambda package
├── deployment/                # Deployment scripts
├── monitoring/                # Monitoring configuration
├── cost-optimization/         # Cost management
├── troubleshooting/           # Troubleshooting guides
├── *.sql                     # SQL queries for data processing
├── *.md                      # Documentation
└── README.md                 # This file
```

## 🚀 Quick Start

### Prerequisites

- AWS CLI configured with appropriate permissions
- Python 3.12+ (for local development)
- Required AWS services enabled:
  - Lambda
  - S3
  - DynamoDB
  - Athena
  - Bedrock
  - Secrets Manager
  - CloudWatch
  - Step Functions (optional)
  - SQS (optional)

### 1. Deploy Infrastructure

```bash
# Make deployment script executable
chmod +x deployment/deploy.sh

# Deploy to dev environment
./deployment/deploy.sh dev us-east-1

# Deploy to production
./deployment/deploy.sh prod us-west-2
```

### 2. Configure API Keys

Create secrets in AWS Secrets Manager:

```bash
# Apollo API Key
aws secretsmanager create-secret \
  --name "thera/apollo/api-key" \
  --secret-string '{"apollo_api_key":"YOUR_APOLLO_API_KEY"}' \
  --region us-east-1

# Firecrawl API Key
aws secretsmanager create-secret \
  --name "thera/firecrawl/api-key" \
  --secret-string '{"firecrawl_api_key":"YOUR_FIRECRAWL_API_KEY"}' \
  --region us-east-1
```

### 3. Run Data Quality Checks

Execute the SQL queries in `10_data_quality_checks.sql` to verify data integrity.

### 4. Test the Pipeline

```bash
# Run load tests
chmod +x deployment/load-test.sh
./deployment/load-test.sh dev us-east-1
```

## 🔧 Lambda Functions

### Apollo Delta Puller (`lambda-apollo-delta-pull.py`)

**Purpose**: Rate-limited Apollo API client that fetches companies and contacts data.

**Features**:
- DynamoDB-based quota tracking (50/min, 200/hour, 600/day)
- Graceful handling of rate limits
- Raw JSONL storage to S3
- Normalized data to bronze layer
- CloudWatch metrics

**Trigger**: Daily at 6 AM UTC

**Input**: None (uses watermark from SSM)

**Output**: 
- Raw data: `s3://thera-raw/apollo/date=YYYY-MM-DD/`
- Bronze data: `s3://thera-bronze/bronze_apollo_*/date=YYYY-MM-DD/`

### Domain Health Gate (`lambda-domain-health-gate.py`)

**Purpose**: Async domain health checks with DNS, HTTP, HTTPS, TLS, and content validation.

**Features**:
- Async processing with concurrency limits
- Comprehensive health scoring
- Flag generation for issues
- S3 JSONL output

**Trigger**: Daily at 8 AM UTC (optional)

**Input**: 
- `domains`: List of domains to check
- `query`: Athena query for domains
- `manifest_key`: S3 manifest file

**Output**: `s3://thera-curated/silver/domain_health/date=YYYY-MM-DD/`

### Firecrawl Orchestrator (`lambda-firecrawl-orchestrator.py`)

**Purpose**: Web scraping orchestration with Firecrawl API.

**Features**:
- Step Functions or SQS orchestration
- Circuit breaker for failures
- Content richness scoring
- Error handling with exponential backoff

**Trigger**: Daily at 10 AM UTC (optional)

**Input**:
- `domains`: List of domains to crawl
- `use_step_functions`: Boolean for Step Functions
- `use_sqs`: Boolean for SQS

**Output**: 
- Raw data: `s3://thera-raw/firecrawl/date=YYYY-MM-DD/`
- Silver data: `s3://thera-curated/silver/firecrawl/date=YYYY-MM-DD/`

### Embeddings Batch (`lambda-embeddings-batch.py`)

**Purpose**: Bedrock Titan embeddings processing with cost guardrails.

**Features**:
- Cost tracking and budget limits
- Parquet output format
- Watermark-based incremental processing
- Feature extraction for ML

**Trigger**: Daily at 12 PM UTC

**Input**: None (uses watermark from SSM)

**Output**: `s3://thera-embeddings/embeddings/date=YYYY-MM-DD/`

### Matcher (`lambda-matcher.py`)

**Purpose**: Similarity matching using embeddings and ML model.

**Features**:
- Bedrock embeddings generation
- Athena candidate queries
- Cosine similarity calculation
- ML model scoring
- Rule-based features

**Trigger**: On-demand via API Gateway

**Input**:
- `challenge_text`: Text to match against
- `industry`: Optional industry filter
- `country`: Optional country filter

**Output**: Top 20 matches with scores and reasons

### AMS Computation (`lambda-ams-computation.py`)

**Purpose**: Daily metrics computation for challenge scoring.

**Features**:
- Shortlist data processing
- AMS calculation (Average Match Score)
- Parquet metrics output
- Challenge-level analytics

**Trigger**: Daily at 2 PM UTC

**Input**: None (processes previous day's data)

**Output**: 
- Overall metrics: `s3://thera-metrics/ams/overall/date=YYYY-MM-DD/`
- Challenge metrics: `s3://thera-metrics/ams/challenges/date=YYYY-MM-DD/`

### DynamoDB Publisher (`lambda-dynamodb-publisher.py`)

**Purpose**: Incremental data publishing to public/private DynamoDB tables.

**Features**:
- PII handling and masking
- Public/private table separation
- Watermark-based incremental updates
- TTL management

**Trigger**: Daily at 4 PM UTC

**Input**: None (uses watermark from SSM)

**Output**: 
- Public table: `thera-startups-public`
- Private table: `thera-startups-private`

### ML Trainer (`lambda-ml-training/lambda_function.py`)

**Purpose**: Weekly scikit-learn model training for matching.

**Features**:
- Logistic regression training
- Feature engineering
- Model comparison and validation
- S3 model storage

**Trigger**: Weekly on Sunday at 6 PM UTC

**Input**: None (uses training data from Athena)

**Output**: `s3://thera-models/match_lr/model.json`

## 📊 Monitoring

### CloudWatch Dashboard

Access the dashboard at: `https://console.aws.amazon.com/cloudwatch/home#dashboards`

### Key Metrics

- **Apollo**: Items fetched, API calls used
- **Domain Health**: Domains checked, average health score
- **Firecrawl**: Domains processed, success/failure rates
- **Embeddings**: Items processed, cost tracking
- **Matcher**: Candidates processed, similarity scores
- **AMS**: Challenges processed, average scores
- **DynamoDB**: Items written, throughput
- **ML Training**: Training samples, model accuracy

### Alarms

Each Lambda function has CloudWatch alarms for:
- Error rates
- Duration thresholds
- Business logic metrics

## 💰 Cost Optimization

### Budget Management

- Daily cost limits for embeddings processing
- S3 lifecycle policies for data retention
- DynamoDB on-demand billing
- Lambda memory optimization

### Cost Monitoring

```bash
# View cost alerts
aws budgets describe-budgets --account-id YOUR_ACCOUNT_ID

# Check S3 costs
aws s3api list-buckets --query 'Buckets[].Name' --output table
```

## 🔧 Configuration

### Environment Variables

Each Lambda function uses environment variables for configuration:

- `RAW_BUCKET`: S3 bucket for raw data
- `CURATED_BUCKET`: S3 bucket for curated data
- `ATHENA_DATABASE`: Athena database name
- `ATHENA_WORKGROUP`: Athena workgroup name
- `BEDROCK_MODEL_ID`: Bedrock model for embeddings
- `MAX_CONCURRENCY`: Concurrency limits
- `DAILY_BUDGET`: Cost limits

### Secrets Management

API keys are stored in AWS Secrets Manager:
- `thera/apollo/api-key`
- `thera/firecrawl/api-key`

## 🚨 Troubleshooting

### Common Issues

1. **Rate Limiting**: Check Apollo quota table in DynamoDB
2. **Memory Issues**: Increase Lambda memory allocation
3. **Timeout Errors**: Check CloudWatch logs for specific errors
4. **Data Quality**: Run SQL queries in `10_data_quality_checks.sql`

### Debug Commands

```bash
# Check Lambda function status
aws lambda get-function --function-name dev-apollo-delta-pull

# View CloudWatch logs
aws logs describe-log-groups --log-group-name-prefix /aws/lambda/dev-

# Check DynamoDB tables
aws dynamodb describe-table --table-name dev-apollo-quota

# Verify S3 buckets
aws s3 ls s3://thera-raw/
```

## 📈 Performance Tuning

### Lambda Optimization

- Memory allocation based on workload
- Reserved concurrency for critical functions
- Dead letter queues for error handling
- Step Functions for complex workflows

### Data Processing

- Partitioned S3 storage by date
- Parquet format for analytics
- Incremental processing with watermarks
- Batch processing for efficiency

## 🔒 Security

### IAM Roles

Each Lambda function has minimal required permissions:
- S3 read/write access
- DynamoDB access
- Athena query execution
- Bedrock model invocation
- Secrets Manager access

### Data Privacy

- PII masking in private tables
- Data encryption at rest
- VPC endpoints for private access
- Audit logging

## 📚 Documentation

- `ATHENA_GLUE_SQL_MODELS.md`: SQL models and queries
- `DYNAMODB_READ_MODELS.md`: DynamoDB table schemas
- `LAMBDA_JOBS_FIRECRAWL_ORCHESTRATION.md`: Lambda job specifications
- `STEP_FUNCTIONS_EVENTBRIDGE.md`: Orchestration patterns
- `MODEL_AMS_SQL_CODE.md`: AMS calculation logic

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

For issues and questions:
1. Check the troubleshooting guide
2. Review CloudWatch logs
3. Check AWS service status
4. Create an issue in the repository

## 🔄 Updates

### Version History

- **v1.0.0**: Initial release with all 8 Lambda functions
- **v1.1.0**: Added monitoring and cost optimization
- **v1.2.0**: Enhanced error handling and retry policies

### Future Enhancements

- [ ] Real-time streaming with Kinesis
- [ ] Advanced ML models with SageMaker
- [ ] Multi-region deployment
- [ ] GraphQL API for data access
- [ ] Advanced analytics dashboard