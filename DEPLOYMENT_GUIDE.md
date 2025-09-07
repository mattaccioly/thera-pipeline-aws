# Thera Pipeline AWS Deployment Guide

## 🚀 Quick Start

This guide will help you deploy the complete Thera Pipeline to AWS. The pipeline processes startup data using Apollo API, Firecrawl web scraping, Bedrock embeddings, and machine learning.

## 📋 Prerequisites

### 1. AWS Account Setup
- AWS account with appropriate permissions
- AWS CLI installed and configured
- Required AWS services enabled in your region

### 2. API Keys Required
- **Apollo API Key**: For startup company data
- **Firecrawl API Key**: For web scraping

### 3. AWS Services Required
- Lambda
- S3
- DynamoDB
- Athena
- Bedrock
- Secrets Manager
- CloudWatch
- Step Functions
- EventBridge

## 🛠️ Step-by-Step Deployment

### Step 1: Configure AWS Environment

```bash
# Run the AWS setup script
./setup-aws.sh

# Or manually configure AWS CLI
aws configure
```

### Step 2: Deploy Infrastructure

```bash
# Deploy to development environment
./deployment/deploy.sh dev us-east-1

# Deploy to production environment
./deployment/deploy.sh prod us-west-2
```

### Step 3: Configure API Keys

```bash
# Create Apollo API key secret
aws secretsmanager create-secret \
  --name "thera/apollo/api-key" \
  --secret-string '{"apollo_api_key":"YOUR_APOLLO_API_KEY"}' \
  --region us-east-1

# Create Firecrawl API key secret
aws secretsmanager create-secret \
  --name "thera/firecrawl/api-key" \
  --secret-string '{"firecrawl_api_key":"YOUR_FIRECRAWL_API_KEY"}' \
  --region us-east-1
```

### Step 4: Test the Pipeline

```bash
# Run load tests
./deployment/load-test.sh dev us-east-1

# Monitor the pipeline
./monitoring/monitor.sh all dev us-east-1
```

## 📊 Architecture Overview

The pipeline consists of 8 Lambda functions orchestrated by Step Functions:

1. **Apollo Delta Pull** - Fetches company data with rate limiting
2. **Athena CTAS BRONZE→SILVER** - Transforms raw data
3. **Domain Health Gate** - Validates domain health scores
4. **Firecrawl Orchestrator** - Web scraping with concurrency control
5. **Athena CTAS SILVER→GOLD** - Builds startup profiles
6. **Embeddings Batch** - Generates text embeddings using Bedrock
7. **AMS Computation** - Calculates matching scores
8. **DynamoDB Publisher** - Updates read models

## 🔧 Configuration

### Environment Variables

Each Lambda function uses these environment variables:

- `ENVIRONMENT`: dev/staging/prod
- `RAW_BUCKET`: S3 bucket for raw data
- `CURATED_BUCKET`: S3 bucket for curated data
- `ATHENA_DATABASE`: Athena database name
- `ATHENA_WORKGROUP`: Athena workgroup name
- `BEDROCK_MODEL_ID`: Bedrock model for embeddings

### S3 Bucket Structure

```
thera-raw/
├── apollo/date=YYYY-MM-DD/
├── firecrawl/date=YYYY-MM-DD/
└── ...

thera-curated/
├── silver/companies/date=YYYY-MM-DD/
├── silver/domain_health/date=YYYY-MM-DD/
├── gold/startup_profiles/date=YYYY-MM-DD/
└── ...

thera-embeddings/
├── embeddings/date=YYYY-MM-DD/
└── ...

thera-metrics/
├── ams/overall/date=YYYY-MM-DD/
├── ams/challenges/date=YYYY-MM-DD/
└── ...

thera-models/
├── match_lr/model.json
└── ...
```

## 📈 Monitoring

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

### Alarms

Each Lambda function has CloudWatch alarms for:
- Error rates
- Duration thresholds
- Business logic metrics

## 💰 Cost Optimization

### Estimated Monthly Costs

- **Lambda**: $50-200 (depends on execution frequency)
- **Step Functions**: $25-100 (based on state transitions)
- **Athena**: $5-50 (based on data scanned)
- **S3**: $10-100 (based on storage and requests)
- **DynamoDB**: $25-150 (based on read/write capacity)
- **EventBridge**: $1-10 (based on rule executions)
- **Total**: $116-610 per month

### Optimization Strategies

- Right-size Lambda memory allocation
- Use Parquet format for Athena queries
- Implement S3 lifecycle policies
- Optimize DynamoDB capacity modes
- Use appropriate storage classes
- Implement caching strategies

## 🔒 Security

### IAM Permissions

Each Lambda function has minimal required permissions:
- S3 read/write access
- DynamoDB access
- Athena query execution
- Bedrock model invocation
- Secrets Manager access

### Data Privacy

- PII masking in private tables
- Data encryption at rest
- VPC endpoints for private access (optional)
- Audit logging

## 🚨 Troubleshooting

### Common Issues

1. **Pipeline Not Starting**: Check EventBridge rules and IAM permissions
2. **API Key Issues**: Verify Secrets Manager configuration
3. **Lambda Timeouts**: Increase timeout settings or optimize code
4. **Athena Query Failures**: Check SQL syntax and table schemas
5. **S3 Access Denied**: Review bucket policies and IAM permissions
6. **DynamoDB Issues**: Verify table creation and permissions

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

## 📚 Additional Resources

- [README.md](README.md) - Main documentation
- [DEPLOYMENT_SUMMARY.md](DEPLOYMENT_SUMMARY.md) - Deployment overview
- [ATHENA_GLUE_SQL_MODELS.md](ATHENA_GLUE_SQL_MODELS.md) - SQL models
- [DYNAMODB_READ_MODELS.md](DYNAMODB_READ_MODELS.md) - DynamoDB schemas
- [LAMBDA_JOBS_FIRECRAWL_ORCHESTRATION.md](LAMBDA_JOBS_FIRECRAWL_ORCHESTRATION.md) - Lambda specifications
- [STEP_FUNCTIONS_EVENTBRIDGE.md](STEP_FUNCTIONS_EVENTBRIDGE.md) - Orchestration patterns
- [MODEL_AMS_SQL_CODE.md](MODEL_AMS_SQL_CODE.md) - AMS calculation logic

## 🆘 Support

For issues and questions:
1. Check the troubleshooting guide
2. Review CloudWatch logs and metrics
3. Use the monitoring scripts for real-time status
4. Consult the cost optimization guide for budget issues
5. Create an issue in the repository

---

**Deployment Status**: Ready for deployment  
**Last Updated**: $(date)  
**Version**: 1.0.0  
**Environment**: Production Ready
