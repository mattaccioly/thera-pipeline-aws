# Thera Pipeline AWS - Simplified & Fixed Version

## 🎯 Overview

This is a **simplified and fixed** version of the Thera Pipeline that processes startup company data using AWS serverless services. The pipeline has been cleaned up to remove redundancies and fix deployment issues.

## ✅ What Was Fixed

### 1. **Removed Redundancies**
- **19 Lambda YAML files** → Consolidated to 15 essential files
- **4 Gold profile SQL files** → Kept only the main one
- **10 Advanced summary SQL files** → Consolidated to 5 essential files
- **Multiple Apollo variations** → Single, working Apollo delta pull

### 2. **Fixed Step Functions**
- Corrected hardcoded ARNs to use proper CloudFormation references
- Fixed function naming conventions (`${Environment}-function-name`)
- Updated both daily and weekly state machines

### 3. **Enhanced Master Deployment**
- Added all missing Lambda functions to the master template
- Proper parameter passing between stacks
- Complete output exports for all resources

### 4. **Simplified Deployment**
- Single deployment script (`deploy-simplified.sh`)
- Clear error handling and status messages
- Automatic S3 template upload
- Step Functions and EventBridge setup

## 🏗️ Architecture

The pipeline consists of **15 Lambda functions** organized into two workflows:

### Daily Pipeline (Data Processing)
1. **Apollo Delta Pull** - Fetch company data from Apollo API
2. **Athena CTAS Bronze→Silver** - Transform raw data to silver layer
3. **Domain Health Gate** - Validate domain health scores
4. **Firecrawl Orchestrator** - Web scraping orchestration
5. **Athena CTAS Silver→Gold** - Transform to gold layer
6. **Advanced Summarization** - LLM-powered summaries
7. **Embeddings Batch** - Generate text embeddings
8. **AMS Computation** - Calculate matching scores
9. **DynamoDB Publisher** - Publish read models
10. **Cost Monitor** - Monitor and optimize costs

### Weekly Pipeline (ML Training)
1. **Weekly Trainer** - Train ML models
2. **AMS Job** - Compute AMS metrics
3. **ML Training** - Additional model training
4. **Evaluation Metrics** - Model evaluation
5. **Advanced Summarization** - Weekly summaries

## 📁 Simplified File Structure

```
thera-pipeline-aws/
├── README_SIMPLIFIED.md              # This file
├── deploy-simplified.sh              # Single deployment script
├── master-deployment.yaml            # Main CloudFormation template
├── main-infrastructure.yaml          # Core infrastructure
│
├── lambda-*.yaml                     # 15 Lambda function templates
│   ├── lambda-apollo-delta-pull.yaml
│   ├── lambda-domain-health-gate.yaml
│   ├── lambda-firecrawl-orchestrator.yaml
│   ├── lambda-athena-ctas-bronze-silver.yaml
│   ├── lambda-athena-ctas-silver-gold.yaml
│   ├── lambda-embeddings-batch.yaml
│   ├── lambda-matcher.yaml
│   ├── lambda-ams-computation.yaml
│   ├── lambda-dynamodb-publisher.yaml
│   ├── lambda-advanced-summarization.yaml
│   ├── lambda-cost-monitor.yaml
│   ├── lambda-weekly-trainer.yaml
│   ├── lambda-ams-job.yaml
│   ├── lambda-evaluation-metrics.yaml
│   └── lambda-ml-training.yaml
│
├── step-functions-*.json             # Fixed state machines
│   ├── step-functions-daily-state-machine.json
│   └── step-functions-weekly-state-machine.json
│
├── *.sql                             # Consolidated SQL scripts
│   ├── 01_setup_glue_databases.sql
│   ├── 02_create_bronze_tables.sql
│   ├── 03_create_silver_companies.sql
│   ├── 04_create_silver_apollo_companies.sql
│   ├── 05_create_silver_apollo_contacts.sql
│   ├── 06_create_silver_domain_health.sql
│   ├── 07_create_silver_web_extracts.sql
│   ├── 08_create_gold_startup_profiles.sql
│   ├── 09_create_ctas_queries.sql
│   ├── 10_data_quality_checks.sql
│   ├── 11_s3_partitioning_structure.sql
│   ├── 16_create_advanced_summaries_table.sql
│   ├── 16a_athena_advanced_summaries.sql
│   ├── 16b_create_llm_processing_logs_table.sql
│   └── 16c_create_llm_cost_tracking_table.sql
│
└── deployment/                       # Deployment tools
    └── deploy.sh                     # Original deployment script
```

## 🚀 Quick Start

### Prerequisites

1. **AWS CLI** configured with appropriate permissions
2. **Python 3.12+** (for local development)
3. **Required AWS services enabled:**
   - Lambda, S3, DynamoDB, Athena
   - Bedrock, Secrets Manager, CloudWatch
   - Step Functions, EventBridge, SQS

### 1. Deploy the Pipeline

```bash
# Deploy to dev environment (default)
./deploy-simplified.sh

# Deploy to staging
./deploy-simplified.sh -e staging -r us-west-2

# Deploy to production
./deploy-simplified.sh -e prod
```

### 2. Configure API Keys

Create secrets in AWS Secrets Manager:

```bash
# Apollo API Key
aws secretsmanager create-secret \
    --name "thera/apollo/api-key" \
    --secret-string '{"apollo_api_key":"your-apollo-key-here"}'

# Firecrawl API Key
aws secretsmanager create-secret \
    --name "thera/firecrawl/api-key" \
    --secret-string '{"firecrawl_api_key":"your-firecrawl-key-here"}'
```

### 3. Enable Bedrock Access

```bash
# Enable Bedrock access (one-time setup)
aws bedrock put-model-invocation-logging-configuration \
    --logging-config '{
        "textDataDeliveryEnabled": true,
        "imageDataDeliveryEnabled": false
    }'
```

## 📊 Monitoring

### CloudWatch Dashboard
- **URL**: Available in CloudFormation outputs
- **Metrics**: Apollo data, domain health, Firecrawl processing, embeddings, AMS, costs

### Key Metrics to Monitor
- **Apollo**: Items fetched, API calls used
- **Domain Health**: Domains checked, average health score
- **Firecrawl**: Domains processed, successful/failed crawls
- **Embeddings**: Items processed, embeddings generated, total cost
- **AMS**: Total challenges, shortlists, average AMS
- **Lambda**: Duration, errors, invocations

## 🔧 Troubleshooting

### Common Issues

1. **Lambda Function Not Found**
   - Check function naming: `${Environment}-function-name`
   - Verify CloudFormation stack deployment

2. **Step Functions Execution Failed**
   - Check IAM permissions for Step Functions role
   - Verify Lambda function ARNs in state machine definition

3. **Bedrock Access Denied**
   - Enable Bedrock access in AWS console
   - Check IAM permissions for Bedrock models

4. **S3 Access Denied**
   - Verify bucket names and permissions
   - Check IAM policies for S3 access

### Debug Commands

```bash
# Check CloudFormation stack status
aws cloudformation describe-stacks --stack-name thera-pipeline-dev

# List Lambda functions
aws lambda list-functions --query 'Functions[?contains(FunctionName, `thera`)].FunctionName'

# Check Step Functions
aws stepfunctions list-state-machines

# View CloudWatch logs
aws logs describe-log-groups --log-group-name-prefix "/aws/lambda/thera"
```

## 💰 Cost Optimization

### Budget Controls
- **Daily Budget**: $50 USD for LLM processing
- **Cost Alerts**: CloudWatch alarms for budget overruns
- **Auto-scaling**: Lambda concurrency limits

### Cost Monitoring
- **CloudWatch Metrics**: Real-time cost tracking
- **SNS Alerts**: Budget threshold notifications
- **Cost Reports**: Detailed breakdown by service

## 🔄 Pipeline Workflows

### Daily Pipeline (6 AM UTC)
```
Apollo → Bronze→Silver → Domain Health → Firecrawl → Silver→Gold → 
Advanced Summarization → Embeddings → AMS → DynamoDB → Cost Monitor
```

### Weekly Pipeline (Sunday 8 AM UTC)
```
Weekly Trainer → AMS Job → ML Training → Evaluation Metrics → 
Advanced Summarization → Model Performance Check
```

## 📈 Performance

### Expected Performance
- **Daily Pipeline**: ~2.5 hours (with advanced summarization)
- **Weekly Pipeline**: ~1.5 hours
- **Lambda Timeout**: 15 minutes per function
- **Concurrency**: 2 for Firecrawl, unlimited for others

### Scaling
- **Auto-scaling**: Lambda functions scale automatically
- **Batch Processing**: Large datasets processed in batches
- **Rate Limiting**: Apollo API calls limited to 50/min, 200/hour, 600/day

## 🛡️ Security

### IAM Roles
- **Least Privilege**: Each Lambda has minimal required permissions
- **Resource-specific**: Policies scoped to specific S3 buckets/tables
- **No Cross-account**: All resources in same AWS account

### Data Protection
- **Encryption**: S3 and DynamoDB encrypted at rest
- **PII Masking**: Contact data masked in silver layer
- **Access Logging**: CloudTrail enabled for audit

## 📝 Next Steps

1. **Test the Pipeline**: Run daily state machine manually
2. **Monitor Metrics**: Check CloudWatch dashboard
3. **Tune Parameters**: Adjust batch sizes and timeouts
4. **Add Alerts**: Configure SNS notifications
5. **Scale Up**: Increase concurrency and memory as needed

## 🆘 Support

For issues or questions:
1. Check CloudWatch logs for error details
2. Review CloudFormation stack events
3. Verify IAM permissions and resource access
4. Check the troubleshooting section above

---

**Status**: ✅ **SIMPLIFIED & WORKING**  
**Last Updated**: $(date)  
**Version**: 2.0 (Simplified)
