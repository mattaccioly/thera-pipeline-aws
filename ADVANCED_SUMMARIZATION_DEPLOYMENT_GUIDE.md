# Advanced Summarization Pipeline - Deployment Guide

## 🎯 Overview

This guide provides step-by-step instructions for deploying the Advanced Summarization Pipeline, which adds intelligent LLM-powered business intelligence summaries to the existing Thera Pipeline.

## 🏗️ Architecture Summary

The Advanced Summarization Pipeline includes:
- **Executive Summaries**: 2-3 sentence company overviews
- **Key Insights**: Business intelligence extraction
- **Technology Stack Analysis**: Tech capabilities assessment
- **Competitive Analysis**: Market positioning insights
- **Risk Assessment**: Due diligence analysis
- **Business Intelligence**: Strategic insights and recommendations

## 📋 Prerequisites

### 1. AWS Services Required
- ✅ AWS Bedrock (Claude 3 Haiku & Sonnet)
- ✅ AWS Lambda (Python 3.12)
- ✅ AWS DynamoDB (for caching)
- ✅ AWS Athena (for data queries)
- ✅ AWS S3 (for data storage)
- ✅ AWS Step Functions (for orchestration)
- ✅ AWS CloudWatch (for monitoring)

### 2. Permissions Required
- Bedrock model access (request access if needed)
- Lambda execution permissions
- DynamoDB read/write access
- Athena query permissions
- S3 read/write access
- CloudWatch metrics permissions

### 3. Cost Considerations
- **Daily Budget**: $50 (configurable)
- **Monthly Estimated Cost**: ~$1,500 (with optimizations)
- **Cost per Company**: ~$0.026 (with caching)

## 🚀 Deployment Steps

### Step 1: Deploy Database Schema

```bash
# Deploy the advanced summarization schema
aws athena start-query-execution \
  --query-string "file://16_create_advanced_summarization_schema.sql" \
  --work-group primary \
  --result-configuration OutputLocation=s3://thera-curated/athena-results/
```

### Step 2: Deploy Lambda Functions

#### 2.1 Advanced Summarization Lambda
```bash
# Deploy the main summarization function
aws cloudformation deploy \
  --template-file lambda-advanced-summarization.yaml \
  --stack-name thera-advanced-summarization \
  --parameter-overrides \
    Environment=dev \
    CuratedBucket=thera-curated-805595753342-v3 \
    AthenaDatabase=thera_gold \
    DailyBudgetUSD=50.0 \
    MaxDailyCompanies=1000
```

#### 2.2 Cost Monitor Lambda
```bash
# Deploy the cost monitoring function
aws cloudformation deploy \
  --template-file lambda-cost-monitor.yaml \
  --stack-name thera-cost-monitor \
  --parameter-overrides \
    Environment=dev \
    DailyBudgetUSD=50.0
```

### Step 3: Update Step Functions

#### 3.1 Deploy Enhanced Daily Pipeline
```bash
# Deploy the enhanced daily state machine
aws stepfunctions create-state-machine \
  --name thera-daily-pipeline-enhanced \
  --definition file://step-functions-daily-state-machine-enhanced.json \
  --role-arn arn:aws:iam::ACCOUNT_ID:role/thera-step-functions-role
```

#### 3.2 Update EventBridge Rules
```bash
# Update the daily trigger to use the enhanced pipeline
aws events put-rule \
  --name thera-daily-trigger-enhanced \
  --schedule-expression "cron(0 6 * * ? *)" \
  --state ENABLED

aws events put-targets \
  --rule thera-daily-trigger-enhanced \
  --targets "Id"="1","Arn"="arn:aws:states:REGION:ACCOUNT_ID:stateMachine:thera-daily-pipeline-enhanced"
```

### Step 4: Configure Bedrock Access

#### 4.1 Request Model Access
```bash
# Request access to Claude models (if not already granted)
aws bedrock put-model-invocation-logging-configuration \
  --logging-config '{
    "textDataDeliveryEnabled": true,
    "imageDataDeliveryEnabled": false,
    "embeddingDataDeliveryEnabled": false
  }'
```

#### 4.2 Test Bedrock Access
```bash
# Test Bedrock access
python3 test_advanced_summarization.py
```

### Step 5: Set Up Monitoring

#### 5.1 Create CloudWatch Dashboard
```bash
# The dashboard is automatically created by the CloudFormation template
# Access it via: AWS Console > CloudWatch > Dashboards > thera-advanced-summarization
```

#### 5.2 Configure Alerts
```bash
# Set up SNS topics for alerts
aws sns create-topic --name thera-llm-cost-alerts
aws sns create-topic --name thera-llm-error-alerts

# Subscribe to alerts (replace with your email)
aws sns subscribe \
  --topic-arn arn:aws:sns:REGION:ACCOUNT_ID:thera-llm-cost-alerts \
  --protocol email \
  --notification-endpoint your-email@example.com
```

### Step 6: Test the Pipeline

#### 6.1 Run Test Suite
```bash
# Run comprehensive tests
python3 test_advanced_summarization.py

# Check test results
cat advanced_summarization_test_results.json
```

#### 6.2 Manual Testing
```bash
# Test individual Lambda function
aws lambda invoke \
  --function-name dev-advanced-summarization \
  --payload '{}' \
  response.json

# Check response
cat response.json
```

## 🔧 Configuration

### Environment Variables

| Variable | Description | Default Value |
|----------|-------------|---------------|
| `CURATED_BUCKET` | S3 bucket for curated data | `thera-curated-805595753342-v3` |
| `ATHENA_DATABASE` | Athena database name | `thera_gold` |
| `ATHENA_WORKGROUP` | Athena workgroup | `primary` |
| `DYNAMODB_TABLE` | DynamoDB cache table | `thera-advanced-summaries` |
| `DAILY_BUDGET_USD` | Daily budget limit | `50.0` |
| `MAX_BATCH_SIZE` | Maximum batch size | `10` |
| `MAX_DAILY_COMPANIES` | Daily processing limit | `1000` |

### Cost Configuration

| Setting | Value | Description |
|---------|-------|-------------|
| Claude 3 Haiku Input | $0.25/1M tokens | Cost-effective for simple tasks |
| Claude 3 Haiku Output | $1.25/1M tokens | Used for 80% of tasks |
| Claude 3 Sonnet Input | $3.00/1M tokens | Used for complex analysis |
| Claude 3 Sonnet Output | $15.00/1M tokens | Used for 20% of tasks |
| Daily Budget | $50.00 | Configurable limit |
| Cache TTL | 7 days | Reduces re-processing costs |

## 📊 Monitoring and Alerts

### Key Metrics to Monitor

1. **Processing Volume**
   - Companies processed per day
   - Summaries generated per day
   - Cache hit rate

2. **Cost Metrics**
   - Daily cost vs budget
   - Cost per company processed
   - Token usage by model

3. **Quality Metrics**
   - Summary quality scores
   - Processing success rate
   - Error rates

4. **Performance Metrics**
   - Processing time per company
   - Lambda execution duration
   - API response times

### Alert Thresholds

| Metric | Warning | Critical | Action |
|--------|---------|----------|--------|
| Daily Cost | 80% of budget | 100% of budget | Send alert, consider throttling |
| Error Rate | 5% | 10% | Investigate and fix |
| Processing Time | 30s per company | 60s per company | Check performance |
| Cache Hit Rate | <50% | <30% | Optimize caching |

## 🔍 Troubleshooting

### Common Issues

#### 1. Bedrock Access Denied
```bash
# Check Bedrock permissions
aws bedrock list-foundation-models

# Request model access if needed
aws bedrock put-model-invocation-logging-configuration \
  --logging-config '{"textDataDeliveryEnabled": true}'
```

#### 2. High Costs
```bash
# Check cost tracking
aws dynamodb get-item \
  --table-name thera-llm-cost-tracking \
  --key '{"date": {"S": "2024-01-15"}}'

# Adjust daily budget
aws ssm put-parameter \
  --name "/thera/llm/daily_budget" \
  --value "25.0" \
  --overwrite
```

#### 3. Poor Quality Summaries
```bash
# Check prompt templates
aws s3 cp s3://thera-curated/prompts/executive_summary.txt .

# Update prompts if needed
aws s3 cp executive_summary.txt s3://thera-curated/prompts/
```

#### 4. Cache Issues
```bash
# Check cache statistics
aws dynamodb scan \
  --table-name thera-advanced-summaries \
  --select COUNT

# Clear expired cache
aws lambda invoke \
  --function-name dev-cost-monitor \
  --payload '{"action": "cleanup_cache"}'
```

### Debug Commands

```bash
# Check Lambda logs
aws logs describe-log-groups --log-group-name-prefix "/aws/lambda/dev-advanced-summarization"

# Get recent logs
aws logs filter-log-events \
  --log-group-name "/aws/lambda/dev-advanced-summarization" \
  --start-time $(date -d '1 hour ago' +%s)000

# Check Step Functions execution
aws stepfunctions list-executions \
  --state-machine-arn arn:aws:states:REGION:ACCOUNT_ID:stateMachine:thera-daily-pipeline-enhanced
```

## 📈 Optimization Recommendations

### 1. Cost Optimization
- **Increase Cache Hit Rate**: Optimize content hashing
- **Batch Processing**: Process similar companies together
- **Model Selection**: Use Haiku for 80% of tasks
- **Prompt Optimization**: Reduce token usage

### 2. Performance Optimization
- **Parallel Processing**: Increase Lambda concurrency
- **Memory Optimization**: Tune Lambda memory settings
- **Database Indexing**: Optimize DynamoDB queries
- **S3 Optimization**: Use appropriate storage classes

### 3. Quality Optimization
- **Prompt Engineering**: Continuously improve prompts
- **Validation Rules**: Add quality checks
- **Human Review**: Implement review workflows
- **A/B Testing**: Test different approaches

## 🎯 Success Metrics

### Deployment Success Criteria
- ✅ All Lambda functions deployed successfully
- ✅ Bedrock access working
- ✅ Test suite passes with >80% success rate
- ✅ Cost monitoring active
- ✅ Alerts configured and working

### Operational Success Criteria
- 📊 Process 1000+ companies daily
- 💰 Maintain costs under $50/day
- ⚡ Process each company in <30 seconds
- 🎯 Achieve >80% cache hit rate
- 📈 Generate high-quality summaries

## 🔄 Maintenance

### Daily Tasks
- Monitor cost metrics
- Check error rates
- Review cache performance
- Validate summary quality

### Weekly Tasks
- Analyze cost trends
- Optimize prompts
- Update cache strategies
- Review alert thresholds

### Monthly Tasks
- Comprehensive cost analysis
- Performance optimization
- Quality assessment
- Architecture review

## 📞 Support

For issues or questions:
1. Check CloudWatch logs
2. Review this troubleshooting guide
3. Run the test suite
4. Check AWS service health
5. Contact the development team

## 🎉 Conclusion

The Advanced Summarization Pipeline is now ready for deployment! This system will transform basic profile text into comprehensive business intelligence summaries, providing valuable insights for startup analysis and investment decisions.

**Key Benefits:**
- 🧠 Intelligent summarization using Claude 3
- 💰 Cost-optimized with caching and batch processing
- 📊 Comprehensive business intelligence
- 🔍 Risk assessment and competitive analysis
- 📈 Investment readiness scoring
- 🚀 Scalable and maintainable architecture

The pipeline is designed to be cost-efficient, reliable, and maintainable while delivering high-quality business intelligence summaries for your startup data.
