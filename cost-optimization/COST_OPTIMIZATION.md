# Thera Pipeline Cost Optimization Guide

This guide provides strategies and best practices for optimizing costs in the Thera Pipeline orchestration system.

## Table of Contents

1. [Cost Overview](#cost-overview)
2. [Lambda Cost Optimization](#lambda-cost-optimization)
3. [Step Functions Cost Optimization](#step-functions-cost-optimization)
4. [Athena Cost Optimization](#athena-cost-optimization)
5. [S3 Cost Optimization](#s3-cost-optimization)
6. [DynamoDB Cost Optimization](#dynamodb-cost-optimization)
7. [EventBridge Cost Optimization](#eventbridge-cost-optimization)
8. [Monitoring and Alerting](#monitoring-and-alerting)
9. [Cost Optimization Strategies](#cost-optimization-strategies)
10. [Budget Management](#budget-management)

## Cost Overview

### Estimated Monthly Costs

| Service | Estimated Cost | Notes |
|---------|----------------|-------|
| Lambda | $50-200 | Depends on execution frequency and memory |
| Step Functions | $25-100 | Based on state transitions |
| Athena | $5-50 | Based on data scanned |
| S3 | $10-100 | Based on storage and requests |
| DynamoDB | $25-150 | Based on read/write capacity |
| EventBridge | $1-10 | Based on rule executions |
| **Total** | **$116-610** | **Monthly estimate** |

### Cost Drivers

1. **Lambda Functions:**
   - Execution time
   - Memory allocation
   - Number of invocations
   - Cold starts

2. **Step Functions:**
   - State transitions
   - Execution duration
   - Retry attempts

3. **Athena:**
   - Data scanned
   - Query complexity
   - Data format

4. **S3:**
   - Storage amount
   - Request frequency
   - Storage class

5. **DynamoDB:**
   - Read/write capacity
   - Storage amount
   - On-demand vs provisioned

## Lambda Cost Optimization

### Memory Allocation

**Current Configuration:**
```yaml
# Example: Apollo Delta Pull Lambda
MemorySize: 512  # MB
Timeout: 300     # seconds
```

**Optimization Strategies:**

1. **Right-size Memory:**
   ```bash
   # Monitor memory usage
   aws cloudwatch get-metric-statistics \
       --namespace AWS/Lambda \
       --metric-name MaxMemoryUsed \
       --dimensions Name=FunctionName,Value="thera-pipeline-prod-apollo-delta-pull" \
       --start-time 2024-01-01T00:00:00Z \
       --end-time 2024-01-02T00:00:00Z \
       --period 300 \
       --statistics Average,Maximum \
       --region us-east-1
   ```

2. **Memory Recommendations:**
   - Start with 512MB
   - Monitor CloudWatch metrics
   - Adjust based on actual usage
   - Consider 256MB for simple functions

3. **Cost Impact:**
   - 256MB: $0.0000004167 per GB-second
   - 512MB: $0.0000008333 per GB-second
   - 1024MB: $0.0000016667 per GB-second

### Execution Time Optimization

**Strategies:**

1. **Code Optimization:**
   ```python
   # Use connection pooling
   import boto3
   from botocore.config import Config
   
   # Reuse connections
   s3_client = boto3.client('s3', config=Config(
       max_pool_connections=50,
       retries={'max_attempts': 3}
   ))
   ```

2. **Parallel Processing:**
   ```python
   # Use concurrent.futures for parallel operations
   import concurrent.futures
   
   def process_items_parallel(items):
       with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
           futures = [executor.submit(process_item, item) for item in items]
           results = [future.result() for future in futures]
       return results
   ```

3. **Caching:**
   ```python
   # Cache frequently accessed data
   import json
   import os
   
   def get_cached_data(key):
       cache_file = f"/tmp/{key}.json"
       if os.path.exists(cache_file):
           with open(cache_file, 'r') as f:
               return json.load(f)
       return None
   ```

### Provisioned Concurrency

**When to Use:**
- Functions with consistent load
- Critical business functions
- Functions with cold start issues

**Configuration:**
```yaml
# CloudFormation template
ProvisionedConcurrencyConfig:
  ProvisionedConcurrencyConfig:
    ProvisionedConcurrencyCount: 2
    FunctionName: !Ref FunctionName
    Qualifier: !GetAtt FunctionVersion.Version
```

**Cost Impact:**
- Provisioned concurrency: $0.0000041667 per GB-second
- Only pay for allocated capacity
- Use for predictable workloads

## Step Functions Cost Optimization

### State Transition Optimization

**Current Configuration:**
```json
{
  "Comment": "Daily Pipeline",
  "StartAt": "ApolloDeltaPull",
  "States": {
    "ApolloDeltaPull": {
      "Type": "Task",
      "Resource": "arn:aws:lambda:us-east-1:ACCOUNT:function:thera-pipeline-prod-apollo-delta-pull",
      "Next": "AthenaCTASBronzeSilver"
    }
  }
}
```

**Optimization Strategies:**

1. **Minimize State Transitions:**
   - Combine related tasks
   - Use parallel states efficiently
   - Avoid unnecessary state transitions

2. **Use Map State Efficiently:**
   ```json
   {
     "Type": "Map",
     "ItemsPath": "$.domains",
     "MaxConcurrency": 2,
     "Iterator": {
       "StartAt": "ProcessDomain",
       "States": {
         "ProcessDomain": {
           "Type": "Task",
           "Resource": "arn:aws:lambda:us-east-1:ACCOUNT:function:thera-pipeline-prod-firecrawl-orchestrator",
           "End": true
         }
       }
     }
   }
   ```

3. **Optimize Retry Logic:**
   ```json
   {
     "Retry": [
       {
         "ErrorEquals": ["States.ALL"],
         "IntervalSeconds": 2,
         "MaxAttempts": 3,
         "BackoffRate": 2.0
       }
     ]
   }
   ```

### Cost Monitoring

**Track Costs:**
```bash
# Get Step Functions metrics
aws cloudwatch get-metric-statistics \
    --namespace AWS/States \
    --metric-name ExecutionsStarted \
    --start-time 2024-01-01T00:00:00Z \
    --end-time 2024-01-02T00:00:00Z \
    --period 300 \
    --statistics Sum \
    --region us-east-1
```

## Athena Cost Optimization

### Data Format Optimization

**Current Configuration:**
- Format: JSON
- Compression: None
- Partitioning: None

**Optimization Strategies:**

1. **Use Parquet Format:**
   ```sql
   -- Convert to Parquet
   CREATE TABLE silver_companies_parquet
   WITH (
     format = 'PARQUET',
     parquet_compression = 'SNAPPY'
   ) AS
   SELECT * FROM silver_companies;
   ```

2. **Implement Partitioning:**
   ```sql
   -- Partition by date
   CREATE TABLE silver_companies_partitioned
   WITH (
     format = 'PARQUET',
     parquet_compression = 'SNAPPY',
     partitioned_by = ARRAY['year', 'month', 'day']
   ) AS
   SELECT 
     *,
     year(date_created) as year,
     month(date_created) as month,
     day(date_created) as day
   FROM silver_companies;
   ```

3. **Use Columnar Storage:**
   - Parquet: 60-80% cost reduction
   - ORC: 50-70% cost reduction
   - Compression: 70-90% cost reduction

### Query Optimization

**Strategies:**

1. **Limit Data Scanned:**
   ```sql
   -- Use WHERE clauses
   SELECT * FROM silver_companies 
   WHERE date_created >= '2024-01-01'
   AND date_created < '2024-02-01';
   ```

2. **Use Appropriate Data Types:**
   ```sql
   -- Use appropriate data types
   CREATE TABLE optimized_table (
     id BIGINT,
     name VARCHAR(255),
     created_date DATE,
     is_active BOOLEAN
   );
   ```

3. **Use CTAS for Transformations:**
   ```sql
   -- Use CTAS instead of multiple queries
   CREATE TABLE silver_companies
   WITH (
     format = 'PARQUET',
     parquet_compression = 'SNAPPY'
   ) AS
   SELECT 
     id,
     name,
     domain,
     industry,
     created_date
   FROM bronze_companies
   WHERE domain IS NOT NULL;
   ```

### Cost Monitoring

**Track Data Scanned:**
```bash
# Get Athena metrics
aws cloudwatch get-metric-statistics \
    --namespace AWS/Athena \
    --metric-name DataScannedInBytes \
    --start-time 2024-01-01T00:00:00Z \
    --end-time 2024-01-02T00:00:00Z \
    --period 300 \
    --statistics Sum \
    --region us-east-1
```

## S3 Cost Optimization

### Storage Class Optimization

**Current Configuration:**
- Storage Class: Standard
- Lifecycle: None
- Versioning: Enabled

**Optimization Strategies:**

1. **Implement Lifecycle Policies:**
   ```json
   {
     "Rules": [
       {
         "Id": "TheraPipelineLifecycle",
         "Status": "Enabled",
         "Transitions": [
           {
             "Days": 30,
             "StorageClass": "STANDARD_IA"
           },
           {
             "Days": 90,
             "StorageClass": "GLACIER"
           },
           {
             "Days": 365,
             "StorageClass": "DEEP_ARCHIVE"
           }
         ]
       }
     ]
   }
   ```

2. **Use Appropriate Storage Classes:**
   - Standard: Frequently accessed data
   - Standard-IA: Infrequently accessed data
   - Glacier: Archive data
   - Deep Archive: Long-term archive

3. **Cost Comparison:**
   - Standard: $0.023 per GB/month
   - Standard-IA: $0.0125 per GB/month
   - Glacier: $0.004 per GB/month
   - Deep Archive: $0.00099 per GB/month

### Request Optimization

**Strategies:**

1. **Batch Operations:**
   ```python
   # Use batch operations
   import boto3
   
   def batch_upload_files(files, bucket, prefix):
       s3_client = boto3.client('s3')
       for file in files:
           s3_client.upload_file(file, bucket, f"{prefix}/{file}")
   ```

2. **Use Transfer Acceleration:**
   ```python
   # Enable transfer acceleration
   s3_client = boto3.client('s3', 
       config=Config(
           s3={
               'use_accelerate_endpoint': True
           }
       )
   )
   ```

3. **Optimize Object Sizes:**
   - Use multipart upload for large files
   - Compress data before upload
   - Use appropriate chunk sizes

## DynamoDB Cost Optimization

### Capacity Mode Optimization

**Current Configuration:**
- Capacity Mode: On-demand
- Read Capacity: Auto-scaling
- Write Capacity: Auto-scaling

**Optimization Strategies:**

1. **Use Provisioned Capacity for Predictable Workloads:**
   ```yaml
   # CloudFormation template
   BillingMode: PROVISIONED
   ProvisionedThroughput:
     ReadCapacityUnits: 10
     WriteCapacityUnits: 5
   ```

2. **Implement Auto-scaling:**
   ```yaml
   # Auto-scaling configuration
   AutoScalingTargets:
     - TargetValue: 70.0
       ScaleInCooldown: 300
       ScaleOutCooldown: 300
       MinCapacity: 5
       MaxCapacity: 50
   ```

3. **Cost Comparison:**
   - On-demand: $1.25 per million read requests
   - Provisioned: $0.25 per million read requests (with 1-year commitment)

### Data Modeling Optimization

**Strategies:**

1. **Optimize Partition Key Design:**
   ```python
   # Use composite keys
   partition_key = f"{company_id}#{date}"
   sort_key = f"{timestamp}#{event_type}"
   ```

2. **Use Global Secondary Indexes Efficiently:**
   ```yaml
   # GSI configuration
   GlobalSecondaryIndexes:
     - IndexName: "CompanyDateIndex"
       KeySchema:
         - AttributeName: "company_id"
           KeyType: "HASH"
         - AttributeName: "date"
           KeyType: "RANGE"
       Projection:
         ProjectionType: "INCLUDE"
         NonKeyAttributes: ["name", "domain"]
   ```

3. **Implement Caching:**
   ```python
   # Use DAX for caching
   import boto3
   from boto3.dynamodb.conditions import Key
   
   # DAX client
   dax_client = boto3.client('dax', region_name='us-east-1')
   ```

## EventBridge Cost Optimization

### Rule Optimization

**Current Configuration:**
- Rules: 2 (daily and weekly)
- Targets: Step Functions
- Schedule: Cron expressions

**Optimization Strategies:**

1. **Minimize Rule Executions:**
   - Use appropriate schedules
   - Avoid overlapping rules
   - Use event filtering

2. **Optimize Target Configuration:**
   ```yaml
   # EventBridge rule
   Targets:
     - Arn: !GetAtt DailyStateMachine.Arn
       Id: "DailyPipelineTarget"
       Input: '{"environment": "prod"}'
   ```

3. **Cost Monitoring:**
   ```bash
   # Get EventBridge metrics
   aws cloudwatch get-metric-statistics \
       --namespace AWS/Events \
       --metric-name SuccessfulInvocations \
       --start-time 2024-01-01T00:00:00Z \
       --end-time 2024-01-02T00:00:00Z \
       --period 300 \
       --statistics Sum \
       --region us-east-1
   ```

## Monitoring and Alerting

### Cost Alerts

**CloudWatch Alarms:**
```yaml
# Cost alarm
CostAlarm:
  Type: AWS::CloudWatch::Alarm
  Properties:
    AlarmName: "TheraPipelineCostAlarm"
    MetricName: "EstimatedCharges"
    Namespace: "AWS/Billing"
    Statistic: Maximum
    Period: 86400
    EvaluationPeriods: 1
    Threshold: 500
    ComparisonOperator: GreaterThanThreshold
    Dimensions:
      - Name: Currency
        Value: USD
```

**Budget Alerts:**
```yaml
# Budget configuration
Budget:
  Type: AWS::Budgets::Budget
  Properties:
    Budget:
      BudgetName: "TheraPipelineBudget"
      BudgetLimit:
        Amount: 1000
        Unit: USD
      TimeUnit: MONTHLY
      BudgetType: COST
      CostFilters:
        Tag:
          - Key: "Project"
            Values: ["TheraPipeline"]
```

### Cost Monitoring Dashboard

**CloudWatch Dashboard:**
```json
{
  "widgets": [
    {
      "type": "metric",
      "properties": {
        "metrics": [
          ["AWS/Lambda", "Duration", "FunctionName", "thera-pipeline-prod-apollo-delta-pull"],
          ["AWS/States", "ExecutionsStarted", "StateMachineArn", "STATE_MACHINE_ARN"]
        ],
        "period": 300,
        "stat": "Average",
        "region": "us-east-1",
        "title": "Pipeline Performance"
      }
    }
  ]
}
```

## Cost Optimization Strategies

### Right-sizing Resources

1. **Lambda Functions:**
   - Monitor memory usage
   - Adjust based on actual needs
   - Use provisioned concurrency selectively

2. **DynamoDB:**
   - Use on-demand for variable workloads
   - Use provisioned for predictable workloads
   - Implement auto-scaling

3. **S3:**
   - Use appropriate storage classes
   - Implement lifecycle policies
   - Optimize object sizes

### Automation

1. **Auto-scaling:**
   - DynamoDB auto-scaling
   - Lambda provisioned concurrency
   - S3 lifecycle policies

2. **Scheduled Optimization:**
   - Regular cost reviews
   - Automated resource cleanup
   - Performance monitoring

3. **Cost Allocation:**
   - Tag resources
   - Use cost allocation tags
   - Implement chargeback

### Best Practices

1. **Regular Monitoring:**
   - Daily cost reviews
   - Weekly optimization checks
   - Monthly budget reviews

2. **Resource Cleanup:**
   - Remove unused resources
   - Clean up old data
   - Optimize storage

3. **Performance Tuning:**
   - Optimize code
   - Use appropriate data formats
   - Implement caching

## Budget Management

### Budget Configuration

**Monthly Budget:**
```yaml
# Budget template
MonthlyBudget:
  Type: AWS::Budgets::Budget
  Properties:
    Budget:
      BudgetName: "TheraPipelineMonthlyBudget"
      BudgetLimit:
        Amount: 1000
        Unit: USD
      TimeUnit: MONTHLY
      BudgetType: COST
      CostFilters:
        Service:
          - "Amazon Lambda"
          - "AWS Step Functions"
          - "Amazon Athena"
          - "Amazon S3"
          - "Amazon DynamoDB"
          - "Amazon EventBridge"
```

**Alert Thresholds:**
```yaml
# Alert configuration
BudgetAlerts:
  - Type: ACTUAL
    Threshold: 80
    ThresholdType: PERCENTAGE
  - Type: FORECASTED
    Threshold: 100
    ThresholdType: PERCENTAGE
```

### Cost Tracking

**Cost Allocation Tags:**
```yaml
# Tag configuration
Tags:
  - Key: "Project"
    Value: "TheraPipeline"
  - Key: "Environment"
    Value: "prod"
  - Key: "CostCenter"
    Value: "DataEngineering"
  - Key: "Owner"
    Value: "DataTeam"
```

**Cost Reports:**
```bash
# Generate cost report
aws ce get-cost-and-usage \
    --time-period Start=2024-01-01,End=2024-01-31 \
    --granularity MONTHLY \
    --metrics BlendedCost \
    --group-by Type=DIMENSION,Key=SERVICE \
    --region us-east-1
```

### Optimization Recommendations

1. **Immediate Actions:**
   - Implement S3 lifecycle policies
   - Right-size Lambda memory
   - Use Parquet format for Athena

2. **Short-term (1-3 months):**
   - Implement DynamoDB auto-scaling
   - Optimize Step Functions state machine
   - Add cost monitoring dashboards

3. **Long-term (3-6 months):**
   - Implement comprehensive cost allocation
   - Optimize data architecture
   - Implement automated cost optimization

---

For additional cost optimization strategies, refer to the AWS Well-Architected Framework and AWS Cost Optimization best practices.
