# Thera Pipeline Troubleshooting Guide

This guide helps diagnose and resolve common issues with the Thera Pipeline orchestration system.

## Table of Contents

1. [Common Issues](#common-issues)
2. [Step Functions Issues](#step-functions-issues)
3. [Lambda Function Issues](#lambda-function-issues)
4. [Athena Issues](#athena-issues)
5. [S3 Issues](#s3-issues)
6. [DynamoDB Issues](#dynamodb-issues)
7. [EventBridge Issues](#eventbridge-issues)
8. [Monitoring and Debugging](#monitoring-and-debugging)
9. [Performance Optimization](#performance-optimization)
10. [Emergency Procedures](#emergency-procedures)

## Common Issues

### Pipeline Not Starting

**Symptoms:**
- No Step Functions executions visible
- EventBridge rules not triggering
- No CloudWatch logs

**Diagnosis:**
```bash
# Check EventBridge rules
aws events list-rules --region us-east-1

# Check Step Functions state machines
aws stepfunctions list-state-machines --region us-east-1

# Check CloudWatch logs
aws logs describe-log-groups --log-group-name-prefix "/aws/lambda/thera-pipeline" --region us-east-1
```

**Solutions:**
1. Verify EventBridge rules are enabled
2. Check timezone settings (America/Sao_Paulo)
3. Verify IAM permissions
4. Check CloudFormation stack status

### API Key Issues

**Symptoms:**
- Lambda functions failing with authentication errors
- 401/403 errors in logs
- "Invalid API key" messages

**Diagnosis:**
```bash
# Check SSM parameters
aws ssm get-parameter --name "/thera/apollo/api_key" --with-decryption --region us-east-1
aws ssm get-parameter --name "/thera/firecrawl/api_key" --with-decryption --region us-east-1
```

**Solutions:**
1. Verify API keys are correctly stored in SSM Parameter Store
2. Check Lambda function has permissions to decrypt parameters
3. Update API keys if expired
4. Verify API key format and permissions

## Step Functions Issues

### Execution Failures

**Symptoms:**
- Step Functions executions showing "Failed" status
- Error messages in execution history
- Tasks not completing

**Diagnosis:**
```bash
# Get execution details
aws stepfunctions describe-execution \
    --execution-arn "arn:aws:states:us-east-1:ACCOUNT:execution:STATE_MACHINE_NAME:EXECUTION_NAME" \
    --region us-east-1

# Get execution history
aws stepfunctions get-execution-history \
    --execution-arn "arn:aws:states:us-east-1:ACCOUNT:execution:STATE_MACHINE_NAME:EXECUTION_NAME" \
    --region us-east-1
```

**Common Causes:**
1. Lambda function timeouts
2. IAM permission issues
3. Resource limits exceeded
4. Invalid input parameters

**Solutions:**
1. Increase Lambda timeout settings
2. Review and update IAM permissions
3. Check resource quotas
4. Validate input parameters

### State Machine Definition Issues

**Symptoms:**
- State machine creation fails
- Invalid JSON errors
- State machine not starting

**Diagnosis:**
```bash
# Validate state machine definition
aws stepfunctions validate-state-machine-definition \
    --definition file://step-functions-daily-state-machine.json
```

**Solutions:**
1. Validate JSON syntax
2. Check state machine definition against ASL specification
3. Verify all referenced resources exist
4. Test with simple input

## Lambda Function Issues

### Function Timeouts

**Symptoms:**
- Lambda functions timing out
- "Task timed out" errors
- Incomplete data processing

**Diagnosis:**
```bash
# Check function configuration
aws lambda get-function-configuration \
    --function-name "thera-pipeline-prod-apollo-delta-pull" \
    --region us-east-1

# Check CloudWatch logs
aws logs filter-log-events \
    --log-group-name "/aws/lambda/thera-pipeline-prod-apollo-delta-pull" \
    --filter-pattern "TIMEOUT" \
    --region us-east-1
```

**Solutions:**
1. Increase timeout settings (max 15 minutes)
2. Optimize function code
3. Implement pagination for large datasets
4. Use Step Functions for long-running tasks

### Memory Issues

**Symptoms:**
- Out of memory errors
- Function crashes
- High memory usage

**Diagnosis:**
```bash
# Check function metrics
aws cloudwatch get-metric-statistics \
    --namespace AWS/Lambda \
    --metric-name Duration \
    --dimensions Name=FunctionName,Value="thera-pipeline-prod-apollo-delta-pull" \
    --start-time 2024-01-01T00:00:00Z \
    --end-time 2024-01-02T00:00:00Z \
    --period 300 \
    --statistics Average,Maximum \
    --region us-east-1
```

**Solutions:**
1. Increase memory allocation
2. Optimize data processing
3. Implement streaming for large datasets
4. Use container-based Lambda for ML tasks

### Cold Start Issues

**Symptoms:**
- Slow function startup
- High latency for first requests
- Timeout on first execution

**Solutions:**
1. Use provisioned concurrency
2. Optimize function package size
3. Use container-based Lambda for consistent performance
4. Implement warm-up strategies

## Athena Issues

### Query Failures

**Symptoms:**
- Athena queries failing
- "Query failed" errors
- No results returned

**Diagnosis:**
```bash
# Check query execution
aws athena get-query-execution \
    --query-execution-id "QUERY_ID" \
    --region us-east-1

# Check query results
aws athena get-query-results \
    --query-execution-id "QUERY_ID" \
    --region us-east-1
```

**Common Causes:**
1. Invalid SQL syntax
2. Missing tables or columns
3. S3 permissions issues
4. Query timeout

**Solutions:**
1. Validate SQL syntax
2. Check table schemas
3. Verify S3 permissions
4. Increase query timeout
5. Use CTAS for large transformations

### Performance Issues

**Symptoms:**
- Slow query execution
- High costs
- Query timeouts

**Solutions:**
1. Use columnar formats (Parquet)
2. Implement partitioning
3. Optimize SQL queries
4. Use appropriate data types
5. Consider data compression

## S3 Issues

### Access Denied

**Symptoms:**
- 403 Forbidden errors
- "Access Denied" messages
- Lambda functions can't read/write S3

**Diagnosis:**
```bash
# Check S3 bucket policy
aws s3api get-bucket-policy --bucket "BUCKET_NAME" --region us-east-1

# Check bucket ACL
aws s3api get-bucket-acl --bucket "BUCKET_NAME" --region us-east-1
```

**Solutions:**
1. Review S3 bucket policies
2. Check IAM permissions
3. Verify bucket ownership
4. Update bucket policies

### Data Not Found

**Symptoms:**
- Files not appearing in S3
- Empty directories
- Lambda functions can't find data

**Diagnosis:**
```bash
# List S3 objects
aws s3 ls "s3://BUCKET_NAME/thera-pipeline/" --recursive --region us-east-1

# Check object metadata
aws s3api head-object --bucket "BUCKET_NAME" --key "OBJECT_KEY" --region us-east-1
```

**Solutions:**
1. Verify data upload process
2. Check S3 event notifications
3. Review Lambda function logic
4. Check for data corruption

## DynamoDB Issues

### Table Not Found

**Symptoms:**
- "Table not found" errors
- Lambda functions failing
- No data in DynamoDB

**Diagnosis:**
```bash
# List DynamoDB tables
aws dynamodb list-tables --region us-east-1

# Check table status
aws dynamodb describe-table --table-name "TABLE_NAME" --region us-east-1
```

**Solutions:**
1. Verify table creation
2. Check CloudFormation deployment
3. Verify table name in Lambda functions
4. Check IAM permissions

### Performance Issues

**Symptoms:**
- Slow DynamoDB operations
- Throttling errors
- High costs

**Solutions:**
1. Optimize partition key design
2. Use appropriate read/write capacity
3. Implement caching
4. Use DynamoDB Accelerator (DAX)

## EventBridge Issues

### Rules Not Triggering

**Symptoms:**
- No Step Functions executions
- Rules not firing
- No CloudWatch logs

**Diagnosis:**
```bash
# Check EventBridge rules
aws events list-rules --region us-east-1

# Check rule targets
aws events list-targets-by-rule --rule "RULE_NAME" --region us-east-1
```

**Solutions:**
1. Verify rule configuration
2. Check timezone settings
3. Verify target permissions
4. Test with manual trigger

### Schedule Issues

**Symptoms:**
- Incorrect execution times
- Missing executions
- Timezone problems

**Solutions:**
1. Verify cron expressions
2. Check timezone settings
3. Test with manual triggers
4. Review EventBridge documentation

## Monitoring and Debugging

### CloudWatch Logs

**View Logs:**
```bash
# List log groups
aws logs describe-log-groups --log-group-name-prefix "/aws/lambda/thera-pipeline" --region us-east-1

# Filter logs
aws logs filter-log-events \
    --log-group-name "/aws/lambda/thera-pipeline-prod-apollo-delta-pull" \
    --filter-pattern "ERROR" \
    --start-time $(date -d '1 hour ago' +%s)000 \
    --region us-east-1
```

### CloudWatch Metrics

**View Metrics:**
```bash
# Get Lambda metrics
aws cloudwatch get-metric-statistics \
    --namespace AWS/Lambda \
    --metric-name Errors \
    --dimensions Name=FunctionName,Value="thera-pipeline-prod-apollo-delta-pull" \
    --start-time 2024-01-01T00:00:00Z \
    --end-time 2024-01-02T00:00:00Z \
    --period 300 \
    --statistics Sum \
    --region us-east-1
```

### Step Functions Monitoring

**View Executions:**
```bash
# List executions
aws stepfunctions list-executions \
    --state-machine-arn "STATE_MACHINE_ARN" \
    --region us-east-1

# Get execution details
aws stepfunctions describe-execution \
    --execution-arn "EXECUTION_ARN" \
    --region us-east-1
```

## Performance Optimization

### Lambda Optimization

1. **Memory Allocation:**
   - Start with 512MB and adjust based on usage
   - Monitor CloudWatch metrics
   - Use provisioned concurrency for critical functions

2. **Code Optimization:**
   - Minimize package size
   - Use connection pooling
   - Implement caching strategies

3. **Timeout Settings:**
   - Set appropriate timeouts
   - Use Step Functions for long-running tasks
   - Implement retry logic

### Athena Optimization

1. **Data Format:**
   - Use Parquet format
   - Implement partitioning
   - Compress data

2. **Query Optimization:**
   - Use appropriate data types
   - Limit columns in SELECT
   - Use WHERE clauses effectively

### S3 Optimization

1. **Storage Class:**
   - Use appropriate storage classes
   - Implement lifecycle policies
   - Consider data archiving

2. **Access Patterns:**
   - Optimize for read patterns
   - Use CloudFront for static content
   - Implement caching

## Emergency Procedures

### Stop Pipeline

**Stop Step Functions:**
```bash
# Stop all executions
aws stepfunctions list-executions \
    --state-machine-arn "STATE_MACHINE_ARN" \
    --status-filter RUNNING \
    --region us-east-1 \
    --query 'executions[].executionArn' \
    --output text | xargs -I {} aws stepfunctions stop-execution \
    --execution-arn {} \
    --region us-east-1
```

**Disable EventBridge Rules:**
```bash
# Disable daily rule
aws events disable-rule --name "thera-pipeline-daily-rule" --region us-east-1

# Disable weekly rule
aws events disable-rule --name "thera-pipeline-weekly-rule" --region us-east-1
```

### Rollback Deployment

**Rollback CloudFormation:**
```bash
# List stack events
aws cloudformation describe-stack-events \
    --stack-name "thera-pipeline-prod" \
    --region us-east-1

# Rollback to previous version
aws cloudformation cancel-update-stack \
    --stack-name "thera-pipeline-prod" \
    --region us-east-1
```

### Data Recovery

**Restore from S3:**
```bash
# List S3 versions
aws s3api list-object-versions \
    --bucket "BUCKET_NAME" \
    --prefix "thera-pipeline/" \
    --region us-east-1

# Restore specific version
aws s3api copy-object \
    --bucket "BUCKET_NAME" \
    --copy-source "BUCKET_NAME/OBJECT_KEY?versionId=VERSION_ID" \
    --key "OBJECT_KEY" \
    --region us-east-1
```

## Support and Escalation

### Internal Support

1. **Check Documentation:**
   - Review this troubleshooting guide
   - Check CloudFormation templates
   - Review Lambda function code

2. **Monitor Resources:**
   - Use CloudWatch dashboards
   - Check SNS notifications
   - Review Step Functions execution history

3. **Test Components:**
   - Test individual Lambda functions
   - Verify S3 data integrity
   - Check DynamoDB table status

### External Support

1. **AWS Support:**
   - Use AWS Support Center
   - Check AWS Service Health Dashboard
   - Review AWS documentation

2. **Third-party Services:**
   - Apollo API support
   - Firecrawl support
   - Check service status pages

## Best Practices

### Prevention

1. **Regular Monitoring:**
   - Set up CloudWatch alarms
   - Monitor resource usage
   - Review execution logs

2. **Testing:**
   - Test deployments in staging
   - Validate API keys regularly
   - Test error scenarios

3. **Documentation:**
   - Keep runbooks updated
   - Document changes
   - Maintain troubleshooting guides

### Recovery

1. **Backup Strategy:**
   - Regular S3 backups
   - DynamoDB point-in-time recovery
   - CloudFormation stack exports

2. **Disaster Recovery:**
   - Multi-region deployment
   - Cross-region replication
   - Automated failover procedures

3. **Incident Response:**
   - Clear escalation procedures
   - Communication plans
   - Post-incident reviews

---

For additional support, contact the development team or refer to the AWS documentation for specific services.
