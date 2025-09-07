#!/bin/bash

# Thera Pipeline Monitoring Script
# This script provides monitoring capabilities for the Thera Pipeline

set -e

# Configuration
ENVIRONMENT=${1:-prod}
REGION=${2:-us-east-1}
STACK_NAME="thera-pipeline-${ENVIRONMENT}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Get stack outputs
get_stack_outputs() {
    log_info "Getting stack outputs..."
    
    aws cloudformation describe-stacks \
        --stack-name "$STACK_NAME" \
        --region "$REGION" \
        --query 'Stacks[0].Outputs' \
        --output table
}

# Monitor Step Functions executions
monitor_step_functions() {
    log_info "Monitoring Step Functions executions..."
    
    # Get state machine ARNs
    DAILY_STATE_MACHINE_ARN=$(aws cloudformation describe-stacks \
        --stack-name "$STACK_NAME" \
        --region "$REGION" \
        --query 'Stacks[0].Outputs[?OutputKey==`DailyStateMachineArn`].OutputValue' \
        --output text)
    
    WEEKLY_STATE_MACHINE_ARN=$(aws cloudformation describe-stacks \
        --stack-name "$STACK_NAME" \
        --region "$REGION" \
        --query 'Stacks[0].Outputs[?OutputKey==`WeeklyStateMachineArn`].OutputValue' \
        --output text)
    
    # Monitor daily executions
    log_info "Daily pipeline executions (last 10):"
    aws stepfunctions list-executions \
        --state-machine-arn "$DAILY_STATE_MACHINE_ARN" \
        --region "$REGION" \
        --max-items 10 \
        --query 'executions[].{Name:name,Status:status,StartDate:startDate,StopDate:stopDate}' \
        --output table
    
    # Monitor weekly executions
    log_info "Weekly pipeline executions (last 5):"
    aws stepfunctions list-executions \
        --state-machine-arn "$WEEKLY_STATE_MACHINE_ARN" \
        --region "$REGION" \
        --max-items 5 \
        --query 'executions[].{Name:name,Status:status,StartDate:startDate,StopDate:stopDate}' \
        --output table
}

# Monitor Lambda functions
monitor_lambda_functions() {
    log_info "Monitoring Lambda functions..."
    
    # Get Lambda function names
    LAMBDA_FUNCTIONS=(
        "thera-pipeline-${ENVIRONMENT}-apollo-delta-pull"
        "thera-pipeline-${ENVIRONMENT}-athena-ctas-bronze-silver"
        "thera-pipeline-${ENVIRONMENT}-domain-health-gate"
        "thera-pipeline-${ENVIRONMENT}-firecrawl-orchestrator"
        "thera-pipeline-${ENVIRONMENT}-athena-ctas-silver-gold"
        "thera-pipeline-${ENVIRONMENT}-embeddings-batch"
        "thera-pipeline-${ENVIRONMENT}-ams-computation"
        "thera-pipeline-${ENVIRONMENT}-dynamodb-publisher"
        "thera-pipeline-${ENVIRONMENT}-ml-training"
        "thera-pipeline-${ENVIRONMENT}-evaluation-metrics"
    )
    
    for func in "${LAMBDA_FUNCTIONS[@]}"; do
        log_info "Function: $func"
        
        # Get function metrics
        aws lambda get-function \
            --function-name "$func" \
            --region "$REGION" \
            --query 'Configuration.{FunctionName:FunctionName,Runtime:Runtime,State:State,LastModified:LastModified}' \
            --output table
        
        # Get recent invocations
        log_info "Recent invocations:"
        aws logs filter-log-events \
            --log-group-name "/aws/lambda/$func" \
            --region "$REGION" \
            --start-time $(date -d '1 hour ago' +%s)000 \
            --query 'events[].{Timestamp:timestamp,Message:message}' \
            --output table \
            --max-items 5 || log_warning "No recent logs found"
        
        echo "---"
    done
}

# Monitor CloudWatch metrics
monitor_cloudwatch_metrics() {
    log_info "Monitoring CloudWatch metrics..."
    
    # Get metrics for the last hour
    END_TIME=$(date -u +%Y-%m-%dT%H:%M:%S)
    START_TIME=$(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%S)
    
    # Lambda errors
    log_info "Lambda errors (last hour):"
    aws cloudwatch get-metric-statistics \
        --namespace AWS/Lambda \
        --metric-name Errors \
        --dimensions Name=FunctionName,Value="thera-pipeline-${ENVIRONMENT}-apollo-delta-pull" \
        --start-time "$START_TIME" \
        --end-time "$END_TIME" \
        --period 300 \
        --statistics Sum \
        --region "$REGION" \
        --query 'Datapoints[].{Timestamp:Timestamp,Errors:Sum}' \
        --output table || log_warning "No error metrics found"
    
    # Step Functions executions
    log_info "Step Functions executions (last hour):"
    aws cloudwatch get-metric-statistics \
        --namespace AWS/States \
        --metric-name ExecutionsStarted \
        --start-time "$START_TIME" \
        --end-time "$END_TIME" \
        --period 300 \
        --statistics Sum \
        --region "$REGION" \
        --query 'Datapoints[].{Timestamp:Timestamp,Executions:Sum}' \
        --output table || log_warning "No execution metrics found"
}

# Monitor S3 data
monitor_s3_data() {
    log_info "Monitoring S3 data..."
    
    # Get S3 bucket from stack outputs
    S3_BUCKET=$(aws cloudformation describe-stacks \
        --stack-name "$STACK_NAME" \
        --region "$REGION" \
        --query 'Stacks[0].Outputs[?OutputKey==`S3Bucket`].OutputValue' \
        --output text)
    
    if [ -n "$S3_BUCKET" ]; then
        log_info "S3 bucket: $S3_BUCKET"
        
        # List recent files
        log_info "Recent files in S3:"
        aws s3 ls "s3://${S3_BUCKET}/thera-pipeline/" --recursive --region "$REGION" | tail -10
        
        # Check data freshness
        log_info "Data freshness check:"
        LATEST_FILE=$(aws s3 ls "s3://${S3_BUCKET}/thera-pipeline/" --recursive --region "$REGION" | sort | tail -1)
        if [ -n "$LATEST_FILE" ]; then
            log_success "Latest file: $LATEST_FILE"
        else
            log_warning "No files found in S3"
        fi
    else
        log_error "Could not retrieve S3 bucket from stack outputs"
    fi
}

# Monitor DynamoDB
monitor_dynamodb() {
    log_info "Monitoring DynamoDB..."
    
    # Get DynamoDB table names from stack outputs
    COMPANIES_TABLE=$(aws cloudformation describe-stacks \
        --stack-name "$STACK_NAME" \
        --region "$REGION" \
        --query 'Stacks[0].Outputs[?OutputKey==`CompaniesTableName`].OutputValue' \
        --output text)
    
    if [ -n "$COMPANIES_TABLE" ]; then
        log_info "Companies table: $COMPANIES_TABLE"
        
        # Get table metrics
        aws dynamodb describe-table \
            --table-name "$COMPANIES_TABLE" \
            --region "$REGION" \
            --query 'Table.{TableName:TableName,ItemCount:ItemCount,TableStatus:TableStatus,TableSizeBytes:TableSizeBytes}' \
            --output table
    else
        log_warning "Could not retrieve DynamoDB table names from stack outputs"
    fi
}

# Generate health report
generate_health_report() {
    log_info "Generating health report..."
    
    REPORT_FILE="thera-pipeline-health-$(date +%Y%m%d-%H%M%S).txt"
    
    {
        echo "Thera Pipeline Health Report"
        echo "Generated: $(date)"
        echo "Environment: $ENVIRONMENT"
        echo "Region: $REGION"
        echo "=========================================="
        echo ""
        
        echo "Stack Status:"
        aws cloudformation describe-stacks \
            --stack-name "$STACK_NAME" \
            --region "$REGION" \
            --query 'Stacks[0].{StackName:StackName,StackStatus:StackStatus,CreationTime:CreationTime}' \
            --output table
        
        echo ""
        echo "Step Functions Status:"
        monitor_step_functions
        
        echo ""
        echo "Lambda Functions Status:"
        monitor_lambda_functions
        
        echo ""
        echo "CloudWatch Metrics:"
        monitor_cloudwatch_metrics
        
        echo ""
        echo "S3 Data Status:"
        monitor_s3_data
        
        echo ""
        echo "DynamoDB Status:"
        monitor_dynamodb
        
    } > "$REPORT_FILE"
    
    log_success "Health report generated: $REPORT_FILE"
}

# Main execution
main() {
    case "${1:-all}" in
        "stack")
            get_stack_outputs
            ;;
        "stepfunctions")
            monitor_step_functions
            ;;
        "lambda")
            monitor_lambda_functions
            ;;
        "metrics")
            monitor_cloudwatch_metrics
            ;;
        "s3")
            monitor_s3_data
            ;;
        "dynamodb")
            monitor_dynamodb
            ;;
        "health")
            generate_health_report
            ;;
        "all")
            get_stack_outputs
            monitor_step_functions
            monitor_lambda_functions
            monitor_cloudwatch_metrics
            monitor_s3_data
            monitor_dynamodb
            ;;
        *)
            echo "Usage: $0 [all|stack|stepfunctions|lambda|metrics|s3|dynamodb|health] [environment] [region]"
            echo "  all: Monitor all components (default)"
            echo "  stack: Show stack outputs"
            echo "  stepfunctions: Monitor Step Functions executions"
            echo "  lambda: Monitor Lambda functions"
            echo "  metrics: Monitor CloudWatch metrics"
            echo "  s3: Monitor S3 data"
            echo "  dynamodb: Monitor DynamoDB"
            echo "  health: Generate comprehensive health report"
            exit 1
            ;;
    esac
}

# Run main function
main "$@"
