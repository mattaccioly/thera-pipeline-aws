#!/bin/bash

# Advanced Summarization Pipeline Deployment Script
# This script deploys only the changes needed for advanced summarization

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
ENVIRONMENT=${1:-dev}
REGION=${2:-us-east-1}
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
CURATED_BUCKET="thera-curated-805595753342-v3"
ATHENA_DATABASE="thera_gold"
ATHENA_WORKGROUP="primary"

echo -e "${BLUE}🚀 Starting Advanced Summarization Pipeline Deployment${NC}"
echo -e "${BLUE}Environment: ${ENVIRONMENT}${NC}"
echo -e "${BLUE}Region: ${REGION}${NC}"
echo -e "${BLUE}Account ID: ${ACCOUNT_ID}${NC}"
echo ""

# Function to check if command succeeded
check_success() {
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✅ $1 completed successfully${NC}"
    else
        echo -e "${RED}❌ $1 failed${NC}"
        exit 1
    fi
}

# Function to wait for completion
wait_for_completion() {
    local query_id=$1
    local max_attempts=30
    local attempt=0
    
    while [ $attempt -lt $max_attempts ]; do
        status=$(aws athena get-query-execution --query-execution-id $query_id --query 'QueryExecution.Status.State' --output text)
        
        if [ "$status" = "SUCCEEDED" ]; then
            echo -e "${GREEN}✅ Query completed successfully${NC}"
            return 0
        elif [ "$status" = "FAILED" ] || [ "$status" = "CANCELLED" ]; then
            echo -e "${RED}❌ Query failed with status: $status${NC}"
            return 1
        fi
        
        echo -e "${YELLOW}⏳ Query in progress... (attempt $((attempt+1))/$max_attempts)${NC}"
        sleep 10
        attempt=$((attempt+1))
    done
    
    echo -e "${RED}❌ Query timed out${NC}"
    return 1
}

echo -e "${BLUE}📋 Step 1: Deploying Database Schema${NC}"
echo "Creating advanced summarization tables..."

# Deploy database schema
QUERY_ID=$(aws athena start-query-execution \
    --query-string "file://16_create_advanced_summarization_schema.sql" \
    --work-group $ATHENA_WORKGROUP \
    --result-configuration OutputLocation=s3://$CURATED_BUCKET/athena-results/ \
    --query 'QueryExecutionId' \
    --output text)

check_success "Database schema query submission"

# Wait for completion
wait_for_completion $QUERY_ID
check_success "Database schema creation"

echo ""
echo -e "${BLUE}📋 Step 2: Deploying Lambda Functions${NC}"

# Deploy Advanced Summarization Lambda
echo "Deploying advanced summarization Lambda function..."
aws cloudformation deploy \
    --template-file lambda-advanced-summarization.yaml \
    --stack-name thera-advanced-summarization-$ENVIRONMENT \
    --parameter-overrides \
        Environment=$ENVIRONMENT \
        CuratedBucket=$CURATED_BUCKET \
        AthenaDatabase=$ATHENA_DATABASE \
        AthenaWorkgroup=$ATHENA_WORKGROUP \
        DailyBudgetUSD=50.0 \
        MaxDailyCompanies=1000 \
        MaxBatchSize=10 \
    --region $REGION \
    --capabilities CAPABILITY_NAMED_IAM

check_success "Advanced summarization Lambda deployment"

# Deploy Cost Monitor Lambda
echo "Deploying cost monitor Lambda function..."
aws cloudformation deploy \
    --template-file lambda-cost-monitor.yaml \
    --stack-name thera-cost-monitor-$ENVIRONMENT \
    --parameter-overrides \
        Environment=$ENVIRONMENT \
        DailyBudgetUSD=50.0 \
    --region $REGION \
    --capabilities CAPABILITY_NAMED_IAM

check_success "Cost monitor Lambda deployment"

echo ""
echo -e "${BLUE}📋 Step 3: Updating Step Functions${NC}"

# Get existing state machine ARN
echo "Finding existing state machine..."
STATE_MACHINE_ARN=$(aws stepfunctions list-state-machines \
    --query "stateMachines[?contains(name, 'thera-daily-pipeline') && contains(name, '$ENVIRONMENT')].stateMachineArn" \
    --output text | head -1)

if [ -z "$STATE_MACHINE_ARN" ]; then
    echo -e "${YELLOW}⚠️  No existing state machine found. Creating new one...${NC}"
    
    # Create new state machine
    STATE_MACHINE_ARN=$(aws stepfunctions create-state-machine \
        --name thera-daily-pipeline-enhanced-$ENVIRONMENT \
        --definition file://step-functions-daily-state-machine-enhanced.json \
        --role-arn arn:aws:iam::$ACCOUNT_ID:role/thera-step-functions-role \
        --query 'stateMachineArn' \
        --output text)
    
    check_success "New state machine creation"
else
    echo "Updating existing state machine: $STATE_MACHINE_ARN"
    
    # Update existing state machine
    aws stepfunctions update-state-machine \
        --state-machine-arn $STATE_MACHINE_ARN \
        --definition file://step-functions-daily-state-machine-enhanced.json
    
    check_success "State machine update"
fi

echo ""
echo -e "${BLUE}📋 Step 4: Configuring Monitoring${NC}"

# Create SNS topic for alerts (if it doesn't exist)
echo "Setting up SNS topic for alerts..."
SNS_TOPIC_ARN=$(aws sns create-topic \
    --name thera-llm-alerts-$ENVIRONMENT \
    --query 'TopicArn' \
    --output text 2>/dev/null || echo "")

if [ -z "$SNS_TOPIC_ARN" ]; then
    SNS_TOPIC_ARN=$(aws sns list-topics \
        --query "Topics[?contains(TopicArn, 'thera-llm-alerts-$ENVIRONMENT')].TopicArn" \
        --output text)
fi

echo "SNS Topic ARN: $SNS_TOPIC_ARN"

# Update Lambda environment variables with SNS topic
if [ ! -z "$SNS_TOPIC_ARN" ]; then
    echo "Updating Lambda environment variables..."
    aws lambda update-function-configuration \
        --function-name $ENVIRONMENT-cost-monitor \
        --environment Variables="{ALERT_TOPIC_ARN=$SNS_TOPIC_ARN}" \
        --region $REGION > /dev/null
    
    check_success "Lambda environment update"
fi

echo ""
echo -e "${BLUE}📋 Step 5: Testing Deployment${NC}"

# Test Lambda functions
echo "Testing advanced summarization Lambda..."
aws lambda invoke \
    --function-name $ENVIRONMENT-advanced-summarization \
    --payload '{}' \
    --region $REGION \
    test-response.json > /dev/null

if [ -f test-response.json ]; then
    RESPONSE=$(cat test-response.json)
    if echo "$RESPONSE" | grep -q "statusCode.*200"; then
        echo -e "${GREEN}✅ Advanced summarization Lambda test passed${NC}"
    else
        echo -e "${YELLOW}⚠️  Advanced summarization Lambda test had issues: $RESPONSE${NC}"
    fi
    rm test-response.json
fi

# Test cost monitor Lambda
echo "Testing cost monitor Lambda..."
aws lambda invoke \
    --function-name $ENVIRONMENT-cost-monitor \
    --payload '{}' \
    --region $REGION \
    test-response.json > /dev/null

if [ -f test-response.json ]; then
    RESPONSE=$(cat test-response.json)
    if echo "$RESPONSE" | grep -q "statusCode.*200"; then
        echo -e "${GREEN}✅ Cost monitor Lambda test passed${NC}"
    else
        echo -e "${YELLOW}⚠️  Cost monitor Lambda test had issues: $RESPONSE${NC}"
    fi
    rm test-response.json
fi

echo ""
echo -e "${GREEN}🎉 Advanced Summarization Pipeline Deployment Complete!${NC}"
echo ""
echo -e "${BLUE}📊 Deployment Summary:${NC}"
echo -e "  • Environment: $ENVIRONMENT"
echo -e "  • Region: $REGION"
echo -e "  • State Machine: $STATE_MACHINE_ARN"
echo -e "  • SNS Topic: $SNS_TOPIC_ARN"
echo ""
echo -e "${BLUE}🔍 Next Steps:${NC}"
echo -e "  1. Monitor CloudWatch logs for any issues"
echo -e "  2. Check cost tracking in DynamoDB"
echo -e "  3. Run comprehensive test suite: python3 test_advanced_summarization.py"
echo -e "  4. Monitor first pipeline execution"
echo ""
echo -e "${BLUE}📈 Monitoring:${NC}"
echo -e "  • CloudWatch Dashboard: https://$REGION.console.aws.amazon.com/cloudwatch/home?region=$REGION#dashboards:name=thera-advanced-summarization"
echo -e "  • Lambda Functions: https://$REGION.console.aws.amazon.com/lambda/home?region=$REGION#/functions"
echo -e "  • Step Functions: https://$REGION.console.aws.amazon.com/states/home?region=$REGION#/statemachines"
echo ""
echo -e "${GREEN}✅ Deployment completed successfully!${NC}"
