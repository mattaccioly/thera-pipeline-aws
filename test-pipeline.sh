#!/bin/bash

# Thera Pipeline - Test Script
# This script tests the deployed pipeline components

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
ENVIRONMENT="dev"
REGION="us-east-1"

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -e|--environment)
            ENVIRONMENT="$2"
            shift 2
            ;;
        -r|--region)
            REGION="$2"
            shift 2
            ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo "Options:"
            echo "  -e, --environment ENV    Environment (dev, staging, prod) [default: dev]"
            echo "  -r, --region REGION      AWS region [default: us-east-1]"
            echo "  -h, --help              Show this help message"
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            exit 1
            ;;
    esac
done

STACK_NAME="thera-pipeline-${ENVIRONMENT}"

# Get AWS Account ID
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

print_status "Testing Thera Pipeline deployment..."
print_status "Environment: $ENVIRONMENT"
print_status "Region: $REGION"
print_status "Stack Name: $STACK_NAME"

# Test 1: Check CloudFormation stack
print_status "1. Checking CloudFormation stack status..."
if aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$REGION" > /dev/null 2>&1; then
    STACK_STATUS=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$REGION" --query 'Stacks[0].StackStatus' --output text)
    if [[ "$STACK_STATUS" == "CREATE_COMPLETE" || "$STACK_STATUS" == "UPDATE_COMPLETE" ]]; then
        print_success "CloudFormation stack is healthy: $STACK_STATUS"
    else
        print_warning "CloudFormation stack status: $STACK_STATUS"
    fi
else
    print_error "CloudFormation stack not found: $STACK_NAME"
    exit 1
fi

# Test 2: Check Lambda functions
print_status "2. Checking Lambda functions..."
EXPECTED_FUNCTIONS=(
    "${ENVIRONMENT}-apollo-delta-pull"
    "${ENVIRONMENT}-domain-health-gate"
    "${ENVIRONMENT}-firecrawl-orchestrator"
    "${ENVIRONMENT}-athena-ctas-bronze-silver"
    "${ENVIRONMENT}-athena-ctas-silver-gold"
    "${ENVIRONMENT}-embeddings-batch"
    "${ENVIRONMENT}-matcher"
    "${ENVIRONMENT}-ams-computation"
    "${ENVIRONMENT}-dynamodb-publisher"
    "${ENVIRONMENT}-advanced-summarization"
    "${ENVIRONMENT}-cost-monitor"
    "${ENVIRONMENT}-weekly-trainer"
    "${ENVIRONMENT}-ams-job"
    "${ENVIRONMENT}-evaluation-metrics"
    "${ENVIRONMENT}-ml-training"
)

MISSING_FUNCTIONS=()
for func in "${EXPECTED_FUNCTIONS[@]}"; do
    if aws lambda get-function --function-name "$func" --region "$REGION" > /dev/null 2>&1; then
        print_success "Lambda function exists: $func"
    else
        print_error "Lambda function missing: $func"
        MISSING_FUNCTIONS+=("$func")
    fi
done

if [[ ${#MISSING_FUNCTIONS[@]} -gt 0 ]]; then
    print_error "Missing ${#MISSING_FUNCTIONS[@]} Lambda functions"
    exit 1
fi

# Test 3: Check Step Functions
print_status "3. Checking Step Functions state machines..."
DAILY_STATE_MACHINE="${ENVIRONMENT}-thera-daily-pipeline"
WEEKLY_STATE_MACHINE="${ENVIRONMENT}-thera-weekly-pipeline"

if aws stepfunctions describe-state-machine --state-machine-arn "arn:aws:stepfunctions:${REGION}:${AWS_ACCOUNT_ID}:stateMachine:${DAILY_STATE_MACHINE}" --region "$REGION" > /dev/null 2>&1; then
    print_success "Daily state machine exists: $DAILY_STATE_MACHINE"
else
    print_error "Daily state machine missing: $DAILY_STATE_MACHINE"
fi

if aws stepfunctions describe-state-machine --state-machine-arn "arn:aws:stepfunctions:${REGION}:${AWS_ACCOUNT_ID}:stateMachine:${WEEKLY_STATE_MACHINE}" --region "$REGION" > /dev/null 2>&1; then
    print_success "Weekly state machine exists: $WEEKLY_STATE_MACHINE"
else
    print_error "Weekly state machine missing: $WEEKLY_STATE_MACHINE"
fi

# Test 4: Check EventBridge rules
print_status "4. Checking EventBridge rules..."
DAILY_RULE="${ENVIRONMENT}-thera-daily-trigger"
WEEKLY_RULE="${ENVIRONMENT}-thera-weekly-trigger"

if aws events describe-rule --name "$DAILY_RULE" --region "$REGION" > /dev/null 2>&1; then
    print_success "Daily EventBridge rule exists: $DAILY_RULE"
else
    print_error "Daily EventBridge rule missing: $DAILY_RULE"
fi

if aws events describe-rule --name "$WEEKLY_RULE" --region "$REGION" > /dev/null 2>&1; then
    print_success "Weekly EventBridge rule exists: $WEEKLY_RULE"
else
    print_error "Weekly EventBridge rule missing: $WEEKLY_RULE"
fi

# Test 5: Check S3 buckets
print_status "5. Checking S3 buckets..."
EXPECTED_BUCKETS=(
    "thera-raw-${ENVIRONMENT}"
    "thera-bronze-${ENVIRONMENT}"
    "thera-curated-${ENVIRONMENT}"
    "thera-embeddings-${ENVIRONMENT}"
    "thera-metrics-${ENVIRONMENT}"
    "thera-models-${ENVIRONMENT}"
)

for bucket in "${EXPECTED_BUCKETS[@]}"; do
    if aws s3 ls "s3://$bucket" > /dev/null 2>&1; then
        print_success "S3 bucket exists: $bucket"
    else
        print_error "S3 bucket missing: $bucket"
    fi
done

# Test 6: Check DynamoDB tables
print_status "6. Checking DynamoDB tables..."
EXPECTED_TABLES=(
    "thera-startups-public-${ENVIRONMENT}"
    "thera-startups-private-${ENVIRONMENT}"
    "${ENVIRONMENT}-apollo-quota"
)

for table in "${EXPECTED_TABLES[@]}"; do
    if aws dynamodb describe-table --table-name "$table" --region "$REGION" > /dev/null 2>&1; then
        print_success "DynamoDB table exists: $table"
    else
        print_error "DynamoDB table missing: $table"
    fi
done

# Test 7: Check secrets
print_status "7. Checking AWS Secrets Manager..."
if aws secretsmanager describe-secret --secret-id "thera/apollo/api-key" --region "$REGION" > /dev/null 2>&1; then
    print_success "Apollo API key secret exists"
else
    print_warning "Apollo API key secret missing - configure it manually"
fi

if aws secretsmanager describe-secret --secret-id "thera/firecrawl/api-key" --region "$REGION" > /dev/null 2>&1; then
    print_success "Firecrawl API key secret exists"
else
    print_warning "Firecrawl API key secret missing - configure it manually"
fi

# Test 8: Test a Lambda function
print_status "8. Testing a Lambda function..."
TEST_FUNCTION="${ENVIRONMENT}-apollo-delta-pull"
print_status "Invoking test function: $TEST_FUNCTION"

TEST_PAYLOAD='{"test": true}'
if aws lambda invoke \
    --function-name "$TEST_FUNCTION" \
    --payload "$TEST_PAYLOAD" \
    --region "$REGION" \
    /tmp/lambda-test-response.json > /dev/null 2>&1; then
    print_success "Lambda function invocation successful"
    if [[ -f /tmp/lambda-test-response.json ]]; then
        RESPONSE=$(cat /tmp/lambda-test-response.json)
        print_status "Response: $RESPONSE"
        rm -f /tmp/lambda-test-response.json
    fi
else
    print_warning "Lambda function invocation failed (this might be expected if API keys are not configured)"
fi

# Summary
print_status ""
print_status "=== TEST SUMMARY ==="
print_status "Environment: $ENVIRONMENT"
print_status "Region: $REGION"
print_status "Stack Name: $STACK_NAME"

if [[ ${#MISSING_FUNCTIONS[@]} -eq 0 ]]; then
    print_success "✅ All Lambda functions are deployed"
else
    print_error "❌ ${#MISSING_FUNCTIONS[@]} Lambda functions are missing"
fi

print_status ""
print_status "Next steps:"
print_status "1. Configure API keys in AWS Secrets Manager if not done"
print_status "2. Enable Bedrock access if not done"
print_status "3. Test the daily pipeline by running the state machine manually"
print_status "4. Monitor CloudWatch logs and metrics"

print_success "Pipeline test completed! 🚀"
