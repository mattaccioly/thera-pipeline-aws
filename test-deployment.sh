#!/bin/bash

# Test Deployment Script for Thera Pipeline
# This script tests the deployed infrastructure

set -e

# Configuration
ENVIRONMENT=${1:-dev}
REGION=${2:-us-east-1}

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
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

# Test AWS connectivity
test_aws_connectivity() {
    log_info "Testing AWS connectivity..."
    
    if aws sts get-caller-identity &> /dev/null; then
        local account_id=$(aws sts get-caller-identity --query Account --output text)
        local user_arn=$(aws sts get-caller-identity --query Arn --output text)
        log_success "AWS connectivity verified"
        log_info "Account ID: ${account_id}"
        log_info "User/Role: ${user_arn}"
    else
        log_error "AWS connectivity failed"
        exit 1
    fi
}

# Test S3 buckets
test_s3_buckets() {
    log_info "Testing S3 buckets..."
    
    local buckets=(
        "thera-raw"
        "thera-bronze"
        "thera-curated"
        "thera-embeddings"
        "thera-metrics"
        "thera-models"
    )
    
    for bucket in "${buckets[@]}"; do
        if aws s3 ls "s3://${bucket}" &> /dev/null; then
            log_success "✓ S3 bucket exists: ${bucket}"
        else
            log_warning "⚠ S3 bucket missing: ${bucket}"
        fi
    done
}

# Test Lambda functions
test_lambda_functions() {
    log_info "Testing Lambda functions..."
    
    local functions=(
        "thera-apollo-delta-pull-${ENVIRONMENT}"
        "thera-domain-health-gate-${ENVIRONMENT}"
        "thera-firecrawl-orchestrator-${ENVIRONMENT}"
        "thera-embeddings-batch-${ENVIRONMENT}"
        "thera-matcher-${ENVIRONMENT}"
        "thera-ams-computation-${ENVIRONMENT}"
        "thera-dynamodb-publisher-${ENVIRONMENT}"
        "thera-ml-training-${ENVIRONMENT}"
    )
    
    for function in "${functions[@]}"; do
        if aws lambda get-function --function-name "${function}" --region "${REGION}" &> /dev/null; then
            log_success "✓ Lambda function exists: ${function}"
        else
            log_warning "⚠ Lambda function missing: ${function}"
        fi
    done
}

# Test DynamoDB tables
test_dynamodb_tables() {
    log_info "Testing DynamoDB tables..."
    
    local tables=(
        "thera-startups-public"
        "thera-startups-private"
        "${ENVIRONMENT}-apollo-quota"
    )
    
    for table in "${tables[@]}"; do
        if aws dynamodb describe-table --table-name "${table}" --region "${REGION}" &> /dev/null; then
            log_success "✓ DynamoDB table exists: ${table}"
        else
            log_warning "⚠ DynamoDB table missing: ${table}"
        fi
    done
}

# Test Step Functions
test_step_functions() {
    log_info "Testing Step Functions..."
    
    local state_machines=(
        "thera-pipeline-daily-${ENVIRONMENT}"
        "thera-pipeline-weekly-${ENVIRONMENT}"
    )
    
    for sm in "${state_machines[@]}"; do
        if aws stepfunctions describe-state-machine --state-machine-arn "arn:aws:states:${REGION}:$(aws sts get-caller-identity --query Account --output text):stateMachine:${sm}" &> /dev/null; then
            log_success "✓ Step Functions state machine exists: ${sm}"
        else
            log_warning "⚠ Step Functions state machine missing: ${sm}"
        fi
    done
}

# Test EventBridge rules
test_eventbridge_rules() {
    log_info "Testing EventBridge rules..."
    
    local rules=(
        "thera-pipeline-daily-rule-${ENVIRONMENT}"
        "thera-pipeline-weekly-rule-${ENVIRONMENT}"
    )
    
    for rule in "${rules[@]}"; do
        if aws events describe-rule --name "${rule}" --region "${REGION}" &> /dev/null; then
            log_success "✓ EventBridge rule exists: ${rule}"
        else
            log_warning "⚠ EventBridge rule missing: ${rule}"
        fi
    done
}

# Test secrets
test_secrets() {
    log_info "Testing secrets in AWS Secrets Manager..."
    
    local secrets=(
        "thera/apollo/api-key"
        "thera/firecrawl/api-key"
    )
    
    for secret in "${secrets[@]}"; do
        if aws secretsmanager describe-secret --secret-id "${secret}" --region "${REGION}" &> /dev/null; then
            log_success "✓ Secret exists: ${secret}"
        else
            log_warning "⚠ Secret missing: ${secret}"
        fi
    done
}

# Test Athena
test_athena() {
    log_info "Testing Athena..."
    
    local database="thera_analytics"
    local workgroup="primary"
    
    if aws athena get-database --catalog-name "AwsDataCatalog" --database-name "${database}" --region "${REGION}" &> /dev/null; then
        log_success "✓ Athena database exists: ${database}"
    else
        log_warning "⚠ Athena database missing: ${database}"
    fi
    
    if aws athena get-work-group --work-group "${workgroup}" --region "${REGION}" &> /dev/null; then
        log_success "✓ Athena workgroup exists: ${workgroup}"
    else
        log_warning "⚠ Athena workgroup missing: ${workgroup}"
    fi
}

# Run a simple Lambda test
test_lambda_execution() {
    log_info "Testing Lambda function execution..."
    
    local function_name="thera-matcher-${ENVIRONMENT}"
    
    if aws lambda get-function --function-name "${function_name}" --region "${REGION}" &> /dev/null; then
        log_info "Testing ${function_name} with sample payload..."
        
        local payload='{
            "challenge_text": "AI-powered startup matching platform",
            "industry": "technology",
            "country": "US"
        }'
        
        local response=$(aws lambda invoke \
            --function-name "${function_name}" \
            --payload "${payload}" \
            --region "${REGION}" \
            /tmp/lambda-response.json 2>&1)
        
        if [ $? -eq 0 ]; then
            log_success "✓ Lambda function executed successfully"
            log_info "Response: $(cat /tmp/lambda-response.json)"
        else
            log_warning "⚠ Lambda function execution failed: ${response}"
        fi
    else
        log_warning "⚠ Lambda function not found: ${function_name}"
    fi
}

# Display test summary
display_test_summary() {
    log_info "Test Summary"
    echo "============"
    echo "Environment: ${ENVIRONMENT}"
    echo "Region: ${REGION}"
    echo
    echo "Tests completed. Check the output above for any warnings or errors."
    echo
    echo "Next steps:"
    echo "1. Fix any missing resources"
    echo "2. Configure API keys in Secrets Manager"
    echo "3. Run the pipeline manually to test end-to-end"
    echo "4. Set up monitoring and alerts"
}

# Main function
main() {
    log_info "Testing Thera Pipeline deployment..."
    log_info "Environment: ${ENVIRONMENT}"
    log_info "Region: ${REGION}"
    echo
    
    # Run tests
    test_aws_connectivity
    test_s3_buckets
    test_lambda_functions
    test_dynamodb_tables
    test_step_functions
    test_eventbridge_rules
    test_secrets
    test_athena
    test_lambda_execution
    
    # Display summary
    display_test_summary
}

# Run main function
main "$@"
