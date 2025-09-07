#!/bin/bash

# Thera Pipeline Load Testing Script
# This script performs load testing on the deployed Lambda functions

set -e

# Configuration
ENVIRONMENT=${1:-dev}
REGION=${2:-us-east-1}
TEST_DURATION=${3:-300}  # 5 minutes default
CONCURRENT_USERS=${4:-10}

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

# Get Lambda function ARNs
get_lambda_arns() {
    log_info "Getting Lambda function ARNs..."
    
    APOLLO_ARN=$(aws lambda get-function --function-name "${ENVIRONMENT}-apollo-delta-pull" --region "${REGION}" --query 'Configuration.FunctionArn' --output text 2>/dev/null || echo "")
    DOMAIN_HEALTH_ARN=$(aws lambda get-function --function-name "${ENVIRONMENT}-domain-health-gate" --region "${REGION}" --query 'Configuration.FunctionArn' --output text 2>/dev/null || echo "")
    FIRECRAWL_ARN=$(aws lambda get-function --function-name "${ENVIRONMENT}-firecrawl-orchestrator" --region "${REGION}" --query 'Configuration.FunctionArn' --output text 2>/dev/null || echo "")
    EMBEDDINGS_ARN=$(aws lambda get-function --function-name "${ENVIRONMENT}-embeddings-batch" --region "${REGION}" --query 'Configuration.FunctionArn' --output text 2>/dev/null || echo "")
    MATCHER_ARN=$(aws lambda get-function --function-name "${ENVIRONMENT}-matcher" --region "${REGION}" --query 'Configuration.FunctionArn' --output text 2>/dev/null || echo "")
    AMS_ARN=$(aws lambda get-function --function-name "${ENVIRONMENT}-ams-computation" --region "${REGION}" --query 'Configuration.FunctionArn' --output text 2>/dev/null || echo "")
    DYNAMODB_ARN=$(aws lambda get-function --function-name "${ENVIRONMENT}-dynamodb-publisher" --region "${REGION}" --query 'Configuration.FunctionArn' --output text 2>/dev/null || echo "")
    ML_TRAINING_ARN=$(aws lambda get-function --function-name "${ENVIRONMENT}-ml-training" --region "${REGION}" --query 'Configuration.FunctionArn' --output text 2>/dev/null || echo "")
    
    log_success "Lambda function ARNs retrieved"
}

# Test Apollo Delta Pull Lambda
test_apollo_delta_pull() {
    if [ -z "$APOLLO_ARN" ]; then
        log_warning "Apollo Delta Pull Lambda not found, skipping test"
        return
    fi
    
    log_info "Testing Apollo Delta Pull Lambda..."
    
    # Test payload
    payload='{}'
    
    # Invoke function
    response=$(aws lambda invoke \
        --function-name "${ENVIRONMENT}-apollo-delta-pull" \
        --payload "$payload" \
        --region "${REGION}" \
        --output json \
        /tmp/apollo_response.json)
    
    # Check response
    if [ $? -eq 0 ]; then
        log_success "Apollo Delta Pull test completed"
        cat /tmp/apollo_response.json
    else
        log_error "Apollo Delta Pull test failed"
    fi
}

# Test Domain Health Gate Lambda
test_domain_health_gate() {
    if [ -z "$DOMAIN_HEALTH_ARN" ]; then
        log_warning "Domain Health Gate Lambda not found, skipping test"
        return
    fi
    
    log_info "Testing Domain Health Gate Lambda..."
    
    # Test payload with sample domains
    payload='{
        "domains": ["google.com", "github.com", "example.com"]
    }'
    
    # Invoke function
    response=$(aws lambda invoke \
        --function-name "${ENVIRONMENT}-domain-health-gate" \
        --payload "$payload" \
        --region "${REGION}" \
        --output json \
        /tmp/domain_health_response.json)
    
    # Check response
    if [ $? -eq 0 ]; then
        log_success "Domain Health Gate test completed"
        cat /tmp/domain_health_response.json
    else
        log_error "Domain Health Gate test failed"
    fi
}

# Test Firecrawl Orchestrator Lambda
test_firecrawl_orchestrator() {
    if [ -z "$FIRECRAWL_ARN" ]; then
        log_warning "Firecrawl Orchestrator Lambda not found, skipping test"
        return
    fi
    
    log_info "Testing Firecrawl Orchestrator Lambda..."
    
    # Test payload with sample domains
    payload='{
        "domains": ["example.com", "github.com"],
        "use_step_functions": false,
        "use_sqs": false
    }'
    
    # Invoke function
    response=$(aws lambda invoke \
        --function-name "${ENVIRONMENT}-firecrawl-orchestrator" \
        --payload "$payload" \
        --region "${REGION}" \
        --output json \
        /tmp/firecrawl_response.json)
    
    # Check response
    if [ $? -eq 0 ]; then
        log_success "Firecrawl Orchestrator test completed"
        cat /tmp/firecrawl_response.json
    else
        log_error "Firecrawl Orchestrator test failed"
    fi
}

# Test Embeddings Batch Lambda
test_embeddings_batch() {
    if [ -z "$EMBEDDINGS_ARN" ]; then
        log_warning "Embeddings Batch Lambda not found, skipping test"
        return
    fi
    
    log_info "Testing Embeddings Batch Lambda..."
    
    # Test payload
    payload='{}'
    
    # Invoke function
    response=$(aws lambda invoke \
        --function-name "${ENVIRONMENT}-embeddings-batch" \
        --payload "$payload" \
        --region "${REGION}" \
        --output json \
        /tmp/embeddings_response.json)
    
    # Check response
    if [ $? -eq 0 ]; then
        log_success "Embeddings Batch test completed"
        cat /tmp/embeddings_response.json
    else
        log_error "Embeddings Batch test failed"
    fi
}

# Test Matcher Lambda
test_matcher() {
    if [ -z "$MATCHER_ARN" ]; then
        log_warning "Matcher Lambda not found, skipping test"
        return
    fi
    
    log_info "Testing Matcher Lambda..."
    
    # Test payload
    payload='{
        "challenge_text": "Looking for AI startups in healthcare that use machine learning for drug discovery",
        "industry": "healthcare",
        "country": "united states"
    }'
    
    # Invoke function
    response=$(aws lambda invoke \
        --function-name "${ENVIRONMENT}-matcher" \
        --payload "$payload" \
        --region "${REGION}" \
        --output json \
        /tmp/matcher_response.json)
    
    # Check response
    if [ $? -eq 0 ]; then
        log_success "Matcher test completed"
        cat /tmp/matcher_response.json
    else
        log_error "Matcher test failed"
    fi
}

# Test AMS Computation Lambda
test_ams_computation() {
    if [ -z "$AMS_ARN" ]; then
        log_warning "AMS Computation Lambda not found, skipping test"
        return
    fi
    
    log_info "Testing AMS Computation Lambda..."
    
    # Test payload
    payload='{
        "target_date": "2024-01-15"
    }'
    
    # Invoke function
    response=$(aws lambda invoke \
        --function-name "${ENVIRONMENT}-ams-computation" \
        --payload "$payload" \
        --region "${REGION}" \
        --output json \
        /tmp/ams_response.json)
    
    # Check response
    if [ $? -eq 0 ]; then
        log_success "AMS Computation test completed"
        cat /tmp/ams_response.json
    else
        log_error "AMS Computation test failed"
    fi
}

# Test DynamoDB Publisher Lambda
test_dynamodb_publisher() {
    if [ -z "$DYNAMODB_ARN" ]; then
        log_warning "DynamoDB Publisher Lambda not found, skipping test"
        return
    fi
    
    log_info "Testing DynamoDB Publisher Lambda..."
    
    # Test payload
    payload='{}'
    
    # Invoke function
    response=$(aws lambda invoke \
        --function-name "${ENVIRONMENT}-dynamodb-publisher" \
        --payload "$payload" \
        --region "${REGION}" \
        --output json \
        /tmp/dynamodb_response.json)
    
    # Check response
    if [ $? -eq 0 ]; then
        log_success "DynamoDB Publisher test completed"
        cat /tmp/dynamodb_response.json
    else
        log_error "DynamoDB Publisher test failed"
    fi
}

# Test ML Training Lambda
test_ml_training() {
    if [ -z "$ML_TRAINING_ARN" ]; then
        log_warning "ML Training Lambda not found, skipping test"
        return
    fi
    
    log_info "Testing ML Training Lambda..."
    
    # Test payload
    payload='{}'
    
    # Invoke function
    response=$(aws lambda invoke \
        --function-name "${ENVIRONMENT}-ml-training" \
        --payload "$payload" \
        --region "${REGION}" \
        --output json \
        /tmp/ml_training_response.json)
    
    # Check response
    if [ $? -eq 0 ]; then
        log_success "ML Training test completed"
        cat /tmp/ml_training_response.json
    else
        log_error "ML Training test failed"
    fi
}

# Run concurrent load test
run_concurrent_test() {
    log_info "Running concurrent load test..."
    log_info "Duration: ${TEST_DURATION} seconds"
    log_info "Concurrent users: ${CONCURRENT_USERS}"
    
    # Create test script for concurrent execution
    cat > /tmp/concurrent_test.sh << 'EOF'
#!/bin/bash
FUNCTION_NAME=$1
ENVIRONMENT=$2
REGION=$3
PAYLOAD=$4

for i in {1..10}; do
    aws lambda invoke \
        --function-name "${ENVIRONMENT}-${FUNCTION_NAME}" \
        --payload "$PAYLOAD" \
        --region "${REGION}" \
        --output json \
        /tmp/${FUNCTION_NAME}_response_${i}.json &
done

wait
EOF
    
    chmod +x /tmp/concurrent_test.sh
    
    # Run concurrent tests for each function
    if [ -n "$MATCHER_ARN" ]; then
        log_info "Running concurrent test for Matcher..."
        /tmp/concurrent_test.sh "matcher" "${ENVIRONMENT}" "${REGION}" '{"challenge_text": "AI startup test", "industry": "technology"}'
    fi
    
    if [ -n "$DOMAIN_HEALTH_ARN" ]; then
        log_info "Running concurrent test for Domain Health Gate..."
        /tmp/concurrent_test.sh "domain-health-gate" "${ENVIRONMENT}" "${REGION}" '{"domains": ["google.com", "github.com"]}'
    fi
    
    log_success "Concurrent load test completed"
}

# Monitor CloudWatch metrics
monitor_metrics() {
    log_info "Monitoring CloudWatch metrics..."
    
    # Get current time
    END_TIME=$(date -u +"%Y-%m-%dT%H:%M:%S")
    START_TIME=$(date -u -d "5 minutes ago" +"%Y-%m-%dT%H:%M:%S")
    
    # Get Lambda metrics
    aws cloudwatch get-metric-statistics \
        --namespace AWS/Lambda \
        --metric-name Duration \
        --dimensions Name=FunctionName,Value="${ENVIRONMENT}-matcher" \
        --start-time "${START_TIME}" \
        --end-time "${END_TIME}" \
        --period 300 \
        --statistics Average \
        --region "${REGION}" \
        --output table
    
    log_success "Metrics monitoring completed"
}

# Cleanup test files
cleanup() {
    log_info "Cleaning up test files..."
    rm -f /tmp/*_response.json
    rm -f /tmp/concurrent_test.sh
    log_success "Cleanup completed"
}

# Display test summary
display_summary() {
    log_success "Load testing completed!"
    echo
    echo "=== LOAD TEST SUMMARY ==="
    echo "Environment: ${ENVIRONMENT}"
    echo "Region: ${REGION}"
    echo "Test Duration: ${TEST_DURATION} seconds"
    echo "Concurrent Users: ${CONCURRENT_USERS}"
    echo
    echo "=== NEXT STEPS ==="
    echo "1. Check CloudWatch logs for any errors"
    echo "2. Review CloudWatch metrics for performance"
    echo "3. Check S3 buckets for data output"
    echo "4. Verify DynamoDB tables for data"
    echo
    echo "=== MONITORING COMMANDS ==="
    echo "View CloudWatch logs:"
    echo "  aws logs describe-log-groups --region ${REGION}"
    echo
    echo "View Lambda metrics:"
    echo "  aws cloudwatch get-metric-statistics --namespace AWS/Lambda --metric-name Duration --region ${REGION}"
    echo
}

# Main testing function
main() {
    log_info "Starting Thera Pipeline load testing..."
    log_info "Environment: ${ENVIRONMENT}"
    log_info "Region: ${REGION}"
    echo
    
    # Get Lambda function ARNs
    get_lambda_arns
    
    # Run individual tests
    test_apollo_delta_pull
    test_domain_health_gate
    test_firecrawl_orchestrator
    test_embeddings_batch
    test_matcher
    test_ams_computation
    test_dynamodb_publisher
    test_ml_training
    
    # Run concurrent tests
    run_concurrent_test
    
    # Monitor metrics
    monitor_metrics
    
    # Cleanup
    cleanup
    
    # Display summary
    display_summary
}

# Run main function
main "$@"