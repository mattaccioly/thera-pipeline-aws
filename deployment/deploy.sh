#!/bin/bash

# Thera Pipeline AWS Deployment Script
# This script deploys the entire Thera pipeline infrastructure to AWS

set -e

# Configuration
ENVIRONMENT=${1:-dev}
REGION=${2:-us-east-1}
STACK_NAME="thera-pipeline-${ENVIRONMENT}"
TEMPLATE_BUCKET="thera-pipeline-templates-${ENVIRONMENT}-$(date +%s)"

# Load deployment configuration if it exists
if [ -f "deployment-config.env" ]; then
    source deployment-config.env
    echo "Loaded deployment configuration from deployment-config.env"
fi

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

# Check if AWS CLI is installed
check_aws_cli() {
    if ! command -v aws &> /dev/null; then
        log_error "AWS CLI is not installed. Please install it first."
        exit 1
    fi
    log_success "AWS CLI is installed"
}

# Check if AWS credentials are configured
check_aws_credentials() {
    if ! aws sts get-caller-identity &> /dev/null; then
        log_error "AWS credentials are not configured. Please run 'aws configure' first."
        exit 1
    fi
    log_success "AWS credentials are configured"
}

# Create S3 bucket for templates
create_template_bucket() {
    log_info "Creating S3 bucket for CloudFormation templates..."
    
    if aws s3 ls "s3://${TEMPLATE_BUCKET}" 2>&1 | grep -q 'NoSuchBucket'; then
        aws s3 mb "s3://${TEMPLATE_BUCKET}" --region "${REGION}"
        log_success "Created S3 bucket: ${TEMPLATE_BUCKET}"
    else
        log_info "S3 bucket already exists: ${TEMPLATE_BUCKET}"
    fi
}

# Create data buckets
create_data_buckets() {
    log_info "Creating S3 buckets for data storage..."
    
    local buckets=(
        "${RAW_BUCKET:-thera-raw-${ENVIRONMENT}}"
        "${BRONZE_BUCKET:-thera-bronze-${ENVIRONMENT}}"
        "${CURATED_BUCKET:-thera-curated-${ENVIRONMENT}}"
        "${EMBEDDINGS_BUCKET:-thera-embeddings-${ENVIRONMENT}}"
        "${METRICS_BUCKET:-thera-metrics-${ENVIRONMENT}}"
        "${MODEL_BUCKET:-thera-models-${ENVIRONMENT}}"
    )
    
    for bucket in "${buckets[@]}"; do
        if aws s3 ls "s3://${bucket}" 2>&1 | grep -q 'NoSuchBucket'; then
            aws s3 mb "s3://${bucket}" --region "${REGION}"
            log_success "Created S3 bucket: ${bucket}"
        else
            log_info "S3 bucket already exists: ${bucket}"
        fi
    done
}

# Upload CloudFormation templates to S3
upload_templates() {
    log_info "Uploading CloudFormation templates to S3..."
    
    # Upload all YAML files
    for template in *.yaml; do
        if [ -f "$template" ]; then
            aws s3 cp "$template" "s3://${TEMPLATE_BUCKET}/${template}"
            log_info "Uploaded: ${template}"
        fi
    done
    
    # Upload Python files
    for py_file in lambda-*.py; do
        if [ -f "$py_file" ]; then
            aws s3 cp "$py_file" "s3://${TEMPLATE_BUCKET}/${py_file}"
            log_info "Uploaded: ${py_file}"
        fi
    done
    
    # Upload ML training directory
    if [ -d "lambda-ml-training" ]; then
        aws s3 cp lambda-ml-training/ "s3://${TEMPLATE_BUCKET}/lambda-ml-training/" --recursive
        log_info "Uploaded: lambda-ml-training/"
    fi
    
    log_success "All templates uploaded to S3"
}

# Deploy main infrastructure
deploy_infrastructure() {
    log_info "Deploying main infrastructure..."
    
    aws cloudformation deploy \
        --template-file main-infrastructure.yaml \
        --stack-name "${STACK_NAME}-infrastructure" \
        --parameter-overrides \
            Environment="${ENVIRONMENT}" \
        --capabilities CAPABILITY_IAM \
        --region "${REGION}"
    
    log_success "Main infrastructure deployed"
}

# Deploy individual Lambda functions
deploy_lambda_functions() {
    log_info "Deploying Lambda functions..."
    
    # Apollo Delta Pull
    log_info "Deploying Apollo Delta Pull Lambda..."
    aws cloudformation deploy \
        --template-file lambda-apollo-delta-pull.yaml \
        --stack-name "${STACK_NAME}-apollo-delta-pull" \
        --parameter-overrides \
            Environment="${ENVIRONMENT}" \
        --capabilities CAPABILITY_IAM \
        --region "${REGION}"
    
    # Domain Health Gate
    log_info "Deploying Domain Health Gate Lambda..."
    aws cloudformation deploy \
        --template-file lambda-domain-health-gate.yaml \
        --stack-name "${STACK_NAME}-domain-health-gate" \
        --parameter-overrides \
            Environment="${ENVIRONMENT}" \
        --capabilities CAPABILITY_IAM \
        --region "${REGION}"
    
    # Firecrawl Orchestrator
    log_info "Deploying Firecrawl Orchestrator Lambda..."
    aws cloudformation deploy \
        --template-file lambda-firecrawl-orchestrator.yaml \
        --stack-name "${STACK_NAME}-firecrawl-orchestrator" \
        --parameter-overrides \
            Environment="${ENVIRONMENT}" \
        --capabilities CAPABILITY_IAM \
        --region "${REGION}"
    
    # Embeddings Batch
    log_info "Deploying Embeddings Batch Lambda..."
    aws cloudformation deploy \
        --template-file lambda-embeddings-batch.yaml \
        --stack-name "${STACK_NAME}-embeddings-batch" \
        --parameter-overrides \
            Environment="${ENVIRONMENT}" \
        --capabilities CAPABILITY_IAM \
        --region "${REGION}"
    
    # Matcher
    log_info "Deploying Matcher Lambda..."
    aws cloudformation deploy \
        --template-file lambda-matcher.yaml \
        --stack-name "${STACK_NAME}-matcher" \
        --parameter-overrides \
            Environment="${ENVIRONMENT}" \
        --capabilities CAPABILITY_IAM \
        --region "${REGION}"
    
    # AMS Computation
    log_info "Deploying AMS Computation Lambda..."
    aws cloudformation deploy \
        --template-file lambda-ams-computation.yaml \
        --stack-name "${STACK_NAME}-ams-computation" \
        --parameter-overrides \
            Environment="${ENVIRONMENT}" \
        --capabilities CAPABILITY_IAM \
        --region "${REGION}"
    
    # DynamoDB Publisher
    log_info "Deploying DynamoDB Publisher Lambda..."
    aws cloudformation deploy \
        --template-file lambda-dynamodb-publisher.yaml \
        --stack-name "${STACK_NAME}-dynamodb-publisher" \
        --parameter-overrides \
            Environment="${ENVIRONMENT}" \
        --capabilities CAPABILITY_IAM \
        --region "${REGION}"
    
    # ML Training
    log_info "Deploying ML Training Lambda..."
    aws cloudformation deploy \
        --template-file lambda-ml-training.yaml \
        --stack-name "${STACK_NAME}-ml-training" \
        --parameter-overrides \
            Environment="${ENVIRONMENT}" \
        --capabilities CAPABILITY_IAM \
        --region "${REGION}"
    
    log_success "All Lambda functions deployed"
}

# Create secrets in AWS Secrets Manager
create_secrets() {
    log_info "Creating secrets in AWS Secrets Manager..."
    
    # Apollo API Key
    if ! aws secretsmanager describe-secret --secret-id "thera/apollo/api-key" --region "${REGION}" &> /dev/null; then
        log_warning "Apollo API key not found. Please create it manually:"
        log_warning "aws secretsmanager create-secret --name 'thera/apollo/api-key' --secret-string '{\"apollo_api_key\":\"YOUR_API_KEY\"}' --region ${REGION}"
    else
        log_info "Apollo API key already exists"
    fi
    
    # Firecrawl API Key
    if ! aws secretsmanager describe-secret --secret-id "thera/firecrawl/api-key" --region "${REGION}" &> /dev/null; then
        log_warning "Firecrawl API key not found. Please create it manually:"
        log_warning "aws secretsmanager create-secret --name 'thera/firecrawl/api-key' --secret-string '{\"firecrawl_api_key\":\"YOUR_API_KEY\"}' --region ${REGION}"
    else
        log_info "Firecrawl API key already exists"
    fi
}

# Run data quality checks
run_data_quality_checks() {
    log_info "Running data quality checks..."
    
    # This would run the SQL queries from 10_data_quality_checks.sql
    # For now, just log that it should be done
    log_warning "Please run data quality checks manually using the SQL queries in 10_data_quality_checks.sql"
}

# Display deployment summary
display_summary() {
    log_success "Deployment completed successfully!"
    echo
    echo "=== DEPLOYMENT SUMMARY ==="
    echo "Environment: ${ENVIRONMENT}"
    echo "Region: ${REGION}"
    echo "Stack Name: ${STACK_NAME}"
    echo "Template Bucket: ${TEMPLATE_BUCKET}"
    echo
    echo "=== NEXT STEPS ==="
    echo "1. Create API keys in AWS Secrets Manager:"
    echo "   - thera/apollo/api-key"
    echo "   - thera/firecrawl/api-key"
    echo
    echo "2. Run data quality checks using 10_data_quality_checks.sql"
    echo
    echo "3. Monitor the pipeline using CloudWatch Dashboard"
    echo
    echo "4. Test individual Lambda functions"
    echo
    echo "=== USEFUL COMMANDS ==="
    echo "View CloudFormation stacks:"
    echo "  aws cloudformation list-stacks --region ${REGION}"
    echo
    echo "View Lambda functions:"
    echo "  aws lambda list-functions --region ${REGION}"
    echo
    echo "View S3 buckets:"
    echo "  aws s3 ls --region ${REGION}"
    echo
}

# Main deployment function
main() {
    log_info "Starting Thera Pipeline deployment..."
    log_info "Environment: ${ENVIRONMENT}"
    log_info "Region: ${REGION}"
    echo
    
    # Pre-deployment checks
    check_aws_cli
    check_aws_credentials
    
    # Deployment steps
    create_template_bucket
    create_data_buckets
    upload_templates
    deploy_infrastructure
    deploy_lambda_functions
    create_secrets
    run_data_quality_checks
    
    # Post-deployment
    display_summary
}

# Run main function
main "$@"