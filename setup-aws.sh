#!/bin/bash

# AWS Setup Script for Thera Pipeline
# This script helps configure AWS credentials and set up the environment

set -e

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
        log_error "AWS CLI is not installed. Please install it first:"
        log_error "brew install awscli"
        exit 1
    fi
    log_success "AWS CLI is installed"
}

# Configure AWS credentials
configure_aws_credentials() {
    log_info "Configuring AWS credentials..."
    
    if aws sts get-caller-identity &> /dev/null; then
        log_success "AWS credentials are already configured"
        return 0
    fi
    
    log_warning "AWS credentials not found. Please configure them:"
    echo
    echo "You can configure AWS credentials in several ways:"
    echo "1. Run 'aws configure' and enter your credentials"
    echo "2. Set environment variables:"
    echo "   export AWS_ACCESS_KEY_ID=your_access_key"
    echo "   export AWS_SECRET_ACCESS_KEY=your_secret_key"
    echo "   export AWS_DEFAULT_REGION=us-east-1"
    echo "3. Use AWS SSO: 'aws configure sso'"
    echo "4. Use IAM roles if running on EC2"
    echo
    echo "After configuring credentials, run this script again."
    exit 1
}

# Set default region
set_default_region() {
    local region=${1:-us-east-1}
    log_info "Setting default region to: ${region}"
    aws configure set default.region ${region}
    log_success "Default region set to: ${region}"
}

# Verify AWS access
verify_aws_access() {
    log_info "Verifying AWS access..."
    
    local account_id=$(aws sts get-caller-identity --query Account --output text)
    local user_arn=$(aws sts get-caller-identity --query Arn --output text)
    local region=$(aws configure get default.region)
    
    log_success "AWS access verified:"
    echo "  Account ID: ${account_id}"
    echo "  User/Role: ${user_arn}"
    echo "  Region: ${region}"
}

# Check required AWS services
check_aws_services() {
    log_info "Checking required AWS services..."
    
    local region=$(aws configure get default.region)
    
    # Check if services are available in the region
    local services=("lambda" "s3" "dynamodb" "athena" "bedrock" "secretsmanager" "cloudwatch" "stepfunctions" "events")
    
    for service in "${services[@]}"; do
        if aws ${service} help &> /dev/null; then
            log_success "✓ ${service} service available"
        else
            log_warning "⚠ ${service} service may not be available in region ${region}"
        fi
    done
}

# Create deployment configuration
create_deployment_config() {
    log_info "Creating deployment configuration..."
    
    local region=$(aws configure get default.region)
    local account_id=$(aws sts get-caller-identity --query Account --output text)
    
    cat > deployment-config.env << EOF
# Thera Pipeline Deployment Configuration
# Generated on $(date)

# AWS Configuration
AWS_ACCOUNT_ID=${account_id}
AWS_REGION=${region}

# Environment
ENVIRONMENT=dev

# S3 Buckets (will be created with random suffixes)
RAW_BUCKET=thera-raw-${account_id}
BRONZE_BUCKET=thera-bronze-${account_id}
CURATED_BUCKET=thera-curated-${account_id}
EMBEDDINGS_BUCKET=thera-embeddings-${account_id}
METRICS_BUCKET=thera-metrics-${account_id}
MODEL_BUCKET=thera-models-${account_id}

# Athena Configuration
ATHENA_DATABASE=thera_analytics
ATHENA_WORKGROUP=primary

# DynamoDB Tables
PUBLIC_TABLE=thera-startups-public
PRIVATE_TABLE=thera-startups-private

# API Keys (to be configured in Secrets Manager)
APOLLO_SECRET_NAME=thera/apollo/api-key
FIRECRAWL_SECRET_NAME=thera/firecrawl/api-key

# Bedrock Configuration
BEDROCK_MODEL_ID=amazon.titan-embed-text-v1
EOF

    log_success "Deployment configuration created: deployment-config.env"
}

# Main function
main() {
    log_info "Setting up AWS environment for Thera Pipeline..."
    echo
    
    # Pre-checks
    check_aws_cli
    configure_aws_credentials
    verify_aws_access
    check_aws_services
    create_deployment_config
    
    echo
    log_success "AWS setup completed successfully!"
    echo
    echo "Next steps:"
    echo "1. Review the deployment-config.env file"
    echo "2. Run './deployment/deploy.sh dev ${region}' to deploy the pipeline"
    echo "3. Configure API keys in AWS Secrets Manager"
    echo "4. Test the pipeline with sample data"
    echo
}

# Run main function
main "$@"
