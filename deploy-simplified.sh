#!/bin/bash

# Thera Pipeline - Simplified Deployment Script
# This script deploys the cleaned up and simplified pipeline

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
STACK_NAME="thera-pipeline-${ENVIRONMENT}"

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

# Function to show usage
show_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -e, --environment ENV    Environment (dev, staging, prod) [default: dev]"
    echo "  -r, --region REGION      AWS region [default: us-east-1]"
    echo "  -h, --help              Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                                    # Deploy to dev in us-east-1"
    echo "  $0 -e staging -r us-west-2           # Deploy to staging in us-west-2"
    echo "  $0 -e prod                           # Deploy to production in us-east-1"
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
            show_usage
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

# Validate environment
if [[ ! "$ENVIRONMENT" =~ ^(dev|staging|prod)$ ]]; then
    print_error "Invalid environment: $ENVIRONMENT. Must be dev, staging, or prod."
    exit 1
fi

# Update stack name
STACK_NAME="thera-pipeline-${ENVIRONMENT}"

print_status "Starting Thera Pipeline deployment..."
print_status "Environment: $ENVIRONMENT"
print_status "Region: $REGION"
print_status "Stack Name: $STACK_NAME"

# Check if AWS CLI is installed and configured
if ! command -v aws &> /dev/null; then
    print_error "AWS CLI is not installed. Please install it first."
    exit 1
fi

# Check AWS credentials
if ! aws sts get-caller-identity &> /dev/null; then
    print_error "AWS credentials not configured. Please run 'aws configure' first."
    exit 1
fi

print_success "AWS CLI is configured"

# Get AWS Account ID
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

# Create S3 bucket for templates if it doesn't exist
TEMPLATE_BUCKET="${AWS_ACCOUNT_ID}-${REGION}-thera-pipeline"
print_status "Checking template bucket: $TEMPLATE_BUCKET"

if ! aws s3 ls "s3://$TEMPLATE_BUCKET" &> /dev/null; then
    print_status "Creating template bucket: $TEMPLATE_BUCKET"
    aws s3 mb "s3://$TEMPLATE_BUCKET" --region "$REGION"
    print_success "Template bucket created"
else
    print_success "Template bucket already exists"
fi

# Upload templates to S3
print_status "Uploading CloudFormation templates to S3..."

# List of template files to upload
TEMPLATES=(
    "main-infrastructure.yaml"
    "lambda-apollo-delta-pull.yaml"
    "lambda-domain-health-gate.yaml"
    "lambda-firecrawl-orchestrator.yaml"
    "lambda-athena-ctas-bronze-silver.yaml"
    "lambda-athena-ctas-silver-gold.yaml"
    "lambda-embeddings-batch.yaml"
    "lambda-matcher.yaml"
    "lambda-ams-computation.yaml"
    "lambda-dynamodb-publisher.yaml"
    "lambda-advanced-summarization.yaml"
    "lambda-cost-monitor.yaml"
    "lambda-weekly-trainer.yaml"
    "lambda-ams-job.yaml"
    "lambda-evaluation-metrics.yaml"
    "lambda-ml-training.yaml"
)

for template in "${TEMPLATES[@]}"; do
    if [[ -f "$template" ]]; then
        print_status "Uploading $template..."
        aws s3 cp "$template" "s3://$TEMPLATE_BUCKET/$template"
        print_success "Uploaded $template"
    else
        print_warning "Template file $template not found, skipping..."
    fi
done

# Deploy the main stack
print_status "Deploying main CloudFormation stack..."

# Use existing infrastructure bucket names
EXISTING_RAW_BUCKET="thera-raw-805595753342-v3"
EXISTING_BRONZE_BUCKET="thera-bronze-805595753342-v3"
EXISTING_CURATED_BUCKET="thera-curated-805595753342-v3"
EXISTING_EMBEDDINGS_BUCKET="thera-embeddings-805595753342-v3"
EXISTING_METRICS_BUCKET="thera-metrics-805595753342-v3"
EXISTING_MODEL_BUCKET="thera-models-805595753342-v3"

aws cloudformation deploy \
    --template-file master-deployment.yaml \
    --stack-name "$STACK_NAME" \
    --parameter-overrides \
        Environment="$ENVIRONMENT" \
        RawBucket="$EXISTING_RAW_BUCKET" \
        BronzeBucket="$EXISTING_BRONZE_BUCKET" \
        CuratedBucket="$EXISTING_CURATED_BUCKET" \
        EmbeddingsBucket="$EXISTING_EMBEDDINGS_BUCKET" \
        MetricsBucket="$EXISTING_METRICS_BUCKET" \
        ModelBucket="$EXISTING_MODEL_BUCKET" \
        AthenaDatabase="thera_analytics" \
        AthenaWorkgroup="primary" \
        PublicTableName="thera-startups-public" \
        PrivateTableName="thera-startups-private" \
        ApolloSecretName="thera/apollo/api-key" \
        FirecrawlSecretName="thera/firecrawl/api-key" \
        BedrockModelId="amazon.titan-embed-text-v1" \
    --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM \
    --region "$REGION"

if [[ $? -eq 0 ]]; then
    print_success "CloudFormation stack deployed successfully!"
else
    print_error "CloudFormation deployment failed!"
    exit 1
fi

# Deploy Step Functions
print_status "Deploying Step Functions state machines..."

# Daily Step Function
DAILY_STATE_MACHINE_NAME="${ENVIRONMENT}-thera-daily-pipeline"
print_status "Deploying daily state machine: $DAILY_STATE_MACHINE_NAME"

aws stepfunctions create-state-machine \
    --name "$DAILY_STATE_MACHINE_NAME" \
    --definition file://step-functions-daily-state-machine.json \
    --role-arn "arn:aws:iam::${AWS_ACCOUNT_ID}:role/StepFunctionsExecutionRole" \
    --region "$REGION" \
    --output json > /dev/null 2>&1 || {
    print_warning "Daily state machine might already exist, updating..."
    aws stepfunctions update-state-machine \
        --state-machine-arn "arn:aws:stepfunctions:${REGION}:${AWS_ACCOUNT_ID}:stateMachine:${DAILY_STATE_MACHINE_NAME}" \
        --definition file://step-functions-daily-state-machine.json \
        --region "$REGION" > /dev/null
}

# Weekly Step Function
WEEKLY_STATE_MACHINE_NAME="${ENVIRONMENT}-thera-weekly-pipeline"
print_status "Deploying weekly state machine: $WEEKLY_STATE_MACHINE_NAME"

aws stepfunctions create-state-machine \
    --name "$WEEKLY_STATE_MACHINE_NAME" \
    --definition file://step-functions-weekly-state-machine.json \
    --role-arn "arn:aws:iam::${AWS_ACCOUNT_ID}:role/StepFunctionsExecutionRole" \
    --region "$REGION" \
    --output json > /dev/null 2>&1 || {
    print_warning "Weekly state machine might already exist, updating..."
    aws stepfunctions update-state-machine \
        --state-machine-arn "arn:aws:stepfunctions:${REGION}:${AWS_ACCOUNT_ID}:stateMachine:${WEEKLY_STATE_MACHINE_NAME}" \
        --definition file://step-functions-weekly-state-machine.json \
        --region "$REGION" > /dev/null
}

print_success "Step Functions deployed successfully!"

# Set up EventBridge rules
print_status "Setting up EventBridge rules..."

# Daily rule
DAILY_RULE_NAME="${ENVIRONMENT}-thera-daily-trigger"
print_status "Creating daily EventBridge rule: $DAILY_RULE_NAME"

aws events put-rule \
    --name "$DAILY_RULE_NAME" \
    --schedule-expression "cron(0 6 * * ? *)" \
    --description "Daily trigger for Thera pipeline" \
    --region "$REGION" > /dev/null

aws events put-targets \
    --rule "$DAILY_RULE_NAME" \
    --targets "Id"="1","Arn"="arn:aws:stepfunctions:${REGION}:${AWS_ACCOUNT_ID}:stateMachine:${DAILY_STATE_MACHINE_NAME}" \
    --region "$REGION" > /dev/null

# Weekly rule
WEEKLY_RULE_NAME="${ENVIRONMENT}-thera-weekly-trigger"
print_status "Creating weekly EventBridge rule: $WEEKLY_RULE_NAME"

aws events put-rule \
    --name "$WEEKLY_RULE_NAME" \
    --schedule-expression "cron(0 8 ? * SUN *)" \
    --description "Weekly trigger for Thera ML pipeline" \
    --region "$REGION" > /dev/null

aws events put-targets \
    --rule "$WEEKLY_RULE_NAME" \
    --targets "Id"="1","Arn"="arn:aws:stepfunctions:${REGION}:${AWS_ACCOUNT_ID}:stateMachine:${WEEKLY_STATE_MACHINE_NAME}" \
    --region "$REGION" > /dev/null

print_success "EventBridge rules created successfully!"

# Final status
print_success "🎉 Thera Pipeline deployment completed successfully!"
print_status "Environment: $ENVIRONMENT"
print_status "Region: $REGION"
print_status "Stack Name: $STACK_NAME"
print_status "Daily State Machine: $DAILY_STATE_MACHINE_NAME"
print_status "Weekly State Machine: $WEEKLY_STATE_MACHINE_NAME"
print_status "Daily Rule: $DAILY_RULE_NAME (runs at 6 AM UTC daily)"
print_status "Weekly Rule: $WEEKLY_RULE_NAME (runs at 8 AM UTC on Sundays)"

print_status ""
print_status "Next steps:"
print_status "1. Configure API keys in AWS Secrets Manager:"
print_status "   - thera/apollo/api-key"
print_status "   - thera/firecrawl/api-key"
print_status "2. Enable Bedrock access in your AWS account"
print_status "3. Test the pipeline by running the daily state machine manually"
print_status "4. Monitor the CloudWatch dashboard for metrics and logs"

print_success "Deployment complete! 🚀"
