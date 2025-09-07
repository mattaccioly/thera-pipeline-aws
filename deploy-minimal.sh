#!/bin/bash

# Thera Pipeline - Minimal Deployment Script
# This script deploys only the missing components

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

# Get AWS Account ID
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

print_status "Starting minimal Thera Pipeline deployment..."
print_status "Environment: $ENVIRONMENT"
print_status "Region: $REGION"
print_status "Account ID: $AWS_ACCOUNT_ID"

# Check existing Lambda functions
print_status "Checking existing Lambda functions..."
EXISTING_FUNCTIONS=$(aws lambda list-functions --query 'Functions[?contains(FunctionName, `dev-`) || contains(FunctionName, `thera`)].FunctionName' --output text)
print_status "Existing functions: $EXISTING_FUNCTIONS"

# Deploy missing Lambda functions one by one
print_status "Deploying missing Lambda functions..."

# List of functions we need
REQUIRED_FUNCTIONS=(
    "dev-domain-health-gate"
    "dev-athena-ctas-bronze-silver"
    "dev-athena-ctas-silver-gold"
    "dev-embeddings-batch"
    "dev-ams-computation"
    "dev-dynamodb-publisher"
    "dev-weekly-trainer"
    "dev-ams-job"
    "dev-evaluation-metrics"
    "dev-ml-training"
)

# Deploy each missing function
for func in "${REQUIRED_FUNCTIONS[@]}"; do
    if [[ ! "$EXISTING_FUNCTIONS" =~ $func ]]; then
        print_status "Deploying $func..."
        
        # Map function names to template files
        case $func in
            "dev-domain-health-gate")
                TEMPLATE="lambda-domain-health-gate.yaml"
                ;;
            "dev-athena-ctas-bronze-silver")
                TEMPLATE="lambda-athena-ctas-bronze-silver.yaml"
                ;;
            "dev-athena-ctas-silver-gold")
                TEMPLATE="lambda-athena-ctas-silver-gold.yaml"
                ;;
            "dev-embeddings-batch")
                TEMPLATE="lambda-embeddings-batch.yaml"
                ;;
            "dev-ams-computation")
                TEMPLATE="lambda-ams-computation.yaml"
                ;;
            "dev-dynamodb-publisher")
                TEMPLATE="lambda-dynamodb-publisher.yaml"
                ;;
            "dev-weekly-trainer")
                TEMPLATE="lambda-weekly-trainer.yaml"
                ;;
            "dev-ams-job")
                TEMPLATE="lambda-ams-job.yaml"
                ;;
            "dev-evaluation-metrics")
                TEMPLATE="lambda-evaluation-metrics.yaml"
                ;;
            "dev-ml-training")
                TEMPLATE="lambda-ml-training.yaml"
                ;;
        esac
        
        if [[ -f "$TEMPLATE" ]]; then
            STACK_NAME="thera-${func}-$(date +%s)"
            print_status "Deploying $TEMPLATE as $STACK_NAME..."
            
            aws cloudformation deploy \
                --template-file "$TEMPLATE" \
                --stack-name "$STACK_NAME" \
                --parameter-overrides \
                    Environment="$ENVIRONMENT" \
                    RawBucket="thera-raw-805595753342-v3" \
                    BronzeBucket="thera-bronze-805595753342-v3" \
                    CuratedBucket="thera-curated-805595753342-v3" \
                    EmbeddingsBucket="thera-embeddings-805595753342-v3" \
                    MetricsBucket="thera-metrics-805595753342-v3" \
                    ModelBucket="thera-models-805595753342-v3" \
                    AthenaDatabase="thera_analytics" \
                    AthenaWorkgroup="primary" \
                    PublicTableName="thera-startups-public" \
                    PrivateTableName="thera-startups-private" \
                    ApolloSecretName="thera/apollo/api-key" \
                    FirecrawlSecretName="thera/firecrawl/api-key" \
                    BedrockModelId="amazon.titan-embed-text-v1" \
                --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM \
                --region "$REGION" > /dev/null 2>&1
            
            if [[ $? -eq 0 ]]; then
                print_success "Deployed $func successfully"
            else
                print_warning "Failed to deploy $func (might already exist or have issues)"
            fi
        else
            print_warning "Template $TEMPLATE not found, skipping $func"
        fi
    else
        print_success "$func already exists, skipping"
    fi
done

# Deploy Step Functions
print_status "Deploying Step Functions state machines..."

# Daily Step Function
DAILY_STATE_MACHINE_NAME="${ENVIRONMENT}-thera-daily-pipeline"
print_status "Deploying daily state machine: $DAILY_STATE_MACHINE_NAME"

# Check if state machine already exists
if aws stepfunctions describe-state-machine --state-machine-arn "arn:aws:stepfunctions:${REGION}:${AWS_ACCOUNT_ID}:stateMachine:${DAILY_STATE_MACHINE_NAME}" --region "$REGION" > /dev/null 2>&1; then
    print_status "Updating existing daily state machine..."
    aws stepfunctions update-state-machine \
        --state-machine-arn "arn:aws:stepfunctions:${REGION}:${AWS_ACCOUNT_ID}:stateMachine:${DAILY_STATE_MACHINE_NAME}" \
        --definition file://step-functions-daily-state-machine.json \
        --region "$REGION" > /dev/null
    print_success "Updated daily state machine"
else
    print_status "Creating new daily state machine..."
    aws stepfunctions create-state-machine \
        --name "$DAILY_STATE_MACHINE_NAME" \
        --definition file://step-functions-daily-state-machine.json \
        --role-arn "arn:aws:iam::${AWS_ACCOUNT_ID}:role/StepFunctionsExecutionRole" \
        --region "$REGION" > /dev/null 2>&1 || {
        print_warning "Failed to create daily state machine (role might not exist)"
    }
fi

# Weekly Step Function
WEEKLY_STATE_MACHINE_NAME="${ENVIRONMENT}-thera-weekly-pipeline"
print_status "Deploying weekly state machine: $WEEKLY_STATE_MACHINE_NAME"

if aws stepfunctions describe-state-machine --state-machine-arn "arn:aws:stepfunctions:${REGION}:${AWS_ACCOUNT_ID}:stateMachine:${WEEKLY_STATE_MACHINE_NAME}" --region "$REGION" > /dev/null 2>&1; then
    print_status "Updating existing weekly state machine..."
    aws stepfunctions update-state-machine \
        --state-machine-arn "arn:aws:stepfunctions:${REGION}:${AWS_ACCOUNT_ID}:stateMachine:${WEEKLY_STATE_MACHINE_NAME}" \
        --definition file://step-functions-weekly-state-machine.json \
        --region "$REGION" > /dev/null
    print_success "Updated weekly state machine"
else
    print_status "Creating new weekly state machine..."
    aws stepfunctions create-state-machine \
        --name "$WEEKLY_STATE_MACHINE_NAME" \
        --definition file://step-functions-weekly-state-machine.json \
        --role-arn "arn:aws:iam::${AWS_ACCOUNT_ID}:role/StepFunctionsExecutionRole" \
        --region "$REGION" > /dev/null 2>&1 || {
        print_warning "Failed to create weekly state machine (role might not exist)"
    }
fi

# Set up EventBridge rules
print_status "Setting up EventBridge rules..."

# Daily rule
DAILY_RULE_NAME="${ENVIRONMENT}-thera-daily-trigger"
print_status "Creating daily EventBridge rule: $DAILY_RULE_NAME"

aws events put-rule \
    --name "$DAILY_RULE_NAME" \
    --schedule-expression "cron(0 6 * * ? *)" \
    --description "Daily trigger for Thera pipeline" \
    --region "$REGION" > /dev/null 2>&1 || print_warning "Daily rule might already exist"

aws events put-targets \
    --rule "$DAILY_RULE_NAME" \
    --targets "Id"="1","Arn"="arn:aws:stepfunctions:${REGION}:${AWS_ACCOUNT_ID}:stateMachine:${DAILY_STATE_MACHINE_NAME}" \
    --region "$REGION" > /dev/null 2>&1 || print_warning "Failed to set daily rule target"

# Weekly rule
WEEKLY_RULE_NAME="${ENVIRONMENT}-thera-weekly-trigger"
print_status "Creating weekly EventBridge rule: $WEEKLY_RULE_NAME"

aws events put-rule \
    --name "$WEEKLY_RULE_NAME" \
    --schedule-expression "cron(0 8 ? * SUN *)" \
    --description "Weekly trigger for Thera ML pipeline" \
    --region "$REGION" > /dev/null 2>&1 || print_warning "Weekly rule might already exist"

aws events put-targets \
    --rule "$WEEKLY_RULE_NAME" \
    --targets "Id"="1","Arn"="arn:aws:stepfunctions:${REGION}:${AWS_ACCOUNT_ID}:stateMachine:${WEEKLY_STATE_MACHINE_NAME}" \
    --region "$REGION" > /dev/null 2>&1 || print_warning "Failed to set weekly rule target"

# Final status
print_success "🎉 Minimal deployment completed!"
print_status "Environment: $ENVIRONMENT"
print_status "Region: $REGION"
print_status "Daily State Machine: $DAILY_STATE_MACHINE_NAME"
print_status "Weekly State Machine: $WEEKLY_STATE_MACHINE_NAME"
print_status "Daily Rule: $DAILY_RULE_NAME (runs at 6 AM UTC daily)"
print_status "Weekly Rule: $WEEKLY_RULE_NAME (runs at 8 AM UTC on Sundays)"

print_status ""
print_status "Next steps:"
print_status "1. Configure API keys in AWS Secrets Manager if not done"
print_status "2. Enable Bedrock access if not done"
print_status "3. Test the pipeline by running the daily state machine manually"
print_status "4. Run ./test-pipeline.sh to validate everything"

print_success "Minimal deployment complete! 🚀"
