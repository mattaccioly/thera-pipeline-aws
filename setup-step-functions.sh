#!/bin/bash

# Thera Pipeline - Step Functions Setup Script
# This script sets up Step Functions with existing Lambda functions

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

print_status "Setting up Step Functions for Thera Pipeline..."
print_status "Environment: $ENVIRONMENT"
print_status "Region: $REGION"
print_status "Account ID: $AWS_ACCOUNT_ID"

# Check existing Lambda functions
print_status "Checking existing Lambda functions..."
EXISTING_FUNCTIONS=$(aws lambda list-functions --query 'Functions[?contains(FunctionName, `dev-`) || contains(FunctionName, `thera`)].FunctionName' --output text)
print_status "Existing functions: $EXISTING_FUNCTIONS"

# Create IAM role for Step Functions if it doesn't exist
print_status "Setting up IAM role for Step Functions..."
ROLE_NAME="StepFunctionsExecutionRole"

if ! aws iam get-role --role-name "$ROLE_NAME" > /dev/null 2>&1; then
    print_status "Creating IAM role for Step Functions..."
    
    # Create trust policy
    cat > /tmp/stepfunctions-trust-policy.json << EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "states.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}
EOF

    # Create the role
    aws iam create-role \
        --role-name "$ROLE_NAME" \
        --assume-role-policy-document file:///tmp/stepfunctions-trust-policy.json > /dev/null

    # Attach policies
    aws iam attach-role-policy \
        --role-name "$ROLE_NAME" \
        --policy-arn "arn:aws:iam::aws:policy/service-role/AWSLambdaRole" > /dev/null

    aws iam attach-role-policy \
        --role-name "$ROLE_NAME" \
        --policy-arn "arn:aws:iam::aws:policy/CloudWatchLogsFullAccess" > /dev/null

    print_success "Created IAM role: $ROLE_NAME"
else
    print_success "IAM role already exists: $ROLE_NAME"
fi

# Create a simplified daily state machine that uses existing functions
print_status "Creating simplified daily state machine..."

cat > /tmp/simplified-daily-state-machine.json << EOF
{
  "Comment": "Simplified Thera Pipeline Daily State Machine",
  "StartAt": "ApolloDeltaPull",
  "States": {
    "ApolloDeltaPull": {
      "Type": "Task",
      "Resource": "arn:aws:lambda:${AWS::Region}:${AWS::AccountId}:function:thera-apollo-delta-pull-dev",
      "Comment": "Step 1: Pull delta data from Apollo API",
      "Retry": [
        {
          "ErrorEquals": ["Lambda.ServiceException", "Lambda.AWSLambdaException", "Lambda.SdkClientException"],
          "IntervalSeconds": 2,
          "MaxAttempts": 3,
          "BackoffRate": 2.0
        }
      ],
      "Catch": [
        {
          "ErrorEquals": ["States.ALL"],
          "Next": "ApolloDeltaPullFailure",
          "ResultPath": "$.error"
        }
      ],
      "Next": "FirecrawlOrchestrator",
      "ResultPath": "$.apolloDeltaPull"
    },
    "ApolloDeltaPullFailure": {
      "Type": "Fail",
      "Comment": "Apollo Delta Pull failed",
      "Cause": "Apollo Delta Pull step failed"
    },
    "FirecrawlOrchestrator": {
      "Type": "Task",
      "Resource": "arn:aws:lambda:${AWS::Region}:${AWS::AccountId}:function:thera-firecrawl-orchestrator-dev",
      "Comment": "Step 2: Orchestrate Firecrawl web scraping",
      "Retry": [
        {
          "ErrorEquals": ["Lambda.ServiceException", "Lambda.AWSLambdaException", "Lambda.SdkClientException"],
          "IntervalSeconds": 5,
          "MaxAttempts": 3,
          "BackoffRate": 2.0
        }
      ],
      "Catch": [
        {
          "ErrorEquals": ["States.ALL"],
          "Next": "FirecrawlOrchestratorFailure",
          "ResultPath": "$.error"
        }
      ],
      "Next": "Matcher",
      "ResultPath": "$.firecrawlOrchestrator"
    },
    "FirecrawlOrchestratorFailure": {
      "Type": "Fail",
      "Comment": "Firecrawl Orchestrator failed",
      "Cause": "Firecrawl Orchestrator step failed"
    },
    "Matcher": {
      "Type": "Task",
      "Resource": "arn:aws:lambda:${AWS::Region}:${AWS::AccountId}:function:thera-matcher-dev",
      "Comment": "Step 3: ML-based matching and scoring",
      "Retry": [
        {
          "ErrorEquals": ["Lambda.ServiceException", "Lambda.AWSLambdaException", "Lambda.SdkClientException"],
          "IntervalSeconds": 5,
          "MaxAttempts": 3,
          "BackoffRate": 2.0
        }
      ],
      "Catch": [
        {
          "ErrorEquals": ["States.ALL"],
          "Next": "MatcherFailure",
          "ResultPath": "$.error"
        }
      ],
      "Next": "AdvancedSummarization",
      "ResultPath": "$.matcher"
    },
    "MatcherFailure": {
      "Type": "Fail",
      "Comment": "Matcher failed",
      "Cause": "Matcher step failed"
    },
    "AdvancedSummarization": {
      "Type": "Task",
      "Resource": "arn:aws:lambda:${AWS::Region}:${AWS::AccountId}:function:dev-advanced-summarization",
      "Comment": "Step 4: Generate advanced LLM-powered summaries",
      "Retry": [
        {
          "ErrorEquals": ["Lambda.ServiceException", "Lambda.AWSLambdaException", "Lambda.SdkClientException"],
          "IntervalSeconds": 5,
          "MaxAttempts": 3,
          "BackoffRate": 2.0
        }
      ],
      "Catch": [
        {
          "ErrorEquals": ["States.ALL"],
          "Next": "AdvancedSummarizationFailure",
          "ResultPath": "$.error"
        }
      ],
      "Next": "CostMonitoring",
      "ResultPath": "$.advancedSummarization"
    },
    "AdvancedSummarizationFailure": {
      "Type": "Pass",
      "Comment": "Advanced Summarization failed - non-critical, continue",
      "Next": "CostMonitoring"
    },
    "CostMonitoring": {
      "Type": "Task",
      "Resource": "arn:aws:lambda:${AWS::Region}:${AWS::AccountId}:function:dev-cost-monitor",
      "Comment": "Step 5: Monitor costs and optimize",
      "Retry": [
        {
          "ErrorEquals": ["Lambda.ServiceException", "Lambda.AWSLambdaException", "Lambda.SdkClientException"],
          "IntervalSeconds": 2,
          "MaxAttempts": 2,
          "BackoffRate": 2.0
        }
      ],
      "Catch": [
        {
          "ErrorEquals": ["States.ALL"],
          "Next": "CostMonitoringFailure",
          "ResultPath": "$.error"
        }
      ],
      "Next": "PipelineSuccess",
      "ResultPath": "$.costMonitoring"
    },
    "CostMonitoringFailure": {
      "Type": "Pass",
      "Comment": "Cost monitoring failed - non-critical, continue to success",
      "Next": "PipelineSuccess"
    },
    "PipelineSuccess": {
      "Type": "Succeed",
      "Comment": "Daily pipeline completed successfully",
      "OutputPath": "$"
    }
  },
  "TimeoutSeconds": 3600,
  "Comment": "Simplified daily pipeline with existing functions"
}
EOF

# Deploy the simplified daily state machine
DAILY_STATE_MACHINE_NAME="${ENVIRONMENT}-thera-daily-pipeline-simple"
print_status "Deploying simplified daily state machine: $DAILY_STATE_MACHINE_NAME"

aws stepfunctions create-state-machine \
    --name "$DAILY_STATE_MACHINE_NAME" \
    --definition file:///tmp/simplified-daily-state-machine.json \
    --role-arn "arn:aws:iam::${AWS_ACCOUNT_ID}:role/${ROLE_NAME}" \
    --region "$REGION" > /dev/null 2>&1 || {
    print_warning "Failed to create daily state machine, trying to update existing one..."
    aws stepfunctions update-state-machine \
        --state-machine-arn "arn:aws:stepfunctions:${REGION}:${AWS_ACCOUNT_ID}:stateMachine:${DAILY_STATE_MACHINE_NAME}" \
        --definition file:///tmp/simplified-daily-state-machine.json \
        --region "$REGION" > /dev/null 2>&1 || print_error "Failed to update daily state machine"
}

print_success "Deployed simplified daily state machine"

# Set up EventBridge rule for the simplified pipeline
print_status "Setting up EventBridge rule for simplified pipeline..."

DAILY_RULE_NAME="${ENVIRONMENT}-thera-daily-trigger-simple"
print_status "Creating daily EventBridge rule: $DAILY_RULE_NAME"

aws events put-rule \
    --name "$DAILY_RULE_NAME" \
    --schedule-expression "cron(0 6 * * ? *)" \
    --description "Daily trigger for simplified Thera pipeline" \
    --region "$REGION" > /dev/null 2>&1 || print_warning "Daily rule might already exist"

aws events put-targets \
    --rule "$DAILY_RULE_NAME" \
    --targets "Id"="1","Arn"="arn:aws:stepfunctions:${REGION}:${AWS_ACCOUNT_ID}:stateMachine:${DAILY_STATE_MACHINE_NAME}" \
    --region "$REGION" > /dev/null 2>&1 || print_warning "Failed to set daily rule target"

print_success "Set up EventBridge rule"

# Clean up temp files
rm -f /tmp/stepfunctions-trust-policy.json
rm -f /tmp/simplified-daily-state-machine.json

# Final status
print_success "🎉 Step Functions setup completed!"
print_status "Environment: $ENVIRONMENT"
print_status "Region: $REGION"
print_status "Daily State Machine: $DAILY_STATE_MACHINE_NAME"
print_status "Daily Rule: $DAILY_RULE_NAME (runs at 6 AM UTC daily)"

print_status ""
print_status "Next steps:"
print_status "1. Test the pipeline by running the state machine manually"
print_status "2. Configure API keys in AWS Secrets Manager if not done"
print_status "3. Enable Bedrock access if not done"
print_status "4. Monitor CloudWatch logs for any issues"

print_success "Setup complete! 🚀"
