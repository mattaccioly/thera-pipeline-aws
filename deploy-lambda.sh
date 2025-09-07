#!/bin/bash

# Script to deploy Lambda functions for Thera Pipeline
set -e

FUNCTION_NAME=$1
PYTHON_FILE=$2
ROLE_NAME="dev-${FUNCTION_NAME}-role"
POLICY_NAME="dev-${FUNCTION_NAME}-policy"
S3_BUCKET="thera-pipeline-templates-dev-1757187400"

if [ -z "$FUNCTION_NAME" ] || [ -z "$PYTHON_FILE" ]; then
    echo "Usage: $0 <function-name> <python-file>"
    echo "Example: $0 firecrawl-orchestrator lambda-firecrawl-orchestrator.py"
    exit 1
fi

echo "Deploying Lambda function: $FUNCTION_NAME"

# Create ZIP package
echo "Creating ZIP package..."
zip "${FUNCTION_NAME}.zip" "$PYTHON_FILE"

# Upload to S3
echo "Uploading to S3..."
aws s3 cp "${FUNCTION_NAME}.zip" "s3://${S3_BUCKET}/${FUNCTION_NAME}.zip"

# Create IAM role
echo "Creating IAM role..."
cat > "${FUNCTION_NAME}-role-trust-policy.json" << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF

aws iam create-role --role-name "$ROLE_NAME" --assume-role-policy-document "file://${FUNCTION_NAME}-role-trust-policy.json" || echo "Role already exists"

# Attach basic execution policy
aws iam attach-role-policy --role-name "$ROLE_NAME" --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole

# Create custom policy based on function type
case $FUNCTION_NAME in
    "firecrawl-orchestrator")
        cat > "${FUNCTION_NAME}-policy.json" << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject"
      ],
      "Resource": [
        "arn:aws:s3:::thera-raw/*",
        "arn:aws:s3:::thera-curated/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "athena:StartQueryExecution",
        "athena:GetQueryExecution",
        "athena:GetQueryResults",
        "athena:StopQueryExecution"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "secretsmanager:GetSecretValue"
      ],
      "Resource": "arn:aws:secretsmanager:us-east-1:805595753342:secret:thera/firecrawl/api-key*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "cloudwatch:PutMetricData"
      ],
      "Resource": "*"
    }
  ]
}
EOF
        ENV_VARS='{RAW_BUCKET=thera-raw,CURATED_BUCKET=thera-curated,ATHENA_DATABASE=thera_analytics,ATHENA_WORKGROUP=primary,FIRECRAWL_SECRET_NAME=thera/firecrawl/api-key}'
        TIMEOUT=600
        MEMORY=512
        ;;
    "embeddings-batch")
        cat > "${FUNCTION_NAME}-policy.json" << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject"
      ],
      "Resource": [
        "arn:aws:s3:::thera-curated/*",
        "arn:aws:s3:::thera-embeddings/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "athena:StartQueryExecution",
        "athena:GetQueryExecution",
        "athena:GetQueryResults",
        "athena:StopQueryExecution"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "bedrock:InvokeModel"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "cloudwatch:PutMetricData"
      ],
      "Resource": "*"
    }
  ]
}
EOF
        ENV_VARS='{CURATED_BUCKET=thera-curated,EMBEDDINGS_BUCKET=thera-embeddings,ATHENA_DATABASE=thera_analytics,ATHENA_WORKGROUP=primary,BEDROCK_MODEL_ID=amazon.titan-embed-text-v1}'
        TIMEOUT=900
        MEMORY=1024
        ;;
    "matcher")
        cat > "${FUNCTION_NAME}-policy.json" << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject"
      ],
      "Resource": [
        "arn:aws:s3:::thera-embeddings/*",
        "arn:aws:s3:::thera-models/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "athena:StartQueryExecution",
        "athena:GetQueryExecution",
        "athena:GetQueryResults",
        "athena:StopQueryExecution"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "bedrock:InvokeModel"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "cloudwatch:PutMetricData"
      ],
      "Resource": "*"
    }
  ]
}
EOF
        ENV_VARS='{ATHENA_DATABASE=thera_analytics,ATHENA_WORKGROUP=primary,EMBEDDINGS_BUCKET=thera-embeddings,MODEL_BUCKET=thera-models,BEDROCK_MODEL_ID=amazon.titan-embed-text-v1}'
        TIMEOUT=600
        MEMORY=1024
        ;;
    "ams-computation")
        cat > "${FUNCTION_NAME}-policy.json" << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject"
      ],
      "Resource": [
        "arn:aws:s3:::thera-curated/*",
        "arn:aws:s3:::thera-metrics/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "athena:StartQueryExecution",
        "athena:GetQueryExecution",
        "athena:GetQueryResults",
        "athena:StopQueryExecution"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "cloudwatch:PutMetricData"
      ],
      "Resource": "*"
    }
  ]
}
EOF
        ENV_VARS='{CURATED_BUCKET=thera-curated,METRICS_BUCKET=thera-metrics,ATHENA_DATABASE=thera_analytics,ATHENA_WORKGROUP=primary}'
        TIMEOUT=600
        MEMORY=512
        ;;
    "dynamodb-publisher")
        cat > "${FUNCTION_NAME}-policy.json" << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "dynamodb:GetItem",
        "dynamodb:PutItem",
        "dynamodb:UpdateItem",
        "dynamodb:DeleteItem",
        "dynamodb:Query",
        "dynamodb:Scan"
      ],
      "Resource": [
        "arn:aws:dynamodb:us-east-1:805595753342:table/thera-startups-public",
        "arn:aws:dynamodb:us-east-1:805595753342:table/thera-startups-private"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "athena:StartQueryExecution",
        "athena:GetQueryExecution",
        "athena:GetQueryResults",
        "athena:StopQueryExecution"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "cloudwatch:PutMetricData"
      ],
      "Resource": "*"
    }
  ]
}
EOF
        ENV_VARS='{PUBLIC_TABLE_NAME=thera-startups-public,PRIVATE_TABLE_NAME=thera-startups-private,ATHENA_DATABASE=thera_analytics,ATHENA_WORKGROUP=primary}'
        TIMEOUT=300
        MEMORY=256
        ;;
    "ml-training")
        cat > "${FUNCTION_NAME}-policy.json" << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject"
      ],
      "Resource": [
        "arn:aws:s3:::thera-curated/*",
        "arn:aws:s3:::thera-models/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "athena:StartQueryExecution",
        "athena:GetQueryExecution",
        "athena:GetQueryResults",
        "athena:StopQueryExecution"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "cloudwatch:PutMetricData"
      ],
      "Resource": "*"
    }
  ]
}
EOF
        ENV_VARS='{CURATED_BUCKET=thera-curated,MODEL_BUCKET=thera-models,ATHENA_DATABASE=thera_analytics,ATHENA_WORKGROUP=primary}'
        TIMEOUT=900
        MEMORY=2048
        ;;
    *)
        echo "Unknown function: $FUNCTION_NAME"
        exit 1
        ;;
esac

# Create policy
aws iam create-policy --policy-name "$POLICY_NAME" --policy-document "file://${FUNCTION_NAME}-policy.json" || echo "Policy already exists"

# Attach policy to role
aws iam attach-role-policy --role-name "$ROLE_NAME" --policy-arn "arn:aws:iam::805595753342:policy/$POLICY_NAME"

# Create Lambda function
echo "Creating Lambda function..."
aws lambda create-function \
    --function-name "dev-$FUNCTION_NAME" \
    --runtime python3.12 \
    --role "arn:aws:iam::805595753342:role/$ROLE_NAME" \
    --handler "${PYTHON_FILE%.*}.lambda_handler" \
    --code "S3Bucket=$S3_BUCKET,S3Key=${FUNCTION_NAME}.zip" \
    --timeout $TIMEOUT \
    --memory-size $MEMORY \
    --environment "Variables=$ENV_VARS" \
    --region us-east-1

echo "Successfully deployed: dev-$FUNCTION_NAME"

# Cleanup
rm -f "${FUNCTION_NAME}.zip" "${FUNCTION_NAME}-role-trust-policy.json" "${FUNCTION_NAME}-policy.json"
