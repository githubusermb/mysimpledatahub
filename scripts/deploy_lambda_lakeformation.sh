#!/bin/bash

# Deploy Lake Formation Setup Lambda Function
# This script packages and deploys the Lambda function to AWS

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
FUNCTION_NAME="setup-lakeformation"
RUNTIME="python3.11"
HANDLER="lambda_lakeformation_setup.lambda_handler"
TIMEOUT=300
MEMORY=512
REGION="${AWS_REGION:-us-east-1}"

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Lake Formation Lambda Deployment${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""

# Check prerequisites
echo -e "${YELLOW}Checking prerequisites...${NC}"

if ! command -v aws &> /dev/null; then
    echo -e "${RED}✗ AWS CLI not found. Please install it first.${NC}"
    exit 1
fi
echo -e "${GREEN}✓ AWS CLI found${NC}"

if ! command -v python3 &> /dev/null; then
    echo -e "${RED}✗ Python 3 not found. Please install it first.${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Python 3 found${NC}"

if ! command -v zip &> /dev/null; then
    echo -e "${RED}✗ zip not found. Please install it first.${NC}"
    exit 1
fi
echo -e "${GREEN}✓ zip found${NC}"

# Get AWS account ID
echo ""
echo -e "${YELLOW}Getting AWS account information...${NC}"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
echo -e "${GREEN}✓ Account ID: ${ACCOUNT_ID}${NC}"

# Check if IAM role exists
ROLE_NAME="LambdaLakeFormationSetupRole"
ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}"

echo ""
echo -e "${YELLOW}Checking IAM role...${NC}"
if aws iam get-role --role-name ${ROLE_NAME} &> /dev/null; then
    echo -e "${GREEN}✓ IAM role exists: ${ROLE_NAME}${NC}"
else
    echo -e "${YELLOW}Creating IAM role: ${ROLE_NAME}${NC}"
    
    # Create trust policy
    cat > /tmp/trust-policy.json << EOF
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
    
    # Create role
    aws iam create-role \
        --role-name ${ROLE_NAME} \
        --assume-role-policy-document file:///tmp/trust-policy.json \
        --description "Role for Lake Formation setup Lambda function"
    
    # Create and attach policy
    cat > /tmp/role-policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "lakeformation:RegisterResource",
        "lakeformation:GrantPermissions",
        "lakeformation:ListPermissions",
        "lakeformation:DescribeResource",
        "glue:GetTable",
        "glue:UpdateTable",
        "sts:GetCallerIdentity",
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "*"
    }
  ]
}
EOF
    
    aws iam put-role-policy \
        --role-name ${ROLE_NAME} \
        --policy-name LakeFormationSetupPolicy \
        --policy-document file:///tmp/role-policy.json
    
    echo -e "${GREEN}✓ IAM role created${NC}"
    echo -e "${YELLOW}⏳ Waiting 10 seconds for IAM role to propagate...${NC}"
    sleep 10
fi

# Package Lambda function
echo ""
echo -e "${YELLOW}Packaging Lambda function...${NC}"

# Create temporary directory
TEMP_DIR=$(mktemp -d)
echo "Using temp directory: ${TEMP_DIR}"

# Copy Lambda function
cp lambda_lakeformation_setup.py ${TEMP_DIR}/

# Install dependencies (boto3 is included in Lambda runtime, but we'll include it anyway)
echo "Installing dependencies..."
pip3 install boto3 -t ${TEMP_DIR}/ --quiet

# Create zip file
cd ${TEMP_DIR}
zip -r lambda_package.zip . > /dev/null
cd - > /dev/null

echo -e "${GREEN}✓ Lambda package created${NC}"

# Check if function exists
echo ""
echo -e "${YELLOW}Checking if Lambda function exists...${NC}"
if aws lambda get-function --function-name ${FUNCTION_NAME} --region ${REGION} &> /dev/null; then
    echo -e "${YELLOW}Function exists. Updating...${NC}"
    
    # Update function code
    aws lambda update-function-code \
        --function-name ${FUNCTION_NAME} \
        --zip-file fileb://${TEMP_DIR}/lambda_package.zip \
        --region ${REGION} > /dev/null
    
    # Update function configuration
    aws lambda update-function-configuration \
        --function-name ${FUNCTION_NAME} \
        --timeout ${TIMEOUT} \
        --memory-size ${MEMORY} \
        --region ${REGION} > /dev/null
    
    echo -e "${GREEN}✓ Lambda function updated${NC}"
else
    echo -e "${YELLOW}Creating new Lambda function...${NC}"
    
    # Create function
    aws lambda create-function \
        --function-name ${FUNCTION_NAME} \
        --runtime ${RUNTIME} \
        --role ${ROLE_ARN} \
        --handler ${HANDLER} \
        --zip-file fileb://${TEMP_DIR}/lambda_package.zip \
        --timeout ${TIMEOUT} \
        --memory-size ${MEMORY} \
        --region ${REGION} \
        --description "Setup Lake Formation for Iceberg tables" > /dev/null
    
    echo -e "${GREEN}✓ Lambda function created${NC}"
fi

# Cleanup
rm -rf ${TEMP_DIR}
rm -f /tmp/trust-policy.json /tmp/role-policy.json

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Deployment Complete!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo -e "Function Name: ${GREEN}${FUNCTION_NAME}${NC}"
echo -e "Region: ${GREEN}${REGION}${NC}"
echo -e "Role ARN: ${GREEN}${ROLE_ARN}${NC}"
echo ""
echo -e "${YELLOW}To invoke the function:${NC}"
echo ""
echo -e "aws lambda invoke \\"
echo -e "  --function-name ${FUNCTION_NAME} \\"
echo -e "  --payload '{"
echo -e "    \"bucket\": \"iceberg-data-storage-bucket\","
echo -e "    \"prefix\": \"iceberg-data\","
echo -e "    \"database\": \"iceberg_db\","
echo -e "    \"table\": \"entity_data\","
echo -e "    \"role_arn\": \"arn:aws:iam::${ACCOUNT_ID}:role/GlueServiceRole\","
echo -e "    \"region\": \"us-east-1\""
echo -e "  }' \\"
echo -e "  --region ${REGION} \\"
echo -e "  response.json"
echo ""
echo -e "${YELLOW}To view logs:${NC}"
echo ""
echo -e "aws logs tail /aws/lambda/${FUNCTION_NAME} --follow --region ${REGION}"
echo ""
