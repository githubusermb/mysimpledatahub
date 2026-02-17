#!/bin/bash

# Script to import existing IAM resources into Terraform state
# This resolves EntityAlreadyExists errors when resources were created outside Terraform

set -e

echo "=========================================="
echo "Importing Existing IAM Resources"
echo "=========================================="
echo ""

# Get AWS Account ID
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
echo "AWS Account ID: $ACCOUNT_ID"
echo ""

# Import IAM Role: GlueServiceRole
echo "1. Importing IAM Role: GlueServiceRole"
if terraform state show aws_iam_role.glue_service_role &>/dev/null; then
    echo "   ✓ Already in state, skipping"
else
    terraform import aws_iam_role.glue_service_role GlueServiceRole && echo "   ✓ Imported successfully" || echo "   ⚠ Import failed (may not exist)"
fi
echo ""

# Import IAM Policy: GlueS3Access
echo "2. Importing IAM Policy: GlueS3Access"
if terraform state show aws_iam_policy.glue_s3_access &>/dev/null; then
    echo "   ✓ Already in state, skipping"
else
    POLICY_ARN=$(aws iam list-policies --query "Policies[?PolicyName=='GlueS3Access'].Arn" --output text)
    if [ -n "$POLICY_ARN" ]; then
        terraform import aws_iam_policy.glue_s3_access "$POLICY_ARN" && echo "   ✓ Imported successfully" || echo "   ⚠ Import failed"
    else
        echo "   ⚠ Policy not found in AWS"
    fi
fi
echo ""

# Import IAM Policy: GlueCatalogAccess
echo "3. Importing IAM Policy: GlueCatalogAccess"
if terraform state show aws_iam_policy.glue_catalog_access &>/dev/null; then
    echo "   ✓ Already in state, skipping"
else
    POLICY_ARN=$(aws iam list-policies --query "Policies[?PolicyName=='GlueCatalogAccess'].Arn" --output text)
    if [ -n "$POLICY_ARN" ]; then
        terraform import aws_iam_policy.glue_catalog_access "$POLICY_ARN" && echo "   ✓ Imported successfully" || echo "   ⚠ Import failed"
    else
        echo "   ⚠ Policy not found in AWS"
    fi
fi
echo ""

# Import IAM Policy: GlueLakeFormationAccess
echo "4. Importing IAM Policy: GlueLakeFormationAccess"
if terraform state show aws_iam_policy.glue_lakeformation_access &>/dev/null; then
    echo "   ✓ Already in state, skipping"
else
    POLICY_ARN=$(aws iam list-policies --query "Policies[?PolicyName=='GlueLakeFormationAccess'].Arn" --output text)
    if [ -n "$POLICY_ARN" ]; then
        terraform import aws_iam_policy.glue_lakeformation_access "$POLICY_ARN" && echo "   ✓ Imported successfully" || echo "   ⚠ Import failed"
    else
        echo "   ⚠ Policy not found in AWS"
    fi
fi
echo ""

# Import IAM Policy: GlueAthenaAccess
echo "5. Importing IAM Policy: GlueAthenaAccess"
if terraform state show aws_iam_policy.glue_athena_access &>/dev/null; then
    echo "   ✓ Already in state, skipping"
else
    POLICY_ARN=$(aws iam list-policies --query "Policies[?PolicyName=='GlueAthenaAccess'].Arn" --output text)
    if [ -n "$POLICY_ARN" ]; then
        terraform import aws_iam_policy.glue_athena_access "$POLICY_ARN" && echo "   ✓ Imported successfully" || echo "   ⚠ Import failed"
    else
        echo "   ⚠ Policy not found in AWS"
    fi
fi
echo ""

# Import IAM Role: LambdaGlueJobTriggerRole
echo "6. Importing IAM Role: LambdaGlueJobTriggerRole"
if terraform state show aws_iam_role.lambda_role &>/dev/null; then
    echo "   ✓ Already in state, skipping"
else
    terraform import aws_iam_role.lambda_role LambdaGlueJobTriggerRole && echo "   ✓ Imported successfully" || echo "   ⚠ Import failed (may not exist)"
fi
echo ""

# Import IAM Policy: LambdaGlueJobPolicy (if it exists)
echo "7. Importing IAM Policy: LambdaGlueJobPolicy"
if terraform state show aws_iam_policy.lambda_glue_policy &>/dev/null; then
    echo "   ✓ Already in state, skipping"
else
    POLICY_ARN=$(aws iam list-policies --query "Policies[?PolicyName=='LambdaGlueJobPolicy'].Arn" --output text)
    if [ -n "$POLICY_ARN" ]; then
        terraform import aws_iam_policy.lambda_glue_policy "$POLICY_ARN" && echo "   ✓ Imported successfully" || echo "   ⚠ Import failed"
    else
        echo "   ⚠ Policy not found in AWS (will be created)"
    fi
fi
echo ""

# 8. Import IAM Policy: GluePassRoleAccess
echo "8. Importing IAM Policy: GluePassRoleAccess"
if terraform state show aws_iam_policy.glue_passrole_access &>/dev/null; then
    echo "   ✓ Already in state, skipping"
else
    POLICY_ARN=$(aws iam list-policies --query "Policies[?PolicyName=='GluePassRoleAccess'].Arn" --output text)
    if [ -n "$POLICY_ARN" ]; then
        terraform import aws_iam_policy.glue_passrole_access "$POLICY_ARN" && echo "   ✓ Imported successfully" || echo "   ⚠ Import failed"
    else
        echo "   ⚠ Policy not found in AWS (will be created)"
    fi
fi
echo ""

# 9. Import Glue Job: csv-to-iceberg-ingestion
echo "9. Importing Glue Job: csv-to-iceberg-ingestion"
if terraform state show aws_glue_job.csv_to_iceberg_job &>/dev/null; then
    echo "   ✓ Already in state, skipping"
else
    terraform import aws_glue_job.csv_to_iceberg_job csv-to-iceberg-ingestion && echo "   ✓ Imported successfully" || echo "   ⚠ Import failed (may not exist)"
fi
echo ""

# 10. Import Glue Job: create-views-dual-engine
echo "10. Importing Glue Job: create-views-dual-engine"
if terraform state show aws_glue_job.views_dual_engine_job &>/dev/null; then
    echo "   ✓ Already in state, skipping"
else
    terraform import aws_glue_job.views_dual_engine_job create-views-dual-engine && echo "   ✓ Imported successfully" || echo "   ⚠ Import failed (may not exist)"
fi
echo ""

# 11. Import Lambda Function: trigger-glue-job-lambda
echo "11. Importing Lambda Function: trigger-glue-job-lambda"
if terraform state show aws_lambda_function.trigger_glue_job &>/dev/null; then
    echo "   ✓ Already in state, skipping"
else
    terraform import aws_lambda_function.trigger_glue_job trigger-glue-job-lambda && echo "   ✓ Imported successfully" || echo "   ⚠ Import failed (may not exist)"
fi
echo ""

# 12. Import Lambda Permission: AllowExecutionFromS3Bucket-v2
echo "12. Importing Lambda Permission: AllowExecutionFromS3Bucket-v2"
if terraform state show aws_lambda_permission.allow_bucket &>/dev/null; then
    echo "   ✓ Already in state, skipping"
else
    terraform import aws_lambda_permission.allow_bucket trigger-glue-job-lambda/AllowExecutionFromS3Bucket-v2 && echo "   ✓ Imported successfully" || echo "   ⚠ Import failed (may not exist)"
fi
echo ""

echo "=========================================="
echo "Import Complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Run 'terraform plan' to see what changes are needed"
echo "2. Run 'terraform apply' to update resources to match configuration"
echo ""
echo "Note: Some resources may show changes due to policy differences."
echo "Review the plan carefully before applying."
