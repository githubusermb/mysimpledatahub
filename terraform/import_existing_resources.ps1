# Script to import existing IAM resources into Terraform state
# This resolves EntityAlreadyExists errors when resources were created outside Terraform

$ErrorActionPreference = "Continue"

Write-Host "==========================================" -ForegroundColor Green
Write-Host "Importing Existing IAM Resources" -ForegroundColor Green
Write-Host "==========================================" -ForegroundColor Green
Write-Host ""

# Get AWS Account ID
try {
    $ACCOUNT_ID = (aws sts get-caller-identity --query Account --output text)
    Write-Host "AWS Account ID: $ACCOUNT_ID" -ForegroundColor Cyan
    Write-Host ""
}
catch {
    Write-Host "Error: Could not get AWS Account ID. Make sure AWS CLI is configured." -ForegroundColor Red
    exit 1
}

# Function to check if resource is in state
function Test-TerraformResource {
    param($ResourceName)
    $null = terraform state show $ResourceName 2>&1
    return $LASTEXITCODE -eq 0
}

# Function to get policy ARN by name
function Get-PolicyArn {
    param($PolicyName)
    $arn = aws iam list-policies --query "Policies[?PolicyName=='$PolicyName'].Arn" --output text
    return $arn
}

# 1. Import IAM Role: GlueServiceRole
Write-Host "1. Importing IAM Role: GlueServiceRole" -ForegroundColor Yellow
if (Test-TerraformResource "aws_iam_role.glue_service_role") {
    Write-Host "   Already in state, skipping" -ForegroundColor Green
}
else {
    terraform import aws_iam_role.glue_service_role GlueServiceRole 2>&1 | Out-Null
    if ($LASTEXITCODE -eq 0) {
        Write-Host "   Imported successfully" -ForegroundColor Green
    }
    else {
        Write-Host "   Import failed (may not exist)" -ForegroundColor Yellow
    }
}
Write-Host ""

# 2. Import IAM Policy: GlueS3Access
Write-Host "2. Importing IAM Policy: GlueS3Access" -ForegroundColor Yellow
if (Test-TerraformResource "aws_iam_policy.glue_s3_access") {
    Write-Host "   Already in state, skipping" -ForegroundColor Green
}
else {
    $POLICY_ARN = Get-PolicyArn "GlueS3Access"
    if ($POLICY_ARN) {
        terraform import aws_iam_policy.glue_s3_access $POLICY_ARN 2>&1 | Out-Null
        if ($LASTEXITCODE -eq 0) {
            Write-Host "   Imported successfully" -ForegroundColor Green
        }
        else {
            Write-Host "   Import failed" -ForegroundColor Yellow
        }
    }
    else {
        Write-Host "   Policy not found in AWS" -ForegroundColor Yellow
    }
}
Write-Host ""

# 3. Import IAM Policy: GlueCatalogAccess
Write-Host "3. Importing IAM Policy: GlueCatalogAccess" -ForegroundColor Yellow
if (Test-TerraformResource "aws_iam_policy.glue_catalog_access") {
    Write-Host "   Already in state, skipping" -ForegroundColor Green
}
else {
    $POLICY_ARN = Get-PolicyArn "GlueCatalogAccess"
    if ($POLICY_ARN) {
        terraform import aws_iam_policy.glue_catalog_access $POLICY_ARN 2>&1 | Out-Null
        if ($LASTEXITCODE -eq 0) {
            Write-Host "   Imported successfully" -ForegroundColor Green
        }
        else {
            Write-Host "   Import failed" -ForegroundColor Yellow
        }
    }
    else {
        Write-Host "   Policy not found in AWS" -ForegroundColor Yellow
    }
}
Write-Host ""

# 4. Import IAM Policy: GlueLakeFormationAccess
Write-Host "4. Importing IAM Policy: GlueLakeFormationAccess" -ForegroundColor Yellow
if (Test-TerraformResource "aws_iam_policy.glue_lakeformation_access") {
    Write-Host "   Already in state, skipping" -ForegroundColor Green
}
else {
    $POLICY_ARN = Get-PolicyArn "GlueLakeFormationAccess"
    if ($POLICY_ARN) {
        terraform import aws_iam_policy.glue_lakeformation_access $POLICY_ARN 2>&1 | Out-Null
        if ($LASTEXITCODE -eq 0) {
            Write-Host "   Imported successfully" -ForegroundColor Green
        }
        else {
            Write-Host "   Import failed" -ForegroundColor Yellow
        }
    }
    else {
        Write-Host "   Policy not found in AWS" -ForegroundColor Yellow
    }
}
Write-Host ""

# 5. Import IAM Policy: GlueAthenaAccess
Write-Host "5. Importing IAM Policy: GlueAthenaAccess" -ForegroundColor Yellow
if (Test-TerraformResource "aws_iam_policy.glue_athena_access") {
    Write-Host "   Already in state, skipping" -ForegroundColor Green
}
else {
    $POLICY_ARN = Get-PolicyArn "GlueAthenaAccess"
    if ($POLICY_ARN) {
        terraform import aws_iam_policy.glue_athena_access $POLICY_ARN 2>&1 | Out-Null
        if ($LASTEXITCODE -eq 0) {
            Write-Host "   Imported successfully" -ForegroundColor Green
        }
        else {
            Write-Host "   Import failed" -ForegroundColor Yellow
        }
    }
    else {
        Write-Host "   Policy not found in AWS" -ForegroundColor Yellow
    }
}
Write-Host ""

# 6. Import IAM Role: LambdaGlueJobTriggerRole
Write-Host "6. Importing IAM Role: LambdaGlueJobTriggerRole" -ForegroundColor Yellow
if (Test-TerraformResource "aws_iam_role.lambda_role") {
    Write-Host "   Already in state, skipping" -ForegroundColor Green
}
else {
    terraform import aws_iam_role.lambda_role LambdaGlueJobTriggerRole 2>&1 | Out-Null
    if ($LASTEXITCODE -eq 0) {
        Write-Host "   Imported successfully" -ForegroundColor Green
    }
    else {
        Write-Host "   Import failed (may not exist)" -ForegroundColor Yellow
    }
}
Write-Host ""

# 7. Import IAM Policy: LambdaGlueJobPolicy
Write-Host "7. Importing IAM Policy: LambdaGlueJobPolicy" -ForegroundColor Yellow
if (Test-TerraformResource "aws_iam_policy.lambda_glue_policy") {
    Write-Host "   Already in state, skipping" -ForegroundColor Green
}
else {
    $POLICY_ARN = Get-PolicyArn "LambdaGlueJobPolicy"
    if ($POLICY_ARN) {
        terraform import aws_iam_policy.lambda_glue_policy $POLICY_ARN 2>&1 | Out-Null
        if ($LASTEXITCODE -eq 0) {
            Write-Host "   Imported successfully" -ForegroundColor Green
        }
        else {
            Write-Host "   Import failed" -ForegroundColor Yellow
        }
    }
    else {
        Write-Host "   Policy not found in AWS (will be created)" -ForegroundColor Yellow
    }
}
Write-Host ""

# 8. Import IAM Policy: GluePassRoleAccess
Write-Host "8. Importing IAM Policy: GluePassRoleAccess" -ForegroundColor Yellow
if (Test-TerraformResource "aws_iam_policy.glue_passrole_access") {
    Write-Host "   Already in state, skipping" -ForegroundColor Green
}
else {
    $POLICY_ARN = Get-PolicyArn "GluePassRoleAccess"
    if ($POLICY_ARN) {
        terraform import aws_iam_policy.glue_passrole_access $POLICY_ARN 2>&1 | Out-Null
        if ($LASTEXITCODE -eq 0) {
            Write-Host "   Imported successfully" -ForegroundColor Green
        }
        else {
            Write-Host "   Import failed" -ForegroundColor Yellow
        }
    }
    else {
        Write-Host "   Policy not found in AWS (will be created)" -ForegroundColor Yellow
    }
}
Write-Host ""

# 9. Import Glue Job: csv-to-iceberg-ingestion
Write-Host "9. Importing Glue Job: csv-to-iceberg-ingestion" -ForegroundColor Yellow
if (Test-TerraformResource "aws_glue_job.csv_to_iceberg_job") {
    Write-Host "   Already in state, skipping" -ForegroundColor Green
}
else {
    terraform import aws_glue_job.csv_to_iceberg_job csv-to-iceberg-ingestion 2>&1 | Out-Null
    if ($LASTEXITCODE -eq 0) {
        Write-Host "   Imported successfully" -ForegroundColor Green
    }
    else {
        Write-Host "   Import failed (may not exist)" -ForegroundColor Yellow
    }
}
Write-Host ""

# 10. Import Glue Job: create-views-dual-engine
Write-Host "10. Importing Glue Job: create-views-dual-engine" -ForegroundColor Yellow
if (Test-TerraformResource "aws_glue_job.views_dual_engine_job") {
    Write-Host "   Already in state, skipping" -ForegroundColor Green
}
else {
    terraform import aws_glue_job.views_dual_engine_job create-views-dual-engine 2>&1 | Out-Null
    if ($LASTEXITCODE -eq 0) {
        Write-Host "   Imported successfully" -ForegroundColor Green
    }
    else {
        Write-Host "   Import failed (may not exist)" -ForegroundColor Yellow
    }
}
Write-Host ""

# 11. Import Lambda Function: trigger-glue-job-lambda
Write-Host "11. Importing Lambda Function: trigger-glue-job-lambda" -ForegroundColor Yellow
if (Test-TerraformResource "aws_lambda_function.trigger_glue_job") {
    Write-Host "   Already in state, skipping" -ForegroundColor Green
}
else {
    terraform import aws_lambda_function.trigger_glue_job trigger-glue-job-lambda 2>&1 | Out-Null
    if ($LASTEXITCODE -eq 0) {
        Write-Host "   Imported successfully" -ForegroundColor Green
    }
    else {
        Write-Host "   Import failed (may not exist)" -ForegroundColor Yellow
    }
}
Write-Host ""

# 12. Import Lambda Permission: AllowExecutionFromS3Bucket-v2
Write-Host "12. Importing Lambda Permission: AllowExecutionFromS3Bucket-v2" -ForegroundColor Yellow
if (Test-TerraformResource "aws_lambda_permission.allow_bucket") {
    Write-Host "   Already in state, skipping" -ForegroundColor Green
}
else {
    terraform import aws_lambda_permission.allow_bucket trigger-glue-job-lambda/AllowExecutionFromS3Bucket-v2 2>&1 | Out-Null
    if ($LASTEXITCODE -eq 0) {
        Write-Host "   Imported successfully" -ForegroundColor Green
    }
    else {
        Write-Host "   Import failed (may not exist)" -ForegroundColor Yellow
    }
}
Write-Host ""

Write-Host "==========================================" -ForegroundColor Green
Write-Host "Import Complete!" -ForegroundColor Green
Write-Host "==========================================" -ForegroundColor Green
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Cyan
Write-Host "1. Run 'terraform plan' to see what changes are needed"
Write-Host "2. Run 'terraform apply' to update resources to match configuration"
Write-Host ""
Write-Host "Note: Some resources may show changes due to policy differences." -ForegroundColor Yellow
Write-Host "Review the plan carefully before applying."
