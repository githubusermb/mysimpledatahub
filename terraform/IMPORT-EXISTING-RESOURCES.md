# Importing Existing IAM Resources into Terraform

## Problem

When running `terraform apply`, you may encounter errors like:

```
Error: creating IAM Role (GlueServiceRole): EntityAlreadyExists: Role with name GlueServiceRole already exists.
Error: creating IAM Policy (GlueS3Access): EntityAlreadyExists: A policy called GlueS3Access already exists.
```

This happens when IAM resources were created outside of Terraform (manually via AWS Console, CLI, or a previous deployment) and Terraform doesn't know they exist.

## Solution

Import the existing resources into Terraform state so Terraform can manage them.

## Quick Fix (Automated)

### On Windows (PowerShell)

```powershell
cd terraform
.\import_existing_resources.ps1
```

### On Linux/Mac (Bash)

```bash
cd terraform
chmod +x import_existing_resources.sh
./import_existing_resources.sh
```

The script will:
1. Check which resources already exist in Terraform state
2. Import existing AWS resources that aren't in state
3. Skip resources that are already managed

## Manual Import (Step by Step)

If you prefer to import manually or the script fails:

### 1. Get Your AWS Account ID

```bash
aws sts get-caller-identity --query Account --output text
```

### 2. Import IAM Roles

```bash
# Import GlueServiceRole
terraform import aws_iam_role.glue_service_role GlueServiceRole

# Import LambdaGlueJobTriggerRole
terraform import aws_iam_role.lambda_role LambdaGlueJobTriggerRole
```

### 3. Import IAM Policies

First, get the policy ARNs:

```bash
# Get GlueS3Access ARN
aws iam list-policies --query "Policies[?PolicyName=='GlueS3Access'].Arn" --output text

# Get GlueCatalogAccess ARN
aws iam list-policies --query "Policies[?PolicyName=='GlueCatalogAccess'].Arn" --output text

# Get GlueLakeFormationAccess ARN
aws iam list-policies --query "Policies[?PolicyName=='GlueLakeFormationAccess'].Arn" --output text

# Get GlueAthenaAccess ARN
aws iam list-policies --query "Policies[?PolicyName=='GlueAthenaAccess'].Arn" --output text

# Get LambdaGlueJobPolicy ARN
aws iam list-policies --query "Policies[?PolicyName=='LambdaGlueJobPolicy'].Arn" --output text
```

Then import each policy (replace `<ARN>` with actual ARN):

```bash
terraform import aws_iam_policy.glue_s3_access <ARN>
terraform import aws_iam_policy.glue_catalog_access <ARN>
terraform import aws_iam_policy.glue_lakeformation_access <ARN>
terraform import aws_iam_policy.glue_athena_access <ARN>
terraform import aws_iam_policy.lambda_glue_policy <ARN>
```

### 4. Verify Import

```bash
# Check what's in Terraform state
terraform state list

# Should see:
# aws_iam_role.glue_service_role
# aws_iam_role.lambda_role
# aws_iam_policy.glue_s3_access
# aws_iam_policy.glue_catalog_access
# aws_iam_policy.glue_lakeformation_access
# aws_iam_policy.glue_athena_access
# aws_iam_policy.lambda_glue_policy
```

## After Import

### 1. Review Changes

```bash
terraform plan
```

You may see some changes due to:
- Policy document formatting differences
- Tag differences
- Description differences

These are usually safe to apply.

### 2. Apply Changes

```bash
terraform apply
```

This will update the existing resources to match your Terraform configuration.

## Common Issues

### Issue: "Resource not found"

If a resource doesn't exist in AWS:

```bash
# Check if role exists
aws iam get-role --role-name GlueServiceRole

# Check if policy exists
aws iam list-policies --query "Policies[?PolicyName=='GlueS3Access']"
```

**Solution**: If the resource doesn't exist, don't import it. Terraform will create it on the next `apply`.

### Issue: "Resource already in state"

```
Error: Resource already managed by Terraform
```

**Solution**: The resource is already imported. Skip it and continue with other resources.

### Issue: Policy ARN not found

```bash
# List all your custom policies
aws iam list-policies --scope Local --query "Policies[*].[PolicyName,Arn]" --output table
```

**Solution**: Use the correct ARN from the list.

### Issue: Permission denied

```
Error: AccessDenied: User is not authorized to perform: iam:GetRole
```

**Solution**: Ensure your AWS credentials have IAM permissions:
- `iam:GetRole`
- `iam:GetPolicy`
- `iam:ListPolicies`

## Alternative: Start Fresh

If importing is too complex, you can delete existing resources and let Terraform create new ones:

### ⚠️ WARNING: This will delete existing IAM resources!

```bash
# Delete IAM roles
aws iam delete-role --role-name GlueServiceRole
aws iam delete-role --role-name LambdaGlueJobTriggerRole

# Delete IAM policies (get ARNs first)
aws iam delete-policy --policy-arn <ARN>
```

**Note**: You must detach policies from roles and delete policy versions before deleting.

## Verification Checklist

After import and apply:

- [ ] Run `terraform plan` - should show no changes or only minor updates
- [ ] Check IAM roles exist: `aws iam get-role --role-name GlueServiceRole`
- [ ] Check IAM policies exist: `aws iam list-policies --query "Policies[?PolicyName=='GlueS3Access']"`
- [ ] Verify Glue jobs can run with the roles
- [ ] Test Lambda function can trigger Glue jobs

## Resources Being Imported

| Resource Type | Terraform Name | AWS Name |
|--------------|----------------|----------|
| IAM Role | `aws_iam_role.glue_service_role` | `GlueServiceRole` |
| IAM Role | `aws_iam_role.lambda_role` | `LambdaGlueJobTriggerRole` |
| IAM Policy | `aws_iam_policy.glue_s3_access` | `GlueS3Access` |
| IAM Policy | `aws_iam_policy.glue_catalog_access` | `GlueCatalogAccess` |
| IAM Policy | `aws_iam_policy.glue_lakeformation_access` | `GlueLakeFormationAccess` |
| IAM Policy | `aws_iam_policy.glue_athena_access` | `GlueAthenaAccess` |
| IAM Policy | `aws_iam_policy.lambda_glue_policy` | `LambdaGlueJobPolicy` |

## Additional Resources

- [Terraform Import Documentation](https://www.terraform.io/docs/cli/import/index.html)
- [AWS IAM Terraform Provider](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_role)

---

**Last Updated**: 2026-02-16
