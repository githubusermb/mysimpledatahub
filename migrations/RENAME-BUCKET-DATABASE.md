# Bucket and Database Rename Summary

## Overview

This document summarizes the renaming of AWS resources to follow consistent naming conventions.

## Changes Made

### 1. S3 Bucket Rename
- **Old Name**: `mb-raw-data-ingestion-bucket`
- **New Name**: `sdh-collections-data-ingestion-bucket`
- **Rationale**: Align with project naming convention (sdh = Simple Data Hub)

### 2. Glue Database Rename
- **Old Name**: `iceberg_db`
- **New Name**: `collections_db`
- **Rationale**: More descriptive name that reflects the data content (collections data)

## Files Updated

### Terraform Configuration Files
- ✅ `terraform/variables.tf` - Updated default values for both bucket and database
- ✅ `terraform/terraform.tfvars` - Updated variable values
- ✅ `terraform/main.tf` - Renamed resource from `iceberg_database` to `collections_database`
- ✅ `terraform/outputs.tf` - Updated all references to `collections_database`
- ✅ `terraform/views_dual_engine_job.tf` - Updated database resource references
- ✅ `terraform/lakeformation.tf` - Globally replaced `iceberg_database` with `collections_database`

### Python Helper Scripts
- ✅ `scripts/setup_lakeformation_complete.py` - Updated example commands
- ✅ `scripts/lambda_lakeformation_setup.py` - Updated example event and test data
- ✅ `scripts/grant_athena_user_permissions.py` - Updated usage examples

### Documentation Files
- ✅ `MIGRATION-SUMMARY.md` - Updated all SQL queries and AWS CLI commands
- ✅ `BUGFIX-INGEST-TIMESTAMP.md` - Updated S3 path example
- ✅ `DUAL-ENGINE-SOLUTION.md` - Updated view names and queries
- ✅ `QUICK-START.md` - Updated configuration examples and queries
- ✅ `ENTITY-DIAGRAMS.md` - Updated all view definitions and queries
- ✅ `jobs/README.md` - Updated parameter examples and queries
- ✅ `UPDATES-REGION-AND-VIEWS.md` - Updated all queries and examples
- ✅ `README.md` - Updated configuration example

### Files NOT Changed (By Design)
- ❌ `jobs/glue_csv_to_iceberg.py` - Uses parameters, not hardcoded values
- ❌ `jobs/glue_create_views_dual_engine.py` - Uses parameters, not hardcoded values
- ❌ `terraform/terraform.tfstate*` - Auto-generated, will update on next apply
- ❌ `terraform/view_presto.out` - Old output file, not used

## Deployment Steps

### Option 1: Fresh Deployment (Recommended)

If you haven't deployed yet or can recreate resources:

```bash
cd terraform

# Review changes
terraform plan

# Apply changes (will create new resources with new names)
terraform apply
```

### Option 2: Existing Deployment (Requires Manual Steps)

If you have existing resources deployed:

#### Step 1: Backup Existing Data
```bash
# Export table metadata
aws glue get-table --database-name iceberg_db --name collections_data_staging > backup_table.json

# List existing views
aws glue get-tables --database-name iceberg_db --query 'TableList[?starts_with(Name, `fry`)].Name'
```

#### Step 2: Update S3 Bucket (Manual)
```bash
# Option A: Rename bucket (if possible in your AWS account)
# Note: S3 bucket renaming is not directly supported, you need to:
# 1. Create new bucket: sdh-collections-data-ingestion-bucket
# 2. Copy data from old bucket to new bucket
# 3. Delete old bucket

aws s3 mb s3://sdh-collections-data-ingestion-bucket
aws s3 sync s3://mb-raw-data-ingestion-bucket s3://sdh-collections-data-ingestion-bucket
# Verify data copied successfully before deleting old bucket
aws s3 rb s3://mb-raw-data-ingestion-bucket --force

# Option B: Keep old bucket name
# If you prefer to keep the old bucket name, update terraform.tfvars:
# raw_data_bucket_name = "mb-raw-data-ingestion-bucket"
```

#### Step 3: Rename Glue Database
```bash
# Create new database
aws glue create-database --database-input '{
  "Name": "collections_db",
  "Description": "Database for collections data"
}'

# Note: You cannot directly rename a Glue database
# You need to recreate tables in the new database
```

#### Step 4: Apply Terraform Changes
```bash
cd terraform

# Import existing resources if needed
terraform import aws_glue_catalog_database.collections_database collections_db

# Review and apply
terraform plan
terraform apply
```

#### Step 5: Recreate Tables and Views
```bash
# Run the CSV to Iceberg job to recreate the table
# Run the views job to recreate all views
```

## Verification

After deployment, verify the changes:

```sql
-- In Athena
SHOW DATABASES;
-- Should see: collections_db

SHOW TABLES IN collections_db;
-- Should see:
-- - collections_data_staging
-- - collections_data_view
-- - fry9c_report_view
-- - fry15_report_view

-- Test queries
SELECT COUNT(*) FROM collections_db.collections_data_staging;
SELECT COUNT(*) FROM collections_db.collections_data_view;
SELECT COUNT(*) FROM collections_db.fry9c_report_view;
```

## Query Migration

### Old Queries
```sql
SELECT * FROM iceberg_db.collections_data_staging;
SELECT * FROM iceberg_db.fry9c_report_view;
```

### New Queries
```sql
SELECT * FROM collections_db.collections_data_staging;
SELECT * FROM collections_db.fry9c_report_view;
```

## Lake Formation Permissions

If you have existing Lake Formation permissions, update them:

```bash
# Grant permissions on new database
python scripts/grant_athena_user_permissions.py \
  --database collections_db \
  --table collections_data_staging \
  --principal <user-arn>

# Grant permissions on views
python scripts/grant_athena_user_permissions.py \
  --database collections_db \
  --table '*' \
  --principal <user-arn>
```

## Rollback Plan

If you need to rollback:

1. Update `terraform/terraform.tfvars`:
   ```hcl
   raw_data_bucket_name = "mb-raw-data-ingestion-bucket"
   glue_database_name   = "iceberg_db"
   ```

2. Run `terraform apply`

3. Restore table metadata from backup

## Impact Assessment

### Breaking Changes
- ✅ All SQL queries must be updated to use `collections_db` instead of `iceberg_db`
- ✅ S3 paths must be updated to use `sdh-collections-data-ingestion-bucket`
- ✅ Lake Formation permissions must be re-granted
- ✅ Any external tools/scripts referencing the old names must be updated

### Non-Breaking Changes
- ✅ Glue job code uses parameters, no changes needed
- ✅ Terraform manages resource names, will handle updates
- ✅ IAM roles and policies reference resources by ARN, will update automatically

## Testing Checklist

After deployment:

- [ ] Verify database exists: `SHOW DATABASES;`
- [ ] Verify tables exist: `SHOW TABLES IN collections_db;`
- [ ] Test staging table query: `SELECT COUNT(*) FROM collections_db.collections_data_staging;`
- [ ] Test unified view query: `SELECT COUNT(*) FROM collections_db.collections_data_view;`
- [ ] Test report view query: `SELECT COUNT(*) FROM collections_db.fry9c_report_view;`
- [ ] Verify S3 bucket exists and contains data
- [ ] Test CSV upload and processing
- [ ] Verify Lake Formation permissions work
- [ ] Test Athena queries from different users

## Support

For issues or questions:
1. Check Terraform plan output before applying
2. Review CloudWatch logs for Glue jobs
3. Verify Lake Formation permissions
4. Check S3 bucket policies and access

---

**Date**: 2026-02-16  
**Status**: ✅ Complete  
**Impact**: Medium (requires query updates)
