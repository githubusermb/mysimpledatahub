# Migration: Glue Jobs to Dedicated Folder

## Overview

Glue job scripts have been moved from the `scripts/` folder to a dedicated `jobs/` folder for better organization and clarity.

---

## Changes Made

### Folder Structure

**Before:**
```
mysimpledatahub/
├── scripts/
│   ├── glue_csv_to_iceberg.py              # Glue job
│   ├── glue_create_normal_views.py    # Glue job
│   ├── generate_sample_csv.py              # Helper script
│   ├── setup_lakeformation_complete.py     # Helper script
│   └── ... (other helper scripts)
└── terraform/
    └── main.tf
```

**After:**
```
mysimpledatahub/
├── jobs/                                    # NEW: Glue jobs only
│   ├── README.md                            # Job documentation
│   ├── glue_csv_to_iceberg.py              # Glue job
│   └── glue_create_normal_views.py    # Glue job
│
├── scripts/                                 # Helper scripts only
│   ├── generate_sample_csv.py
│   ├── setup_lakeformation_complete.py
│   ├── grant_athena_user_permissions.py
│   └── ... (other helper scripts)
│
└── terraform/
    └── main.tf
```

### Files Moved

| File | Old Location | New Location |
|------|-------------|--------------|
| `glue_csv_to_iceberg.py` | `scripts/` | `jobs/` |
| `glue_create_normal_views.py` | `scripts/` | `jobs/` |

### Terraform Updates

**main.tf:**
```hcl
# Before
resource "aws_s3_object" "glue_script" {
  source = "../scripts/glue_csv_to_iceberg.py"
  etag   = filemd5("../scripts/glue_csv_to_iceberg.py")
}

# After
resource "aws_s3_object" "glue_script" {
  source = "../jobs/glue_csv_to_iceberg.py"
  etag   = filemd5("../jobs/glue_csv_to_iceberg.py")
}
```

**views_dual_engine_job.tf:**
```hcl
# Before
resource "aws_s3_object" "glue_views_dual_engine_script" {
  source = "../scripts/glue_create_normal_views.py"
  etag   = filemd5("../scripts/glue_create_normal_views.py")
}

# After
resource "aws_s3_object" "glue_views_dual_engine_script" {
  source = "../jobs/glue_create_normal_views.py"
  etag   = filemd5("../jobs/glue_create_normal_views.py")
}
```

---

## Rationale

### Why Separate Folders?

#### 1. Clear Separation of Concerns
```
jobs/       → AWS Glue ETL jobs (run in Glue environment)
scripts/    → Helper/utility scripts (run locally or in Lambda)
```

#### 2. Better Organization
- Easier to find Glue-specific code
- Clear distinction between job types
- Simpler onboarding for new developers

#### 3. Deployment Clarity
- `jobs/` folder = deployed to Glue
- `scripts/` folder = used for setup/maintenance

#### 4. Documentation
- Dedicated README in `jobs/` folder
- Job-specific documentation separate from helper scripts

---

## What's in Each Folder?

### jobs/ (AWS Glue ETL Jobs)

**Purpose**: Production ETL jobs that run in AWS Glue

**Files**:
- `glue_csv_to_iceberg.py` - Data ingestion job
- `glue_create_normal_views.py` - View creation job
- `README.md` - Job documentation

**Characteristics**:
- Use AWS Glue libraries (`awsglue.transforms`, `awsglue.context`)
- Run in Glue Spark environment
- Deployed via Terraform
- Scheduled or triggered execution

### scripts/ (Helper Scripts)

**Purpose**: Setup, maintenance, and utility scripts

**Files**:
- `generate_sample_csv.py` - Generate test data
- `generate_sample_csv_mdrm.py` - Generate MDRM test data
- `setup_lakeformation_complete.py` - Lake Formation setup
- `grant_athena_user_permissions.py` - Grant permissions
- `register_table_with_lakeformation.py` - Register tables
- `test_views_in_spark.py` - Test views
- `diagnose_view.ps1` - Diagnose view issues
- `drop_all_views.ps1` - Drop all views
- And more...

**Characteristics**:
- Run locally or in Lambda
- Use standard Python/boto3
- Not deployed to Glue
- Manual or scripted execution

---

## Migration Steps

### For Existing Deployments

If you have an existing deployment, follow these steps:

#### Step 1: Pull Latest Changes

```bash
git pull origin main
```

#### Step 2: Verify New Structure

```bash
# Check jobs folder exists
ls mysimpledatahub/jobs/

# Should see:
# - glue_csv_to_iceberg.py
# - glue_create_normal_views.py
# - README.md
```

#### Step 3: Apply Terraform Changes

```bash
cd mysimpledatahub/terraform

# Review changes
terraform plan

# Apply updates (will re-upload scripts from new location)
terraform apply
```

#### Step 4: Verify Jobs Still Work

```bash
# Test ingestion job
aws glue start-job-run --job-name csv-to-iceberg-ingestion

# Test views job
aws glue start-job-run --job-name create-views-normal

# Check job status
aws glue get-job-runs --job-name csv-to-iceberg-ingestion --max-results 1
```

### For New Deployments

No special steps needed - just follow the standard deployment process:

```bash
cd mysimpledatahub/terraform
terraform init
terraform apply
```

---

## Impact Assessment

### ✅ No Breaking Changes

- Job names remain the same
- Job parameters unchanged
- S3 upload location unchanged (still `s3://bucket/scripts/`)
- Job functionality identical

### ✅ Backward Compatible

- Existing jobs continue to work
- No data migration needed
- No configuration changes required

### ✅ Transparent to Users

- Query syntax unchanged
- View names unchanged
- Table names unchanged

---

## Verification

### Check Terraform State

```bash
cd terraform

# Verify resources will be updated (not recreated)
terraform plan

# Should show:
# ~ update in-place (for aws_s3_object resources)
# No resources destroyed
```

### Check S3 Upload

```bash
# After terraform apply, verify scripts are uploaded
aws s3 ls s3://your-bucket/scripts/

# Should see:
# glue_csv_to_iceberg.py
# glue_create_normal_views.py
```

### Check Job Configuration

```bash
# Verify job script location
aws glue get-job --job-name csv-to-iceberg-ingestion \
  --query 'Job.Command.ScriptLocation' \
  --output text

# Should return:
# s3://your-bucket/scripts/glue_csv_to_iceberg.py
```

---

## Rollback Plan

If issues occur, you can rollback:

### Option 1: Git Revert

```bash
git revert <commit-hash>
git push
cd terraform
terraform apply
```

### Option 2: Manual Revert

```bash
# Copy files back to scripts folder
cp jobs/glue_csv_to_iceberg.py scripts/
cp jobs/glue_create_normal_views.py scripts/

# Update Terraform
# Edit main.tf and views_dual_engine_job.tf
# Change source paths back to ../scripts/

# Apply
terraform apply
```

---

## Benefits

### 1. Improved Organization
```
✅ Clear separation: jobs vs scripts
✅ Easier to navigate codebase
✅ Better for new team members
```

### 2. Better Documentation
```
✅ Dedicated README for jobs
✅ Job-specific documentation
✅ Clear deployment instructions
```

### 3. Scalability
```
✅ Easy to add new jobs
✅ Clear pattern to follow
✅ Organized as project grows
```

### 4. Maintenance
```
✅ Easier to find job code
✅ Clear what gets deployed to Glue
✅ Simpler CI/CD pipelines
```

---

## Future Enhancements

### Potential Additions to jobs/ Folder

1. **Job Testing**
   ```
   jobs/
   ├── tests/
   │   ├── test_csv_to_iceberg.py
   │   └── test_create_views.py
   ```

2. **Job Configuration**
   ```
   jobs/
   ├── configs/
   │   ├── dev.json
   │   ├── staging.json
   │   └── prod.json
   ```

3. **Job Dependencies**
   ```
   jobs/
   ├── requirements.txt
   └── setup.py
   ```

---

## Related Documentation

- **Jobs README**: See `jobs/README.md` for detailed job documentation
- **Architecture**: See `ARCHITECTURE.md` for system overview
- **Deployment**: See `README.md` for deployment instructions

---

## Summary

### What Changed
- ✅ Moved Glue jobs to dedicated `jobs/` folder
- ✅ Updated Terraform to reference new location
- ✅ Added comprehensive job documentation

### What Stayed the Same
- ✅ Job names and functionality
- ✅ Job parameters and configuration
- ✅ S3 upload location
- ✅ Query syntax and view names

### Action Required
```bash
# For existing deployments
cd terraform
terraform apply

# Verify jobs work
aws glue start-job-run --job-name csv-to-iceberg-ingestion
```

---

**Migration Date**: February 11, 2026  
**Status**: Complete ✅  
**Breaking Changes**: None  
**Rollback Available**: Yes
