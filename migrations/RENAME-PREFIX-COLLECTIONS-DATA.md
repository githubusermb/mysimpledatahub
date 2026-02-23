# S3 Prefix Rename: raw-data → collections-data

## Overview

Renamed the S3 prefix from `raw-data/` to `collections-data/` to better align with the project naming convention and reflect the actual data content.

## Changes Made

### S3 Prefix Rename
- **Old Prefix**: `raw-data/`
- **New Prefix**: `collections-data/`
- **Rationale**: More descriptive name that aligns with the collections data content and overall naming convention

### Updated Bucket Name (Combined Change)
- **Old Name**: `sdh-raw-data-ingestion-bucket`
- **New Name**: `sdh-collections-data-ingestion-bucket`
- **Rationale**: Bucket name now reflects the prefix naming convention

## Files Updated

### Terraform Configuration
- ✅ `terraform/variables.tf` - Updated default values for bucket name and prefix
- ✅ `terraform/terraform.tfvars` - Updated variable values

### Python Scripts
- ✅ `jobs/glue_csv_to_iceberg.py` - Updated comment in ingest_ts extraction
- ✅ `scripts/glue_csv_to_iceberg.py` - Updated comment in ingest_ts extraction
- ✅ `scripts/upload_and_process.sh` - Updated RAW_PREFIX default value

### Documentation
- ✅ `README.md` - Updated all S3 path examples and configuration
- ✅ `QUICK-START.md` - Updated configuration examples
- ✅ `ARCHITECTURE.md` - Updated S3 bucket structure diagrams
- ✅ `ENTITY-DIAGRAMS.md` - Updated data flow diagrams
- ✅ `jobs/README.md` - Updated parameter examples

### Migration Documents
- ✅ `migrations/BUGFIX-INGEST-TIMESTAMP.md` - Updated S3 path examples
- ✅ `migrations/RENAME-BUCKET-DATABASE.md` - Updated bucket name references
- ✅ `migrations/UPDATES-REGION-AND-VIEWS.md` - Updated S3 path examples

## S3 Path Changes

### Before
```
s3://sdh-raw-data-ingestion-bucket/raw-data/ingest_ts=<timestamp>/file.csv
```

### After
```
s3://sdh-collections-data-ingestion-bucket/collections-data/ingest_ts=<timestamp>/file.csv
```

## Deployment Impact

### For New Deployments
No action needed. The new naming convention will be used automatically when you run `terraform apply`.

### For Existing Deployments

#### Option 1: Fresh Start (Recommended)
If you can recreate resources:

1. Destroy existing resources:
   ```bash
   cd terraform
   terraform destroy
   ```

2. Deploy with new configuration:
   ```bash
   terraform apply
   ```

3. Re-upload data to new S3 paths:
   ```bash
   aws s3 cp data.csv s3://sdh-collections-data-ingestion-bucket/collections-data/ingest_ts=$(date +%s)/
   ```

#### Option 2: Migrate Existing Data

1. Create new bucket:
   ```bash
   aws s3 mb s3://sdh-collections-data-ingestion-bucket
   ```

2. Copy data from old location to new:
   ```bash
   # Copy with new prefix structure
   aws s3 sync \
     s3://sdh-raw-data-ingestion-bucket/raw-data/ \
     s3://sdh-collections-data-ingestion-bucket/collections-data/
   ```

3. Update Terraform:
   ```bash
   cd terraform
   terraform apply
   ```

4. Verify data is accessible:
   ```bash
   aws s3 ls s3://sdh-collections-data-ingestion-bucket/collections-data/ --recursive
   ```

5. After verification, delete old bucket (optional):
   ```bash
   aws s3 rb s3://sdh-raw-data-ingestion-bucket --force
   ```

## Configuration Examples

### Terraform Variables (terraform.tfvars)
```hcl
# S3 Bucket Names
raw_data_bucket_name = "sdh-collections-data-ingestion-bucket"
iceberg_data_bucket_name = "iceberg-data-storage-bucket"

# S3 Prefixes
raw_data_prefix = "collections-data/"
iceberg_data_prefix = "iceberg-data/"
```

### Upload Script Example
```bash
#!/bin/bash
BUCKET="sdh-collections-data-ingestion-bucket"
PREFIX="collections-data"
TIMESTAMP=$(date +%s)

aws s3 cp data.csv s3://${BUCKET}/${PREFIX}/ingest_ts=${TIMESTAMP}/
```

### Glue Job Parameters
```python
--raw_data_bucket sdh-collections-data-ingestion-bucket
--raw_data_prefix collections-data/
--database_name collections_db
--table_name collections_data_tbl
```

## Verification

After deployment, verify the changes:

```bash
# List files in new location
aws s3 ls s3://sdh-collections-data-ingestion-bucket/collections-data/ --recursive

# Check Terraform outputs
cd terraform
terraform output

# Test upload
TIMESTAMP=$(date +%s)
echo "test" > test.csv
aws s3 cp test.csv s3://sdh-collections-data-ingestion-bucket/collections-data/ingest_ts=${TIMESTAMP}/

# Verify upload
aws s3 ls s3://sdh-collections-data-ingestion-bucket/collections-data/ingest_ts=${TIMESTAMP}/
```

## Rollback Plan

If you need to rollback to the old naming:

1. Update `terraform/terraform.tfvars`:
   ```hcl
   raw_data_bucket_name = "sdh-raw-data-ingestion-bucket"
   raw_data_prefix = "raw-data/"
   ```

2. Apply changes:
   ```bash
   cd terraform
   terraform apply
   ```

3. Move data back to old location if needed

## Impact Assessment

### Breaking Changes
- ✅ S3 upload paths must use new prefix: `collections-data/`
- ✅ Existing data in `raw-data/` prefix won't be processed automatically
- ✅ Any external scripts/tools must be updated to use new paths

### Non-Breaking Changes
- ✅ Glue jobs use parameters, will automatically use new paths
- ✅ Terraform manages resources, will handle updates
- ✅ No changes to database or table names

## Testing Checklist

After deployment:

- [ ] Verify bucket exists: `aws s3 ls s3://sdh-collections-data-ingestion-bucket/`
- [ ] Verify prefix structure: `aws s3 ls s3://sdh-collections-data-ingestion-bucket/collections-data/`
- [ ] Test file upload to new location
- [ ] Run CSV to Iceberg job and verify it reads from new location
- [ ] Check CloudWatch logs for correct S3 paths
- [ ] Verify data appears in Iceberg table
- [ ] Test queries in Athena

## Related Changes

This change is part of a broader naming convention update:
- Database: `iceberg_db` → `collections_db`
- Bucket: `mb-raw-data-ingestion-bucket` → `sdh-collections-data-ingestion-bucket`
- Prefix: `raw-data/` → `collections-data/`

See also:
- `RENAME-BUCKET-DATABASE.md` - Database and bucket rename documentation
- `MIGRATION-SUMMARY.md` - Table and view naming changes

---

**Date**: 2026-02-16  
**Status**: ✅ Complete  
**Impact**: Medium (requires S3 path updates)
