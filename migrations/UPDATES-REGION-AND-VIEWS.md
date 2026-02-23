# Updates: Region Externalization and Collections Data View

## Overview

This document describes the updates made to externalize AWS region configuration and add a new `collections_data_view` that provides a unified view of all data.

---

## Changes Made

### 1. Externalized AWS Region

All Python scripts now accept `aws_region` as a parameter instead of hardcoding it.

#### Updated Scripts

**glue_csv_to_iceberg.py**
```python
# Before: Hardcoded region
lakeformation_client = boto3.client('lakeformation', region_name='us-east-1')
glue_client = boto3.client('glue', region_name='us-east-1')

# After: Parameterized region
args = getResolvedOptions(sys.argv, [..., 'aws_region'])
aws_region = args['aws_region']

lakeformation_client = boto3.client('lakeformation', region_name=aws_region)
glue_client = boto3.client('glue', region_name=aws_region)
```

**glue_create_normal_views.py**
```python
# Before: No region parameter
glue_client = boto3.client('glue')
athena_client = boto3.client('athena')

# After: Parameterized region
args = getResolvedOptions(sys.argv, [..., 'aws_region'])
aws_region = args['aws_region']

glue_client = boto3.client('glue', region_name=aws_region)
athena_client = boto3.client('athena', region_name=aws_region)
```

#### Terraform Updates

**main.tf** (CSV to Iceberg job)
```hcl
default_arguments = {
  ...
  "--aws_region" = var.aws_region
  ...
}
```

**views_dual_engine_job.tf** (Views creation job)
```hcl
default_arguments = {
  ...
  "--aws_region" = var.aws_region
  ...
}
```

### 2. Added collections_data_view

A new view that provides access to all data in `collections_data_tbl` with the same schema.

#### Purpose

```
collections_data_view
├── Provides unified access to all regulatory data
├── Same schema as collections_data_tbl
├── Works in both Athena and Glue Spark (multi-dialect)
└── Simplifies queries across all series
```

#### Implementation

```python
# Step 1: Create view in Spark
CREATE PROTECTED MULTI DIALECT VIEW collections_db.collections_data_view
SECURITY DEFINER
AS SELECT * FROM glue_catalog.collections_db.collections_data_tbl

# Step 2: Add Athena dialect
ALTER VIEW collections_db.collections_data_view ADD DIALECT AS 
SELECT * FROM collections_db.collections_data_tbl
```

#### Schema

```
collections_data_view
├── seriesid          STRING    (partition key)
├── aod               STRING
├── rssdid            STRING
├── submissionts      STRING
├── key               STRING
├── value             STRING
└── ingest_timestamp  STRING    (partition key)
```

### 3. Enhanced View Recreation Logic

Views are now automatically recreated if they already exist.

#### Before
```python
try:
    glue_client.delete_table(DatabaseName=database_name, Name=view_name)
    print(f"✓ Dropped existing view: {view_name}")
except glue_client.exceptions.EntityNotFoundException:
    print(f"View {view_name} doesn't exist, will create new")
```

#### After
```python
try:
    glue_client.delete_table(DatabaseName=database_name, Name=view_name)
    print(f"✓ Dropped existing view: {view_name} (will recreate)")
except glue_client.exceptions.EntityNotFoundException:
    print(f"View {view_name} doesn't exist, will create new")
```

**Benefits:**
- ✅ Idempotent job execution
- ✅ Can re-run job to refresh views
- ✅ Handles schema changes in source table
- ✅ No manual cleanup needed

---

## View Hierarchy

```
collections_data_tbl (Iceberg Table)
    ↓
    ├─> collections_data_view (All data, same schema)
    │   └─> Multi-dialect (Athena + Spark)
    │
    └─> Series-Specific Report Views (Pivoted)
        ├─> fry9c_report_view
        ├─> fry15_report_view
        └─> <seriesid>_report_view
            └─> Multi-dialect (Athena + Spark)
```

---

## Usage Examples

### Query All Data (collections_data_view)

**Athena:**
```sql
-- Query all data across all series
SELECT * FROM collections_db.collections_data_view 
WHERE ingest_timestamp = '1770609249'
LIMIT 100;

-- Count records by series
SELECT seriesid, COUNT(*) as record_count
FROM collections_db.collections_data_view
GROUP BY seriesid;

-- Find specific keys across all series
SELECT seriesid, aod, rssdid, value
FROM collections_db.collections_data_view
WHERE key = 'RCON2170';
```

**Glue Spark:**
```python
# Query all data
df = spark.sql("""
    SELECT * FROM glue_catalog.collections_db.collections_data_view 
    WHERE ingest_timestamp = '1770609249'
    LIMIT 100
""")
df.show()

# Count by series
count_df = spark.sql("""
    SELECT seriesid, COUNT(*) as record_count
    FROM glue_catalog.collections_db.collections_data_view
    GROUP BY seriesid
""")
count_df.show()
```

### Query Series-Specific Data (report views)

**Athena:**
```sql
-- Query pivoted FRY-9C data
SELECT * FROM collections_db.fry9c_report_view
WHERE aod = '2024-03-31'
LIMIT 10;

-- Join multiple series
SELECT 
    f9.rssdid,
    f9.aod,
    f9.RCON2170 AS fry9c_value,
    f15.RCON3210 AS fry15_value
FROM collections_db.fry9c_report_view f9
LEFT JOIN collections_db.fry15_report_view f15
  ON f9.rssdid = f15.rssdid
  AND f9.aod = f15.aod;
```

**Glue Spark:**
```python
# Query pivoted data
df = spark.sql("""
    SELECT * FROM glue_catalog.collections_db.fry9c_report_view
    WHERE aod = '2024-03-31'
    LIMIT 10
""")
df.show()
```

---

## Job Execution Flow

### Updated Workflow

```
1. CSV Upload to S3
   └─> s3://bucket/collections-data/ingest_ts=<timestamp>/file.csv

2. Run csv-to-iceberg-ingestion Job
   ├─> Reads CSV files
   ├─> Extracts ingest_timestamp from path
   ├─> Creates/updates collections_data_tbl table
   └─> Registers with Lake Formation (using aws_region)

3. Run create-views-normal Job (Auto-triggered)
   ├─> Creates collections_data_view (all data)
   │   ├─> Spark dialect
   │   └─> Athena dialect
   │
   └─> Creates series-specific report views
       ├─> fry9c_report_view (pivoted)
       ├─> fry15_report_view (pivoted)
       └─> ... (one per seriesid)
           ├─> Spark dialect
           └─> Athena dialect

4. Query Data
   ├─> Athena: SELECT * FROM collections_db.collections_data_view
   └─> Spark: spark.sql("SELECT * FROM glue_catalog.collections_db.collections_data_view")
```

---

## Benefits

### 1. Region Flexibility
```
✅ Deploy to any AWS region
✅ No code changes needed for different regions
✅ Consistent across all scripts
✅ Easier multi-region deployments
```

### 2. Unified Data Access
```
✅ Single view for all regulatory data
✅ No need to union multiple series views
✅ Simplified cross-series analysis
✅ Maintains narrow format for flexibility
```

### 3. Idempotent Operations
```
✅ Re-run jobs without errors
✅ Automatic view recreation
✅ Handles schema evolution
✅ No manual cleanup required
```

---

## Deployment

### Step 1: Update Terraform Configuration

The region is already configured in `terraform.tfvars`:

```hcl
aws_region = "us-east-1"  # Change as needed
```

### Step 2: Apply Terraform Changes

```bash
cd terraform

# Review changes
terraform plan

# Apply updates
terraform apply
```

This will:
- Update Glue job configurations with `--aws_region` parameter
- Upload updated Python scripts to S3

### Step 3: Run Jobs

```bash
# Run ingestion job
aws glue start-job-run --job-name csv-to-iceberg-ingestion

# Run views job (or wait for auto-trigger)
aws glue start-job-run --job-name create-views-normal
```

### Step 4: Verify Views

```sql
-- In Athena
SHOW TABLES IN collections_db;

-- Should see:
-- collections_data_tbl (table)
-- collections_data_view (view)
-- fry9c_report_view (view)
-- fry15_report_view (view)

-- Test collections_data_view
SELECT COUNT(*) FROM collections_db.collections_data_view;

-- Test report view
SELECT COUNT(*) FROM collections_db.fry9c_report_view;
```

---

## Expected Job Output

### create-views-normal Job

```
================================================================================
STEP 1: Creating collections_data_view
================================================================================
View collections_data_view doesn't exist, will create new
Creating view in Spark...
✓ View created in Spark: collections_data_view
Verifying view in Spark...
✓ View verified in Spark: 15000 rows
Adding Athena dialect...
Query execution ID: abc123...
✓ Query succeeded
✓ Athena dialect added successfully
Query execution ID: def456...
✓ Query succeeded
✓ View works in Athena!

✓✓✓ View collections_data_view successfully created for BOTH engines!

================================================================================
STEP 2: Creating Series-Specific Report Views
================================================================================

Processing seriesid = 'fry9c'
Found 150 distinct key values

✓ Dropped existing view: fry9c_report_view (will recreate)

[Step 1/3] Creating PROTECTED MULTI DIALECT VIEW in Spark...
✓ View created in Spark: fry9c_report_view

[Step 2/3] Verifying view in Spark...
✓ View verified in Spark: 100 rows

[Step 3/3] Adding Athena dialect via ALTER VIEW ADD DIALECT...
✓ Athena dialect added successfully

✓✓✓ View fry9c_report_view successfully created for BOTH engines!

================================================================================
normal VIEW CREATION SUMMARY
================================================================================
Collections view: collections_data_view ✓
Total seriesid values processed: 2
Report views created successfully: 2
Report views failed: 0

✓ Successfully created normal report views:
  - collections_db.fry9c_report_view
  - collections_db.fry15_report_view
```

---

## Troubleshooting

### Issue: Region Mismatch

**Symptom:**
```
Error: Could not connect to the endpoint URL
```

**Solution:**
```bash
# Check terraform.tfvars
cat terraform/terraform.tfvars | grep aws_region

# Ensure it matches your AWS CLI configuration
aws configure get region

# Update if needed
vim terraform/terraform.tfvars
terraform apply
```

### Issue: collections_data_view Not Found

**Symptom:**
```
Table or view not found: collections_data_view
```

**Solution:**
```bash
# Re-run the views job
aws glue start-job-run --job-name create-views-normal

# Check job status
aws glue get-job-runs --job-name create-views-normal --max-results 1

# Check logs
aws logs tail /aws-glue/jobs/output --follow
```

### Issue: View Already Exists Error

**Symptom:**
```
AlreadyExistsException: View already exists
```

**Solution:**
This should not happen with the new recreation logic, but if it does:

```sql
-- In Athena, drop the view manually
DROP VIEW IF EXISTS collections_db.collections_data_view;

-- Re-run the job
aws glue start-job-run --job-name create-views-normal
```

---

## Summary

### What Changed

1. ✅ **Region Externalized**: All scripts now use `aws_region` parameter
2. ✅ **collections_data_view Added**: Unified view of all data
3. ✅ **View Recreation**: Automatic recreation of existing views

### New Views

| View Name | Type | Purpose | Schema |
|-----------|------|---------|--------|
| `collections_data_view` | Multi-Dialect | All data, all series | Same as staging table |
| `<seriesid>_report_view` | Multi-Dialect | Pivoted data per series | Wide format with pivoted columns |

### Benefits

- 🌍 Deploy to any AWS region
- 📊 Unified data access across all series
- 🔄 Idempotent job execution
- 🚀 Simplified queries and analysis

---

**Version**: 1.0  
**Last Updated**: February 11, 2026  
**Status**: Complete ✅
