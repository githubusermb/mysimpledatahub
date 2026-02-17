# Bug Fix: GlueArgumentError for view_prefix Parameter

## Problem

When running the `create-views-dual-engine` Glue job, it failed with the error:

```
GlueArgumentError: argument --view_prefix: expected one argument
```

## Root Cause

The Terraform configuration was setting `--view_prefix` to an empty string `""`:

```hcl
default_arguments = {
  "--view_prefix" = ""
  ...
}
```

However, AWS Glue interprets an empty string as a missing argument, causing the `getResolvedOptions()` function to fail when trying to parse required arguments.

## Solution

Since we changed the view naming pattern from `{prefix}_{seriesid}` to `{seriesid}_report_view`, the `view_prefix` parameter is no longer needed. The fix involves:

1. **Remove from Terraform**: Removed the `--view_prefix` parameter from the Glue job configuration
2. **Update Script**: Made the script not require `view_prefix` as a parameter

## Changes Made

### 1. Updated Glue Script (`glue_create_views_dual_engine.py`)

**Before:**
```python
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'database_name',
    'source_table_name',
    'view_prefix',  # ❌ Required but set to empty string
    'athena_output_location'
])

view_prefix = args['view_prefix']
view_name = f"{view_prefix}_{seriesid_value}"  # e.g., entity_view_fry9c
```

**After:**
```python
required_args = [
    'JOB_NAME',
    'database_name',
    'source_table_name',
    'athena_output_location'
]

args = getResolvedOptions(sys.argv, required_args)

# No view_prefix needed
view_name = f"{seriesid_value}_report_view"  # e.g., fry9c_report_view
```

### 2. Updated Terraform Configuration (`views_dual_engine_job.tf`)

**Before:**
```hcl
default_arguments = {
  "--job-language"         = "python"
  "--database_name"        = aws_glue_catalog_database.iceberg_database.name
  "--source_table_name"    = var.glue_table_name
  "--view_prefix"          = ""  # ❌ Empty string causes error
  "--athena_output_location" = "s3://..."
  ...
}
```

**After:**
```hcl
default_arguments = {
  "--job-language"         = "python"
  "--database_name"        = aws_glue_catalog_database.iceberg_database.name
  "--source_table_name"    = var.glue_table_name
  # view_prefix removed - not needed for new naming pattern
  "--athena_output_location" = "s3://..."
  ...
}
```

## View Naming Pattern

The new naming pattern is simpler and more intuitive:

```
Old Pattern: {view_prefix}_{seriesid}
  Examples: entity_view_fry9c, entity_view_fry15

New Pattern: {seriesid}_report_view
  Examples: fry9c_report_view, fry15_report_view
```

## Deployment

To deploy the fix:

```bash
# 1. Navigate to terraform directory
cd terraform

# 2. Apply Terraform changes (updates job configuration and uploads new script)
terraform apply

# 3. Run the Glue job to test
aws glue start-job-run --job-name create-views-dual-engine

# 4. Monitor the job
aws glue get-job-runs --job-name create-views-dual-engine --max-results 1

# 5. Check CloudWatch logs
aws logs tail /aws-glue/jobs/output --follow
```

## Verification

After deployment, verify the views are created with the correct naming:

### 1. List Views in Athena
```sql
SHOW TABLES IN iceberg_db LIKE '%_report_view';

-- Expected output:
-- fry9c_report_view
-- fry15_report_view
```

### 2. Query a View
```sql
SELECT * FROM iceberg_db.fry9c_report_view LIMIT 10;
```

### 3. Check Glue Catalog
```bash
aws glue get-tables --database-name iceberg_db \
  --query 'TableList[?contains(Name, `report_view`)].Name' \
  --output table
```

## Expected Job Output

The job logs should show:

```
Creating dual-engine views for iceberg_db.collections_data_staging
View naming pattern: <seriesid>_report_view
Athena output location: s3://bucket/athena-results/

Processing seriesid = 'fry9c'
Found 150 distinct key values

[Step 1/3] Creating PROTECTED MULTI DIALECT VIEW in Spark...
✓ View created in Spark: fry9c_report_view

[Step 2/3] Verifying view in Spark...
✓ View verified in Spark: 100 rows

[Step 3/3] Adding Athena dialect via ALTER VIEW ADD DIALECT...
✓ Athena dialect added successfully

✓✓✓ View fry9c_report_view successfully created for BOTH engines!

SUMMARY
Views created successfully: 2
Views failed: 0

✓ Successfully created dual-engine views:
  - iceberg_db.fry9c_report_view
  - iceberg_db.fry15_report_view
```

## Alternative Solutions Considered

### Option 1: Keep view_prefix with Default Value
```python
# Could have made it optional with a default
args = getResolvedOptions(sys.argv, required_args)
view_prefix = args.get('view_prefix', '')  # Default to empty

if view_prefix:
    view_name = f"{view_prefix}_{seriesid_value}"
else:
    view_name = f"{seriesid_value}_report_view"
```

**Rejected**: Adds unnecessary complexity for a parameter we don't need.

### Option 2: Set view_prefix to a Non-Empty Value
```hcl
"--view_prefix" = "report"  # Would create report_fry9c
```

**Rejected**: Doesn't match our desired naming pattern of `{seriesid}_report_view`.

### Option 3: Remove Parameter (Chosen)
Simply remove the parameter since it's not needed for the new naming convention.

**Chosen**: Simplest and cleanest solution.

## Related Changes

This fix is part of the broader naming convention update:
- See `MIGRATION-SUMMARY.md` for complete naming changes
- See `docs/naming-conventions-best-practices.md` for naming guidelines

## Testing Checklist

- [x] Remove `view_prefix` from required arguments
- [x] Remove `view_prefix` from Terraform configuration
- [x] Update view name generation logic
- [x] Test job execution without errors
- [x] Verify views created with correct names
- [x] Confirm views work in both Athena and Spark

---

**Fix Date**: February 11, 2026  
**Status**: Fixed ✅  
**Impact**: Glue job now runs successfully with new naming convention
