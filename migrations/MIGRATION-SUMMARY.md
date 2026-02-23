# Migration Summary: Updated Table and View Names

## Overview

The table and view naming conventions have been updated to better reflect the regulatory reporting use case.

## Changes Made

### Table Name Change
```
OLD: entity_data
NEW: collections_data_tbl
```

### View Name Pattern Change
```
OLD: entity_view_<seriesid>
     Example: entity_view_fry9c, entity_view_fry15

NEW: <seriesid>_report_view
     Example: fry9c_report_view, fry15_report_view
```

## Updated Files

### Glue Job Scripts
- ✅ `scripts/glue_csv_to_iceberg.py` - Updated table name references
- ✅ `scripts/glue_create_normal_views.py` - Updated view naming pattern

### Terraform Configuration
- ✅ `terraform/variables.tf` - Updated default table name to `collections_data_tbl`
- ✅ `terraform/terraform.tfvars` - Updated table name value
- ✅ `terraform/views_normal_job.tf` - Removed view_prefix (now uses `<seriesid>_report_view` pattern)

### Helper Scripts
- ✅ `scripts/setup_lakeformation_complete.py` - Updated example table name
- ✅ `scripts/lambda_lakeformation_setup.py` - Updated example table name
- ✅ `scripts/grant_athena_user_permissions.py` - Updated example table name

### Documentation
- ✅ `ARCHITECTURE.md` - Updated all architecture diagrams and references
- ✅ `ENTITY-DIAGRAMS.md` - Updated all entity diagrams and examples
- ✅ `docs/naming-conventions-best-practices.md` - Added new naming pattern as recommended approach

## Migration Steps

If you have an existing deployment, follow these steps:

### Step 1: Backup Existing Data
```bash
# Export existing table metadata
aws glue get-table --database-name collections_db --name entity_data > entity_data_backup.json

# List existing views
aws glue get-tables --database-name collections_db --query 'TableList[?starts_with(Name, `entity_view`)].Name'
```

### Step 2: Update Terraform Configuration
```bash
cd terraform

# Update terraform.tfvars
# Change: glue_table_name = "entity_data"
# To:     glue_table_name = "collections_data_tbl"

# Review changes
terraform plan

# Apply changes (this will update job parameters)
terraform apply
```

### Step 3: Rename Existing Table (Optional)

If you want to rename the existing table:

```sql
-- In Athena or Glue Spark
ALTER TABLE collections_db.entity_data RENAME TO collections_db.collections_data_tbl;
```

Or create a new table and migrate data:

```bash
# Run the updated ingestion job to create new table
aws glue start-job-run --job-name csv-to-iceberg-ingestion
```

### Step 4: Recreate Views with New Names

```bash
# Drop old views (optional - if you want to clean up)
aws glue delete-table --database-name collections_db --name entity_view_fry9c
aws glue delete-table --database-name collections_db --name entity_view_fry15

# Run the updated views job to create new views
aws glue start-job-run --job-name create-views-normal
```

### Step 5: Update Queries and Applications

Update any existing queries or applications that reference the old names:

```sql
-- OLD queries
SELECT * FROM collections_db.entity_data;
SELECT * FROM collections_db.entity_view_fry9c;

-- NEW queries
SELECT * FROM collections_db.collections_data_tbl;
SELECT * FROM collections_db.fry9c_report_view;
```

### Step 6: Update Lake Formation Permissions

If you have existing Lake Formation permissions, update them:

```bash
# Grant permissions on new table name
aws lakeformation grant-permissions \
  --principal DataLakePrincipalIdentifier=arn:aws:iam::ACCOUNT:user/analyst \
  --resource '{"Table":{"DatabaseName":"collections_db","Name":"collections_data_tbl"}}' \
  --permissions SELECT DESCRIBE

# Grant permissions on new view pattern
aws lakeformation grant-permissions \
  --principal DataLakePrincipalIdentifier=arn:aws:iam::ACCOUNT:user/analyst \
  --resource '{"Table":{"DatabaseName":"collections_db","Name":"fry9c_report_view"}}' \
  --permissions SELECT DESCRIBE
```

## New Deployment

For new deployments, simply use the updated configuration:

```bash
cd terraform
terraform init
terraform apply
```

The new naming convention will be used automatically.

## Query Examples

### Query the Staging Table

**Athena:**
```sql
SELECT * FROM collections_db.collections_data_tbl 
WHERE seriesid = 'fry9c' 
LIMIT 10;
```

**Glue Spark:**
```python
df = spark.sql("""
    SELECT * FROM glue_catalog.collections_db.collections_data_tbl 
    WHERE seriesid = 'fry9c' 
    LIMIT 10
""")
df.show()
```

### Query Report Views

**Athena:**
```sql
-- Query specific report
SELECT * FROM collections_db.fry9c_report_view LIMIT 10;

-- List all report views
SHOW TABLES IN collections_db LIKE '%_report_view';

-- Join multiple reports
SELECT 
    f9.seriesid,
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
# Query specific report
df = spark.sql("SELECT * FROM glue_catalog.collections_db.fry9c_report_view LIMIT 10")
df.show()

# Join multiple reports
joined_df = spark.sql("""
    SELECT 
        f9.seriesid,
        f9.aod,
        f9.RCON2170 AS fry9c_value,
        f15.RCON3210 AS fry15_value
    FROM glue_catalog.collections_db.fry9c_report_view f9
    LEFT JOIN glue_catalog.collections_db.fry15_report_view f15
      ON f9.rssdid = f15.rssdid
      AND f9.aod = f15.aod
""")
joined_df.show()
```

## Benefits of New Naming Convention

### Table Name: `collections_data_tbl`
✅ More descriptive - indicates this is staging data for collections
✅ Follows naming best practice: `<entity>_<data_type>_<purpose>`
✅ Clearly indicates the data layer (staging)
✅ Better aligns with regulatory reporting terminology

### View Pattern: `<seriesid>_report_view`
✅ Cleaner and more concise
✅ Puts the most important identifier (seriesid) first
✅ Clearly indicates these are report views
✅ Easier to discover and understand
✅ Follows regulatory reporting conventions

## Rollback Plan

If you need to rollback to the old naming:

1. Update `terraform/terraform.tfvars`:
   ```hcl
   glue_table_name = "entity_data"
   ```

2. Update `terraform/views_normal_job.tf`:
   ```hcl
   "--view_prefix" = "entity_view"
   ```

3. Revert the Glue script changes in:
   - `scripts/glue_create_normal_views.py`
   - `scripts/glue_csv_to_iceberg.py`

4. Run `terraform apply` to update job configurations

## Support

For questions or issues with the migration:
- Review the updated documentation in `ARCHITECTURE.md` and `ENTITY-DIAGRAMS.md`
- Check the naming conventions guide in `docs/naming-conventions-best-practices.md`
- Review CloudWatch logs for Glue jobs if issues occur

---

**Migration Date**: February 11, 2026  
**Status**: Complete ✅  
**Breaking Change**: Yes - requires updates to existing queries and applications
