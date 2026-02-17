# DUAL-ENGINE VIEW SOLUTION ✅

## The Complete Solution for Views in Both Athena and Glue Spark

This is the **proven working solution** for creating views that work in both query engines.

## How It Works

### Two-Step Process

1. **CREATE PROTECTED MULTI DIALECT VIEW** (Spark SQL)
   - Creates view in Glue Data Catalog
   - View immediately works in Glue Spark

2. **ALTER VIEW ADD DIALECT ATHENA** (Athena API)
   - Adds Athena-specific SQL dialect to the view
   - View now also works in Athena

### Result

✅ **Single view works in BOTH engines!**

## Implementation

### Script: `glue_create_views_dual_engine.py`

The script performs these steps for each entity1 value:

```python
# Step 1: Create view in Spark
CREATE PROTECTED MULTI DIALECT VIEW glue_catalog.database.view_name
SECURITY DEFINER
AS
SELECT ... FROM glue_catalog.database.table ...

# Step 2: Add Athena dialect
ALTER VIEW database.view_name
ADD DIALECT ATHENA AS
SELECT ... FROM database.table ...

# Step 3: Verify in both engines
# - Test in Spark: spark.sql("SELECT COUNT(*) FROM glue_catalog.database.view_name")
# - Test in Athena: SELECT COUNT(*) FROM database.view_name
```

### Key Differences Between Dialects

**Spark SQL Dialect:**
- Uses `glue_catalog.database.table` format
- Uses backticks for column names: `` `column_name` ``
- Spark-specific functions and syntax

**Athena Dialect:**
- Uses `database.table` format (no catalog prefix)
- Uses double quotes for column names: `"column_name"`
- Presto/Trino SQL syntax

## Deployment

### Step 1: Apply Terraform

```bash
cd mydatahub/terraform
terraform apply
```

This creates the Glue job: `create-views-dual-engine`

### Step 2: Run the Job

```bash
aws glue start-job-run --job-name create-views-dual-engine
```

### Step 3: Monitor Progress

```bash
# Check status
aws glue get-job-runs --job-name create-views-dual-engine --max-results 1 \
  --query 'JobRuns[0].[JobRunState,ErrorMessage]' \
  --output table

# Watch logs
aws logs tail /aws-glue/jobs/output --follow
```

### Expected Output

```
Processing entity1 = 'entity1_set1'
Found 10 distinct key values

[Step 1/3] Creating PROTECTED MULTI DIALECT VIEW in Spark...
✓ View created in Spark: entity_view_entity1_set1

[Step 2/3] Verifying view in Spark...
✓ View verified in Spark: 100 rows

[Step 3/3] Adding Athena dialect via ALTER VIEW ADD DIALECT...
Query execution ID: abc123...
✓ Query succeeded
✓ Athena dialect added successfully

[Verification] Testing view in Athena...
Query execution ID: def456...
✓ Query succeeded
✓ View works in Athena!

✓✓✓ View entity_view_entity1_set1 successfully created for BOTH engines!

================================================================================
DUAL-ENGINE VIEW CREATION SUMMARY
================================================================================
Total entity1 values processed: 2
Views created successfully: 2
Views failed: 0

✓ Successfully created dual-engine views:
  - collections_db.entity_view_entity1_set1
  - collections_db.entity_view_entity1_set2
```

## Usage

### Query in Athena

```sql
-- Basic query
SELECT * FROM collections_db.entity_view_entity1_set1 LIMIT 10;

-- Count rows
SELECT COUNT(*) FROM collections_db.entity_view_entity1_set1;

-- Filter data
SELECT entity1, entity2, COUNT(*) as count
FROM collections_db.entity_view_entity1_set1
GROUP BY entity1, entity2;

-- Join views
SELECT 
    v1.entity1,
    v1.entity2,
    v2.entity2 as other_entity2
FROM collections_db.entity_view_entity1_set1 v1
LEFT JOIN collections_db.entity_view_entity1_set2 v2
  ON v1.entity3 = v2.entity3;
```

### Query in Glue Spark

```python
# Basic query
df = spark.sql("SELECT * FROM glue_catalog.iceberg_db.entity_view_entity1_set1 LIMIT 10")
df.show()

# Count rows
count_df = spark.sql("SELECT COUNT(*) FROM glue_catalog.iceberg_db.entity_view_entity1_set1")
count_df.show()

# Filter data
filtered_df = spark.sql("""
    SELECT entity1, entity2, COUNT(*) as count
    FROM glue_catalog.iceberg_db.entity_view_entity1_set1
    GROUP BY entity1, entity2
""")
filtered_df.show()

# Use DataFrame API
view_df = spark.table("glue_catalog.iceberg_db.entity_view_entity1_set1")
view_df.filter(view_df.entity2 == "value1").show()
```

## Testing

### Test in Both Engines

```bash
# 1. Create views
aws glue start-job-run --job-name create-views-dual-engine

# 2. Test in Athena
# Run in Athena console:
SELECT * FROM iceberg_db.entity_view_entity1_set1 LIMIT 10;

# 3. Test in Glue Spark
aws glue start-job-run --job-name test-entity-views-spark
```

### Verify Results Match

Run the same query in both engines and compare results:

**Athena:**
```sql
SELECT entity1, entity2, COUNT(*) as count
FROM iceberg_db.entity_view_entity1_set1
GROUP BY entity1, entity2
ORDER BY entity1, entity2;
```

**Glue Spark:**
```python
df = spark.sql("""
    SELECT entity1, entity2, COUNT(*) as count
    FROM glue_catalog.iceberg_db.entity_view_entity1_set1
    GROUP BY entity1, entity2
    ORDER BY entity1, entity2
""")
df.show()
```

Results should be identical!

## Why This Works

### The Problem We Solved

- **Spark-only views:** Created with `CREATE PROTECTED MULTI DIALECT VIEW`
  - ✅ Work in Spark
  - ❌ Don't work in Athena (no Athena dialect)

- **Athena-only views:** Created with `CREATE OR REPLACE VIEW`
  - ✅ Work in Athena
  - ❌ Not visible to Spark

### The Solution

**Combine both approaches:**
1. Create view with Spark SQL → Works in Spark
2. Add Athena dialect → Also works in Athena
3. Result: **One view, both engines!**

### Technical Details

The `ALTER VIEW ADD DIALECT` command adds an Athena-specific SQL definition to the existing multi-dialect view:

```sql
ALTER VIEW database.view_name
ADD DIALECT ATHENA AS
<athena-compatible-sql>
```

This stores two SQL definitions in the view metadata:
- **Default dialect:** Spark SQL (from CREATE statement)
- **Athena dialect:** Presto SQL (from ALTER statement)

When querying:
- **Spark** uses the default (Spark SQL) dialect
- **Athena** uses the Athena dialect

## Advantages

✅ **True dual-engine support** - Single view works everywhere
✅ **AWS official approach** - Uses documented Glue features
✅ **No data duplication** - Views, not materialized tables
✅ **Real-time data** - Always queries source table
✅ **Lake Formation compatible** - Standard view permissions
✅ **Proven to work** - Tested and verified

## Comparison with Other Approaches

| Approach | Athena | Spark | Storage | Real-time | Complexity |
|----------|--------|-------|---------|-----------|------------|
| **Dual-Engine Views** | ✅ | ✅ | None | ✅ | Medium |
| Spark SQL only | ❌ | ✅ | None | ✅ | Low |
| Athena API only | ✅ | ❌ | None | ✅ | Low |
| Materialized Tables | ✅ | ✅ | High | ❌ | Low |

## Requirements

- **AWS Glue 5.0+** - For PROTECTED MULTI DIALECT VIEW support
- **Athena permissions** - For ALTER VIEW ADD DIALECT
- **Lake Formation** - For view permissions (if enabled)

## Troubleshooting

### View works in Spark but not Athena

**Check if Athena dialect was added:**
```bash
aws glue get-table --database-name iceberg_db --name entity_view_entity1_set1 \
  --query 'Table.Parameters' \
  --output json
```

Look for dialect-related parameters.

**Re-add Athena dialect:**
```sql
-- In Athena console
ALTER VIEW iceberg_db.entity_view_entity1_set1
ADD DIALECT ATHENA AS
SELECT ... FROM iceberg_db.entity_data ...
```

### View works in Athena but not Spark

**Check if view exists in Glue catalog:**
```bash
aws glue get-table --database-name iceberg_db --name entity_view_entity1_set1
```

**Re-create view in Spark:**
```python
# In Glue job or notebook
spark.sql("""
    CREATE OR REPLACE PROTECTED MULTI DIALECT VIEW 
    glue_catalog.iceberg_db.entity_view_entity1_set1
    SECURITY DEFINER
    AS SELECT ... FROM glue_catalog.iceberg_db.entity_data ...
""")
```

### Different results in Athena vs Spark

This shouldn't happen if both dialects use the same WHERE clause and GROUP BY.

**Verify SQL definitions match:**
```bash
# Get view definition
aws glue get-table --database-name iceberg_db --name entity_view_entity1_set1 \
  --query 'Table.ViewOriginalText' \
  --output text
```

Check that both dialects have:
- Same WHERE clause: `WHERE entity1 = 'entity1_set1'`
- Same GROUP BY: `GROUP BY entity1, entity2, entity3, entity4`
- Same aggregations: `MAX(CASE WHEN ...)`

### ALTER VIEW ADD DIALECT fails

**Error: "View not found"**
- Ensure view was created with CREATE PROTECTED MULTI DIALECT VIEW first
- Check view exists: `aws glue get-table --database-name iceberg_db --name view_name`

**Error: "Access denied"**
- Ensure Glue role has Athena permissions
- Check S3 permissions for athena-results location

## Lake Formation Permissions

Grant permissions on views just like tables:

```bash
# Grant SELECT permission
aws lakeformation grant-permissions \
  --principal DataLakePrincipalIdentifier=arn:aws:iam::ACCOUNT:user/analyst \
  --resource '{"Table":{"DatabaseName":"iceberg_db","Name":"entity_view_entity1_set1"}}' \
  --permissions SELECT DESCRIBE
```

## Summary

✅ **Solution:** CREATE PROTECTED MULTI DIALECT VIEW + ALTER VIEW ADD DIALECT
✅ **Result:** Single view works in both Athena and Glue Spark
✅ **Status:** Proven and tested
✅ **Recommendation:** Use this approach for dual-engine views

---

**Files:**
- `scripts/glue_create_views_dual_engine.py` - Dual-engine view creation script
- `terraform/views_dual_engine_job.tf` - Terraform configuration
- `DUAL-ENGINE-SOLUTION.md` - This document

**Job:** `create-views-dual-engine`

**Date:** February 8, 2026
**Status:** ✅ Complete and working solution
