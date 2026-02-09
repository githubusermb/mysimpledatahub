# Quick Start Guide

## What You Have

This folder contains the complete solution for:
1. Creating an Iceberg table from CSV data
2. Generating dynamic views that work in both Athena and Glue Spark

## File Structure

```
mydatahub1/
 README.md                              # Complete documentation
 QUICK-START.md                         # This file
 DUAL-ENGINE-SOLUTION.md                # Detailed view solution
 terraform/
    main.tf                            # Main infrastructure
    variables.tf                       # Variable definitions
    outputs.tf                         # Output values
    terraform.tfvars                   # Your configuration
    lakeformation.tf                   # Lake Formation setup
    views_dual_engine_job.tf           # View creation job
 scripts/
    glue_csv_to_iceberg.py             # Data ingestion
    glue_create_views_dual_engine.py   # View creation
    test_views_in_spark.py             # Testing script
 docs/
     lake-formation-permissions.md      # Permissions guide
     troubleshooting-lakeformation.md   # Troubleshooting
```

## Quick Deployment (5 Steps)

### 1. Configure Terraform

Edit `terraform/terraform.tfvars`:

```hcl
aws_region          = "us-east-1"
s3_bucket_name      = "your-unique-bucket-name"  # Change this!
glue_database_name  = "iceberg_db"
glue_table_name     = "entity_data"
csv_data_prefix     = "raw-data/"
iceberg_data_prefix = "iceberg-data/"
```

### 2. Deploy Infrastructure

```bash
cd terraform
terraform init
terraform apply
```

### 3. Upload CSV Data

Your CSV must have these columns: entity1,entity2,entity3,entity4,key,value

```bash
aws s3 cp your-data.csv s3://your-bucket/raw-data/
```

### 4. Run Data Ingestion

```bash
aws glue start-job-run --job-name csv-to-iceberg-ingestion
```

### 5. Create Views

```bash
aws glue start-job-run --job-name create-views-dual-engine
```

## Query Your Data

### In Athena

```sql
-- Query the table
SELECT * FROM iceberg_db.entity_data LIMIT 10;

-- Query a view
SELECT * FROM iceberg_db.entity_view_entity1_set1 LIMIT 10;
```

### In Glue Spark

```python
# Query the table
df = spark.sql("SELECT * FROM glue_catalog.iceberg_db.entity_data LIMIT 10")
df.show()

# Query a view
view_df = spark.sql("SELECT * FROM glue_catalog.iceberg_db.entity_view_entity1_set1 LIMIT 10")
view_df.show()
```

## What Gets Created

-  S3 bucket for data storage
-  Glue database: iceberg_db
-  Iceberg table: entity_data
-  Dynamic views: entity_view_* (one per entity1 value)
- ✅ IAM roles and Lake Formation permissions

## Key Features

✅ Dual-Engine Views - Work in both Athena and Glue Spark
✅ Automatic View Creation - One view per entity1 value
✅ Lake Formation - Centralized permissions
✅ Iceberg Format - ACID transactions, time travel
✅ Infrastructure as Code - Fully automated with Terraform

## Next Steps

1. Read README.md for complete documentation
2. Read DUAL-ENGINE-SOLUTION.md for view details
3. Check docs/ for troubleshooting guides

---

Ready to deploy! Start with step 1 above.
