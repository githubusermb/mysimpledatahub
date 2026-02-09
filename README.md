# AWS Data Lake with Iceberg Tables and Dynamic Views

Complete solution for creating a narrow Iceberg table from CSV data and generating dynamic views that work in both Athena and Glue Spark.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Detailed Setup](#detailed-setup)
- [Usage](#usage)
- [Troubleshooting](#troubleshooting)
- [Cost Considerations](#cost-considerations)

## Overview

This solution provides:

1. **Iceberg Table Creation** - Converts CSV data to narrow Iceberg format
2. **Dynamic View Generation** - Creates pivoted views for each distinct entity1 value
3. **Dual-Engine Support** - Tables and views work in both Athena and Glue Spark
4. **Lake Formation Integration** - Automated permissions management
5. **Infrastructure as Code** - Complete Terraform configuration

### Data Flow

```
CSV Files (S3)
    ↓
Glue ETL Job (csv-to-iceberg-ingestion)
    ↓
Iceberg Table (entity_data)
    ↓
Glue ETL Job (create-views-dual-engine)
    ↓
Dynamic Views (entity_view_*)
    ↓
Query from Athena or Glue Spark
```

### Table Structure

**Source CSV Format:**
```
entity1,entity2,entity3,entity4,key,value
entity1_set1,val2,val3,val4,key_set1_1001,value_1001
entity1_set1,val2,val3,val4,key_set1_1002,value_1002
```

**Iceberg Table (entity_data):**
```
entity1 | entity2 | entity3 | entity4 | key           | value
--------|---------|---------|---------|---------------|----------
set1    | val2    | val3    | val4    | key_set1_1001 | value_1001
set1    | val2    | val3    | val4    | key_set1_1002 | value_1002
```

**Dynamic Views (entity_view_entity1_set1):**
```
entity1 | entity2 | entity3 | entity4 | key_set1_1001 | key_set1_1002 | ...
--------|---------|---------|---------|---------------|---------------|----
set1    | val2    | val3    | val4    | value_1001    | value_1002    | ...
```

## Architecture

### Components

1. **S3 Buckets**
   - Source data (CSV files)
   - Iceberg table data
   - Glue scripts
   - Athena query results

2. **AWS Glue**
   - Data Catalog (database and tables)
   - ETL Jobs (data ingestion and view creation)
   - Triggers (automated workflow)

3. **Lake Formation**
   - Data location registration
   - Table permissions
   - View permissions

4. **IAM Roles**
   - Glue service role
   - User/analyst roles

### Glue Jobs

| Job Name | Purpose | Trigger |
|----------|---------|---------|
| `csv-to-iceberg-ingestion` | Convert CSV to Iceberg table | Manual/Scheduled |
| `create-views-dual-engine` | Create dynamic views | After ingestion succeeds |

## Prerequisites

### Required Tools

- **Terraform** >= 1.0
- **AWS CLI** >= 2.0
- **Python** >= 3.8 (for local scripts)

### AWS Account Setup

1. **AWS Account** with appropriate permissions
2. **AWS CLI configured** with credentials
3. **S3 bucket** for Terraform state (optional)

### Required AWS Permissions

Your AWS user/role needs:
- IAM: Create roles and policies
- S3: Create and manage buckets
- Glue: Create databases, tables, jobs
- Lake Formation: Register resources, grant permissions
- Athena: Execute queries

## Quick Start

### 1. Clone and Configure

```bash
cd mydatahub/terraform

# Edit terraform.tfvars with your settings
vi terraform.tfvars
```

**Required variables in `terraform.tfvars`:**
```hcl
aws_region          = "us-east-1"
s3_bucket_name      = "your-unique-bucket-name"
glue_database_name  = "iceberg_db"
glue_table_name     = "entity_data"
csv_data_prefix     = "raw-data/"
iceberg_data_prefix = "iceberg-data/"
```

### 2. Deploy Infrastructure

```bash
# Initialize Terraform
terraform init

# Review planned changes
terraform plan

# Deploy all resources
terraform apply
```

This creates:
- ✅ S3 bucket for data
- ✅ Glue database and IAM roles
- ✅ Lake Formation permissions
- ✅ Glue ETL jobs
- ✅ Automated triggers

### 3. Upload Source Data

```bash
# Upload your CSV files
aws s3 cp your-data.csv s3://your-bucket/raw-data/

# Or upload entire directory
aws s3 sync ./local-data/ s3://your-bucket/raw-data/
```

**CSV Requirements:**
- Must have columns: `entity1`, `entity2`, `entity3`, `entity4`, `key`, `value`
- UTF-8 encoding
- Header row required

### 4. Run Data Ingestion

```bash
# Start the ingestion job
aws glue start-job-run --job-name csv-to-iceberg-ingestion

# Monitor progress
aws glue get-job-runs --job-name csv-to-iceberg-ingestion --max-results 1
```

### 5. Create Dynamic Views

The view creation job runs automatically after ingestion completes, or run manually:

```bash
# Manual execution
aws glue start-job-run --job-name create-views-dual-engine

# Monitor progress
aws glue get-job-runs --job-name create-views-dual-engine --max-results 1
```

### 6. Query Your Data

**In Athena:**
```sql
-- Query the Iceberg table
SELECT * FROM iceberg_db.entity_data LIMIT 10;

-- Query a dynamic view
SELECT * FROM iceberg_db.entity_view_entity1_set1 LIMIT 10;

-- List all views
SHOW TABLES IN iceberg_db LIKE 'entity_view%';
```

**In Glue Spark:**
```python
# Query the Iceberg table
df = spark.sql("SELECT * FROM glue_catalog.iceberg_db.entity_data LIMIT 10")
df.show()

# Query a dynamic view
view_df = spark.sql("SELECT * FROM glue_catalog.iceberg_db.entity_view_entity1_set1 LIMIT 10")
view_df.show()
```

## Detailed Setup

### Step 1: Prepare Your Environment

#### Install Required Tools

**Terraform:**
```bash
# macOS
brew install terraform

# Linux
wget https://releases.hashicorp.com/terraform/1.6.0/terraform_1.6.0_linux_amd64.zip
unzip terraform_1.6.0_linux_amd64.zip
sudo mv terraform /usr/local/bin/

# Windows
choco install terraform
```

**AWS CLI:**
```bash
# macOS
brew install awscli

# Linux
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install

# Windows
msiexec.exe /i https://awscli.amazonaws.com/AWSCLIV2.msi
```

#### Configure AWS Credentials

```bash
aws configure
# AWS Access Key ID: YOUR_ACCESS_KEY
# AWS Secret Access Key: YOUR_SECRET_KEY
# Default region name: us-east-1
# Default output format: json
```

### Step 2: Configure Terraform Variables

Edit `terraform/terraform.tfvars`:

```hcl
# AWS Configuration
aws_region = "us-east-1"

# S3 Configuration
s3_bucket_name      = "my-data-lake-bucket-12345"  # Must be globally unique
csv_data_prefix     = "raw-data/"
iceberg_data_prefix = "iceberg-data/"

# Glue Configuration
glue_database_name = "iceberg_db"
glue_table_name    = "entity_data"

# Optional: Tags
tags = {
  Environment = "production"
  Project     = "data-lake"
  ManagedBy   = "terraform"
}
```

### Step 3: Deploy Infrastructure

```bash
cd mydatahub/terraform

# Initialize Terraform (first time only)
terraform init

# Validate configuration
terraform validate

# Preview changes
terraform plan

# Deploy infrastructure
terraform apply

# Type 'yes' when prompted
```

**What gets created:**

1. **S3 Resources**
   - Data bucket with versioning
   - Folders for source data, Iceberg data, scripts, JARs
   - Lifecycle policies

2. **IAM Resources**
   - Glue service role
   - Policies for S3, Glue, Lake Formation, Athena
   - Trust relationships

3. **Glue Resources**
   - Database: `iceberg_db`
   - Job: `csv-to-iceberg-ingestion`
   - Job: `create-views-dual-engine`
   - Trigger: Auto-run views after ingestion

4. **Lake Formation**
   - Data location registration
   - Database permissions
   - Table permissions (wildcard)

### Step 4: Upload Iceberg JARs

The Terraform configuration expects Iceberg JARs in S3. Upload them:

```bash
# Download Iceberg JARs
wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.4_2.12/1.4.2/iceberg-spark-runtime-3.4_2.12-1.4.2.jar
wget -O jars/iceberg-aws-1.4.2.jar https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws/1.4.2/iceberg-aws-1.4.2.jar
wget -O jars/bundle-2.20.18.jar https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.20.18/bundle-2.20.18.jar
wget -O jars/apache-client-2.20.18.jar https://repo1.maven.org/maven2/software/amazon/awssdk/apache-client/2.20.18/apache-client-2.20.18.jar
wget -O jars/url-connection-client-2.20.18.jar https://repo1.maven.org/maven2/software/amazon/awssdk/url-connection-client/2.20.18/url-connection-client-2.20.18.jar

# Upload to S3
aws s3 cp iceberg-spark-runtime-3.4_2.12-1.4.2.jar s3://your-bucket/jars/
aws s3 cp iceberg-aws-1.4.2.jar s3://your-bucket/jars/
aws s3 cp bundle-2.20.18.jar s3://your-bucket/jars/
aws s3 cp apache-client-2.20.18.jar s3://your-bucket/jars/
aws s3 cp url-connection-client-2.20.18.jar s3://your-bucket/jars/
```

### Step 5: Prepare Source Data

Your CSV files must have this structure:

```csv
entity1,entity2,entity3,entity4,key,value
entity1_set1,value2_1,value3_1,value4_1,key_set1_1001,value_1001
entity1_set1,value2_1,value3_1,value4_1,key_set1_1002,value_1002
entity1_set2,value2_2,value3_2,value4_2,key_set2_1006,value_1006
```

**Upload to S3:**
```bash
aws s3 cp data.csv s3://your-bucket/raw-data/
```

### Step 6: Run Data Ingestion

```bash
# Start the job
aws glue start-job-run --job-name csv-to-iceberg-ingestion

# Get the job run ID from output
JOB_RUN_ID="jr_xxxxx"

# Monitor status
aws glue get-job-run \
  --job-name csv-to-iceberg-ingestion \
  --run-id $JOB_RUN_ID \
  --query 'JobRun.[JobRunState,ErrorMessage]'

# Watch logs
aws logs tail /aws-glue/jobs/output --follow
```

**Expected output:**
```
Reading CSV files from s3://bucket/raw-data/
Found 1000 rows
Writing to Iceberg table: glue_catalog.iceberg_db.entity_data
✓ Successfully wrote 1000 rows
✓ Table registered with Lake Formation
Job completed successfully
```

### Step 7: Create Dynamic Views

Views are created automatically via trigger, or run manually:

```bash
# Manual execution
aws glue start-job-run --job-name create-views-dual-engine

# Monitor
aws glue get-job-runs --job-name create-views-dual-engine --max-results 1
```

**Expected output:**
```
Found 2 distinct entity1 values

Processing entity1 = 'entity1_set1'
[Step 1/3] Creating PROTECTED MULTI DIALECT VIEW in Spark...
✓ View created in Spark
[Step 2/3] Verifying view in Spark...
✓ View verified: 100 rows
[Step 3/3] Adding Athena dialect...
✓ Athena dialect added
✓✓✓ View entity_view_entity1_set1 created for BOTH engines!

SUMMARY
Views created successfully: 2
Views failed: 0
```

### Step 8: Verify Deployment

```bash
# List tables
aws glue get-tables --database-name iceberg_db \
  --query 'TableList[].Name' \
  --output table

# Check Iceberg table
aws glue get-table --database-name iceberg_db --name entity_data

# List views
aws glue get-tables --database-name iceberg_db \
  --query 'TableList[?TableType==`VIRTUAL_VIEW`].Name' \
  --output table
```

## Usage

### Querying in Athena

#### Query the Iceberg Table

```sql
-- Basic query
SELECT * FROM iceberg_db.entity_data LIMIT 10;

-- Count rows
SELECT COUNT(*) FROM iceberg_db.entity_data;

-- Filter by entity1
SELECT * FROM iceberg_db.entity_data 
WHERE entity1 = 'entity1_set1' 
LIMIT 10;

-- Aggregate data
SELECT entity1, COUNT(*) as count
FROM iceberg_db.entity_data
GROUP BY entity1;
```

#### Query Dynamic Views

```sql
-- List all views
SHOW TABLES IN iceberg_db LIKE 'entity_view%';

-- Query a specific view
SELECT * FROM iceberg_db.entity_view_entity1_set1 LIMIT 10;

-- Count rows in view
SELECT COUNT(*) FROM iceberg_db.entity_view_entity1_set1;

-- Join views
SELECT 
    v1.entity1,
    v1.entity2,
    v1.key_set1_1001,
    v2.key_set2_1006
FROM iceberg_db.entity_view_entity1_set1 v1
LEFT JOIN iceberg_db.entity_view_entity1_set2 v2
  ON v1.entity2 = v2.entity2;
```

### Querying in Glue Spark

#### In Glue ETL Job

```python
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Query Iceberg table
df = spark.sql("SELECT * FROM glue_catalog.iceberg_db.entity_data LIMIT 10")
df.show()

# Query dynamic view
view_df = spark.sql("SELECT * FROM glue_catalog.iceberg_db.entity_view_entity1_set1")
view_df.show()

# Use DataFrame API
table_df = spark.table("glue_catalog.iceberg_db.entity_data")
filtered = table_df.filter(table_df.entity1 == "entity1_set1")
filtered.show()

job.commit()
```

#### In Glue Notebook

```python
# Query table
df = spark.sql("""
    SELECT entity1, entity2, COUNT(*) as count
    FROM glue_catalog.iceberg_db.entity_data
    GROUP BY entity1, entity2
""")
df.show()

# Query view
view_df = spark.sql("""
    SELECT * 
    FROM glue_catalog.iceberg_db.entity_view_entity1_set1
    WHERE entity2 = 'value2_1'
""")
view_df.show()
```

### Updating Data

#### Add New Data

```bash
# Upload new CSV files
aws s3 cp new-data.csv s3://your-bucket/raw-data/

# Re-run ingestion (appends data)
aws glue start-job-run --job-name csv-to-iceberg-ingestion

# Views update automatically via trigger
```

#### Refresh Views

```bash
# Manual refresh
aws glue start-job-run --job-name create-views-dual-engine
```

### Granting User Access

#### Grant Lake Formation Permissions

```bash
# Grant SELECT on Iceberg table
aws lakeformation grant-permissions \
  --principal DataLakePrincipalIdentifier=arn:aws:iam::ACCOUNT:user/analyst \
  --resource '{"Table":{"DatabaseName":"iceberg_db","Name":"entity_data"}}' \
  --permissions SELECT DESCRIBE

# Grant SELECT on all views
aws lakeformation grant-permissions \
  --principal DataLakePrincipalIdentifier=arn:aws:iam::ACCOUNT:user/analyst \
  --resource '{"Table":{"DatabaseName":"iceberg_db","TableWildcard":{}}}' \
  --permissions SELECT DESCRIBE
```

#### Grant Athena Access

Users also need:
- S3 permissions for Athena query results
- Athena workgroup access

```bash
# Create IAM policy for Athena
aws iam create-policy \
  --policy-name AthenaQueryAccess \
  --policy-document file://athena-policy.json

# Attach to user
aws iam attach-user-policy \
  --user-name analyst \
  --policy-arn arn:aws:iam::ACCOUNT:policy/AthenaQueryAccess
```

## Troubleshooting

### Common Issues

#### 1. Glue Job Fails: "Table not found"

**Cause:** Iceberg table doesn't exist

**Solution:**
```bash
# Check if table exists
aws glue get-table --database-name iceberg_db --name entity_data

# If not, run ingestion job
aws glue start-job-run --job-name csv-to-iceberg-ingestion
```

#### 2. View Creation Fails: "Access Denied"

**Cause:** Missing Athena permissions

**Solution:**
```bash
# Check IAM role has Athena permissions
aws iam get-role-policy \
  --role-name GlueServiceRole \
  --policy-name GlueAthenaAccess

# If missing, re-run terraform apply
cd terraform
terraform apply
```

#### 3. Athena Query Fails: "HIVE_CANNOT_OPEN_SPLIT"

**Cause:** Lake Formation permissions not set

**Solution:**
```bash
# Grant data location access
aws lakeformation grant-permissions \
  --principal DataLakePrincipalIdentifier=arn:aws:iam::ACCOUNT:role/GlueServiceRole \
  --resource '{"DataLocation":{"ResourceArn":"arn:aws:s3:::your-bucket/iceberg-data/"}}' \
  --permissions DATA_LOCATION_ACCESS
```

#### 4. View Works in Athena but Not Spark

**Cause:** Athena dialect not added

**Solution:**
```bash
# Re-run view creation job
aws glue start-job-run --job-name create-views-dual-engine
```

#### 5. Different Results in Athena vs Spark

**Cause:** Views using different SQL dialects

**Solution:**
```sql
-- Verify view definition in Athena
SHOW CREATE VIEW iceberg_db.entity_view_entity1_set1;

-- Check both have same WHERE clause and GROUP BY
```

### Debugging

#### Check Glue Job Logs

```bash
# Output logs
aws logs tail /aws-glue/jobs/output --follow

# Error logs
aws logs tail /aws-glue/jobs/error --follow

# Specific job run
aws logs get-log-events \
  --log-group-name /aws-glue/jobs/output \
  --log-stream-name <job-run-id>
```

#### Verify Lake Formation Setup

```bash
# Check data location registration
aws lakeformation list-resources

# Check permissions
aws lakeformation list-permissions \
  --principal DataLakePrincipalIdentifier=arn:aws:iam::ACCOUNT:role/GlueServiceRole
```

#### Test Queries

```sql
-- In Athena, test each component

-- 1. Test source table
SELECT COUNT(*) FROM iceberg_db.entity_data;

-- 2. Test pivot query directly
SELECT 
    entity1,
    MAX(CASE WHEN key = 'key_set1_1001' THEN value END) AS key_set1_1001
FROM iceberg_db.entity_data
WHERE entity1 = 'entity1_set1'
GROUP BY entity1;

-- 3. Test view
SELECT COUNT(*) FROM iceberg_db.entity_view_entity1_set1;
```

## Cost Considerations

### Estimated Monthly Costs

**Small Dataset (< 1GB, < 1M rows):**
- S3 Storage: ~$0.50
- Glue Jobs: ~$5 (2 runs/month)
- Athena Queries: ~$1 (100 queries)
- **Total: ~$6.50/month**

**Medium Dataset (10GB, 10M rows):**
- S3 Storage: ~$5
- Glue Jobs: ~$20 (10 runs/month)
- Athena Queries: ~$10 (1000 queries)
- **Total: ~$35/month**

**Large Dataset (100GB, 100M rows):**
- S3 Storage: ~$50
- Glue Jobs: ~$100 (20 runs/month)
- Athena Queries: ~$50 (5000 queries)
- **Total: ~$200/month**

### Cost Optimization

1. **Use S3 Lifecycle Policies**
   ```hcl
   # In terraform/main.tf
   lifecycle_rule {
     enabled = true
     transition {
       days          = 30
       storage_class = "STANDARD_IA"
     }
   }
   ```

2. **Partition Iceberg Tables**
   ```python
   # In glue_csv_to_iceberg.py
   df.writeTo(full_table_name) \
       .partitionedBy("entity1") \
       .using("iceberg") \
       .createOrReplace()
   ```

3. **Use Athena Query Result Reuse**
   - Enable in Athena workgroup settings
   - Saves on repeated queries

4. **Schedule Glue Jobs During Off-Peak**
   - Use CloudWatch Events for scheduling
   - Run during low-cost hours

## Advanced Topics

### Custom View Logic

Edit `scripts/glue_create_views_dual_engine.py` to customize view SQL:

```python
# Add custom columns
pivot_columns.append("CURRENT_TIMESTAMP AS created_at")

# Add custom filters
view_sql = f"""
SELECT ...
FROM ...
WHERE entity1 = '{entity1_value}'
  AND entity2 IS NOT NULL
GROUP BY ...
"""
```

### Incremental Updates

Modify ingestion to support incremental loads:

```python
# In glue_csv_to_iceberg.py
df.writeTo(full_table_name) \
    .using("iceberg") \
    .option("merge-schema", "true") \
    .append()  # Instead of createOrReplace()
```

### Multiple Environments

Use Terraform workspaces:

```bash
# Create dev environment
terraform workspace new dev
terraform apply -var-file=dev.tfvars

# Create prod environment
terraform workspace new prod
terraform apply -var-file=prod.tfvars
```

## File Structure

```
mydatahub/
├── README.md                          # This file
├── terraform/
│   ├── main.tf                        # Main infrastructure
│   ├── lakeformation.tf               # Lake Formation setup
│   ├── views_dual_engine_job.tf       # View creation job
│   ├── terraform.tfvars               # Configuration
│   └── variables.tf                   # Variable definitions
├── scripts/
│   ├── glue_csv_to_iceberg.py         # Data ingestion
│   └── glue_create_views_dual_engine.py  # View creation
└── docs/
    ├── DUAL-ENGINE-SOLUTION.md        # Detailed view solution
    └── troubleshooting-lakeformation.md  # Lake Formation guide
```

## Support and Documentation

### Key Documentation Files

- **DUAL-ENGINE-SOLUTION.md** - Complete view creation guide
- **terraform/README.md** - Terraform-specific documentation
- **docs/lake-formation-permissions.md** - Lake Formation setup

### Getting Help

1. Check CloudWatch Logs for Glue jobs
2. Review Athena query history for errors
3. Verify Lake Formation permissions
4. Check S3 bucket policies

## Summary

This solution provides a complete, production-ready data lake with:

✅ **Iceberg Tables** - ACID transactions, time travel, schema evolution
✅ **Dynamic Views** - Automatically generated from data
✅ **Dual-Engine Support** - Works in both Athena and Glue Spark
✅ **Lake Formation** - Centralized permissions management
✅ **Infrastructure as Code** - Fully automated with Terraform
✅ **Cost Optimized** - Pay only for what you use

---

**Version:** 1.0
**Last Updated:** February 8, 2026
**Status:** Production Ready ✅
