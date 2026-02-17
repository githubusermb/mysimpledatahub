# Glue Jobs

This folder contains AWS Glue ETL job scripts for the data pipeline.

## Job Scripts

### 1. glue_csv_to_iceberg.py

**Purpose**: Ingests CSV data from S3 and creates/updates Iceberg tables in the Glue Data Catalog.

**Glue Job Name**: `csv-to-iceberg-ingestion`

**Parameters**:
- `raw_data_bucket`: S3 bucket containing raw CSV files
- `raw_data_prefix`: S3 prefix for raw data (e.g., `collections-data/`)
- `database_name`: Glue database name (e.g., `collections_db`)
- `table_name`: Target table name (e.g., `collections_data_staging`)
- `iceberg_data_bucket`: S3 bucket for Iceberg data storage
- `iceberg_data_prefix`: S3 prefix for Iceberg data (e.g., `iceberg-data/`)
- `catalog_id`: AWS account ID
- `aws_region`: AWS region (e.g., `us-east-1`)

**Features**:
- ✅ Validates CSV files before processing
- ✅ Extracts `ingest_timestamp` from S3 path
- ✅ Creates partitioned Iceberg tables (by `seriesid` and `ingest_timestamp`)
- ✅ Supports both table creation and data insertion
- ✅ Registers tables with Lake Formation
- ✅ ACID transaction support via Iceberg

**Input Format**:
```
s3://bucket/collections-data/ingest_ts=<timestamp>/file.csv
```

**Output**:
```
Iceberg Table: collections_data_staging
Partitions: seriesid=<id>/ingest_timestamp=<ts>/
```

**Usage**:
```bash
aws glue start-job-run --job-name csv-to-iceberg-ingestion
```

---

### 2. glue_create_views_dual_engine.py

**Purpose**: Creates multi-dialect views that work in both Athena and Glue Spark.

**Glue Job Name**: `create-views-dual-engine`

**Parameters**:
- `database_name`: Glue database name (e.g., `collections_db`)
- `source_table_name`: Source table name (e.g., `collections_data_staging`)
- `athena_output_location`: S3 location for Athena query results
- `aws_region`: AWS region (e.g., `us-east-1`)

**Features**:
- ✅ Creates `collections_data_view` (unified view of all data)
- ✅ Creates series-specific report views (e.g., `fry9c_report_view`)
- ✅ Pivots key-value pairs into columns
- ✅ Multi-dialect support (Spark SQL + Athena/Presto SQL)
- ✅ Automatic view recreation if exists
- ✅ Verifies views in both engines

**Views Created**:

1. **collections_data_view**
   - Same schema as source table
   - Provides unified access to all data
   - Multi-dialect (Athena + Spark)

2. **<seriesid>_report_view** (one per series)
   - Pivoted format (key-value → columns)
   - Example: `fry9c_report_view`, `fry15_report_view`
   - Multi-dialect (Athena + Spark)

**Usage**:
```bash
aws glue start-job-run --job-name create-views-dual-engine
```

**Query Examples**:
```sql
-- Athena
SELECT * FROM collections_db.collections_data_view;
SELECT * FROM collections_db.fry9c_report_view;

-- Glue Spark
spark.sql("SELECT * FROM glue_catalog.collections_db.collections_data_view")
spark.sql("SELECT * FROM glue_catalog.collections_db.fry9c_report_view")
```

---

## Job Dependencies

```
glue_csv_to_iceberg.py
    ↓ (creates table)
collections_data_staging
    ↓ (triggers)
glue_create_views_dual_engine.py
    ↓ (creates views)
collections_data_view + <seriesid>_report_view
```

---

## Deployment

Jobs are deployed via Terraform:

```bash
cd terraform

# Upload scripts to S3 and create/update Glue jobs
terraform apply

# Verify jobs are created
aws glue list-jobs --query 'JobNames' --output table
```

---

## Local Development

### Testing Scripts Locally

While these scripts are designed to run in AWS Glue, you can test the logic locally:

```bash
# Install dependencies
pip install pyspark boto3 awsglue-local

# Set AWS credentials
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
export AWS_DEFAULT_REGION=us-east-1

# Run with spark-submit (requires local Spark installation)
spark-submit \
  --jars iceberg-spark-runtime.jar \
  jobs/glue_csv_to_iceberg.py \
  --JOB_NAME test \
  --raw_data_bucket my-bucket \
  --raw_data_prefix collections-data/ \
  ...
```

### Linting

```bash
# Install pylint
pip install pylint

# Lint scripts
pylint jobs/glue_csv_to_iceberg.py
pylint jobs/glue_create_views_dual_engine.py
```

---

## Monitoring

### CloudWatch Logs

```bash
# View output logs
aws logs tail /aws-glue/jobs/output --follow

# View error logs
aws logs tail /aws-glue/jobs/error --follow

# View specific job run
aws logs get-log-events \
  --log-group-name /aws-glue/jobs/output \
  --log-stream-name <job-run-id>
```

### Job Status

```bash
# Check recent job runs
aws glue get-job-runs \
  --job-name csv-to-iceberg-ingestion \
  --max-results 5

# Get specific job run details
aws glue get-job-run \
  --job-name csv-to-iceberg-ingestion \
  --run-id jr_xxxxx
```

---

## Troubleshooting

### Common Issues

#### 1. Script Not Found
```
Error: Script not found in S3
```

**Solution**:
```bash
cd terraform
terraform apply  # Re-upload scripts
```

#### 2. Permission Denied
```
Error: Access Denied when accessing S3/Glue
```

**Solution**: Check IAM role permissions in `terraform/main.tf`

#### 3. Iceberg Dependencies Missing
```
Error: ClassNotFoundException: org.apache.iceberg...
```

**Solution**: Ensure Iceberg JARs are uploaded to S3:
```bash
aws s3 ls s3://your-bucket/jars/
# Should see: iceberg-spark-runtime-*.jar, iceberg-aws-*.jar, etc.
```

#### 4. View Creation Fails
```
Error: ALTER VIEW ADD DIALECT failed
```

**Solution**: Check Athena permissions and output location

---

## File Structure

```
mysimpledatahub/
├── jobs/                           # Glue job scripts (this folder)
│   ├── README.md                   # This file
│   ├── glue_csv_to_iceberg.py      # Data ingestion job
│   └── glue_create_views_dual_engine.py  # View creation job
│
├── scripts/                        # Helper scripts (not Glue jobs)
│   ├── generate_sample_csv.py
│   ├── setup_lakeformation_complete.py
│   ├── grant_athena_user_permissions.py
│   └── ...
│
└── terraform/                      # Infrastructure as Code
    ├── main.tf                     # Main Glue job configuration
    ├── views_dual_engine_job.tf    # Views job configuration
    └── ...
```

---

## Best Practices

### 1. Version Control
- ✅ Keep job scripts in version control
- ✅ Use meaningful commit messages
- ✅ Tag releases for production deployments

### 2. Testing
- ✅ Test with sample data before production
- ✅ Verify views in both Athena and Spark
- ✅ Check partition pruning works correctly

### 3. Monitoring
- ✅ Set up CloudWatch alarms for job failures
- ✅ Monitor job duration and costs
- ✅ Review logs regularly

### 4. Error Handling
- ✅ Jobs include comprehensive error logging
- ✅ Failed jobs don't leave partial data
- ✅ Idempotent operations (can re-run safely)

---

## Related Documentation

- **Architecture**: See `../ARCHITECTURE.md`
- **Entity Diagrams**: See `../ENTITY-DIAGRAMS.md`
- **Naming Conventions**: See `../docs/naming-conventions-best-practices.md`
- **Updates**: See `../UPDATES-REGION-AND-VIEWS.md`

---

**Last Updated**: February 11, 2026  
**Status**: Production Ready ✅
