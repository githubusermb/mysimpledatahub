# Glue Job Triggers

## Overview

This document describes the triggers configured for AWS Glue jobs in the Simple Data Hub project.

## Trigger Configuration

### 1. CSV to Iceberg Ingestion Job

**Job Name**: `csv-to-iceberg-ingestion`

**Trigger Type**: S3 Event Notification via Lambda

**Trigger Flow**:
```
S3 Upload Event
    └─> Lambda Function (trigger-glue-job-lambda)
        └─> Starts Glue Job (csv-to-iceberg-ingestion)
```

**Details**:
- **S3 Bucket**: Raw data bucket (configured in terraform.tfvars)
- **S3 Prefix**: `collections-data/`
- **Event Type**: `s3:ObjectCreated:*`
- **Lambda Function**: `trigger-glue-job-lambda`
- **Trigger**: Automatic when CSV files are uploaded to S3

**Configuration** (in `main.tf`):
```hcl
resource "aws_s3_bucket_notification" "bucket_notification" {
  bucket = aws_s3_bucket.raw_data_bucket.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.trigger_glue_job.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = var.raw_data_prefix
  }
}
```

### 2. Create Views Dual-Engine Job

**Job Name**: `create-views-dual-engine`

**Trigger Type**: Conditional (Glue Trigger)

**Trigger Flow**:
```
csv-to-iceberg-ingestion Job
    └─> Completes Successfully (SUCCEEDED state)
        └─> Automatically Starts create-views-dual-engine Job
```

**Details**:
- **Trigger Name**: `dual-engine-views-trigger`
- **Trigger Type**: `CONDITIONAL`
- **Condition**: Previous job (`csv-to-iceberg-ingestion`) must complete with `SUCCEEDED` state
- **Trigger**: Automatic after successful data ingestion

**Configuration** (in `views_dual_engine_job.tf`):
```hcl
resource "aws_glue_trigger" "views_dual_engine_trigger" {
  name          = "dual-engine-views-trigger"
  type          = "CONDITIONAL"
  description   = "Trigger to create dual-engine views after data ingestion completes"
  
  predicate {
    conditions {
      job_name = aws_glue_job.csv_to_iceberg_job.name
      state    = "SUCCEEDED"
    }
  }
  
  actions {
    job_name = aws_glue_job.views_dual_engine_job.name
  }
}
```

## Complete Workflow

```
┌─────────────────────────────────────────────────────────────────┐
│  1. User uploads CSV file to S3                                 │
│     s3://bucket/collections-data/ingest_ts=<timestamp>/file.csv │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  2. S3 Event Notification triggers Lambda                       │
│     Function: trigger-glue-job-lambda                           │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  3. Lambda starts Glue Job                                      │
│     Job: csv-to-iceberg-ingestion                               │
│     - Reads CSV from S3                                         │
│     - Converts to Iceberg format                                │
│     - Writes to collections_data_staging table                  │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼ (on SUCCESS)
┌─────────────────────────────────────────────────────────────────┐
│  4. Glue Trigger activates                                      │
│     Trigger: dual-engine-views-trigger                          │
│     Condition: Previous job SUCCEEDED                           │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  5. Views Job starts automatically                              │
│     Job: create-views-dual-engine                               │
│     - Creates collections_data_view (unified view)              │
│     - Creates series-specific views (fry9c_report_view, etc.)   │
│     - Adds Athena dialect to all views                          │
└─────────────────────────────────────────────────────────────────┘
```

## Manual Execution

### Run CSV Ingestion Job Manually

```bash
aws glue start-job-run \
  --job-name csv-to-iceberg-ingestion \
  --region us-east-1
```

### Run Views Job Manually

```bash
aws glue start-job-run \
  --job-name create-views-dual-engine \
  --region us-east-1
```

### Disable Automatic Trigger

To disable the automatic trigger for the views job:

```bash
# Stop the trigger
aws glue stop-trigger --name dual-engine-views-trigger

# Or delete it
aws glue delete-trigger --name dual-engine-views-trigger
```

### Re-enable Trigger

```bash
aws glue start-trigger --name dual-engine-views-trigger
```

## Monitoring

### Check Trigger Status

```bash
# Get trigger details
aws glue get-trigger --name dual-engine-views-trigger

# List all triggers
aws glue get-triggers
```

### Check Job Run History

```bash
# Get runs for ingestion job
aws glue get-job-runs --job-name csv-to-iceberg-ingestion --max-results 10

# Get runs for views job
aws glue get-job-runs --job-name create-views-dual-engine --max-results 10
```

### CloudWatch Logs

Both jobs write logs to CloudWatch:
- Log Group: `/aws-glue/jobs/output`
- Log Stream: Job name + run ID

## Troubleshooting

### Views Job Not Triggering

**Problem**: Views job doesn't start after ingestion job completes.

**Possible Causes**:
1. Trigger is stopped or disabled
2. Ingestion job failed (trigger only fires on SUCCESS)
3. IAM permissions issue

**Solutions**:
```bash
# Check trigger state
aws glue get-trigger --name dual-engine-views-trigger

# Start trigger if stopped
aws glue start-trigger --name dual-engine-views-trigger

# Check last ingestion job status
aws glue get-job-runs --job-name csv-to-iceberg-ingestion --max-results 1
```

### Lambda Not Triggering Ingestion Job

**Problem**: CSV upload doesn't trigger the ingestion job.

**Possible Causes**:
1. S3 event notification not configured
2. Lambda function error
3. Wrong S3 prefix

**Solutions**:
```bash
# Check S3 event notifications
aws s3api get-bucket-notification-configuration \
  --bucket your-bucket-name

# Check Lambda logs
aws logs tail /aws/lambda/trigger-glue-job-lambda --follow

# Test Lambda manually
aws lambda invoke \
  --function-name trigger-glue-job-lambda \
  --payload '{"Records":[{"s3":{"bucket":{"name":"test"},"object":{"key":"test.csv"}}}]}' \
  response.json
```

## Best Practices

1. **Monitor Job Runs**: Set up CloudWatch alarms for job failures
2. **Test Triggers**: Test the complete workflow after deployment
3. **Error Handling**: Ensure jobs have proper error handling and retry logic
4. **Concurrent Runs**: Both jobs are configured with `max_concurrent_runs = 1` to prevent conflicts
5. **Timeout Settings**: Adjust timeout values based on data volume

## Related Files

- `terraform/main.tf` - Lambda and S3 event notification configuration
- `terraform/views_dual_engine_job.tf` - Views job and trigger configuration
- `jobs/glue_csv_to_iceberg.py` - Ingestion job script
- `jobs/glue_create_views_dual_engine.py` - Views creation job script

---

**Last Updated**: 2026-02-16
