
#!/bin/bash
# Script to upload data to S3 and run the AWS Glue job

set -e

# Default values
OUTPUT_DIR="/tmp/data"
TIMESTAMP=$(date +%s)
RAW_BUCKET=""
RAW_PREFIX="collections-data/"
GLUE_JOB=""

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  key="$1"
  case $key in
    --output-dir)
      OUTPUT_DIR="$2"
      shift
      shift
      ;;
    --timestamp)
      TIMESTAMP="$2"
      shift
      shift
      ;;
    --bucket)
      RAW_BUCKET="$2"
      shift
      shift
      ;;
    --prefix)
      RAW_PREFIX="$2"
      shift
      shift
      ;;
    --job)
      GLUE_JOB="$2"
      shift
      shift
      ;;
    --generate)
      GENERATE_DATA=true
      shift
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

# Validate required parameters
if [ -z "$RAW_BUCKET" ]; then
  echo "Error: S3 bucket name is required (--bucket)"
  exit 1
fi

if [ -z "$GLUE_JOB" ]; then
  echo "Error: Glue job name is required (--job)"
  exit 1
fi

echo "=== AWS Data Ingestion Pipeline Execution ==="
echo "S3 Bucket: $RAW_BUCKET"
echo "S3 Prefix: $RAW_PREFIX"
echo "Glue Job: $GLUE_JOB"
echo "Timestamp: $TIMESTAMP"
echo "Data directory: $OUTPUT_DIR"

# Generate sample data if requested
if [ "$GENERATE_DATA" = true ]; then
  echo -e "\n=== Generating sample CSV data ==="
  python3 $(dirname "$0")/generate_sample_csv.py --output-dir "$OUTPUT_DIR" --timestamp "$TIMESTAMP"
fi

# Check if the data directory exists
if [ ! -d "$OUTPUT_DIR/ingest_ts=$TIMESTAMP" ]; then
  echo "Error: Data directory not found: $OUTPUT_DIR/ingest_ts=$TIMESTAMP"
  exit 1
fi

# Step 1: Upload data to S3
echo -e "\n=== Step 1: Uploading data to S3 ==="
S3_PATH="s3://$RAW_BUCKET/$RAW_PREFIX"
echo "Uploading data to: $S3_PATH/ingest_ts=$TIMESTAMP/"

# Check if AWS CLI is installed
if ! command -v aws &> /dev/null; then
  echo "Error: AWS CLI is not installed"
  exit 1
fi

# Upload data to S3
aws s3 cp "$OUTPUT_DIR/ingest_ts=$TIMESTAMP/" "$S3_PATH/ingest_ts=$TIMESTAMP/" --recursive

# Step 2: Run the AWS Glue job
echo -e "\n=== Step 2: Running AWS Glue job ==="
echo "Starting Glue job: $GLUE_JOB"

# Run the Glue job
JOB_RUN_ID=$(aws glue start-job-run --job-name "$GLUE_JOB" --query 'JobRunId' --output text)
echo "Glue job started with run ID: $JOB_RUN_ID"

# Wait for the job to complete
echo "Waiting for job to complete..."
aws glue wait job-run-complete --job-name "$GLUE_JOB" --run-id "$JOB_RUN_ID"

# Check job status
JOB_STATUS=$(aws glue get-job-run --job-name "$GLUE_JOB" --run-id "$JOB_RUN_ID" --query 'JobRun.JobRunState' --output text)
echo "Job completed with status: $JOB_STATUS"

if [ "$JOB_STATUS" = "SUCCEEDED" ]; then
  echo -e "\n=== Success! ==="
  echo "Data has been successfully processed and loaded into the Iceberg table."
  echo "You can now query the data using AWS Athena or other compatible services."
else
  echo -e "\n=== Error! ==="
  echo "The Glue job did not complete successfully. Check the CloudWatch logs for details."
  exit 1
fi

