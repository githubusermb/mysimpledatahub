#!/bin/bash

# Script to recreate views with correct Glue 5.0 syntax
# This script:
# 1. Drops old views with dialect errors
# 2. Uploads updated script to S3
# 3. Runs Glue job to recreate views
# 4. Verifies views are in catalog

set -e  # Exit on error

# Configuration
DATABASE_NAME="iceberg_db"
VIEW_PREFIX="entity_view"
GLUE_JOB_NAME="create-dynamic-entity-views"
S3_BUCKET="iceberg-data-storage-bucket"
SCRIPT_PATH="scripts/glue_create_dynamic_views.py"

echo "=========================================="
echo "Recreating Views with Glue 5.0 Syntax"
echo "=========================================="
echo ""

# Step 1: List existing views
echo "Step 1: Checking for existing views..."
VIEWS=$(aws glue get-tables --database-name $DATABASE_NAME \
  --query "TableList[?starts_with(Name, '$VIEW_PREFIX')].Name" \
  --output text)

if [ -z "$VIEWS" ]; then
  echo "No existing views found."
else
  echo "Found views:"
  echo "$VIEWS"
  echo ""
  
  # Step 2: Drop existing views
  echo "Step 2: Dropping existing views..."
  for view in $VIEWS; do
    echo "  Dropping: $view"
    aws glue delete-table --database-name $DATABASE_NAME --name $view || true
  done
  echo "All views dropped."
fi

echo ""

# Step 3: Upload updated script
echo "Step 3: Uploading updated script to S3..."
if [ -f "$SCRIPT_PATH" ]; then
  aws s3 cp $SCRIPT_PATH s3://$S3_BUCKET/scripts/
  echo "Script uploaded successfully."
else
  echo "ERROR: Script not found at $SCRIPT_PATH"
  exit 1
fi

echo ""

# Step 4: Run Glue job
echo "Step 4: Starting Glue job..."
JOB_RUN_ID=$(aws glue start-job-run --job-name $GLUE_JOB_NAME \
  --query 'JobRunId' --output text)

echo "Job started with ID: $JOB_RUN_ID"
echo "Waiting for job to complete..."

# Wait for job to complete
while true; do
  JOB_STATE=$(aws glue get-job-run --job-name $GLUE_JOB_NAME --run-id $JOB_RUN_ID \
    --query 'JobRun.JobRunState' --output text)
  
  if [ "$JOB_STATE" == "SUCCEEDED" ]; then
    echo "Job completed successfully!"
    break
  elif [ "$JOB_STATE" == "FAILED" ] || [ "$JOB_STATE" == "STOPPED" ] || [ "$JOB_STATE" == "ERROR" ]; then
    echo "Job failed with state: $JOB_STATE"
    echo "Check logs for details:"
    echo "aws logs tail /aws-glue/jobs/error --follow"
    exit 1
  else
    echo "Job state: $JOB_STATE (waiting...)"
    sleep 10
  fi
done

echo ""

# Step 5: Verify views
echo "Step 5: Verifying views in Glue Data Catalog..."
NEW_VIEWS=$(aws glue get-tables --database-name $DATABASE_NAME \
  --query "TableList[?starts_with(Name, '$VIEW_PREFIX')].[Name,TableType]" \
  --output table)

if [ -z "$NEW_VIEWS" ]; then
  echo "WARNING: No views found in catalog!"
  echo "Check Glue job logs for errors."
  exit 1
else
  echo "Views created successfully:"
  echo "$NEW_VIEWS"
fi

echo ""

# Step 6: Get first view for testing
FIRST_VIEW=$(aws glue get-tables --database-name $DATABASE_NAME \
  --query "TableList[?starts_with(Name, '$VIEW_PREFIX')].Name | [0]" \
  --output text)

if [ ! -z "$FIRST_VIEW" ]; then
  echo "=========================================="
  echo "SUCCESS! Views are ready to use."
  echo "=========================================="
  echo ""
  echo "Test in Athena with this query:"
  echo ""
  echo "  SELECT * FROM $DATABASE_NAME.$FIRST_VIEW LIMIT 10;"
  echo ""
  echo "Or check view details:"
  echo ""
  echo "  aws glue get-table --database-name $DATABASE_NAME --name $FIRST_VIEW"
  echo ""
fi

echo "Done!"
