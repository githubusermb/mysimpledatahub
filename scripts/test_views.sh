#!/bin/bash

# Test Views in Spark - Quick Test Script
# This script runs the Glue job to test views in Spark

set -e

echo "=========================================="
echo "Testing Views in Glue Spark"
echo "=========================================="
echo ""

# Configuration
JOB_NAME="test-entity-views-spark"
DATABASE_NAME="iceberg_db"
VIEW_PREFIX="entity_view"

# Check if views exist
echo "Step 1: Checking if views exist..."
VIEWS=$(aws glue get-tables --database-name $DATABASE_NAME \
  --query "TableList[?TableType=='VIRTUAL_VIEW' && starts_with(Name, '$VIEW_PREFIX')].Name" \
  --output text)

if [ -z "$VIEWS" ]; then
  echo "❌ No views found with prefix '$VIEW_PREFIX' in database '$DATABASE_NAME'"
  echo ""
  echo "Please run the view creation job first:"
  echo "  aws glue start-job-run --job-name create-dynamic-entity-views"
  exit 1
fi

VIEW_COUNT=$(echo $VIEWS | wc -w)
echo "✓ Found $VIEW_COUNT views to test"
echo "  Views: $VIEWS"
echo ""

# Run the test job
echo "Step 2: Starting test job..."
RUN_OUTPUT=$(aws glue start-job-run --job-name $JOB_NAME)
JOB_RUN_ID=$(echo $RUN_OUTPUT | grep -o '"JobRunId": "[^"]*"' | cut -d'"' -f4)

if [ -z "$JOB_RUN_ID" ]; then
  echo "❌ Failed to start job"
  exit 1
fi

echo "✓ Job started successfully"
echo "  Job Run ID: $JOB_RUN_ID"
echo ""

# Wait for job to complete
echo "Step 3: Waiting for job to complete..."
echo "(This usually takes 2-3 minutes)"
echo ""

MAX_WAIT=300  # 5 minutes
WAIT_TIME=0
SLEEP_INTERVAL=10

while [ $WAIT_TIME -lt $MAX_WAIT ]; do
  STATUS=$(aws glue get-job-run \
    --job-name $JOB_NAME \
    --run-id $JOB_RUN_ID \
    --query 'JobRun.JobRunState' \
    --output text)
  
  if [ "$STATUS" = "SUCCEEDED" ]; then
    echo "✓ Job completed successfully!"
    echo ""
    break
  elif [ "$STATUS" = "FAILED" ] || [ "$STATUS" = "STOPPED" ] || [ "$STATUS" = "TIMEOUT" ]; then
    echo "❌ Job $STATUS"
    echo ""
    ERROR=$(aws glue get-job-run \
      --job-name $JOB_NAME \
      --run-id $JOB_RUN_ID \
      --query 'JobRun.ErrorMessage' \
      --output text)
    if [ "$ERROR" != "None" ]; then
      echo "Error: $ERROR"
    fi
    echo ""
    echo "Check logs for details:"
    echo "  aws logs tail /aws-glue/jobs/output --follow"
    exit 1
  else
    echo "  Status: $STATUS (waited ${WAIT_TIME}s)"
    sleep $SLEEP_INTERVAL
    WAIT_TIME=$((WAIT_TIME + SLEEP_INTERVAL))
  fi
done

if [ $WAIT_TIME -ge $MAX_WAIT ]; then
  echo "❌ Job timed out after ${MAX_WAIT}s"
  exit 1
fi

# Get job results from logs
echo "Step 4: Fetching test results..."
echo ""

# Wait a bit for logs to be available
sleep 5

# Get the latest log stream
LOG_GROUP="/aws-glue/jobs/output"
LOG_STREAM=$(aws logs describe-log-streams \
  --log-group-name $LOG_GROUP \
  --order-by LastEventTime \
  --descending \
  --max-items 1 \
  --query 'logStreams[0].logStreamName' \
  --output text 2>/dev/null || echo "")

if [ -n "$LOG_STREAM" ] && [ "$LOG_STREAM" != "None" ]; then
  echo "Fetching logs from: $LOG_STREAM"
  echo ""
  
  # Get logs and filter for test summary
  aws logs get-log-events \
    --log-group-name $LOG_GROUP \
    --log-stream-name "$LOG_STREAM" \
    --query 'events[*].message' \
    --output text | grep -A 20 "TEST SUMMARY" || echo "Could not find test summary in logs"
else
  echo "⚠ Could not fetch logs automatically"
  echo ""
  echo "View logs manually:"
  echo "  aws logs tail /aws-glue/jobs/output --follow"
fi

echo ""
echo "=========================================="
echo "Test Complete"
echo "=========================================="
echo ""
echo "To view full logs:"
echo "  aws logs tail /aws-glue/jobs/output --follow"
echo ""
echo "To view in AWS Console:"
echo "  https://console.aws.amazon.com/glue/home#/v2/etl-jobs/view/$JOB_NAME"
echo ""
