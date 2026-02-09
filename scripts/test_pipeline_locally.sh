#!/bin/bash
# Script to test the data ingestion pipeline locally
# This script generates sample CSV data and simulates the AWS Glue job using Docker

set -e

# Default values
OUTPUT_DIR="/tmp/data"
TIMESTAMP=$(date +%s)
DOCKER_IMAGE="apache/spark-py:3.3.1"

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
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

echo "=== AWS Data Ingestion Pipeline Local Test ==="
echo "Output directory: $OUTPUT_DIR"
echo "Timestamp: $TIMESTAMP"

# Create output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

# Step 1: Generate sample CSV data
echo -e "\n=== Step 1: Generating sample CSV data ==="
python3 $(dirname "$0")/generate_sample_csv.py --output-dir "$OUTPUT_DIR" --timestamp "$TIMESTAMP"

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
  echo "Docker is not installed. Please install Docker to run the PySpark simulation."
  exit 1
fi

# Step 2: Simulate AWS Glue job using Docker with PySpark
echo -e "\n=== Step 2: Simulating AWS Glue job with PySpark ==="
echo "Pulling Docker image: $DOCKER_IMAGE"
docker pull "$DOCKER_IMAGE"

# Create a simple PySpark script to simulate the Glue job
TEMP_SCRIPT=$(mktemp)
cat > "$TEMP_SCRIPT" << EOF
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

# Initialize Spark session
# Note: spark.sql.extensions is a static config that can only be set during initialization
spark = SparkSession.builder \
    .appName("CSV to Iceberg Simulation") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .getOrCreate()

# Define input and output paths
input_path = "$OUTPUT_DIR/ingest_ts=$TIMESTAMP"
output_path = "$OUTPUT_DIR/iceberg_output"

# Read CSV data with header
print(f"Reading CSV data from: {input_path}")
csv_df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)

# Print inferred schema
print("Inferred schema:")
csv_df.printSchema()

# Print sample data
print("Sample data:")
csv_df.show(5)

# Write data in Parquet format (simulating Iceberg)
print(f"Writing data to: {output_path}")
csv_df.write.mode("overwrite").parquet(output_path)

# Print record count
record_count = csv_df.count()
print(f"Processed {record_count} records")

spark.stop()
EOF

# Run the PySpark script in Docker
echo "Running PySpark simulation..."
docker run --rm -v "$OUTPUT_DIR:$OUTPUT_DIR" -v "$TEMP_SCRIPT:/app/script.py" "$DOCKER_IMAGE" \
  /opt/spark/bin/spark-submit /app/script.py

# Clean up temporary script
rm "$TEMP_SCRIPT"

# Step 3: Verify the output
echo -e "\n=== Step 3: Verifying output ==="
if [ -d "$OUTPUT_DIR/iceberg_output" ]; then
  echo "Output directory exists: $OUTPUT_DIR/iceberg_output"
  echo "Files in output directory:"
  ls -la "$OUTPUT_DIR/iceberg_output"
  echo -e "\nTest completed successfully!"
else
  echo "Error: Output directory not found: $OUTPUT_DIR/iceberg_output"
  exit 1
fi

echo -e "\n=== Summary ==="
echo "Sample CSV data: $OUTPUT_DIR/ingest_ts=$TIMESTAMP"
echo "Parquet output data: $OUTPUT_DIR/iceberg_output"
echo -e "\nNext steps:"
echo "1. Deploy the AWS infrastructure using Terraform"
echo "2. Upload the CSV data to S3"
echo "3. Run the AWS Glue job"
echo "4. Query the data using AWS Athena"
