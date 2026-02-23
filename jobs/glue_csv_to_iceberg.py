
"""
AWS Glue PySpark job to infer schema from CSV files and create/update an Iceberg table.

This script:
1. Reads CSV files from a specified S3 location
2. Infers the schema from the CSV data
3. Creates an Iceberg table if it doesn't exist
4. Loads the CSV data into the Iceberg table
"""

import sys
import os
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp

# Get job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'raw_data_bucket',
    'raw_data_prefix',
    'database_name',
    'table_name',
    'iceberg_data_bucket',
    'iceberg_data_prefix',
    'catalog_id',
    'aws_region'
])

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Note: Spark configuration for Hive metastore is now set via the job parameters in Terraform
# These configurations are passed via --conf in the Glue job definition

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Set job parameters
raw_data_bucket = args['raw_data_bucket']
raw_data_prefix = args['raw_data_prefix']
database_name = args['database_name']
table_name = args['table_name']  # Should be 'collections_data_tbl'
iceberg_data_bucket = args['iceberg_data_bucket']
iceberg_data_prefix = args['iceberg_data_prefix']
catalog_id = args['catalog_id']
aws_region = args['aws_region']

print(f"AWS Region: {aws_region}")

# Spark configuration for Iceberg is now set via the job parameters in Terraform
# These configurations are passed via --conf in the Glue job definition
# See the terraform/main.tf file for the complete configuration

# Define S3 input path
input_path = f"s3://{raw_data_bucket}/{raw_data_prefix}"
print(f"Reading CSV data from: {input_path}")

# Import required libraries
import boto3
import io
import csv
import pandas as pd
import hashlib
import time
from urllib.parse import urlparse
from pyspark.sql.types import StructType, StructField, StringType

def validate_and_read_s3_files(s3_path):
    """
    Custom function to validate and read S3 files with explicit checksum validation
    Returns a list of valid file paths that can be processed by Spark
    """
    parsed_url = urlparse(s3_path)
    bucket_name = parsed_url.netloc
    prefix = parsed_url.path.lstrip('/')
    
    s3_client = boto3.client('s3')
    
    # List objects in the bucket with the given prefix
    response = s3_client.list_objects_v2(
        Bucket=bucket_name,
        Prefix=prefix
    )
    
    # Check if there are any objects
    if 'Contents' not in response:
        print(f"No files found in {s3_path}")
        return []
    
    # Filter for CSV files that are not empty and not directories
    csv_files = [
        obj for obj in response['Contents'] 
        if obj['Size'] > 0 and 
        not obj['Key'].endswith('/') and 
        obj['Key'].lower().endswith('.csv')
    ]
    
    if not csv_files:
        print(f"No valid CSV files found in {s3_path}")
        return []
    
    print(f"Found {len(csv_files)} potential CSV files in {s3_path}")
    
    # Validate each file by reading a small sample and checking integrity
    valid_files = []
    for file_obj in csv_files:
        file_key = file_obj['Key']
        file_path = f"s3://{bucket_name}/{file_key}"
        
        try:
            # Get file metadata including ETag (checksum)
            file_metadata = s3_client.head_object(
                Bucket=bucket_name,
                Key=file_key
            )
            
            # Read a small sample to validate file integrity
            response = s3_client.get_object(
                Bucket=bucket_name,
                Key=file_key
            )
            
            # Read first few bytes to check if it's a valid CSV
            sample = response['Body'].read(1024)
            
            # Check if sample contains valid CSV content
            if not sample or not is_valid_csv(sample):
                print(f"File {file_path} is not a valid CSV file. Skipping.")
                continue
                
            # Calculate MD5 hash of the sample for validation
            sample_md5 = hashlib.md5(sample).hexdigest()
            print(f"File {file_path} passed validation with sample MD5: {sample_md5}")
            
            valid_files.append(file_path)
        except Exception as e:
            print(f"Error validating file {file_path}: {str(e)}")
            continue
    
    print(f"Found {len(valid_files)} valid CSV files after validation")
    return valid_files

def is_valid_csv(sample_bytes):
    """Check if the sample bytes represent a valid CSV file"""
    try:
        # Try to decode as UTF-8
        sample_str = sample_bytes.decode('utf-8')
        
        # Check if it has commas or other CSV delimiters
        if ',' not in sample_str and ';' not in sample_str and '\t' not in sample_str:
            return False
            
        # Try to parse as CSV
        reader = csv.reader(io.StringIO(sample_str))
        rows = list(reader)
        
        # Valid CSV should have at least one row with content
        return len(rows) > 0 and len(rows[0]) > 0
    except Exception:
        return False

def register_table_with_lake_formation(catalog_id, database_name, table_name, bucket, prefix, region):
    """Register Iceberg table with Lake Formation to enable multi-dialect views"""
    try:
        lakeformation_client = boto3.client('lakeformation', region_name=region)
        glue_client = boto3.client('glue', region_name=region)
        
        # Get table location from Glue catalog
        try:
            table_response = glue_client.get_table(
                CatalogId=catalog_id,
                DatabaseName=database_name,
                Name=table_name
            )
            table_location = table_response['Table'].get('StorageDescriptor', {}).get('Location', f"s3://{bucket}/{prefix}/{table_name}")
        except Exception as e:
            print(f"Could not get table location from Glue, using default: {str(e)}")
            table_location = f"s3://{bucket}/{prefix}/{table_name}"
        
        print(f"Registering table {database_name}.{table_name} with Lake Formation at location: {table_location}")
        
        # Register the resource with Lake Formation
        try:
            lakeformation_client.register_resource(
                ResourceArn=f"arn:aws:s3:::{bucket}/{prefix}/{table_name}",
                UseServiceLinkedRole=True
            )
            print(f"Successfully registered S3 location with Lake Formation")
        except lakeformation_client.exceptions.AlreadyExistsException:
            print(f"S3 location already registered with Lake Formation")
        except Exception as e:
            print(f"Note: Could not register S3 location (may already be registered): {str(e)}")
        
        # Update table to be Lake Formation managed
        try:
            # Get current table definition
            table_response = glue_client.get_table(
                CatalogId=catalog_id,
                DatabaseName=database_name,
                Name=table_name
            )
            
            table_input = table_response['Table']
            
            # Remove all read-only fields that cannot be included in TableInput
            # This list includes all known read-only fields from AWS Glue API
            read_only_fields = [
                'DatabaseName', 'CreateTime', 'UpdateTime', 'CreatedBy', 
                'IsRegisteredWithLakeFormation', 'CatalogId', 'VersionId',
                'IsMultiDialectView', 'FederatedTable', 'ViewDefinition'
            ]
            
            for field in read_only_fields:
                table_input.pop(field, None)
            
            # Ensure StorageDescriptor exists and has InputFormat
            if 'StorageDescriptor' not in table_input or not table_input['StorageDescriptor']:
                print("WARNING: StorageDescriptor is missing. This should not happen for Iceberg tables.")
            else:
                # Ensure InputFormat and OutputFormat are set in StorageDescriptor
                if 'InputFormat' not in table_input['StorageDescriptor'] or not table_input['StorageDescriptor']['InputFormat']:
                    print("WARNING: InputFormat is missing in StorageDescriptor. Setting it now...")
                    table_input['StorageDescriptor']['InputFormat'] = 'org.apache.iceberg.mr.hive.HiveIcebergInputFormat'
                
                if 'OutputFormat' not in table_input['StorageDescriptor'] or not table_input['StorageDescriptor']['OutputFormat']:
                    print("WARNING: OutputFormat is missing in StorageDescriptor. Setting it now...")
                    table_input['StorageDescriptor']['OutputFormat'] = 'org.apache.iceberg.mr.hive.HiveIcebergOutputFormat'
                
                # Ensure SerdeInfo exists
                if 'SerdeInfo' not in table_input['StorageDescriptor'] or not table_input['StorageDescriptor']['SerdeInfo']:
                    print("WARNING: SerdeInfo is missing. Setting it now...")
                    table_input['StorageDescriptor']['SerdeInfo'] = {
                        'SerializationLibrary': 'org.apache.iceberg.mr.hive.HiveIcebergSerDe',
                        'Parameters': {}
                    }
            
            # Ensure Parameters exist
            if 'Parameters' not in table_input:
                table_input['Parameters'] = {}
            
            # Mark as Lake Formation managed
            table_input['Parameters']['GOVERNED'] = 'true'
            
            # Update the table
            glue_client.update_table(
                CatalogId=catalog_id,
                DatabaseName=database_name,
                TableInput=table_input
            )
            print(f"Successfully marked table as Lake Formation managed")
            
        except Exception as e:
            print(f"Error updating table to be Lake Formation managed: {str(e)}")
            raise
        
        print(f"Table {database_name}.{table_name} is now registered with Lake Formation")
        
    except Exception as e:
        print(f"Error registering table with Lake Formation: {str(e)}")
        print("This may require additional IAM permissions:")
        print("  - lakeformation:RegisterResource")
        print("  - lakeformation:GrantPermissions")
        print("  - glue:UpdateTable")
        raise

def create_dataframe_from_validated_files(spark, valid_files):
    """Create a DataFrame from validated files with retry logic"""
    max_retries = 5
    retry_delay = 5  # seconds
    
    for attempt in range(max_retries):
        try:
            if not valid_files:
                raise Exception("No valid files to process")
                
            # Read the validated files with proper CSV options
            df = spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .option("quote", "\"") \
                .option("escape", "\"") \
                .option("multiLine", "true") \
                .option("mode", "DROPMALFORMED") \
                .csv(valid_files)
            
            # Force evaluation to check for errors
            count = df.count()
            if count == 0:
                raise Exception("DataFrame is empty after reading files")
            
            # Validate that seriesid column exists and has valid data
            if "seriesid" in df.columns:
                invalid_count = df.filter(
                    (col("seriesid").isNull()) | 
                    (col("seriesid") == "") |
                    (~col("seriesid").rlike("^(FRY9C|FRY15|FR2004A)"))
                ).count()
                
                if invalid_count > 0:
                    print(f"WARNING: Found {invalid_count} rows with invalid seriesid values")
                    print("Sample invalid rows:")
                    df.filter(
                        (col("seriesid").isNull()) | 
                        (col("seriesid") == "") |
                        (~col("seriesid").rlike("^(FRY9C|FRY15|FR2004A)"))
                    ).show(5, truncate=False)
                    
                    # Filter out invalid rows
                    print("Filtering out invalid rows...")
                    df = df.filter(col("seriesid").rlike("^(FRY9C|FRY15|FR2004A)"))
                    count = df.count()
                    print(f"After filtering: {count} valid rows remaining")
                
            print(f"Successfully created DataFrame with {count} rows")
            return df
            
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"Attempt {attempt+1} failed: {str(e)}. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                # Increase delay for next attempt
                retry_delay *= 2
            else:
                print(f"All {max_retries} attempts failed. Last error: {str(e)}")
                raise

# Configure S3 client with optimized settings
print("Configuring S3 client settings...")
# Removed deprecated s3n protocol configuration
sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
sc._jsc.hadoopConfiguration().set("fs.s3a.attempts.maximum", "20")
sc._jsc.hadoopConfiguration().set("fs.s3a.retry.limit", "20")
sc._jsc.hadoopConfiguration().set("fs.s3a.connection.timeout", "30000")
sc._jsc.hadoopConfiguration().set("fs.s3a.connection.establish.timeout", "10000")
sc._jsc.hadoopConfiguration().set("fs.s3a.multipart.size", "104857600")
sc._jsc.hadoopConfiguration().set("fs.s3a.multipart.threshold", "104857600")
sc._jsc.hadoopConfiguration().set("fs.s3a.fast.upload", "true")
sc._jsc.hadoopConfiguration().set("fs.s3a.fast.upload.buffer", "disk")
sc._jsc.hadoopConfiguration().set("fs.s3a.checksum.enable", "true")
sc._jsc.hadoopConfiguration().set("fs.s3a.experimental.input.fadvise", "sequential")
sc._jsc.hadoopConfiguration().set("fs.s3a.connection.maximum", "100")
sc._jsc.hadoopConfiguration().set("fs.s3a.threads.max", "20")
sc._jsc.hadoopConfiguration().set("fs.s3a.socket.send.buffer", "8192")
sc._jsc.hadoopConfiguration().set("fs.s3a.socket.recv.buffer", "8192")
sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "true")
sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "false")

# Validate and read S3 files
print(f"Validating files in: {input_path}")
valid_files = validate_and_read_s3_files(input_path)

if not valid_files:
    raise Exception(f"No valid CSV files found in {input_path}. Aborting job.")

# Extract ingest_timestamp from the first valid file path
ingest_ts = "unknown"
try:
    # Extract timestamp from path like "s3://bucket/collections-data/ingest_ts=1234567890/file.csv"
    first_file_path = valid_files[0]
    print(f"Extracting ingest_ts from file path: {first_file_path}")
    
    path_parts = first_file_path.split("/")
    for part in path_parts:
        if part.startswith("ingest_ts="):
            ingest_ts = part.split("=")[1]
            print(f"✓ Extracted ingest_ts from path: {ingest_ts}")
            break
    
    if ingest_ts == "unknown":
        print(f"WARNING: Could not extract ingest_ts from path: {first_file_path}")
        print(f"Path parts: {path_parts}")
except Exception as e:
    print(f"Error extracting timestamp from path: {str(e)}")

# Create DataFrame from validated files
try:
    print("Creating DataFrame from validated files...")
    csv_df = create_dataframe_from_validated_files(spark, valid_files)
    
    # Add ingest_timestamp column BEFORE any other operations
    print(f"Adding ingest_timestamp column with value: {ingest_ts}")
    csv_df = csv_df.withColumn("ingest_timestamp", lit(ingest_ts))
    
    # Print inferred schema
    print("Inferred schema:")
    csv_df.printSchema()
    
    # Show sample data with ingest_timestamp
    print("Sample data with ingest_timestamp:")
    csv_df.select("ingest_timestamp").show(5, truncate=False)
    
except Exception as e:
    print(f"Error creating DataFrame: {str(e)}")
    raise

# Check if table exists
table_exists = False
try:
    # Try to read the table to check if it exists
    # Use the glue_catalog prefix as configured in the Spark session
    spark.sql(f"SELECT 1 FROM glue_catalog.{database_name}.{table_name} LIMIT 1")
    table_exists = True
    print(f"Table {database_name}.{table_name} exists")
except Exception as e:
    print(f"Table {database_name}.{table_name} does not exist: {str(e)}")

# Create table if it doesn't exist
if not table_exists:
    print(f"Creating Iceberg table: {database_name}.{table_name}")
    print(f"Using ingest_timestamp value: {ingest_ts}")
    
    # Register the DataFrame as a temporary view
    csv_df.createOrReplaceTempView("csv_df")
    
    # Create the Iceberg table with partitioning by seriesid and ingest_timestamp
    create_table_sql = f"""
    CREATE TABLE glue_catalog.{database_name}.{table_name} 
    USING iceberg
    PARTITIONED BY (seriesid, ingest_timestamp)
    TBLPROPERTIES (
        'format-version'='2',
        'table_type'='ICEBERG',
        'storage_handler'='org.apache.iceberg.mr.hive.HiveIcebergStorageHandler',
        'serialization.format'='1',
        'storage.location.provider'='org.apache.iceberg.aws.s3.S3FileIO',
        'hive.input.format'='org.apache.iceberg.mr.hive.HiveIcebergInputFormat',
        'hive.output.format'='org.apache.iceberg.mr.hive.HiveIcebergOutputFormat'
    )
    AS SELECT * FROM csv_df
    """
    
    try:
        print(f"Executing table creation query: {create_table_sql}")
        spark.sql(create_table_sql)
        print(f"Created Iceberg table: {database_name}.{table_name}")
        
        # Verify table was created properly
        print(f"Verifying table metadata for {database_name}.{table_name}:")
        table_info = spark.sql(f"DESCRIBE TABLE glue_catalog.{database_name}.{table_name}")
        table_info.show(truncate=False)
        
        # Verify table properties
        print(f"Verifying table properties for {database_name}.{table_name}:")
        table_props = spark.sql(f"SHOW TBLPROPERTIES glue_catalog.{database_name}.{table_name}")
        table_props.show(truncate=False)
        
        # Check for storage descriptor and input format
        storage_info = spark.sql(f"DESCRIBE FORMATTED glue_catalog.{database_name}.{table_name}")
        print(f"Detailed table information for {database_name}.{table_name}:")
        storage_info.show(truncate=False)
        
        # Verify if InputFormat is defined
        input_format_row = storage_info.filter(storage_info.col_name.contains("InputFormat")).collect()
        if not input_format_row or not input_format_row[0]["data_type"]:
            print("WARNING: InputFormat is not defined in the table. Attempting to fix...")
            # Set the InputFormat explicitly for Iceberg tables
            spark.sql(f"""
            ALTER TABLE glue_catalog.{database_name}.{table_name} 
            SET TBLPROPERTIES (
                'storage_handler'='org.apache.iceberg.mr.hive.HiveIcebergStorageHandler',
                'format-version'='2',
                'table_type'='ICEBERG',
                'serialization.format'='1',
                'storage.location.provider'='org.apache.iceberg.aws.s3.S3FileIO',
                'hive.input.format'='org.apache.iceberg.mr.hive.HiveIcebergInputFormat',
                'hive.output.format'='org.apache.iceberg.mr.hive.HiveIcebergOutputFormat'
            )
            """)
            print("Table properties updated with InputFormat and OutputFormat")
        
        # Register table with Lake Formation for multi-dialect view support
        register_table_with_lake_formation(catalog_id, database_name, table_name, iceberg_data_bucket, iceberg_data_prefix, aws_region)
    except Exception as e:
        print(f"Error creating table: {str(e)}")
        print("This error may be related to Hive metastore configuration.")
        print("Ensure the IAM role has glue:CreateDatabase and other required permissions.")
        raise
else:
    # If table exists, insert data into it
    print(f"Inserting data into existing table: {database_name}.{table_name}")
    print(f"Using ingest_timestamp value: {ingest_ts}")
    
    # Register the DataFrame as a temporary view
    csv_df.createOrReplaceTempView("csv_df")
    
    # Insert data into the Iceberg table
    insert_sql = f"""
    INSERT INTO glue_catalog.{database_name}.{table_name}
    SELECT * FROM csv_df
    """
    
    try:
        print(f"Executing insert query: {insert_sql}")
        spark.sql(insert_sql)
        print(f"Data inserted into table: {database_name}.{table_name}")
        
        # Verify data was inserted properly
        print(f"Verifying data in table {database_name}.{table_name}:")
        row_count = spark.sql(f"SELECT COUNT(*) FROM glue_catalog.{database_name}.{table_name}").collect()[0][0]
        print(f"Table contains {row_count} rows after insert")
        
        # Show sample data
        print("Sample data from table:")
        sample_data = spark.sql(f"SELECT * FROM glue_catalog.{database_name}.{table_name} LIMIT 5")
        sample_data.show(truncate=False)
        
        # Check for storage descriptor and input format after insert
        storage_info = spark.sql(f"DESCRIBE FORMATTED glue_catalog.{database_name}.{table_name}")
        print(f"Detailed table information after insert for {database_name}.{table_name}:")
        storage_info.show(truncate=False)
        
        # Verify if InputFormat is defined
        input_format_row = storage_info.filter(storage_info.col_name.contains("InputFormat")).collect()
        if not input_format_row or not input_format_row[0]["data_type"]:
            print("WARNING: InputFormat is not defined in the table after insert. Attempting to fix...")
            # Set the InputFormat explicitly for Iceberg tables
            spark.sql(f"""
            ALTER TABLE glue_catalog.{database_name}.{table_name} 
            SET TBLPROPERTIES (
                'storage_handler'='org.apache.iceberg.mr.hive.HiveIcebergStorageHandler',
                'format-version'='2',
                'table_type'='ICEBERG',
                'serialization.format'='1',
                'storage.location.provider'='org.apache.iceberg.aws.s3.S3FileIO',
                'hive.input.format'='org.apache.iceberg.mr.hive.HiveIcebergInputFormat',
                'hive.output.format'='org.apache.iceberg.mr.hive.HiveIcebergOutputFormat'
            )
            """)
            print("Table properties updated with InputFormat and OutputFormat after insert")
        
        # Register table with Lake Formation for multi-dialect view support
        register_table_with_lake_formation(catalog_id, database_name, table_name, iceberg_data_bucket, iceberg_data_prefix, aws_region)
    except Exception as e:
        print(f"Error inserting data into table: {str(e)}")
        print("This error may be related to Hive metastore configuration.")
        print("Ensure the IAM role has glue:CreateDatabase and other required permissions.")
        raise

# Print record count
record_count = csv_df.count()
print(f"Processed {record_count} records")

# Commit the job
job.commit()
