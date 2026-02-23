"""
Create Dynamic Views for Both Spark and Athena
Creates two views:
1. collections_data_vw - Same as collections_data_tbl (all data)
2. cdp_data_vw - Filtered by specific seriesid (externalized parameter)

Uses CREATE PROTECTED MULTI DIALECT VIEW + ALTER VIEW ADD DIALECT
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import boto3
import time

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)

# Get job parameters
required_args = [
    'JOB_NAME',
    'database_name',
    'source_table_name',
    'cdp_seriesid_filter',
    'athena_output_location',
    'aws_region'
]

args = getResolvedOptions(sys.argv, required_args)

database_name = args['database_name']
source_table_name = args['source_table_name']
cdp_seriesid_filter = args['cdp_seriesid_filter']  # e.g., 'FRY9C' or 'FRY15'
athena_output_location = args['athena_output_location']
aws_region = args['aws_region']

print(f"Creating normal views for {database_name}.{source_table_name}")
print(f"CDP seriesid filter: {cdp_seriesid_filter}")
print(f"Athena output location: {athena_output_location}")
print(f"AWS Region: {aws_region}")

# Initialize clients with region
glue_client = boto3.client('glue', region_name=aws_region)
athena_client = boto3.client('athena', region_name=aws_region)

# Helper function to execute Athena query and wait for completion
def execute_athena_query(query_string, description):
    """Execute an Athena query and wait for it to complete"""
    print(f"\n{description}")
    print(f"Query preview: {query_string[:300]}...")
    
    try:
        response = athena_client.start_query_execution(
            QueryString=query_string,
            QueryExecutionContext={
                'Database': database_name
            },
            ResultConfiguration={
                'OutputLocation': athena_output_location
            }
        )
        
        query_execution_id = response['QueryExecutionId']
        print(f"Query execution ID: {query_execution_id}")
        
        # Wait for query to complete
        max_attempts = 30
        attempt = 0
        while attempt < max_attempts:
            attempt += 1
            query_status = athena_client.get_query_execution(
                QueryExecutionId=query_execution_id
            )
            
            status = query_status['QueryExecution']['Status']['State']
            
            if status == 'SUCCEEDED':
                print(f"✓ Query succeeded")
                return True
            elif status in ['FAILED', 'CANCELLED']:
                reason = query_status['QueryExecution']['Status'].get('StateChangeReason', 'Unknown')
                print(f"✗ Query {status.lower()}: {reason}")
                return False
            else:
                if attempt % 5 == 0:
                    print(f"  Waiting... ({attempt}/{max_attempts})")
                time.sleep(2)
        
        print(f"✗ Query timed out after {max_attempts * 2} seconds")
        return False
        
    except Exception as e:
        print(f"✗ Error executing query: {str(e)}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        return False

# Step 1: Create collections_data_vw (same as source table)
print(f"\n{'='*80}")
print(f"STEP 1: Creating collections_data_vw")
print(f"{'='*80}")

collections_view_name = "collections_data_vw"
full_collections_view_name = f"{database_name}.{collections_view_name}"

try:
    # Drop existing view if it exists
    try:
        glue_client.delete_table(
            DatabaseName=database_name,
            Name=collections_view_name
        )
        print(f"✓ Dropped existing view: {collections_view_name}")
    except glue_client.exceptions.EntityNotFoundException:
        print(f"View {collections_view_name} doesn't exist, will create new")
    except Exception as e:
        print(f"Note: Could not drop view {collections_view_name}: {str(e)}")
    
    # Create view with same schema as source table (Spark SQL)
    print(f"Creating view in Spark...")
    create_collections_view_sql = f"""
    CREATE PROTECTED MULTI DIALECT VIEW {full_collections_view_name}
    SECURITY DEFINER
    AS SELECT * FROM glue_catalog.{database_name}.{source_table_name}
    """
    
    spark.sql(create_collections_view_sql)
    print(f"✓ View created in Spark: {collections_view_name}")
    
    # Verify the view works in Spark
    print(f"Verifying view in Spark...")
    verify_df = spark.sql(f"SELECT COUNT(*) as count FROM {full_collections_view_name}")
    row_count = verify_df.collect()[0]['count']
    print(f"✓ View verified in Spark: {row_count} rows")
    
    # Add Athena dialect
    print(f"Adding Athena dialect...")
    alter_collections_view_sql = f"""ALTER VIEW {database_name}.{collections_view_name} ADD DIALECT AS SELECT * FROM {database_name}.{source_table_name}"""
    
    if execute_athena_query(alter_collections_view_sql, f"Adding Athena dialect to {collections_view_name}"):
        print(f"✓ Athena dialect added successfully")
        
        # Test in Athena
        test_query = f"SELECT COUNT(*) as row_count FROM {database_name}.{collections_view_name}"
        if execute_athena_query(test_query, f"Testing view {collections_view_name} in Athena"):
            print(f"✓ View works in Athena!")
            print(f"\n✓✓✓ View {collections_view_name} successfully created for BOTH engines!")
        else:
            print(f"⚠ View created but Athena test failed")
    else:
        print(f"✗ Failed to add Athena dialect")
        
except Exception as e:
    print(f"\n✗✗✗ Error creating view {collections_view_name}: {str(e)}")
    import traceback
    print(f"Traceback: {traceback.format_exc()}")

# Step 2: Create cdp_data_vw (filtered by seriesid)
print(f"\n{'='*80}")
print(f"STEP 2: Creating cdp_data_vw")
print(f"{'='*80}")

cdp_view_name = "cdp_data_vw"
full_cdp_view_name = f"{database_name}.{cdp_view_name}"

try:
    # Drop existing view if it exists
    try:
        glue_client.delete_table(
            DatabaseName=database_name,
            Name=cdp_view_name
        )
        print(f"✓ Dropped existing view: {cdp_view_name}")
    except glue_client.exceptions.EntityNotFoundException:
        print(f"View {cdp_view_name} doesn't exist, will create new")
    except Exception as e:
        print(f"Note: Could not drop view {cdp_view_name}: {str(e)}")
    
    # Create view filtered by seriesid (Spark SQL)
    print(f"Creating view in Spark with filter: seriesid = '{cdp_seriesid_filter}'")
    create_cdp_view_sql = f"""
    CREATE PROTECTED MULTI DIALECT VIEW {full_cdp_view_name}
    SECURITY DEFINER
    AS SELECT * FROM glue_catalog.{database_name}.{source_table_name}
    WHERE seriesid = '{cdp_seriesid_filter}'
    """
    
    spark.sql(create_cdp_view_sql)
    print(f"✓ View created in Spark: {cdp_view_name}")
    
    # Verify the view works in Spark
    print(f"Verifying view in Spark...")
    verify_df = spark.sql(f"SELECT COUNT(*) as count FROM {full_cdp_view_name}")
    row_count = verify_df.collect()[0]['count']
    print(f"✓ View verified in Spark: {row_count} rows for seriesid = '{cdp_seriesid_filter}'")
    
    # Show sample data
    print(f"Sample data from Spark view:")
    sample_df = spark.sql(f"SELECT * FROM {full_cdp_view_name} LIMIT 3")
    sample_df.show(truncate=False)
    
    # Add Athena dialect
    print(f"Adding Athena dialect...")
    alter_cdp_view_sql = f"""ALTER VIEW {database_name}.{cdp_view_name} ADD DIALECT AS SELECT * FROM {database_name}.{source_table_name} WHERE seriesid = '{cdp_seriesid_filter}'"""
    
    if execute_athena_query(alter_cdp_view_sql, f"Adding Athena dialect to {cdp_view_name}"):
        print(f"✓ Athena dialect added successfully")
        
        # Test in Athena
        test_query = f"SELECT COUNT(*) as row_count FROM {database_name}.{cdp_view_name}"
        if execute_athena_query(test_query, f"Testing view {cdp_view_name} in Athena"):
            print(f"✓ View works in Athena!")
            print(f"\n✓✓✓ View {cdp_view_name} successfully created for BOTH engines!")
        else:
            print(f"⚠ View created but Athena test failed")
    else:
        print(f"✗ Failed to add Athena dialect")
        
except Exception as e:
    print(f"\n✗✗✗ Error creating view {cdp_view_name}: {str(e)}")
    import traceback
    print(f"Traceback: {traceback.format_exc()}")

# Summary
print(f"\n{'='*80}")
print(f"SUMMARY")
print(f"{'='*80}")
print(f"Views created:")
print(f"  1. {collections_view_name} - All data from {source_table_name}")
print(f"  2. {cdp_view_name} - Filtered data for seriesid = '{cdp_seriesid_filter}'")
print(f"\nBoth views support Spark SQL and Athena queries.")

job.commit()
