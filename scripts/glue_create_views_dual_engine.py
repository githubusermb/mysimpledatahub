"""
Create Dynamic Views for Both Spark and Athena
Uses CREATE PROTECTED MULTI DIALECT VIEW + ALTER VIEW ADD DIALECT

This is the COMPLETE solution for views that work in both engines:
1. Create view using Spark SQL PROTECTED MULTI DIALECT VIEW
2. Add Athena dialect using ALTER VIEW ADD DIALECT via Athena API

Based on: https://docs.aws.amazon.com/glue/latest/dg/catalog-views.html
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
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'database_name',
    'source_table_name',
    'view_prefix',
    'athena_output_location'
])

database_name = args['database_name']
source_table_name = args['source_table_name']
view_prefix = args['view_prefix']
athena_output_location = args['athena_output_location']

print(f"Creating dual-engine views for {database_name}.{source_table_name}")
print(f"View prefix: {view_prefix}")
print(f"Athena output location: {athena_output_location}")

# Initialize clients
glue_client = boto3.client('glue')
athena_client = boto3.client('athena')

# Read the source table using the Glue catalog
source_df = spark.sql(f"SELECT * FROM glue_catalog.{database_name}.{source_table_name}")

# Get distinct seriesid values
seriesid_values = [row.seriesid for row in source_df.select("seriesid").distinct().collect()]
print(f"Found {len(seriesid_values)} distinct seriesid values")

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
                if attempt % 5 == 0:  # Print every 5 attempts
                    print(f"  Waiting... ({attempt}/{max_attempts})")
                time.sleep(2)
        
        print(f"✗ Query timed out after {max_attempts * 2} seconds")
        return False
        
    except Exception as e:
        print(f"✗ Error executing query: {str(e)}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        return False

# Create views for each seriesid value
created_views = []
failed_views = []

for seriesid_value in seriesid_values:
    print(f"\n{'='*80}")
    print(f"Processing seriesid = '{seriesid_value}'")
    print(f"{'='*80}")
    
    # Filter data for this seriesid value
    entity_df = source_df.filter(col("seriesid") == seriesid_value)
    
    # Get distinct key values for this specific seriesid value
    entity_key_values = [row.key for row in entity_df.select("key").distinct().collect()]
    print(f"Found {len(entity_key_values)} distinct key values")
    
    # Create view name based on seriesid value
    view_name = f"{view_prefix}_{seriesid_value}"
    full_view_name = f"{database_name}.{view_name}"
    
    # Build the pivot columns for Spark SQL (using backticks)
    spark_pivot_columns = []
    for key in entity_key_values:
        col_expr = f"MAX(CASE WHEN key = '{key}' THEN value ELSE NULL END) AS `{key}`"
        spark_pivot_columns.append(col_expr)
    
    spark_pivot_columns_str = ',\n        '.join(spark_pivot_columns)
    
    # Build the SQL for Spark view definition
    spark_view_sql = f"""
    SELECT 
        seriesid,
        aod,
        rssdid,
        submissionts,
        {spark_pivot_columns_str}
    FROM 
        glue_catalog.{database_name}.{source_table_name}
    WHERE
        seriesid = '{seriesid_value}'
    GROUP BY 
        seriesid, aod, rssdid, submissionts
    """
    
    # Build the pivot columns for Athena SQL (using double quotes)
    athena_pivot_columns = []
    for key in entity_key_values:
        col_expr = f'MAX(CASE WHEN key = \'{key}\' THEN value ELSE NULL END) AS "{key}"'
        athena_pivot_columns.append(col_expr)
    
    athena_pivot_columns_str = ',\n        '.join(athena_pivot_columns)
    
    # Build the SQL for Athena dialect (no catalog prefix)
    athena_view_sql = f"""SELECT 
        seriesid,
        aod,
        rssdid,
        submissionts,
        {athena_pivot_columns_str}
    FROM 
        {database_name}.{source_table_name}
    WHERE
        seriesid = '{seriesid_value}'
    GROUP BY 
        seriesid, aod, rssdid, submissionts"""
    
    try:
        # Step 1: Drop existing view if it exists
        try:
            glue_client.delete_table(
                DatabaseName=database_name,
                Name=view_name
            )
            print(f"✓ Dropped existing view: {view_name}")
        except glue_client.exceptions.EntityNotFoundException:
            print(f"View {view_name} doesn't exist, will create new")
        except Exception as e:
            print(f"Note: Could not drop view {view_name}: {str(e)}")
        
        # Step 2: Create PROTECTED MULTI DIALECT VIEW using Spark SQL
        print(f"\n[Step 1/3] Creating PROTECTED MULTI DIALECT VIEW in Spark...")
        
        create_view_sql = f"""
        CREATE PROTECTED MULTI DIALECT VIEW {full_view_name}
        SECURITY DEFINER
        AS {spark_view_sql}
        """
        
        spark.sql(create_view_sql)
        print(f"✓ View created in Spark: {view_name}")
        
        # Step 3: Verify the view works in Spark
        print(f"\n[Step 2/3] Verifying view in Spark...")
        verify_df = spark.sql(f"SELECT COUNT(*) as count FROM {full_view_name}")
        row_count = verify_df.collect()[0]['count']
        print(f"✓ View verified in Spark: {row_count} rows")
        
        # Step 4: Add Athena dialect using ALTER VIEW ADD DIALECT
        print(f"\n[Step 3/3] Adding Athena dialect via ALTER VIEW ADD DIALECT...")
        
        alter_view_sql = f"""
        ALTER VIEW {database_name}.{view_name}
        ADD DIALECT ATHENA AS
        {athena_view_sql}
        """
        
        if execute_athena_query(alter_view_sql, f"Adding Athena dialect to {view_name}"):
            print(f"✓ Athena dialect added successfully")
            
            # Step 5: Test the view in Athena
            print(f"\n[Verification] Testing view in Athena...")
            test_query = f"SELECT COUNT(*) as row_count FROM {database_name}.{view_name}"
            if execute_athena_query(test_query, f"Testing view {view_name} in Athena"):
                print(f"✓ View works in Athena!")
                
                # All steps succeeded
                print(f"\n✓✓✓ View {view_name} successfully created for BOTH engines!")
                created_views.append(view_name)
            else:
                print(f"⚠ View created but Athena test failed")
                failed_views.append({
                    'view': view_name,
                    'error': 'Athena test query failed'
                })
        else:
            print(f"✗ Failed to add Athena dialect")
            failed_views.append({
                'view': view_name,
                'error': 'ALTER VIEW ADD DIALECT failed'
            })
        
    except Exception as e:
        print(f"\n✗✗✗ Error creating view {view_name}: {str(e)}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        failed_views.append({
            'view': view_name,
            'error': str(e)
        })

# Print summary
print(f"\n{'='*80}")
print(f"DUAL-ENGINE VIEW CREATION SUMMARY")
print(f"{'='*80}")
print(f"Total seriesid values processed: {len(seriesid_values)}")
print(f"Views created successfully: {len(created_views)}")
print(f"Views failed: {len(failed_views)}")

if created_views:
    print(f"\n✓ Successfully created dual-engine views:")
    for view in created_views:
        print(f"  - {database_name}.{view}")

if failed_views:
    print(f"\n✗ Failed views:")
    for result in failed_views:
        print(f"  - {result['view']}: {result['error']}")

print(f"\n{'='*80}")
print(f"USAGE")
print(f"{'='*80}")
print(f"\nThese views work in BOTH engines:")
print(f"\n  Athena:")
print(f"    SELECT * FROM {database_name}.<view_name>")
print(f"\n  Glue Spark:")
print(f"    spark.sql('SELECT * FROM glue_catalog.{database_name}.<view_name>')")

print(f"\n{'='*80}")
print(f"HOW IT WORKS")
print(f"{'='*80}")
print(f"1. CREATE PROTECTED MULTI DIALECT VIEW (Spark SQL)")
print(f"   - Creates view in Glue Data Catalog")
print(f"   - Works in Glue Spark immediately")
print(f"\n2. ALTER VIEW ADD DIALECT ATHENA (Athena API)")
print(f"   - Adds Athena-specific SQL dialect")
print(f"   - Makes view work in Athena")
print(f"\n3. Result: Single view works in both engines!")

print("\nDual-engine view creation job completed")
job.commit()
