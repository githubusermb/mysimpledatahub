import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, struct, lit, expr
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

print(f"Creating dynamic views for table {database_name}.{source_table_name}")
print(f"Athena output location: {athena_output_location}")

# Initialize Athena client for creating permanent views
athena_client = boto3.client('athena')

# Read the source table using the Glue catalog
source_df = spark.sql(f"SELECT * FROM glue_catalog.{database_name}.{source_table_name}")

# Get distinct entity1 values
entity1_values = [row.entity1 for row in source_df.select("entity1").distinct().collect()]
print(f"Found {len(entity1_values)} distinct entity1 values")

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

# Create views for each entity1 value
created_views = []
failed_views = []

for entity1_value in entity1_values:
    print(f"\n{'='*80}")
    print(f"Processing entity1 = '{entity1_value}'")
    print(f"{'='*80}")
    
    # Filter data for this entity1 value
    entity_df = source_df.filter(col("entity1") == entity1_value)
    
    # Get distinct key values for this specific entity1 value
    entity_key_values = [row.key for row in entity_df.select("key").distinct().collect()]
    print(f"Found {len(entity_key_values)} distinct key values")
    
    # Create view name based on entity1 value
    view_name = f"{view_prefix}_{entity1_value}"
    
    # Build the pivot columns for the SQL
    pivot_columns = []
    for key in entity_key_values:
        # Use double quotes for column names (works in both Athena and Spark)
        col_expr = f'MAX(CASE WHEN key = \'{key}\' THEN value ELSE NULL END) AS "{key}"'
        pivot_columns.append(col_expr)
    
    pivot_columns_str = ',\n        '.join(pivot_columns)
    
    # Build the SQL for the view definition
    # Use database.table format (works in Athena)
    view_sql = f"""SELECT 
        entity1,
        entity2,
        entity3,
        entity4,
        {pivot_columns_str}
    FROM 
        {database_name}.{source_table_name}
    WHERE
        entity1 = '{entity1_value}'
    GROUP BY 
        entity1, entity2, entity3, entity4"""
    
    try:
        # Step 1: Drop existing view in Athena (if exists)
        drop_query = f"DROP VIEW IF EXISTS {database_name}.{view_name}"
        execute_athena_query(drop_query, f"Dropping view {view_name} if it exists")
        
        # Step 2: Create view in Athena using CREATE OR REPLACE VIEW
        create_query = f"CREATE OR REPLACE VIEW {database_name}.{view_name} AS\n{view_sql}"
        
        if execute_athena_query(create_query, f"Creating view {view_name} in Athena"):
            print(f"✓ Successfully created Athena view: {database_name}.{view_name}")
            created_views.append(view_name)
            
            # Step 3: Test the view in Athena
            test_query = f"SELECT COUNT(*) as row_count FROM {database_name}.{view_name}"
            if execute_athena_query(test_query, f"Testing view {view_name}"):
                print(f"✓ View {view_name} is queryable in Athena")
            else:
                print(f"⚠ View {view_name} created but test query failed")
        else:
            print(f"✗ Failed to create view {view_name}")
            failed_views.append(view_name)
        
    except Exception as e:
        print(f"✗ Error processing view {view_name}: {str(e)}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        failed_views.append(view_name)

# Print summary
print(f"\n{'='*80}")
print(f"VIEW CREATION SUMMARY")
print(f"{'='*80}")
print(f"Total entity1 values processed: {len(entity1_values)}")
print(f"Views created successfully: {len(created_views)}")
print(f"Views failed: {len(failed_views)}")

if created_views:
    print(f"\n✓ Successfully created views:")
    for view in created_views:
        print(f"  - {database_name}.{view}")

if failed_views:
    print(f"\n✗ Failed views:")
    for view in failed_views:
        print(f"  - {database_name}.{view}")

print(f"\nViews can be queried in:")
print(f"  - Athena: SELECT * FROM {database_name}.<view_name>")
print(f"  - Glue Spark: spark.sql('SELECT * FROM glue_catalog.{database_name}.<view_name>')")

print("\nDynamic view creation job completed")
job.commit()
