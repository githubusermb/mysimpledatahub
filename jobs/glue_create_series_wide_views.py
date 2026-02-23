"""
Create Series-Specific Wide Views with Concatenated Context Pattern
Creates pivoted views where each distinct item_mdrm becomes a column with concatenated context.

Pattern: <context_level1_mdrm>=<context_level1_value>:<context_level2_mdrm>=<context_level2_value>:<context_level3_mdrm>=<context_level3_value>:<item_value>
If no context exists, only item_value is stored.

View columns: seriesid, aod, rssdid, submission_ts, <item_mdrm_columns>
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
    'athena_output_location',
    'aws_region'
]

args = getResolvedOptions(sys.argv, required_args)

database_name = args['database_name']
source_table_name = args['source_table_name']
athena_output_location = args['athena_output_location']
aws_region = args['aws_region']

print(f"Creating series-specific wide views for {database_name}.{source_table_name}")
print(f"View naming pattern: <seriesid>_wide_view")
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

# Read the source table using the Glue catalog
source_df = spark.sql(f"SELECT * FROM glue_catalog.{database_name}.{source_table_name}")

# Get distinct seriesid values and filter for valid ones
valid_seriesid_prefixes = ['FRY9C', 'FRY15', 'FR2004A']
all_seriesid_values = [row.seriesid for row in source_df.select("seriesid").distinct().collect()]
print(f"Found {len(all_seriesid_values)} total distinct seriesid values")

# Filter to only valid seriesid values
seriesid_values = [s for s in all_seriesid_values if any(s.startswith(prefix) for prefix in valid_seriesid_prefixes)]
print(f"Filtered to {len(seriesid_values)} valid seriesid values: {seriesid_values}")

if len(all_seriesid_values) > len(seriesid_values):
    invalid_values = [s for s in all_seriesid_values if s not in seriesid_values]
    print(f"Skipping {len(invalid_values)} invalid seriesid values: {invalid_values[:10]}")  # Show first 10

# Create wide views for each seriesid
created_views = []
failed_views = []

for seriesid_value in seriesid_values:
    print(f"\n{'='*80}")
    print(f"Processing seriesid = '{seriesid_value}'")
    print(f"{'='*80}")
    
    # Filter data for this seriesid value
    entity_df = source_df.filter(col("seriesid") == seriesid_value)
    
    # Get distinct item_mdrm values for this specific seriesid
    item_mdrm_values = [row.item_mdrm for row in entity_df.select("item_mdrm").distinct().collect()]
    print(f"Found {len(item_mdrm_values)} distinct item_mdrm values")
    
    # Create view name: <seriesid>_wide_view
    view_name = f"{seriesid_value.lower()}_wide_view"
    full_view_name = f"{database_name}.{view_name}"
    
    # Build the pivot columns for Spark SQL with concatenated context pattern (using backticks)
    spark_pivot_columns = []
    for item_mdrm in item_mdrm_values:
        # Escape single quotes in item_mdrm for SQL
        escaped_item_mdrm = item_mdrm.replace("'", "''")
        
        col_expr = f"""MAX(CASE WHEN item_mdrm = '{escaped_item_mdrm}' THEN 
            CASE 
                WHEN context_level1_mdrm IS NOT NULL AND context_level1_mdrm != '' THEN
                    CONCAT(
                        COALESCE(CAST(context_level1_mdrm AS STRING), ''), '=', COALESCE(CAST(context_level1_value AS STRING), ''),
                        CASE WHEN context_level2_mdrm IS NOT NULL AND context_level2_mdrm != '' 
                             THEN CONCAT(':', CAST(context_level2_mdrm AS STRING), '=', COALESCE(CAST(context_level2_value AS STRING), ''))
                             ELSE '' END,
                        CASE WHEN context_level3_mdrm IS NOT NULL AND context_level3_mdrm != '' 
                             THEN CONCAT(':', CAST(context_level3_mdrm AS STRING), '=', COALESCE(CAST(context_level3_value AS STRING), ''))
                             ELSE '' END,
                        ':', CAST(item_value AS STRING)
                    )
                ELSE CAST(item_value AS STRING)
            END
        ELSE NULL END) AS `{item_mdrm}`"""
        
        spark_pivot_columns.append(col_expr)
    
    spark_pivot_columns_str = ',\n        '.join(spark_pivot_columns)
    
    # Build the pivot columns for Athena/view SQL with concatenated context pattern (using double quotes)
    view_pivot_columns = []
    for item_mdrm in item_mdrm_values:
        # Escape single quotes in item_mdrm for SQL
        escaped_item_mdrm = item_mdrm.replace("'", "''")
        
        col_expr = f"""MAX(CASE WHEN item_mdrm = '{escaped_item_mdrm}' THEN 
            CASE 
                WHEN context_level1_mdrm IS NOT NULL AND context_level1_mdrm != '' THEN
                    CONCAT(
                        COALESCE(CAST(context_level1_mdrm AS VARCHAR), ''), '=', COALESCE(CAST(context_level1_value AS VARCHAR), ''),
                        CASE WHEN context_level2_mdrm IS NOT NULL AND context_level2_mdrm != '' 
                             THEN CONCAT(':', CAST(context_level2_mdrm AS VARCHAR), '=', COALESCE(CAST(context_level2_value AS VARCHAR), ''))
                             ELSE '' END,
                        CASE WHEN context_level3_mdrm IS NOT NULL AND context_level3_mdrm != '' 
                             THEN CONCAT(':', CAST(context_level3_mdrm AS VARCHAR), '=', COALESCE(CAST(context_level3_value AS VARCHAR), ''))
                             ELSE '' END,
                        ':', CAST(item_value AS VARCHAR)
                    )
                ELSE CAST(item_value AS VARCHAR)
            END
        ELSE NULL END) AS "{item_mdrm}" """
        
        view_pivot_columns.append(col_expr)
    
    view_pivot_columns_str = ',\n        '.join(view_pivot_columns)
    
    # Build the SQL for Spark view definition (with glue_catalog prefix for Spark)
    spark_view_sql = f"""
    SELECT 
        seriesid,
        aod,
        rssdid,
        submission_ts,
        {spark_pivot_columns_str}
    FROM 
        glue_catalog.{database_name}.{source_table_name}
    WHERE
        seriesid = '{seriesid_value}'
    GROUP BY 
        seriesid, aod, rssdid, submission_ts
    """
    
    # Build the SQL for Spark dialect in PROTECTED MULTI DIALECT VIEW (no catalog prefix for Athena API)
    spark_dialect_view_sql = f"""SELECT 
        seriesid,
        aod,
        rssdid,
        submission_ts,
        {view_pivot_columns_str}
    FROM 
        {database_name}.{source_table_name}
    WHERE
        seriesid = '{seriesid_value}'
    GROUP BY 
        seriesid, aod, rssdid, submission_ts"""
    
    # Build the pivot columns for Athena SQL with concatenated context pattern
    athena_pivot_columns = []
    for item_mdrm in item_mdrm_values:
        # Escape single quotes in item_mdrm for SQL
        escaped_item_mdrm = item_mdrm.replace("'", "''")
        
        col_expr = f"""MAX(CASE WHEN item_mdrm = '{escaped_item_mdrm}' THEN 
            CASE 
                WHEN context_level1_mdrm IS NOT NULL AND context_level1_mdrm != '' THEN
                    CONCAT(
                        COALESCE(CAST(context_level1_mdrm AS VARCHAR), ''), '=', COALESCE(CAST(context_level1_value AS VARCHAR), ''),
                        CASE WHEN context_level2_mdrm IS NOT NULL AND context_level2_mdrm != '' 
                             THEN CONCAT(':', CAST(context_level2_mdrm AS VARCHAR), '=', COALESCE(CAST(context_level2_value AS VARCHAR), ''))
                             ELSE '' END,
                        CASE WHEN context_level3_mdrm IS NOT NULL AND context_level3_mdrm != '' 
                             THEN CONCAT(':', CAST(context_level3_mdrm AS VARCHAR), '=', COALESCE(CAST(context_level3_value AS VARCHAR), ''))
                             ELSE '' END,
                        ':', CAST(item_value AS VARCHAR)
                    )
                ELSE CAST(item_value AS VARCHAR)
            END
        ELSE NULL END) AS "{item_mdrm}" """
        
        athena_pivot_columns.append(col_expr)
    
    athena_pivot_columns_str = ',\n        '.join(athena_pivot_columns)
    
    # Build the SQL for Athena dialect (no catalog prefix)
    athena_view_sql = f"""SELECT 
        seriesid,
        aod,
        rssdid,
        submission_ts,
        {athena_pivot_columns_str}
    FROM 
        {database_name}.{source_table_name}
    WHERE
        seriesid = '{seriesid_value}'
    GROUP BY 
        seriesid, aod, rssdid, submission_ts"""
    
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
        
        # Check if query is too large for Athena (>250KB)
        spark_dialect_query_size = len(spark_dialect_view_sql)
        athena_query_size = len(athena_view_sql)
        print(f"Query sizes - Spark dialect: {spark_dialect_query_size:,} bytes, Athena: {athena_query_size:,} bytes")
        
        # If query is too large, use Spark to create a temp table and create a simple view
        if spark_dialect_query_size > 250000 or athena_query_size > 250000:
            print(f"⚠ Query too large for Athena API, using Spark materialization approach...")
            
            try:
                # Create the pivoted data using Spark
                print(f"Creating pivoted DataFrame in Spark...")
                pivoted_df = spark.sql(f"""
                    SELECT 
                        seriesid,
                        aod,
                        rssdid,
                        submission_ts,
                        {spark_pivot_columns_str}
                    FROM 
                        glue_catalog.{database_name}.{source_table_name}
                    WHERE
                        seriesid = '{seriesid_value}'
                    GROUP BY 
                        seriesid, aod, rssdid, submission_ts
                """)
                
                # Register as temp view in Spark
                temp_table_name = f"{view_name}_temp"
                pivoted_df.createOrReplaceTempView(temp_table_name)
                print(f"✓ Created temp view in Spark: {temp_table_name}")
                
                # Write to Glue catalog as a table (this will be queryable by both engines)
                print(f"Writing to Glue catalog table...")
                pivoted_df.write \
                    .format("iceberg") \
                    .mode("overwrite") \
                    .option("path", f"s3://{athena_output_location.split('/')[2]}/wide-views/{view_name}/") \
                    .saveAsTable(f"glue_catalog.{database_name}.{view_name}")
                
                print(f"✓ Created Iceberg table: {view_name}")
                print(f"✓✓✓ Table {view_name} successfully created and queryable by BOTH engines!")
                created_views.append(view_name)
            except Exception as table_error:
                print(f"✗ Error creating Iceberg table: {str(table_error)}")
                import traceback
                print(f"Traceback: {traceback.format_exc()}")
                failed_views.append(view_name)
            
        else:
            # Query is small enough, use the original approach
            print(f"Creating PROTECTED MULTI DIALECT view using Athena API...")
            print(f"DEBUG - Spark dialect SQL preview (first 500 chars):")
            print(spark_dialect_view_sql[:500])
            
            # Create view with Spark dialect first
            create_view_sql = f"""CREATE PROTECTED MULTI DIALECT VIEW {database_name}.{view_name}
            SECURITY DEFINER
            AS {spark_dialect_view_sql}"""
            
            print(f"DEBUG - Full CREATE VIEW statement preview (first 500 chars):")
            print(create_view_sql[:500])
            
            if execute_athena_query(create_view_sql, f"Creating view {view_name} with Spark dialect"):
                print(f"✓ View created with Spark dialect: {view_name}")
                
                # Step 3: Add Athena dialect using ALTER VIEW
                print(f"\nAdding Athena dialect...")
                alter_view_sql = f"""ALTER VIEW {database_name}.{view_name} ADD DIALECT AS {athena_view_sql}"""
                
                if execute_athena_query(alter_view_sql, f"Adding Athena dialect to {view_name}"):
                    print(f"✓ Athena dialect added successfully")
                    
                    # Test in Athena
                    test_query = f"SELECT COUNT(*) as row_count FROM {database_name}.{view_name}"
                    if execute_athena_query(test_query, f"Testing view {view_name} in Athena"):
                        print(f"✓ View works in Athena!")
                        print(f"\n✓✓✓ View {view_name} successfully created for BOTH engines!")
                        created_views.append(view_name)
                    else:
                        print(f"⚠ View created but Athena test failed")
                        failed_views.append(view_name)
                else:
                    print(f"✗ Failed to add Athena dialect")
                    failed_views.append(view_name)
            else:
                print(f"✗ Failed to create view with Spark dialect")
                failed_views.append(view_name)
            
    except Exception as e:
        print(f"\n✗✗✗ Error creating view {view_name}: {str(e)}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        failed_views.append(view_name)

# Summary
print(f"\n{'='*80}")
print(f"SUMMARY")
print(f"{'='*80}")
print(f"Total seriesid values processed: {len(seriesid_values)}")
print(f"Successfully created views: {len(created_views)}")
if created_views:
    for view in created_views:
        print(f"  ✓ {view}")

if failed_views:
    print(f"\nFailed views: {len(failed_views)}")
    for view in failed_views:
        print(f"  ✗ {view}")
else:
    print(f"\n✓✓✓ All views created successfully!")

print(f"\nView naming pattern: <seriesid>_wide_view")
print(f"Column pattern: <context_level1_mdrm>=<context_level1_value>:...:item_value")
print(f"View columns: seriesid, aod, rssdid, submission_ts, <item_mdrm_columns>")

job.commit()
