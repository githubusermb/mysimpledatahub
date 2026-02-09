"""
Create Dynamic Views using Spark SQL with PROTECTED MULTI DIALECT VIEW
Based on: https://docs.aws.amazon.com/glue/latest/dg/catalog-views.html

This creates views that work in both Glue Spark and Athena using the
AWS Glue Data Catalog multi-dialect view feature.
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
    'view_prefix'
])

database_name = args['database_name']
source_table_name = args['source_table_name']
view_prefix = args['view_prefix']

print(f"Creating PROTECTED MULTI DIALECT views for {database_name}.{source_table_name}")
print(f"View prefix: {view_prefix}")

# Initialize Glue client for dropping views
glue_client = boto3.client('glue')

# Read the source table using the Glue catalog
source_df = spark.sql(f"SELECT * FROM glue_catalog.{database_name}.{source_table_name}")

# Get distinct seriesid values
seriesid_values = [row.seriesid for row in source_df.select("seriesid").distinct().collect()]
print(f"Found {len(seriesid_values)} distinct seriesid values")

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
    full_view_name = f"glue_catalog.{database_name}.{view_name}"
    
    # Build the pivot columns for the SQL
    pivot_columns = []
    for key in entity_key_values:
        # Use backticks for column names in Spark SQL
        col_expr = f"MAX(CASE WHEN key = '{key}' THEN value ELSE NULL END) AS `{key}`"
        pivot_columns.append(col_expr)
    
    pivot_columns_str = ',\n        '.join(pivot_columns)
    
    # Build the SQL for the view definition
    # Use glue_catalog.database.table format for Spark
    view_sql = f"""
    SELECT 
        seriesid,
        aod,
        rssdid,
        submissionts,
        {pivot_columns_str}
    FROM 
        glue_catalog.{database_name}.{source_table_name}
    WHERE
        seriesid = '{seriesid_value}'
    GROUP BY 
        seriesid, aod, rssdid, submissionts
    """
    
    try:
        # Drop existing view if it exists
        try:
            glue_client.delete_table(
                DatabaseName=database_name,
                Name=view_name
            )
            print(f"Dropped existing view: {view_name}")
        except glue_client.exceptions.EntityNotFoundException:
            print(f"View {view_name} doesn't exist, will create new")
        except Exception as e:
            print(f"Note: Could not drop view {view_name}: {str(e)}")
        
        # Create PROTECTED MULTI DIALECT VIEW using Spark SQL
        # This is the AWS Glue Data Catalog approach for multi-engine views
        print(f"Creating PROTECTED MULTI DIALECT VIEW: {view_name}")
        
        create_view_sql = f"""
        CREATE PROTECTED MULTI DIALECT VIEW {full_view_name}
        SECURITY DEFINER
        AS {view_sql}
        """
        
        print(f"Executing CREATE VIEW statement...")
        spark.sql(create_view_sql)
        
        print(f"✓ Successfully created view: {view_name}")
        created_views.append(view_name)
        
        # Verify the view by querying it in Spark
        print(f"Verifying view in Spark...")
        verify_df = spark.sql(f"SELECT COUNT(*) as count FROM {full_view_name}")
        row_count = verify_df.collect()[0]['count']
        print(f"✓ View verified in Spark: {row_count} rows")
        
        # Show sample data
        print(f"Sample data from view:")
        sample_df = spark.sql(f"SELECT * FROM {full_view_name} LIMIT 3")
        sample_df.show(truncate=False)
        
    except Exception as e:
        print(f"✗ Error creating view {view_name}: {str(e)}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        failed_views.append({
            'view': view_name,
            'error': str(e)
        })

# Print summary
print(f"\n{'='*80}")
print(f"VIEW CREATION SUMMARY")
print(f"{'='*80}")
print(f"Total seriesid values processed: {len(seriesid_values)}")
print(f"Views created successfully: {len(created_views)}")
print(f"Views failed: {len(failed_views)}")

if created_views:
    print(f"\n✓ Successfully created views:")
    for view in created_views:
        print(f"  - {database_name}.{view}")

if failed_views:
    print(f"\n✗ Failed views:")
    for result in failed_views:
        print(f"  - {result['view']}: {result['error']}")

print(f"\nViews can be queried in:")
print(f"  - Athena: SELECT * FROM {database_name}.<view_name>")
print(f"  - Glue Spark: spark.sql('SELECT * FROM glue_catalog.{database_name}.<view_name>')")

print(f"\nNote: These are PROTECTED MULTI DIALECT views created using Spark SQL.")
print(f"They should work in both Athena and Glue Spark (AWS Glue 5.0+).")

print("\nDynamic view creation job completed")
job.commit()
