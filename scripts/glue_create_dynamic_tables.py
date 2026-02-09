import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, struct, lit, expr

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
    'table_prefix'
])

database_name = args['database_name']
source_table_name = args['source_table_name']
table_prefix = args['table_prefix']

print(f"Creating dynamic Iceberg tables for {database_name}.{source_table_name}")
print(f"Table prefix: {table_prefix}")

# Read the source table using the Glue catalog
source_df = spark.sql(f"SELECT * FROM glue_catalog.{database_name}.{source_table_name}")

# Get distinct entity1 values
entity1_values = [row.entity1 for row in source_df.select("entity1").distinct().collect()]
print(f"Found {len(entity1_values)} distinct entity1 values")

# Create materialized tables for each entity1 value
created_tables = []
failed_tables = []

for entity1_value in entity1_values:
    print(f"\n{'='*80}")
    print(f"Processing entity1 = '{entity1_value}'")
    print(f"{'='*80}")
    
    # Filter data for this entity1 value
    entity_df = source_df.filter(col("entity1") == entity1_value)
    
    # Get distinct key values for this specific entity1 value
    entity_key_values = [row.key for row in entity_df.select("key").distinct().collect()]
    print(f"Found {len(entity_key_values)} distinct key values")
    
    # Create table name based on entity1 value
    table_name = f"{table_prefix}_{entity1_value}"
    full_table_name = f"glue_catalog.{database_name}.{table_name}"
    
    # Build the pivot query
    case_statements = []
    for key in entity_key_values:
        case_statements.append(f"MAX(CASE WHEN key = '{key}' THEN value ELSE NULL END) AS `{key}`")
    
    pivot_query = f"""
    SELECT 
        entity1,
        entity2,
        entity3,
        entity4,
        {', '.join(case_statements)}
    FROM 
        glue_catalog.{database_name}.{source_table_name}
    WHERE
        entity1 = '{entity1_value}'
    GROUP BY 
        entity1, entity2, entity3, entity4
    """
    
    try:
        print(f"Executing pivot query...")
        pivot_df = spark.sql(pivot_query)
        
        row_count = pivot_df.count()
        column_count = len(pivot_df.columns)
        print(f"Pivot result: {row_count} rows, {column_count} columns")
        
        # Write as Iceberg table
        print(f"Writing Iceberg table: {full_table_name}")
        pivot_df.writeTo(full_table_name) \
            .using("iceberg") \
            .tableProperty("format-version", "2") \
            .tableProperty("write.parquet.compression-codec", "snappy") \
            .createOrReplace()
        
        print(f"✓ Successfully created table: {table_name}")
        created_tables.append(table_name)
        
        # Verify the table
        print(f"Verifying table...")
        verify_df = spark.sql(f"SELECT COUNT(*) as count FROM {full_table_name}")
        verify_count = verify_df.collect()[0]['count']
        print(f"✓ Table verified: {verify_count} rows")
        
        # Show sample data
        print(f"Sample data:")
        sample_df = spark.sql(f"SELECT * FROM {full_table_name} LIMIT 3")
        sample_df.show(truncate=False)
        
    except Exception as e:
        print(f"✗ Error creating table {table_name}: {str(e)}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        failed_tables.append({
            'table': table_name,
            'error': str(e)
        })

# Print summary
print(f"\n{'='*80}")
print(f"TABLE CREATION SUMMARY")
print(f"{'='*80}")
print(f"Total entity1 values processed: {len(entity1_values)}")
print(f"Tables created successfully: {len(created_tables)}")
print(f"Tables failed: {len(failed_tables)}")

if created_tables:
    print(f"\n✓ Successfully created tables:")
    for table in created_tables:
        print(f"  - {database_name}.{table}")

if failed_tables:
    print(f"\n✗ Failed tables:")
    for result in failed_tables:
        print(f"  - {result['table']}: {result['error']}")

print(f"\nTables can be queried in:")
print(f"  - Athena: SELECT * FROM {database_name}.<table_name>")
print(f"  - Glue Spark: spark.sql('SELECT * FROM glue_catalog.{database_name}.<table_name>')")

print(f"\nNote: These are materialized Iceberg tables, not views.")
print(f"They work in both Athena and Spark, but need to be refreshed when source data changes.")

print("\nDynamic table creation job completed")
job.commit()
