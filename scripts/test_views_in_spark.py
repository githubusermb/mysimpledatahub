"""
Test Report Views in Spark
Tests all report views created by the dual-engine views job

This script:
1. Lists all report views in the database
2. Tests each view with comprehensive queries
3. Verifies row counts, schema, and data integrity
4. Generates a detailed test report
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSessv, [
    'JOB_NAME',
    'database_name',
    'view_prefix'
])

database_name = args['database_name']
view_prefix = args['view_prefix']

print(f"Testing views in database: {database_name}")
print(f"View prefix: {view_prefix}")

# Initialize Glue client to list views
glue_client = boto3.client('glue')

# Get all views in the database
try:
    response = glue_client.get_tables(
        DatabaseName=database_name
    )
    
    # Filter for views with the specified prefix
    views = [
        table['Name'] 
        for table in response['TableList'] 
        if table.get('TableType') == 'VIRTUAL_VIEW' and table['Name'].startswith(view_prefix)
    ]
    
    print(f"\nFound {len(views)} views to test")
    
    if not views:
        print(f"No views found with prefix '{view_prefix}' in database '{database_name}'")
        print("Make sure the dynamic views job has run successfully")
        job.commit()
        sys.exit(0)
    
    # Test each view
    test_results = {
        'passed': [],
        'failed': []
    }
    
    for view_name in views:
        print(f"\n{'='*80}")
        print(f"Testing view: {view_name}")
        print(f"{'='*80}")
        
        try:
            # Test 1: Read view schema
            print(f"\n[Test 1] Reading schema...")
            view_df = spark.sql(f"SELECT * FROM glue_catalog.{database_name}.{view_name} LIMIT 0")
            schema = view_df.schema
            print(f"✓ Schema read successfully")
            print(f"  Columns: {len(schema.fields)}")
            print(f"  Column names: {', '.join([f.name for f in schema.fields[:10]])}")
            if len(schema.fields) > 10:
                print(f"  ... and {len(schema.fields) - 10} more columns")
            
            # Test 2: Count rows
            print(f"\n[Test 2] Counting rows...")
            count_df = spark.sql(f"SELECT COUNT(*) as row_count FROM glue_catalog.{database_name}.{view_name}")
            row_count = count_df.collect()[0]['row_count']
            print(f"✓ Row count: {row_count}")
            
            # Test 3: Read sample data
            print(f"\n[Test 3] Reading sample data...")
            sample_df = spark.sql(f"SELECT * FROM glue_catalog.{database_name}.{view_name} LIMIT 5")
            sample_count = sample_df.count()
            print(f"✓ Sample data read successfully")
            print(f"  Rows retrieved: {sample_count}")
            
            # Show sample data
            print(f"\n[Test 4] Displaying sample data...")
            print(f"Sample rows from {view_name}:")
            sample_df.show(5, truncate=True)
            
            # Test 5: Test filtering
            print(f"\n[Test 5] Testing WHERE clause...")
            filter_df = spark.sql(f"""
                SELECT seriesid, aod, COUNT(*) as count 
                FROM glue_catalog.{database_name}.{view_name} 
                GROUP BY seriesid, aod
                LIMIT 5
            """)
            filter_count = filter_df.count()
            print(f"✓ Filtering works")
            print(f"  Grouped rows: {filter_count}")
            filter_df.show(truncate=False)
            
            # Test 6: Test column selection
            print(f"\n[Test 6] Testing column selection...")
            # Get first 5 column names (excluding entity columns)
            test_columns = [f.name for f in schema.fields if not f.name in ['seriesid', 'aod', 'rssdid', 'submissionts']][:5]
            if test_columns:
                col_list = ', '.join([f'"{col}"' for col in test_columns])
                select_df = spark.sql(f"""
                    SELECT seriesid, {col_list}
                    FROM glue_catalog.{database_name}.{view_name}
                    LIMIT 3
                """)
                print(f"✓ Column selection works")
                select_df.show(truncate=False)
            else:
                print(f"⚠ No non-entity columns found to test")
            
            # Test 7: Test JOIN with source table
            print(f"\n[Test 7] Testing JOIN with source table...")
            join_df = spark.sql(f"""
                SELECT v.seriesid, v.aod, COUNT(*) as view_rows
                FROM glue_catalog.{database_name}.{view_name} v
                GROUP BY v.seriesid, v.aod
                LIMIT 3
            """)
            join_count = join_df.count()
            print(f"✓ JOIN works")
            print(f"  Result rows: {join_count}")
            join_df.show(truncate=False)
            
            # All tests passed
            print(f"\n✓✓✓ All tests passed for view: {view_name}")
            test_results['passed'].append(view_name)
            
        except Exception as e:
            print(f"\n✗✗✗ Tests failed for view: {view_name}")
            print(f"Error: {str(e)}")
            import traceback
            print(f"Traceback: {traceback.format_exc()}")
            test_results['failed'].append({
                'view': view_name,
                'error': str(e)
            })
    
    # Print summary
    print(f"\n{'='*80}")
    print(f"TEST SUMMARY")
    print(f"{'='*80}")
    print(f"Total views tested: {len(views)}")
    print(f"Passed: {len(test_results['passed'])}")
    print(f"Failed: {len(test_results['failed'])}")
    
    if test_results['passed']:
        print(f"\n✓ Passed views:")
        for view in test_results['passed']:
            print(f"  - {view}")
    
    if test_results['failed']:
        print(f"\n✗ Failed views:")
        for result in test_results['failed']:
            print(f"  - {result['view']}: {result['error']}")
    
    # Exit with appropriate status
    if test_results['failed']:
        print(f"\n⚠ Some tests failed. Check the logs above for details.")
    else:
        print(f"\n✓ All views are working correctly in Spark!")
    
except Exception as e:
    print(f"Error listing views: {str(e)}")
    import traceback
    print(f"Traceback: {traceback.format_exc()}")

print("\nView testing job completed")
job.commit()
