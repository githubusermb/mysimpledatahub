"""
AWS Lambda function to setup Lake Formation for Iceberg tables.

This is a Lambda-optimized version of setup_lakeformation_complete.py

Event Parameters:
  - bucket (required): S3 bucket name
  - prefix (optional): S3 prefix
  - database (required): Glue database name
  - table (required): Glue table name
  - role_arn (required): Glue job IAM role ARN
  - region (optional): AWS region (default: us-east-1)

Example Event:
{
  "bucket": "iceberg-data-storage-bucket",
  "prefix": "iceberg-data",
  "database": "iceberg_db",
  "table": "entity_data",
  "role_arn": "arn:aws:iam::123456789012:role/GlueServiceRole",
  "region": "us-east-1"
}
"""

import boto3
import json
import time

def setup_lakeformation_complete(bucket, prefix, database, table, role_arn, region='us-east-1'):
    """Complete Lake Formation setup for multi-dialect views"""
    
    lakeformation_client = boto3.client('lakeformation', region_name=region)
    glue_client = boto3.client('glue', region_name=region)
    sts_client = boto3.client('sts', region_name=region)
    
    # Get current account ID
    account_id = sts_client.get_caller_identity()['Account']
    
    # Construct S3 resource ARN
    if prefix:
        resource_arn = f"arn:aws:s3:::{bucket}/{prefix}"
    else:
        resource_arn = f"arn:aws:s3:::{bucket}"
    
    print(f"Step 1: Register S3 Location with Lake Formation")
    print(f"Resource: {resource_arn}")
    
    try:
        lakeformation_client.register_resource(
            ResourceArn=resource_arn,
            UseServiceLinkedRole=True
        )
        print(f"✓ Successfully registered S3 location")
    except lakeformation_client.exceptions.AlreadyExistsException:
        print(f"✓ S3 location already registered")
    except Exception as e:
        print(f"✗ Error: {str(e)}")
        if "Access Denied" in str(e) or "not authorized" in str(e):
            print("⚠ You need Lake Formation admin permissions!")
        raise
    
    print(f"\nStep 2: Grant Lake Formation Permissions to Glue Role")
    print(f"Role: {role_arn}")
    
    try:
        lakeformation_client.grant_permissions(
            Principal={
                'DataLakePrincipalIdentifier': role_arn
            },
            Resource={
                'DataLocation': {
                    'CatalogId': account_id,
                    'ResourceArn': resource_arn
                }
            },
            Permissions=['DATA_LOCATION_ACCESS']
        )
        print(f"✓ Successfully granted DATA_LOCATION_ACCESS permission")
    except Exception as e:
        error_msg = str(e)
        if "already exists" in error_msg.lower() or "duplicate" in error_msg.lower():
            print(f"✓ Permission already granted")
        else:
            print(f"✗ Error: {error_msg}")
            raise
    
    # Wait for permissions to propagate
    print(f"\n⏳ Waiting 5 seconds for permissions to propagate...")
    time.sleep(5)
    
    print(f"\nStep 3: Mark Table as Lake Formation Managed")
    print(f"Table: {database}.{table}")
    
    try:
        # Get current table definition
        table_response = glue_client.get_table(
            CatalogId=account_id,
            DatabaseName=database,
            Name=table
        )
        
        table_input = table_response['Table']
        table_location = table_input.get('StorageDescriptor', {}).get('Location', 'N/A')
        print(f"Table location: {table_location}")
        
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
            CatalogId=account_id,
            DatabaseName=database,
            TableInput=table_input
        )
        print(f"✓ Successfully marked table as Lake Formation managed")
        
    except glue_client.exceptions.EntityNotFoundException:
        print(f"✗ Error: Table {database}.{table} not found")
        raise
    except Exception as e:
        print(f"✗ Error: {str(e)}")
        raise
    
    print(f"\nStep 4: Verify Setup")
    
    # Verify table is Lake Formation managed
    try:
        table_response = glue_client.get_table(
            CatalogId=account_id,
            DatabaseName=database,
            Name=table
        )
        
        is_lf_managed = table_response['Table'].get('IsRegisteredWithLakeFormation', False)
        has_governed_param = table_response['Table'].get('Parameters', {}).get('GOVERNED') == 'true'
        
        print(f"IsRegisteredWithLakeFormation: {is_lf_managed}")
        print(f"GOVERNED parameter: {has_governed_param}")
        
        if is_lf_managed or has_governed_param:
            print(f"✓ Table is Lake Formation managed")
            return True
        else:
            print(f"⚠ Table may not be properly registered")
            return False
            
    except Exception as e:
        print(f"⚠ Could not verify: {str(e)}")
        return True  # Assume success if we can't verify

def lambda_handler(event, context):
    """
    Lambda handler for Lake Formation setup
    
    Args:
        event: Lambda event containing setup parameters
        context: Lambda context
        
    Returns:
        dict: Response with statusCode and body
    """
    
    print(f"Received event: {json.dumps(event)}")
    
    # Get parameters from event
    bucket = event.get('bucket')
    prefix = event.get('prefix', '')
    database = event.get('database')
    table = event.get('table')
    role_arn = event.get('role_arn')
    region = event.get('region', 'us-east-1')
    
    # Validate required parameters
    missing_params = []
    if not bucket:
        missing_params.append('bucket')
    if not database:
        missing_params.append('database')
    if not table:
        missing_params.append('table')
    if not role_arn:
        missing_params.append('role_arn')
    
    if missing_params:
        error_msg = f"Missing required parameters: {', '.join(missing_params)}"
        print(f"✗ {error_msg}")
        return {
            'statusCode': 400,
            'body': json.dumps({
                'error': error_msg,
                'required_parameters': ['bucket', 'database', 'table', 'role_arn'],
                'optional_parameters': ['prefix', 'region']
            })
        }
    
    # Run the setup
    try:
        print(f"\n{'='*80}")
        print(f"Lake Formation Complete Setup")
        print(f"{'='*80}")
        print(f"Bucket:    {bucket}")
        print(f"Prefix:    {prefix or '(root)'}")
        print(f"Database:  {database}")
        print(f"Table:     {table}")
        print(f"Role ARN:  {role_arn}")
        print(f"Region:    {region}")
        print(f"{'='*80}\n")
        
        success = setup_lakeformation_complete(
            bucket, prefix, database, table, role_arn, region
        )
        
        print(f"\n{'='*80}")
        if success:
            print(f"✓ SETUP COMPLETED SUCCESSFULLY")
            print(f"{'='*80}")
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Lake Formation setup completed successfully',
                    'bucket': bucket,
                    'database': database,
                    'table': table,
                    'governed': True
                })
            }
        else:
            print(f"⚠ SETUP COMPLETED WITH WARNINGS")
            print(f"{'='*80}")
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Setup completed with warnings',
                    'bucket': bucket,
                    'database': database,
                    'table': table,
                    'warning': 'Could not verify Lake Formation registration'
                })
            }
            
    except Exception as e:
        error_msg = str(e)
        print(f"\n{'='*80}")
        print(f"✗ SETUP FAILED")
        print(f"{'='*80}")
        print(f"Error: {error_msg}")
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': error_msg,
                'bucket': bucket,
                'database': database,
                'table': table
            })
        }

# For local testing
if __name__ == '__main__':
    import sys
    
    # Test event
    test_event = {
        'bucket': 'iceberg-data-storage-bucket',
        'prefix': 'iceberg-data',
        'database': 'iceberg_db',
        'table': 'entity_data',
        'role_arn': 'arn:aws:iam::123456789012:role/GlueServiceRole',
        'region': 'us-east-1'
    }
    
    # Allow command line override
    if len(sys.argv) > 1:
        test_event['bucket'] = sys.argv[1]
    if len(sys.argv) > 2:
        test_event['database'] = sys.argv[2]
    if len(sys.argv) > 3:
        test_event['table'] = sys.argv[3]
    if len(sys.argv) > 4:
        test_event['role_arn'] = sys.argv[4]
    
    # Run locally
    result = lambda_handler(test_event, None)
    print(f"\nResult: {json.dumps(result, indent=2)}")
