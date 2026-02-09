"""
Complete Lake Formation setup script.
This script performs all necessary steps to enable multi-dialect views on Iceberg tables.

Usage:
  python setup_lakeformation_complete.py \
    --bucket iceberg-data-storage-bucket \
    --prefix iceberg-data \
    --database iceberg_db \
    --table entity_data \
    --role-arn arn:aws:iam::123456789012:role/GlueJobRole \
    --region us-east-1
"""

import boto3
import argparse
import sys
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
    
    print(f"\n{'='*80}")
    print(f"STEP 1: Register S3 Location with Lake Formation")
    print(f"{'='*80}")
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
            print("\n⚠ You need Lake Formation admin permissions!")
            print("Add yourself as a Lake Formation admin:")
            print("  1. Go to AWS Lake Formation console")
            print("  2. Click 'Administrative roles and tasks'")
            print("  3. Add your IAM user/role as 'Data lake administrator'")
        return False
    
    print(f"\n{'='*80}")
    print(f"STEP 2: Grant Lake Formation Permissions to Glue Role")
    print(f"{'='*80}")
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
            return False
    
    # Wait for permissions to propagate
    print(f"\n⏳ Waiting 5 seconds for permissions to propagate...")
    time.sleep(5)
    
    print(f"\n{'='*80}")
    print(f"STEP 3: Mark Table as Lake Formation Managed")
    print(f"{'='*80}")
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
        print(f"  Make sure the table exists in the Glue catalog")
        return False
    except Exception as e:
        print(f"✗ Error: {str(e)}")
        return False
    
    print(f"\n{'='*80}")
    print(f"STEP 4: Verify Setup")
    print(f"{'='*80}")
    
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
        else:
            print(f"⚠ Table may not be properly registered")
            print(f"  Wait a few minutes and check again")
            
    except Exception as e:
        print(f"⚠ Could not verify: {str(e)}")
    
    # Verify Lake Formation permissions
    try:
        response = lakeformation_client.list_permissions(
            Principal={
                'DataLakePrincipalIdentifier': role_arn
            },
            ResourceType='DATA_LOCATION',
            MaxResults=100
        )
        
        found = False
        for permission in response.get('PrincipalResourcePermissions', []):
            resource = permission.get('Resource', {})
            data_location = resource.get('DataLocation', {})
            if data_location.get('ResourceArn') == resource_arn:
                found = True
                perms = permission.get('Permissions', [])
                print(f"✓ Lake Formation permissions verified: {', '.join(perms)}")
                break
        
        if not found:
            print(f"⚠ Could not find Lake Formation permissions (may still be propagating)")
            
    except Exception as e:
        print(f"⚠ Could not verify permissions: {str(e)}")
    
    return True

def main():
    parser = argparse.ArgumentParser(
        description='Complete Lake Formation setup for multi-dialect views on Iceberg tables',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example:
  python setup_lakeformation_complete.py \\
    --bucket iceberg-data-storage-bucket \\
    --prefix iceberg-data \\
    --database iceberg_db \\
    --table entity_data \\
    --role-arn arn:aws:iam::123456789012:role/GlueJobRole \\
    --region us-east-1

Prerequisites:
  - You must have Lake Formation admin permissions
  - The Glue table must already exist
  - The IAM role must exist
        """
    )
    
    parser.add_argument('--bucket', required=True, help='S3 bucket name')
    parser.add_argument('--prefix', default='', help='S3 prefix (optional)')
    parser.add_argument('--database', required=True, help='Glue database name')
    parser.add_argument('--table', required=True, help='Glue table name')
    parser.add_argument('--role-arn', required=True, help='Glue job IAM role ARN')
    parser.add_argument('--region', default='us-east-1', help='AWS region (default: us-east-1)')
    
    args = parser.parse_args()
    
    print(f"\n{'='*80}")
    print(f"Lake Formation Complete Setup")
    print(f"{'='*80}")
    print(f"Bucket:    {args.bucket}")
    print(f"Prefix:    {args.prefix or '(root)'}")
    print(f"Database:  {args.database}")
    print(f"Table:     {args.table}")
    print(f"Role ARN:  {args.role_arn}")
    print(f"Region:    {args.region}")
    print(f"{'='*80}")
    
    success = setup_lakeformation_complete(
        args.bucket,
        args.prefix,
        args.database,
        args.table,
        args.role_arn,
        args.region
    )
    
    print(f"\n{'='*80}")
    if success:
        print(f"✓ SETUP COMPLETED SUCCESSFULLY")
        print(f"{'='*80}")
        print(f"\nNext steps:")
        print(f"1. Wait 1-2 minutes for all changes to propagate")
        print(f"2. Re-run your Glue job: create-dynamic-entity-views")
        print(f"3. Multi-dialect views should now be created successfully")
        print(f"\nIf views still fail, check:")
        print(f"  - Glue job IAM role has lakeformation:GetDataAccess permission")
        print(f"  - Run: python scripts/troubleshooting-lakeformation.md")
        sys.exit(0)
    else:
        print(f"✗ SETUP FAILED")
        print(f"{'='*80}")
        print(f"\nSee error messages above for details.")
        print(f"For troubleshooting, see: docs/troubleshooting-lakeformation.md")
        sys.exit(1)

if __name__ == '__main__':
    main()
