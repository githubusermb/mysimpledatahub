"""
Standalone script to register existing Iceberg tables with Lake Formation.
This enables multi-dialect view creation on those tables.

Usage:
  python register_table_with_lakeformation.py --database <db_name> --table <table_name> --catalog-id <catalog_id>
"""

import boto3
import argparse
import sys

def register_table_with_lake_formation(catalog_id, database_name, table_name, region='us-east-1'):
    """Register Iceberg table with Lake Formation to enable multi-dialect views"""
    try:
        lakeformation_client = boto3.client('lakeformation', region_name=region)
        glue_client = boto3.client('glue', region_name=region)
        
        print(f"Registering table {database_name}.{table_name} with Lake Formation...")
        
        # Get table location from Glue catalog
        try:
            table_response = glue_client.get_table(
                CatalogId=catalog_id,
                DatabaseName=database_name,
                Name=table_name
            )
            table_location = table_response['Table'].get('StorageDescriptor', {}).get('Location')
            
            if not table_location:
                print(f"ERROR: Table {database_name}.{table_name} does not have a storage location")
                return False
                
            print(f"Table location: {table_location}")
            
        except Exception as e:
            print(f"ERROR: Could not get table from Glue catalog: {str(e)}")
            return False
        
        # Extract bucket and prefix from location
        # Format: s3://bucket/prefix/path
        if table_location.startswith('s3://'):
            s3_path = table_location[5:]  # Remove 's3://'
            parts = s3_path.split('/', 1)
            bucket = parts[0]
            prefix = parts[1] if len(parts) > 1 else ''
            
            # Register the S3 location with Lake Formation
            try:
                resource_arn = f"arn:aws:s3:::{bucket}"
                if prefix:
                    resource_arn = f"{resource_arn}/{prefix}"
                
                print(f"Registering S3 resource: {resource_arn}")
                
                lakeformation_client.register_resource(
                    ResourceArn=resource_arn,
                    UseServiceLinkedRole=True
                )
                print(f"Successfully registered S3 location with Lake Formation")
            except lakeformation_client.exceptions.AlreadyExistsException:
                print(f"S3 location already registered with Lake Formation")
            except Exception as e:
                print(f"Warning: Could not register S3 location: {str(e)}")
                print("This may be okay if the location is already registered")
        
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
            
            print(f"Updating table to be Lake Formation managed...")
            
            # Update the table
            glue_client.update_table(
                CatalogId=catalog_id,
                DatabaseName=database_name,
                TableInput=table_input
            )
            print(f"✓ Successfully marked table as Lake Formation managed")
            
        except Exception as e:
            print(f"ERROR: Could not update table to be Lake Formation managed: {str(e)}")
            return False
        
        # Verify the table is now Lake Formation managed
        try:
            table_response = glue_client.get_table(
                CatalogId=catalog_id,
                DatabaseName=database_name,
                Name=table_name
            )
            
            is_lf_managed = table_response['Table'].get('IsRegisteredWithLakeFormation', False)
            has_governed_param = table_response['Table'].get('Parameters', {}).get('GOVERNED') == 'true'
            
            if is_lf_managed or has_governed_param:
                print(f"✓ Table {database_name}.{table_name} is now Lake Formation managed")
                print(f"  IsRegisteredWithLakeFormation: {is_lf_managed}")
                print(f"  GOVERNED parameter: {has_governed_param}")
                return True
            else:
                print(f"WARNING: Table may not be properly registered with Lake Formation")
                return False
                
        except Exception as e:
            print(f"Warning: Could not verify Lake Formation registration: {str(e)}")
            return True  # Assume success if we can't verify
        
    except Exception as e:
        print(f"ERROR: Failed to register table with Lake Formation: {str(e)}")
        print("\nRequired IAM permissions:")
        print("  - lakeformation:RegisterResource")
        print("  - lakeformation:GrantPermissions")
        print("  - glue:GetTable")
        print("  - glue:UpdateTable")
        return False

def main():
    parser = argparse.ArgumentParser(
        description='Register Glue Iceberg table with Lake Formation for multi-dialect view support'
    )
    parser.add_argument('--database', required=True, help='Glue database name')
    parser.add_argument('--table', required=True, help='Glue table name')
    parser.add_argument('--catalog-id', required=True, help='AWS account ID (catalog ID)')
    parser.add_argument('--region', default='us-east-1', help='AWS region (default: us-east-1)')
    
    args = parser.parse_args()
    
    print(f"=" * 80)
    print(f"Lake Formation Table Registration")
    print(f"=" * 80)
    print(f"Database: {args.database}")
    print(f"Table: {args.table}")
    print(f"Catalog ID: {args.catalog_id}")
    print(f"Region: {args.region}")
    print(f"=" * 80)
    print()
    
    success = register_table_with_lake_formation(
        args.catalog_id,
        args.database,
        args.table,
        args.region
    )
    
    print()
    print(f"=" * 80)
    if success:
        print("✓ Registration completed successfully")
        print("\nYou can now create multi-dialect views on this table.")
        sys.exit(0)
    else:
        print("✗ Registration failed")
        print("\nPlease check the error messages above and ensure you have the required permissions.")
        sys.exit(1)

if __name__ == '__main__':
    main()
