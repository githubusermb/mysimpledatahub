"""
Grant Lake Formation permissions to Athena users for querying tables.

This script grants SELECT permissions to IAM users/roles so they can query
Lake Formation managed tables in Athena.

Usage:
  python grant_athena_user_permissions.py \
    --database iceberg_db \
    --table entity_data \
    --principal arn:aws:iam::123456789012:user/your-user \
    --region us-east-1
"""

import boto3
import argparse
import sys

def grant_athena_permissions(database, table, principal_arn, region='us-east-1'):
    """Grant Lake Formation permissions for Athena queries"""
    
    lakeformation_client = boto3.client('lakeformation', region_name=region)
    sts_client = boto3.client('sts', region_name=region)
    
    # Get current account ID
    account_id = sts_client.get_caller_identity()['Account']
    
    print(f"Granting Lake Formation permissions for Athena queries")
    print(f"Database: {database}")
    print(f"Table: {table}")
    print(f"Principal: {principal_arn}")
    print(f"Region: {region}")
    print()
    
    # Grant database permissions
    print("Step 1: Granting database permissions...")
    try:
        lakeformation_client.grant_permissions(
            Principal={
                'DataLakePrincipalIdentifier': principal_arn
            },
            Resource={
                'Database': {
                    'CatalogId': account_id,
                    'Name': database
                }
            },
            Permissions=['DESCRIBE']
        )
        print(f"✓ Granted DESCRIBE permission on database {database}")
    except Exception as e:
        error_msg = str(e)
        if "already exists" in error_msg.lower() or "duplicate" in error_msg.lower():
            print(f"✓ Database permission already granted")
        else:
            print(f"✗ Error granting database permission: {error_msg}")
            raise
    
    # Grant table permissions
    print("\nStep 2: Granting table permissions...")
    try:
        if table == '*':
            # Grant on all tables
            lakeformation_client.grant_permissions(
                Principal={
                    'DataLakePrincipalIdentifier': principal_arn
                },
                Resource={
                    'Table': {
                        'CatalogId': account_id,
                        'DatabaseName': database,
                        'TableWildcard': {}
                    }
                },
                Permissions=['SELECT', 'DESCRIBE', 'DROP', 'DELETE', 'INSERT', 'ALTER']
            )
            print(f"✓ Granted full table permissions on all tables in {database}")
        else:
            # Grant on specific table
            lakeformation_client.grant_permissions(
                Principal={
                    'DataLakePrincipalIdentifier': principal_arn
                },
                Resource={
                    'Table': {
                        'CatalogId': account_id,
                        'DatabaseName': database,
                        'Name': table
                    }
                },
                Permissions=['SELECT', 'DESCRIBE', 'DROP', 'DELETE', 'INSERT', 'ALTER']
            )
            print(f"✓ Granted full table permissions on table {database}.{table}")
    except Exception as e:
        error_msg = str(e)
        if "already exists" in error_msg.lower() or "duplicate" in error_msg.lower():
            print(f"✓ Table permission already granted")
        else:
            print(f"✗ Error granting table permission: {error_msg}")
            raise
    
    # Verify permissions
    print("\nStep 3: Verifying permissions...")
    try:
        response = lakeformation_client.list_permissions(
            Principal={
                'DataLakePrincipalIdentifier': principal_arn
            },
            ResourceType='TABLE',
            MaxResults=100
        )
        
        found = False
        for permission in response.get('PrincipalResourcePermissions', []):
            resource = permission.get('Resource', {})
            table_resource = resource.get('Table', {})
            if table_resource.get('DatabaseName') == database:
                if table == '*' or table_resource.get('Name') == table or 'TableWildcard' in table_resource:
                    found = True
                    perms = permission.get('Permissions', [])
                    print(f"✓ Verified permissions: {', '.join(perms)}")
                    break
        
        if not found:
            print(f"⚠ Could not verify permissions (may still be propagating)")
    except Exception as e:
        print(f"⚠ Could not verify permissions: {str(e)}")
    
    return True

def get_current_user_arn():
    """Get the ARN of the current IAM user/role"""
    sts_client = boto3.client('sts')
    identity = sts_client.get_caller_identity()
    return identity['Arn']

def main():
    parser = argparse.ArgumentParser(
        description='Grant Lake Formation permissions to Athena users',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Grant permissions to current user
  python grant_athena_user_permissions.py \\
    --database iceberg_db \\
    --table entity_data

  # Grant permissions to specific user
  python grant_athena_user_permissions.py \\
    --database iceberg_db \\
    --table entity_data \\
    --principal arn:aws:iam::123456789012:user/john.doe

  # Grant permissions on all tables
  python grant_athena_user_permissions.py \\
    --database iceberg_db \\
    --table '*' \\
    --principal arn:aws:iam::123456789012:role/AthenaUserRole

  # Grant permissions to multiple users (run multiple times)
  python grant_athena_user_permissions.py --database iceberg_db --table entity_data --principal arn:aws:iam::123456789012:user/user1
  python grant_athena_user_permissions.py --database iceberg_db --table entity_data --principal arn:aws:iam::123456789012:user/user2
        """
    )
    
    parser.add_argument('--database', required=True, help='Glue database name')
    parser.add_argument('--table', default='*', help='Glue table name (use * for all tables)')
    parser.add_argument('--principal', help='IAM user/role ARN (defaults to current user)')
    parser.add_argument('--region', default='us-east-1', help='AWS region (default: us-east-1)')
    
    args = parser.parse_args()
    
    # Get principal ARN
    if args.principal:
        principal_arn = args.principal
    else:
        print("No principal specified, using current user...")
        principal_arn = get_current_user_arn()
        print(f"Current user ARN: {principal_arn}\n")
    
    print(f"=" * 80)
    print(f"Grant Lake Formation Permissions for Athena")
    print(f"=" * 80)
    print()
    
    try:
        success = grant_athena_permissions(
            args.database,
            args.table,
            principal_arn,
            args.region
        )
        
        print()
        print(f"=" * 80)
        if success:
            print(f"✓ PERMISSIONS GRANTED SUCCESSFULLY")
            print(f"=" * 80)
            print()
            print("You can now query the table in Athena:")
            print(f"  SELECT * FROM {args.database}.{args.table} LIMIT 10;")
            print()
            print("Note: It may take 30-60 seconds for permissions to propagate.")
            sys.exit(0)
        else:
            print(f"✗ FAILED TO GRANT PERMISSIONS")
            print(f"=" * 80)
            sys.exit(1)
            
    except Exception as e:
        print()
        print(f"=" * 80)
        print(f"✗ ERROR")
        print(f"=" * 80)
        print(f"Error: {str(e)}")
        print()
        print("Required permissions:")
        print("  - lakeformation:GrantPermissions")
        print("  - lakeformation:ListPermissions")
        print("  - sts:GetCallerIdentity")
        print()
        print("You must be a Lake Formation administrator to grant permissions.")
        sys.exit(1)

if __name__ == '__main__':
    main()
