"""
Script to grant Lake Formation permissions on S3 locations.
This must be run BEFORE registering tables with Lake Formation.

Usage:
  python grant_lakeformation_s3_permissions.py --bucket <bucket_name> --prefix <prefix> --role-arn <glue_role_arn>
"""

import boto3
import argparse
import sys
import time

def grant_lakeformation_permissions(bucket, prefix, role_arn, region='us-east-1'):
    """Grant Lake Formation permissions on S3 location"""
    try:
        lakeformation_client = boto3.client('lakeformation', region_name=region)
        sts_client = boto3.client('sts', region_name=region)
        
        # Get current account ID
        account_id = sts_client.get_caller_identity()['Account']
        
        # Construct S3 resource ARN
        if prefix:
            resource_arn = f"arn:aws:s3:::{bucket}/{prefix}"
        else:
            resource_arn = f"arn:aws:s3:::{bucket}"
        
        print(f"Step 1: Registering S3 location with Lake Formation")
        print(f"  Resource: {resource_arn}")
        
        # Register the S3 location with Lake Formation
        try:
            response = lakeformation_client.register_resource(
                ResourceArn=resource_arn,
                UseServiceLinkedRole=True
            )
            print(f"  ✓ Successfully registered S3 location")
        except lakeformation_client.exceptions.AlreadyExistsException:
            print(f"  ✓ S3 location already registered")
        except Exception as e:
            print(f"  ✗ Error registering S3 location: {str(e)}")
            if "Access Denied" in str(e):
                print("\n  Required permissions:")
                print("    - lakeformation:RegisterResource")
                print("    - iam:CreateServiceLinkedRole (if service-linked role doesn't exist)")
            raise
        
        # Wait a moment for registration to propagate
        time.sleep(2)
        
        print(f"\nStep 2: Granting Lake Formation permissions to role")
        print(f"  Role: {role_arn}")
        
        # Grant permissions to the Glue role on the S3 location
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
            print(f"  ✓ Successfully granted DATA_LOCATION_ACCESS permission")
        except lakeformation_client.exceptions.ConcurrentModificationException:
            print(f"  ⚠ Concurrent modification - retrying...")
            time.sleep(2)
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
            print(f"  ✓ Successfully granted DATA_LOCATION_ACCESS permission")
        except Exception as e:
            error_msg = str(e)
            if "already exists" in error_msg.lower() or "duplicate" in error_msg.lower():
                print(f"  ✓ Permission already granted")
            else:
                print(f"  ✗ Error granting permissions: {error_msg}")
                raise
        
        # Verify the permissions
        print(f"\nStep 3: Verifying permissions")
        try:
            response = lakeformation_client.list_permissions(
                Principal={
                    'DataLakePrincipalIdentifier': role_arn
                },
                ResourceType='DATA_LOCATION',
                MaxResults=100
            )
            
            # Check if our resource is in the list
            found = False
            for permission in response.get('PrincipalResourcePermissions', []):
                resource = permission.get('Resource', {})
                data_location = resource.get('DataLocation', {})
                if data_location.get('ResourceArn') == resource_arn:
                    found = True
                    perms = permission.get('Permissions', [])
                    print(f"  ✓ Verified permissions: {', '.join(perms)}")
                    break
            
            if not found:
                print(f"  ⚠ Could not verify permissions (may still be propagating)")
        except Exception as e:
            print(f"  ⚠ Could not verify permissions: {str(e)}")
        
        return True
        
    except Exception as e:
        print(f"\n✗ Failed to grant Lake Formation permissions: {str(e)}")
        return False

def main():
    parser = argparse.ArgumentParser(
        description='Grant Lake Formation permissions on S3 location for Glue role'
    )
    parser.add_argument('--bucket', required=True, help='S3 bucket name')
    parser.add_argument('--prefix', default='', help='S3 prefix (optional)')
    parser.add_argument('--role-arn', required=True, help='Glue job IAM role ARN')
    parser.add_argument('--region', default='us-east-1', help='AWS region (default: us-east-1)')
    
    args = parser.parse_args()
    
    print(f"=" * 80)
    print(f"Lake Formation S3 Permissions Setup")
    print(f"=" * 80)
    print(f"Bucket: {args.bucket}")
    print(f"Prefix: {args.prefix or '(root)'}")
    print(f"Role ARN: {args.role_arn}")
    print(f"Region: {args.region}")
    print(f"=" * 80)
    print()
    
    success = grant_lakeformation_permissions(
        args.bucket,
        args.prefix,
        args.role_arn,
        args.region
    )
    
    print()
    print(f"=" * 80)
    if success:
        print("✓ Lake Formation permissions granted successfully")
        print("\nNext steps:")
        print("1. Wait 30-60 seconds for permissions to propagate")
        print("2. Run the table registration script:")
        print("   python register_table_with_lakeformation.py \\")
        print("     --database <your_database> \\")
        print("     --table <your_table> \\")
        print("     --catalog-id <account_id>")
        sys.exit(0)
    else:
        print("✗ Failed to grant Lake Formation permissions")
        print("\nRequired IAM permissions for the user running this script:")
        print("  - lakeformation:RegisterResource")
        print("  - lakeformation:GrantPermissions")
        print("  - lakeformation:ListPermissions")
        print("  - sts:GetCallerIdentity")
        sys.exit(1)

if __name__ == '__main__':
    main()
