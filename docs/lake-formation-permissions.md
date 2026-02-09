# Lake Formation Permissions for Multi-Dialect Views

## Problem
When creating multi-dialect views in AWS Glue, you may encounter this error:
```
InvalidObjectException: Multi Dialect views may only reference Lake Formation managed tables.
```

## Solution
The Iceberg table must be registered with AWS Lake Formation before creating multi-dialect views.

## Required IAM Permissions

Add these permissions to your Glue job IAM role:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "lakeformation:RegisterResource",
        "lakeformation:DeregisterResource",
        "lakeformation:DescribeResource",
        "lakeformation:ListResources",
        "lakeformation:GrantPermissions",
        "lakeformation:RevokePermissions",
        "lakeformation:GetDataAccess"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "glue:GetTable",
        "glue:UpdateTable",
        "glue:GetDatabase",
        "glue:CreateTable",
        "glue:DeleteTable"
      ],
      "Resource": [
        "arn:aws:glue:us-east-1:*:catalog",
        "arn:aws:glue:us-east-1:*:database/*",
        "arn:aws:glue:us-east-1:*:table/*/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::your-iceberg-bucket/*",
        "arn:aws:s3:::your-iceberg-bucket"
      ]
    }
  ]
}
```

## Implementation Options

### Option 1: Automatic Registration (Recommended)
The updated `glue_csv_to_iceberg.py` script now automatically registers tables with Lake Formation when they are created or updated.

### Option 2: Manual Registration
Use the standalone script to register existing tables:

```bash
python mydatahub/scripts/register_table_with_lakeformation.py \
  --database your_database \
  --table your_table \
  --catalog-id 123456789012 \
  --region us-east-1
```

### Option 3: AWS Console
1. Open AWS Lake Formation console
2. Go to "Data lake locations"
3. Click "Register location" and add your S3 bucket
4. Go to "Tables" in the Data Catalog
5. Select your table
6. Click "Actions" → "Edit table"
7. Check "Use Lake Formation permissions"

### Option 4: AWS CLI
```bash
# Register S3 location
aws lakeformation register-resource \
  --resource-arn arn:aws:s3:::your-bucket/your-prefix \
  --use-service-linked-role

# Update table to be Lake Formation managed
aws glue update-table \
  --database-name your_database \
  --table-input '{
    "Name": "your_table",
    "Parameters": {
      "GOVERNED": "true"
    }
  }'
```

## Verification

After registration, verify the table is Lake Formation managed:

```python
import boto3

glue = boto3.client('glue')
response = glue.get_table(
    DatabaseName='your_database',
    Name='your_table'
)

print(f"IsRegisteredWithLakeFormation: {response['Table'].get('IsRegisteredWithLakeFormation', False)}")
print(f"GOVERNED parameter: {response['Table'].get('Parameters', {}).get('GOVERNED')}")
```

## Troubleshooting

### Error: Access Denied
- Ensure your IAM role has the Lake Formation permissions listed above
- Check that you have permissions on the S3 bucket

### Error: Resource Already Exists
- The S3 location is already registered - this is okay
- Continue with updating the table to be Lake Formation managed

### Views Still Failing
- Verify the table shows as Lake Formation managed in the Glue console
- Check that the S3 location is registered in Lake Formation
- Ensure the Glue job role has Lake Formation data access permissions
