# Troubleshooting Lake Formation Issues

### Error: Unknown parameter in TableInput: "IsMultiDialectView"

**Full Error:**
```
Parameter validation failed:
Unknown parameter in TableInput: "IsMultiDialectView", must be one of: Name, Description, Owner, LastAccessTime, LastAnalyzedTime, Retention, StorageDescriptor, PartitionKeys, ViewOriginalText, ViewExpandedText, TableType, Parameters, TargetTable, ViewDefinition
```

**Root Cause:** The Glue API returns read-only fields when you call `get_table()`, but these fields cannot be included when calling `update_table()`. The `IsMultiDialectView` field is one of these read-only fields.

**Solution:**

The scripts have been updated to remove all known read-only fields. If you're using an older version, update your scripts or manually remove these fields:

```python
# Remove all read-only fields
read_only_fields = [
    'DatabaseName', 'CreateTime', 'UpdateTime', 'CreatedBy', 
    'IsRegisteredWithLakeFormation', 'CatalogId', 'VersionId',
    'IsMultiDialectView', 'FederatedTable', 'ViewDefinition'
]

for field in read_only_fields:
    table_input.pop(field, None)
```

**If you still get this error with other fields:**

Add the field name to the `read_only_fields` list in the script.

---

### Error 6: Athena Query - Insufficient permissions to execute the query

**Error:**
```
Insufficient permissions to execute the query. 
Principal does not have any privilege on specified resource
```

**Root Cause:** Once a table is Lake Formation managed, IAM permissions alone are not sufficient. You need Lake Formation permissions to query the table in Athena.

**Solution:**

Grant Lake Formation permissions to your IAM user/role:

```bash
# Grant permissions to current user
python scripts/grant_athena_user_permissions.py \
  --database iceberg_db \
  --table entity_data

# Or grant to specific user
python scripts/grant_athena_user_permissions.py \
  --database iceberg_db \
  --table entity_data \
  --principal arn:aws:iam::123456789012:user/your-username

# Or grant on all tables
python scripts/grant_athena_user_permissions.py \
  --database iceberg_db \
  --table '*' \
  --principal arn:aws:iam::123456789012:user/your-username
```

**Alternative: AWS Console Method**

1. Go to AWS Lake Formation console
2. Click "Permissions" → "Data lake permissions"
3. Click "Grant"
4. Select your IAM user/role
5. Choose "Tables" and select your database and table
6. Grant "Select" and "Describe" permissions
7. Click "Grant"

**Alternative: AWS CLI Method**

```bash
# Get your user ARN
USER_ARN=$(aws sts get-caller-identity --query Arn --output text)
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

# Grant database permissions
aws lakeformation grant-permissions \
  --principal DataLakePrincipalIdentifier=$USER_ARN \
  --resource '{"Database":{"CatalogId":"'$ACCOUNT_ID'","Name":"iceberg_db"}}' \
  --permissions DESCRIBE

# Grant table permissions
aws lakeformation grant-permissions \
  --principal DataLakePrincipalIdentifier=$USER_ARN \
  --resource '{"Table":{"CatalogId":"'$ACCOUNT_ID'","DatabaseName":"iceberg_db","Name":"entity_data"}}' \
  --permissions SELECT DESCRIBE
```

**Wait for Propagation:**

After granting permissions, wait 30-60 seconds, then retry your Athena query.

**Verify Permissions:**

```bash
USER_ARN=$(aws sts get-caller-identity --query Arn --output text)

aws lakeformation list-permissions \
  --principal DataLakePrincipalIdentifier=$USER_ARN \
  --resource-type TABLE
```

---

### Error 7: StorageDescriptor#InputFormat Cannot Be Null

**Error:**
```
org.apache.hadoop.hive.ql.metadata.HiveException: 
Unable to fetch table entity_data. 
StorageDescriptor#InputFormat cannot be null for table: entity_data
```

**Root Cause:** The table's `StorageDescriptor` is missing required fields (`InputFormat`, `OutputFormat`, or `SerdeInfo`). This can happen when updating a table to be Lake Formation managed if these fields aren't properly preserved.

**Solution:**

Run the registration script to fix the table:

```bash
python scripts/register_table_with_lakeformation.py \
  --database iceberg_db \
  --table entity_data \
  --catalog-id YOUR_ACCOUNT_ID \
  --region us-east-1
```

The script will automatically detect and fix missing StorageDescriptor fields.

**Verify the fix:**

```bash
aws glue get-table --database-name iceberg_db --name entity_data \
  --query 'Table.StorageDescriptor.[InputFormat,OutputFormat,SerdeInfo.SerializationLibrary]'
```

Should return:
```json
[
  "org.apache.iceberg.mr.hive.HiveIcebergInputFormat",
  "org.apache.iceberg.mr.hive.HiveIcebergOutputFormat",
  "org.apache.iceberg.mr.hive.HiveIcebergSerDe"
]
```

**Alternative: Manual Fix**

```bash
# Get table
aws glue get-table --database-name iceberg_db --name entity_data > table.json

# Edit table.json to add to StorageDescriptor:
{
  "InputFormat": "org.apache.iceberg.mr.hive.HiveIcebergInputFormat",
  "OutputFormat": "org.apache.iceberg.mr.hive.HiveIcebergOutputFormat",
  "SerdeInfo": {
    "SerializationLibrary": "org.apache.iceberg.mr.hive.HiveIcebergSerDe",
    "Parameters": {}
  }
}

# Update table
aws glue update-table --database-name iceberg_db --table-input file://table_input.json
```

---

### Error 8: Insufficient Lake Formation permission - Required Drop

**Error:**
```
Insufficient Lake Formation permission(s): Required Drop on entity_data
```

**Root Cause:** You don't have DROP permission in Lake Formation to delete the table.

**Quick Fix:**

Grant DROP permission using the script:

```bash
python scripts/grant_athena_user_permissions.py \
  --database iceberg_db \
  --table entity_data
```

The script now grants full table permissions including DROP.

**Alternative: AWS CLI**

```bash
USER_ARN=$(aws sts get-caller-identity --query Arn --output text)
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

aws lakeformation grant-permissions \
  --principal DataLakePrincipalIdentifier=$USER_ARN \
  --resource "{\"Table\":{\"CatalogId\":\"$ACCOUNT_ID\",\"DatabaseName\":\"iceberg_db\",\"Name\":\"entity_data\"}}" \
  --permissions DROP DELETE ALTER INSERT \
  --region us-east-1
```

**Alternative: Terraform**

Update your `terraform.tfvars` with your user ARN and apply:

```hcl
athena_user_arns = ["arn:aws:iam::123456789012:user/your-username"]
```

```bash
terraform apply
```

Terraform now grants full permissions including DROP, DELETE, INSERT, and ALTER.

**Verify Permissions:**

```bash
USER_ARN=$(aws sts get-caller-identity --query Arn --output text)

aws lakeformation list-permissions \
  --principal DataLakePrincipalIdentifier=$USER_ARN \
  --resource-type TABLE | grep -A 5 entity_data
```

Should show: `["SELECT", "DESCRIBE", "DROP", "DELETE", "INSERT", "ALTER"]`

---

### Error 9: TABLE_OR_VIEW_NOT_FOUND with glue_catalog prefix

**Error:**
```
[TABLE_OR_VIEW_NOT_FOUND] The table or view `glue_catalog`.`iceberg_db`.`entity_view_entity1_set2` cannot be found.
```

**Root Cause:** You're using Spark SQL syntax (`glue_catalog.database.table`) in Athena, which uses different syntax.

**Solution:**

Remove the `glue_catalog` prefix from your query:

```sql
-- ❌ WRONG (Spark SQL syntax)
SELECT * FROM glue_catalog.iceberg_db.entity_view_entity1_set1 LIMIT 5;

-- ✅ CORRECT (Athena syntax)
SELECT * FROM iceberg_db.entity_view_entity1_set1 LIMIT 5;
```

**List available views:**

```sql
SHOW TABLES IN iceberg_db LIKE 'entity_view%';
```

**Verify view exists:**

```bash
aws glue get-tables --database-name iceberg_db \
  --query 'TableList[?starts_with(Name, `entity_view`)].Name'
```

**See detailed guide:** `docs/querying-views-in-athena.md`

---

### Error 10: HIVE_METASTORE_ERROR - Dialect not present

**Error:**
```
HIVE_METASTORE_ERROR: Dialect [ATHENA 3] not present
(Service: AWSGlue; Status Code: 400; Error Code: InvalidInputException)
```

**Root Cause:** Views were created with `PROTECTED MULTI DIALECT` syntax which causes dialect specification errors.

**Solution:**

The script has been updated to use standard Glue 5.0 `CREATE OR REPLACE VIEW` syntax.

**Step 1: Drop existing views**

```bash
# Get all view names
VIEWS=$(aws glue get-tables --database-name iceberg_db \
  --query 'TableList[?starts_with(Name, `entity_view`)].Name' \
  --output text)

# Drop each view
for view in $VIEWS; do
  aws glue delete-table --database-name iceberg_db --name $view
done
```

**Step 2: Recreate views**

```bash
# Re-run the Glue job with updated script
aws glue start-job-run --job-name create-dynamic-entity-views
```

**Step 3: Verify**

```sql
SELECT * FROM iceberg_db.entity_view_entity1_set1 LIMIT 10;
```

**See detailed guide:** `BUGFIX-Dialect-Error.md`

---

## Common Errors and Solutions

### Error 1: Insufficient Lake Formation permission(s) on S3 location

**Full Error:**
```
software.amazon.awssdk.services.glue.model.AccessDeniedException: 
Insufficient Lake Formation permission(s) on s3://iceberg-data-storage-bucket/iceberg-data/iceberg_db.db/entity_data
```

**Root Cause:** Lake Formation doesn't have permissions on the S3 location.

**Solution:**

1. **Grant Lake Formation permissions on S3 FIRST:**
   ```bash
   python scripts/grant_lakeformation_s3_permissions.py \
     --bucket iceberg-data-storage-bucket \
     --prefix iceberg-data \
     --role-arn arn:aws:iam::123456789012:role/YourGlueRole \
     --region us-east-1
   ```

2. **Wait 30-60 seconds** for permissions to propagate

3. **Then register the table:**
   ```bash
   python scripts/register_table_with_lakeformation.py \
     --database iceberg_db \
     --table entity_data \
     --catalog-id 123456789012 \
     --region us-east-1
   ```

**Required Permissions:**
The user/role running the grant script needs:
- `lakeformation:RegisterResource`
- `lakeformation:GrantPermissions`
- `lakeformation:ListPermissions`
- `sts:GetCallerIdentity`

---

### Error 2: Access Denied when running grant script

**Error:**
```
AccessDeniedException: User is not authorized to perform: lakeformation:RegisterResource
```

**Solution:**

You need Lake Formation admin permissions. Add this policy to your IAM user/role:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "lakeformation:*",
        "glue:*",
        "iam:ListRoles",
        "iam:GetRole",
        "iam:GetRolePolicy"
      ],
      "Resource": "*"
    }
  ]
}
```

Or make yourself a Lake Formation admin:
1. Go to AWS Lake Formation console
2. Click "Administrative roles and tasks"
3. Click "Add" under "Data lake administrators"
4. Add your IAM user/role

---

### Error 3: Multi Dialect views may only reference Lake Formation managed tables

**Error:**
```
InvalidObjectException: Multi Dialect views may only reference Lake Formation managed tables
```

**Solution:**

This means you skipped the Lake Formation registration. Follow the complete process:

1. Grant S3 permissions (see Error 1)
2. Register table with Lake Formation
3. Verify registration:
   ```bash
   aws glue get-table \
     --database-name iceberg_db \
     --name entity_data \
     --query 'Table.[IsRegisteredWithLakeFormation,Parameters.GOVERNED]'
   ```
   Should return: `[true, "true"]` or `[null, "true"]`

---

### Error 4: Resource Already Exists

**Error:**
```
AlreadyExistsException: Resource already exists
```

**Solution:**

This is actually OK! The S3 location is already registered. Continue with the next step (granting permissions or registering the table).

---

### Error 5: ConcurrentModificationException

**Error:**
```
ConcurrentModificationException: The resource is being modified by another operation
```

**Solution:**

Wait a few seconds and retry. Lake Formation operations are eventually consistent.

---

## Verification Commands

### Check if S3 location is registered:
```bash
aws lakeformation list-resources \
  --region us-east-1 \
  --query "ResourceInfoList[?ResourceArn=='arn:aws:s3:::iceberg-data-storage-bucket/iceberg-data']"
```

### Check Lake Formation permissions:
```bash
aws lakeformation list-permissions \
  --principal DataLakePrincipalIdentifier=arn:aws:iam::123456789012:role/YourGlueRole \
  --resource-type DATA_LOCATION \
  --region us-east-1
```

### Check if table is Lake Formation managed:
```bash
aws glue get-table \
  --database-name iceberg_db \
  --name entity_data \
  --region us-east-1 \
  --query 'Table.{IsLF:IsRegisteredWithLakeFormation,Governed:Parameters.GOVERNED,Location:StorageDescriptor.Location}'
```

### List all Lake Formation permissions for a role:
```bash
aws lakeformation list-permissions \
  --principal DataLakePrincipalIdentifier=arn:aws:iam::123456789012:role/YourGlueRole \
  --region us-east-1
```

---

## Complete Setup Checklist

- [ ] **Step 1**: Ensure you have Lake Formation admin permissions
- [ ] **Step 2**: Register S3 location with Lake Formation
  ```bash
  python scripts/grant_lakeformation_s3_permissions.py \
    --bucket <bucket> --prefix <prefix> --role-arn <role>
  ```
- [ ] **Step 3**: Wait 30-60 seconds for propagation
- [ ] **Step 4**: Register table with Lake Formation
  ```bash
  python scripts/register_table_with_lakeformation.py \
    --database <db> --table <table> --catalog-id <account>
  ```
- [ ] **Step 5**: Verify table is Lake Formation managed
  ```bash
  aws glue get-table --database-name <db> --name <table> \
    --query 'Table.Parameters.GOVERNED'
  ```
  Should return: `"true"`
- [ ] **Step 6**: Update Glue job IAM role with Lake Formation permissions
- [ ] **Step 7**: Re-run view creation Glue job

---

## AWS Console Alternative

If scripts fail, you can do this manually in AWS Console:

### 1. Register S3 Location:
1. Open AWS Lake Formation console
2. Go to "Data lake locations" (left menu)
3. Click "Register location"
4. Enter S3 path: `s3://iceberg-data-storage-bucket/iceberg-data`
5. Choose IAM role: Select your Glue job role
6. Click "Register location"

### 2. Grant Permissions:
1. In Lake Formation console, go to "Permissions" → "Data locations"
2. Click "Grant"
3. Select your Glue job IAM role
4. Select the S3 location you just registered
5. Grant "Data location" permission
6. Click "Grant"

### 3. Mark Table as Lake Formation Managed:
1. Open AWS Glue console
2. Go to "Tables"
3. Select your table (`entity_data`)
4. Click "Edit table"
5. Scroll to "Table properties"
6. Add property: Key=`GOVERNED`, Value=`true`
7. Click "Save"

### 4. Verify:
1. Go back to table details
2. Check if "Lake Formation permissions" section appears
3. Should show "This table is managed by Lake Formation"

---

## Still Having Issues?

### Check IAM Role Trust Relationship:
Your Glue job role needs to trust both Glue and Lake Formation:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": [
          "glue.amazonaws.com",
          "lakeformation.amazonaws.com"
        ]
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

### Enable CloudTrail Logging:
To debug permission issues, enable CloudTrail and check for denied API calls:
1. Go to CloudTrail console
2. Create a trail if you don't have one
3. Look for `AccessDenied` events related to Lake Formation or Glue

### Contact Support:
If all else fails, open an AWS Support case with:
- Complete error message
- IAM role ARN
- S3 bucket and prefix
- Database and table names
- Output from verification commands above
