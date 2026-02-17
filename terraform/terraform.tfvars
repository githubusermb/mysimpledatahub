
# AWS Region
aws_region = "us-east-1"

# S3 Bucket Names - Make sure these are globally unique
raw_data_bucket_name = "sdh-raw-data-ingestion-bucket"
iceberg_data_bucket_name = "sdh-staging-data-storage-bucket"

# S3 Prefixes
raw_data_prefix = "collections-data/"
iceberg_data_prefix = "iceberg-data/"

# Glue Database and Table Names
glue_database_name = "collections_db"
glue_table_name = "collections_data_staging"


# Athena User Permissions
# Add IAM user/role ARNs that need to query tables in Athena
# Example: ["arn:aws:iam::123456789012:user/john.doe", "arn:aws:iam::123456789012:role/DataAnalyst"]
athena_user_arns = ["arn:aws:iam::742993817231:role/aws-reserved/sso.amazonaws.com/AWSReservedSSO_AWSAdministratorAccess_6e81f49c9e3aec36"]

# Grant permissions on all tables (true) or just the main table (false)
grant_athena_permissions_to_all_tables = true
