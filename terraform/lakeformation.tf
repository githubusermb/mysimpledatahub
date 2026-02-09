# Lake Formation Configuration for Multi-Dialect Views Support

# Configure Lake Formation data lake settings
# This removes default permissions which is required for Lake Formation managed databases
resource "aws_lakeformation_data_lake_settings" "default" {
  admins = [data.aws_caller_identity.current.arn]
  
  # Remove default create table and create database permissions
  create_database_default_permissions {
    permissions = []
    principal   = "IAM_ALLOWED_PRINCIPALS"
  }
  
  create_table_default_permissions {
    permissions = []
    principal   = "IAM_ALLOWED_PRINCIPALS"
  }
}

# Register S3 location with Lake Formation
resource "aws_lakeformation_resource" "iceberg_data_location" {
  arn = "${aws_s3_bucket.iceberg_data_bucket.arn}/${var.iceberg_data_prefix}"
  
  # Use service-linked role for Lake Formation
  use_service_linked_role = true
  
  depends_on = [
    aws_s3_bucket.iceberg_data_bucket,
    aws_lakeformation_data_lake_settings.default
  ]
}

# Grant Lake Formation permissions to Glue role for data location access
resource "aws_lakeformation_permissions" "glue_data_location_permissions" {
  principal   = aws_iam_role.glue_service_role.arn
  permissions = ["DATA_LOCATION_ACCESS"]
  
  data_location {
    catalog_id = data.aws_caller_identity.current.account_id
    arn        = "${aws_s3_bucket.iceberg_data_bucket.arn}/${var.iceberg_data_prefix}"
  }
  
  depends_on = [aws_lakeformation_resource.iceberg_data_location]
}

# Grant Lake Formation permissions to Glue role for database access
resource "aws_lakeformation_permissions" "glue_database_permissions" {
  principal   = aws_iam_role.glue_service_role.arn
  permissions = ["CREATE_TABLE", "ALTER", "DROP", "DESCRIBE"]
  
  database {
    catalog_id = data.aws_caller_identity.current.account_id
    name       = aws_glue_catalog_database.iceberg_database.name
  }
  
  depends_on = [aws_glue_catalog_database.iceberg_database]
}

# Grant Lake Formation permissions to Glue role for table access
# Note: This grants permissions on ALL tables in the database
resource "aws_lakeformation_permissions" "glue_table_permissions" {
  principal   = aws_iam_role.glue_service_role.arn
  permissions = ["SELECT", "INSERT", "DELETE", "DESCRIBE", "ALTER"]
  
  table {
    catalog_id    = data.aws_caller_identity.current.account_id
    database_name = aws_glue_catalog_database.iceberg_database.name
    wildcard      = true
  }
  
  depends_on = [aws_glue_catalog_database.iceberg_database]
}

# Optional: Make the current user/role a Lake Formation admin
# Uncomment if you want to automatically add the deploying user as LF admin
# resource "aws_lakeformation_data_lake_settings" "admin_settings" {
#   admins = [
#     data.aws_caller_identity.current.arn
#   ]
# }

# Grant Lake Formation permissions to Athena users for querying tables
# This allows specified IAM users/roles to query Lake Formation managed tables in Athena

# Grant database DESCRIBE permission to Athena users
resource "aws_lakeformation_permissions" "athena_database_permissions" {
  count = length(var.athena_user_arns)
  
  principal   = var.athena_user_arns[count.index]
  permissions = ["DESCRIBE"]
  
  database {
    catalog_id = data.aws_caller_identity.current.account_id
    name       = aws_glue_catalog_database.iceberg_database.name
  }
  
  depends_on = [aws_glue_catalog_database.iceberg_database]
}

# Grant table SELECT and DESCRIBE permissions to Athena users (all tables)
resource "aws_lakeformation_permissions" "athena_all_tables_permissions" {
  count = var.grant_athena_permissions_to_all_tables ? length(var.athena_user_arns) : 0
  
  principal   = var.athena_user_arns[count.index]
  permissions = ["SELECT", "DESCRIBE", "DROP", "DELETE", "INSERT", "ALTER"]
  
  table {
    catalog_id    = data.aws_caller_identity.current.account_id
    database_name = aws_glue_catalog_database.iceberg_database.name
    wildcard      = true
  }
  
  depends_on = [aws_glue_catalog_database.iceberg_database]
}

# Grant table SELECT and DESCRIBE permissions to Athena users (specific table)
resource "aws_lakeformation_permissions" "athena_specific_table_permissions" {
  count = var.grant_athena_permissions_to_all_tables ? 0 : length(var.athena_user_arns)
  
  principal   = var.athena_user_arns[count.index]
  permissions = ["SELECT", "DESCRIBE", "DROP", "DELETE", "INSERT", "ALTER"]
  
  table {
    catalog_id    = data.aws_caller_identity.current.account_id
    database_name = aws_glue_catalog_database.iceberg_database.name
    name          = var.glue_table_name
  }
  
  depends_on = [aws_glue_catalog_database.iceberg_database]
}
