
# AWS Region
variable "aws_region" {
  description = "AWS region to deploy resources"
  type        = string
  default     = "us-east-1"
}

# S3 Bucket Names
variable "raw_data_bucket_name" {
  description = "Name of the S3 bucket for raw CSV data"
  type        = string
  default     = "mb-raw-data-ingestion-bucket"
}

variable "iceberg_data_bucket_name" {
  description = "Name of the S3 bucket for Iceberg data"
  type        = string
  default     = "iceberg-data-storage-bucket"
}

# S3 Prefixes
variable "raw_data_prefix" {
  description = "S3 prefix for raw CSV data"
  type        = string
  default     = "raw-data/"
}

variable "iceberg_data_prefix" {
  description = "S3 prefix for Iceberg data"
  type        = string
  default     = "iceberg-data/"
}

# Glue Database and Table Names
variable "glue_database_name" {
  description = "Name of the AWS Glue database"
  type        = string
  default     = "iceberg_db"
}

variable "glue_table_name" {
  description = "Name of the AWS Glue table"
  type        = string
  default     = "collections_data_staging"
}

# Athena User Permissions
variable "athena_user_arns" {
  description = "List of IAM user/role ARNs that need Athena query permissions on Lake Formation tables"
  type        = list(string)
  default     = []
}

variable "grant_athena_permissions_to_all_tables" {
  description = "Whether to grant Athena permissions on all tables in the database (true) or just the main table (false)"
  type        = bool
  default     = true
}
