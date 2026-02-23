

# Output the S3 bucket names
output "raw_data_bucket_name" {
  description = "Name of the S3 bucket for raw CSV data"
  value       = aws_s3_bucket.raw_data_bucket.id
}

output "iceberg_data_bucket_name" {
  description = "Name of the S3 bucket for Iceberg data"
  value       = aws_s3_bucket.iceberg_data_bucket.id
}

# Output the Glue database and table names
output "glue_database_name" {
  description = "Name of the AWS Glue database"
  value       = aws_glue_catalog_database.collections_database.name
}

output "glue_table_name" {
  description = "Name of the AWS Glue table"
  value       = var.glue_table_name
}

# Output the Glue job name
output "glue_job_name" {
  description = "Name of the AWS Glue job"
  value       = aws_glue_job.csv_to_iceberg_job.name
}

# Output the S3 path for raw data ingestion
output "raw_data_ingestion_path" {
  description = "S3 path for raw data ingestion"
  value       = "s3://${aws_s3_bucket.raw_data_bucket.id}/${var.raw_data_prefix}ingest_ts=<timestamp>/"
}

# Output example AWS CLI command to upload data
output "example_upload_command" {
  description = "Example AWS CLI command to upload CSV data"
  value       = "aws s3 cp sample_data.csv s3://${aws_s3_bucket.raw_data_bucket.id}/${var.raw_data_prefix}ingest_ts=$(date +%s)/"
}

# Output example AWS CLI command to run the Glue job
output "example_run_glue_job_command" {
  description = "Example AWS CLI command to run the Glue job"
  value       = "aws glue start-job-run --job-name ${aws_glue_job.csv_to_iceberg_job.name}"
}

# Output example Athena query to view data
output "example_athena_query" {
  description = "Example Athena query to view data"
  value       = "SELECT * FROM ${aws_glue_catalog_database.collections_database.name}.${var.glue_table_name} LIMIT 10;"
}

# Lake Formation outputs
output "lakeformation_registered_location" {
  description = "S3 location registered with Lake Formation"
  value       = "${aws_s3_bucket.iceberg_data_bucket.arn}/${var.iceberg_data_prefix}"
}

output "glue_role_arn" {
  description = "ARN of the Glue service role (for Lake Formation setup)"
  value       = aws_iam_role.glue_service_role.arn
}

output "lakeformation_setup_command" {
  description = "Command to run Lake Formation setup for existing tables"
  value       = "python scripts/setup_lakeformation_complete.py --bucket ${aws_s3_bucket.iceberg_data_bucket.id} --prefix ${var.iceberg_data_prefix} --database ${aws_glue_catalog_database.collections_database.name} --table ${var.glue_table_name} --role-arn ${aws_iam_role.glue_service_role.arn} --region ${var.aws_region}"
}

output "normal_views_job_name" {
  description = "Name of the normal views Glue job"
  value       = aws_glue_job.views_normal_job.name
}

output "wide_views_job_name" {
  description = "Name of the wide views Glue job"
  value       = aws_glue_job.views_wide_job.name
}

output "example_run_normal_views_job_command" {
  description = "Example AWS CLI command to run the normal views job"
  value       = "aws glue start-job-run --job-name ${aws_glue_job.views_normal_job.name}"
}

output "example_run_wide_views_job_command" {
  description = "Example AWS CLI command to run the wide views job"
  value       = "aws glue start-job-run --job-name ${aws_glue_job.views_wide_job.name}"
}

output "athena_users_with_permissions" {
  description = "List of IAM users/roles granted Athena query permissions"
  value       = var.athena_user_arns
}

output "grant_athena_permissions_command" {
  description = "Command to grant Athena permissions to additional users"
  value       = "python scripts/grant_athena_user_permissions.py --database ${aws_glue_catalog_database.collections_database.name} --table ${var.glue_table_name} --principal <user-arn>"
}

