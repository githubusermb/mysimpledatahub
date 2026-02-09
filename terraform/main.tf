
# AWS Provider Configuration
provider "aws" {
  region = var.aws_region
}

# S3 Buckets for Raw Data and Iceberg Data
resource "aws_s3_bucket" "raw_data_bucket" {
  bucket = var.raw_data_bucket_name
  force_destroy = true  # Be careful with this in production
}

resource "aws_s3_bucket" "iceberg_data_bucket" {
  bucket = var.iceberg_data_bucket_name
  force_destroy = true  # Be careful with this in production
}

# S3 Bucket Versioning
resource "aws_s3_bucket_versioning" "raw_data_versioning" {
  bucket = aws_s3_bucket.raw_data_bucket.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_versioning" "iceberg_data_versioning" {
  bucket = aws_s3_bucket.iceberg_data_bucket.id
  versioning_configuration {
    status = "Enabled"
  }
}

# AWS Glue Database
resource "aws_glue_catalog_database" "iceberg_database" {
  name = var.glue_database_name
  description = "Database for Iceberg tables"
}

# IAM Role for Glue
resource "aws_iam_role" "glue_service_role" {
  name = "GlueServiceRole"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = [
            "glue.amazonaws.com",
            "lakeformation.amazonaws.com"
          ]
        }
      }
    ]
  })
}

# Attach AWS managed policies for Glue
resource "aws_iam_role_policy_attachment" "glue_service_policy" {
  role = aws_iam_role.glue_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Custom policy for S3 access
resource "aws_iam_policy" "glue_s3_access" {
  name = "GlueS3Access"
  description = "Policy for Glue to access S3 buckets"
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Effect = "Allow"
        Resource = [
          aws_s3_bucket.raw_data_bucket.arn,
          "${aws_s3_bucket.raw_data_bucket.arn}/*",
          aws_s3_bucket.iceberg_data_bucket.arn,
          "${aws_s3_bucket.iceberg_data_bucket.arn}/*"
        ]
      }
    ]
  })
}

# Custom policy for Glue catalog permissions
resource "aws_iam_policy" "glue_catalog_access" {
  name = "GlueCatalogAccess"
  description = "Policy for Glue to access Glue Data Catalog as Hive metastore"
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "glue:CreateDatabase",
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:UpdateDatabase",
          "glue:DeleteDatabase",
          "glue:CreateTable",
          "glue:GetTable",
          "glue:GetTables",
          "glue:UpdateTable",
          "glue:DeleteTable",
          "glue:BatchCreatePartition",
          "glue:BatchGetPartition",
          "glue:GetPartition",
          "glue:GetPartitions",
          "glue:BatchDeletePartition",
          "glue:CreatePartition",
          "glue:DeletePartition",
          "glue:UpdatePartition",
          "glue:CreateUserDefinedFunction",
          "glue:UpdateUserDefinedFunction",
          "glue:GetUserDefinedFunction",
          "glue:GetUserDefinedFunctions",
          "glue:DeleteUserDefinedFunction",
          "glue:CreateConnection",
          "glue:GetConnection",
          "glue:GetConnections",
          "glue:UpdateConnection",
          "glue:DeleteConnection",
          "glue:BatchGetPartition",
          "glue:BatchCreatePartition",
          "glue:BatchDeletePartition",
          "glue:BatchDeleteTable",
          "glue:BatchDeleteTableVersion",
          "glue:BatchGetCustomEntityTypes",
          "glue:BatchGetTableOptimizer",
          "glue:GetTableVersion",
          "glue:GetTableVersions",
          "glue:GetTags",
          "glue:PutResourcePolicy",
          "glue:GetResourcePolicy",
          "glue:DeleteResourcePolicy",
          "glue:TagResource",
          "glue:UntagResource"
        ]
        Effect = "Allow"
        Resource = "*"
      }
    ]
  })
}

# Lake Formation permissions policy for Glue role
resource "aws_iam_policy" "glue_lakeformation_access" {
  name = "GlueLakeFormationAccess"
  description = "Policy for Glue to access Lake Formation for multi-dialect views"
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "lakeformation:GetDataAccess",
          "lakeformation:GrantPermissions",
          "lakeformation:RevokePermissions",
          "lakeformation:ListPermissions",
          "lakeformation:RegisterResource",
          "lakeformation:DeregisterResource",
          "lakeformation:DescribeResource"
        ]
        Effect = "Allow"
        Resource = "*"
      }
    ]
  })
}

# Athena permissions policy for Glue role (for view creation)
resource "aws_iam_policy" "glue_athena_access" {
  name = "GlueAthenaAccess"
  description = "Policy for Glue to create views via Athena"
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "athena:StartQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults",
          "athena:StopQueryExecution",
          "athena:GetWorkGroup"
        ]
        Effect = "Allow"
        Resource = "*"
      }
    ]
  })
}

# Attach custom S3 policy to Glue role
resource "aws_iam_role_policy_attachment" "glue_s3_policy_attachment" {
  role = aws_iam_role.glue_service_role.name
  policy_arn = aws_iam_policy.glue_s3_access.arn
}

# Attach Glue catalog policy to Glue role
resource "aws_iam_role_policy_attachment" "glue_catalog_policy_attachment" {
  role = aws_iam_role.glue_service_role.name
  policy_arn = aws_iam_policy.glue_catalog_access.arn
}

# Attach Lake Formation policy to Glue role
resource "aws_iam_role_policy_attachment" "glue_lakeformation_policy_attachment" {
  role = aws_iam_role.glue_service_role.name
  policy_arn = aws_iam_policy.glue_lakeformation_access.arn
}

# Attach Athena policy to Glue role
resource "aws_iam_role_policy_attachment" "glue_athena_policy_attachment" {
  role = aws_iam_role.glue_service_role.name
  policy_arn = aws_iam_policy.glue_athena_access.arn
}

# Upload Glue script to S3
resource "aws_s3_object" "glue_script" {
  bucket = aws_s3_bucket.iceberg_data_bucket.id
  key    = "scripts/glue_csv_to_iceberg.py"
  source = "../scripts/glue_csv_to_iceberg.py"
  etag   = filemd5("../scripts/glue_csv_to_iceberg.py")
}

# Upload Lake Formation setup scripts to S3
resource "aws_s3_object" "lakeformation_setup_script" {
  bucket = aws_s3_bucket.iceberg_data_bucket.id
  key    = "scripts/setup_lakeformation_complete.py"
  source = "../scripts/setup_lakeformation_complete.py"
  etag   = filemd5("../scripts/setup_lakeformation_complete.py")
}

resource "aws_s3_object" "lakeformation_grant_script" {
  bucket = aws_s3_bucket.iceberg_data_bucket.id
  key    = "scripts/grant_lakeformation_s3_permissions.py"
  source = "../scripts/grant_lakeformation_s3_permissions.py"
  etag   = filemd5("../scripts/grant_lakeformation_s3_permissions.py")
}

resource "aws_s3_object" "lakeformation_register_script" {
  bucket = aws_s3_bucket.iceberg_data_bucket.id
  key    = "scripts/register_table_with_lakeformation.py"
  source = "../scripts/register_table_with_lakeformation.py"
  etag   = filemd5("../scripts/register_table_with_lakeformation.py")
}

# Create a directory for JAR files in S3
resource "aws_s3_object" "jars_directory" {
  bucket  = aws_s3_bucket.iceberg_data_bucket.id
  key     = "jars/"
  content = "This directory contains JAR files for AWS Glue jobs."
}

# Note: The actual JAR files need to be uploaded manually or via a separate process
# Example command to upload the Iceberg JAR:
# aws s3 cp iceberg-spark-runtime-3.3_2.12-1.3.1.jar s3://iceberg-data-storage-bucket/jars/

# AWS Glue Job
resource "aws_glue_job" "csv_to_iceberg_job" {
  name     = "csv-to-iceberg-ingestion"
  role_arn = aws_iam_role.glue_service_role.arn
  
  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.iceberg_data_bucket.id}/scripts/glue_csv_to_iceberg.py"
    python_version  = "3"
  }
  
  default_arguments = {
    "--job-language"         = "python"
    "--raw_data_bucket"      = aws_s3_bucket.raw_data_bucket.id
    "--raw_data_prefix"      = var.raw_data_prefix
    "--database_name"        = aws_glue_catalog_database.iceberg_database.name
    "--table_name"           = var.glue_table_name
    "--iceberg_data_bucket"  = aws_s3_bucket.iceberg_data_bucket.id
    "--iceberg_data_prefix"  = var.iceberg_data_prefix
    "--catalog_id"           = data.aws_caller_identity.current.account_id
    "--enable-metrics"       = ""
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui"      = "true"
    "--spark-event-logs-path" = "s3://${aws_s3_bucket.iceberg_data_bucket.id}/spark-logs/"
    # Iceberg AWS integration configuration based on official documentation with enhanced S3 settings
    "--conf"                 = "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.warehouse=s3://${aws_s3_bucket.iceberg_data_bucket.id}/${var.iceberg_data_prefix} --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory --conf spark.sql.catalogImplementation=hive --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem --conf spark.hadoop.fs.s3a.connection.maximum=100 --conf spark.hadoop.fs.s3a.connection.establish.timeout=10000 --conf spark.hadoop.fs.s3a.connection.timeout=30000 --conf spark.hadoop.fs.s3a.attempts.maximum=20 --conf spark.hadoop.fs.s3a.retry.limit=20 --conf spark.hadoop.fs.s3a.multipart.size=104857600 --conf spark.hadoop.fs.s3a.multipart.threshold=104857600 --conf spark.hadoop.fs.s3a.fast.upload=true --conf spark.hadoop.fs.s3a.fast.upload.buffer=disk --conf spark.hadoop.fs.s3a.checksum.enable=true --conf spark.hadoop.fs.s3a.experimental.input.fadvise=sequential --conf spark.hadoop.fs.s3a.threads.max=20 --conf spark.hadoop.fs.s3a.socket.send.buffer=8192 --conf spark.hadoop.fs.s3a.socket.recv.buffer=8192 --conf spark.hadoop.fs.s3a.connection.ssl.enabled=true --conf spark.hadoop.fs.s3a.path.style.access=false --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain --conf spark.hadoop.fs.s3a.impl.disable.cache=true --conf spark.sql.parquet.mergeSchema=false --conf spark.sql.parquet.filterPushdown=true --conf spark.sql.hive.metastorePartitionPruning=true"
    
    # Add Iceberg connector JARs for AWS Glue 5.0
    # These JARs need to be uploaded to the S3 bucket before running the job
    # Using the Iceberg Spark runtime and AWS module compatible with Spark 3.4 (Glue 5.0)
    # Also including AWS SDK bundle and Apache HTTP Client for S3FileIO
    "--extra-jars"           = "s3://${aws_s3_bucket.iceberg_data_bucket.id}/jars/iceberg-spark-runtime-3.4_2.12-1.4.2.jar,s3://${aws_s3_bucket.iceberg_data_bucket.id}/jars/iceberg-aws-1.4.2.jar,s3://${aws_s3_bucket.iceberg_data_bucket.id}/jars/bundle-2.20.18.jar,s3://${aws_s3_bucket.iceberg_data_bucket.id}/jars/apache-client-2.20.18.jar,s3://${aws_s3_bucket.iceberg_data_bucket.id}/jars/url-connection-client-2.20.18.jar"
    
    # Add Iceberg dependencies - using a version compatible with AWS Glue 5.0
    "--additional-python-modules" = "pyiceberg==0.10.0"
  }
  
  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 60
  
  execution_property {
    max_concurrent_runs = 1
  }
  
  depends_on = [aws_s3_object.glue_script]
}

# Get current AWS account ID
data "aws_caller_identity" "current" {}

# AWS Glue Trigger for scheduled job execution
resource "aws_glue_trigger" "csv_to_iceberg_trigger" {
  name     = "csv-to-iceberg-daily-trigger"
  type     = "SCHEDULED"
  schedule = "cron(0 0 * * ? *)"  # Run daily at midnight UTC
  
  actions {
    job_name = aws_glue_job.csv_to_iceberg_job.name
  }
}

# S3 Event Notification to trigger Lambda when new data arrives
resource "aws_s3_bucket_notification" "bucket_notification" {
  bucket = aws_s3_bucket.raw_data_bucket.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.trigger_glue_job.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = var.raw_data_prefix
  }

  depends_on = [aws_lambda_permission.allow_bucket]
}

# Lambda function to trigger Glue job
resource "aws_lambda_function" "trigger_glue_job" {
  filename      = "${path.module}/lambda_function.zip"
  function_name = "trigger-glue-job-lambda"
  role          = aws_iam_role.lambda_role.arn
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.9"
  timeout       = 30

  environment {
    variables = {
      GLUE_JOB_NAME = aws_glue_job.csv_to_iceberg_job.name
    }
  }

  depends_on = [
    aws_iam_role_policy_attachment.lambda_glue_policy_attachment,
    aws_s3_object.lambda_package
  ]
}

# Lambda deployment package
resource "aws_s3_object" "lambda_package" {
  bucket = aws_s3_bucket.iceberg_data_bucket.id
  key    = "lambda/lambda_function.zip"
  source = "${path.module}/lambda_function.zip"
  etag   = filemd5("${path.module}/lambda_function.zip")
}

# Lambda permission to allow S3 to invoke function
resource "aws_lambda_permission" "allow_bucket" {
  statement_id  = "AllowExecutionFromS3Bucket"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.trigger_glue_job.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.raw_data_bucket.arn
}

# IAM Role for Lambda
resource "aws_iam_role" "lambda_role" {
  name = "LambdaGlueJobTriggerRole"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
}

# Policy to allow Lambda to start Glue job and write logs
resource "aws_iam_policy" "lambda_glue_policy" {
  name        = "LambdaGlueJobPolicy"
  description = "Allow Lambda to start Glue job and write logs"
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action   = "glue:StartJobRun"
        Effect   = "Allow"
        Resource = aws_glue_job.csv_to_iceberg_job.arn
      },
      {
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Effect   = "Allow"
        Resource = "arn:aws:logs:*:*:*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_glue_policy_attachment" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.lambda_glue_policy.arn
}
