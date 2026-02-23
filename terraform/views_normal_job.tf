# Upload Glue script for normal views to S3
resource "aws_s3_object" "glue_views_normal_script" {
  bucket = aws_s3_bucket.iceberg_data_bucket.id
  key    = "scripts/glue_create_normal_views.py"
  source = "../jobs/glue_create_normal_views.py"
  etag   = filemd5("../jobs/glue_create_normal_views.py")
}

# AWS Glue Job for creating normal views
# Uses CREATE PROTECTED MULTI DIALECT VIEW + ALTER VIEW ADD DIALECT
resource "aws_glue_job" "views_normal_job" {
  name     = "create-views-normal"
  role_arn = aws_iam_role.glue_service_role.arn
  
  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.iceberg_data_bucket.id}/scripts/glue_create_normal_views.py"
    python_version  = "3"
  }
  
  default_arguments = {
    "--job-language"         = "python"
    "--database_name"        = aws_glue_catalog_database.collections_database.name
    "--source_table_name"    = var.glue_table_name
    "--cdp_seriesid_filter"  = "FRY9C"
    "--athena_output_location" = "s3://${aws_s3_bucket.iceberg_data_bucket.id}/athena-results/"
    "--aws_region"           = var.aws_region
    "--enable-metrics"       = ""
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui"      = "true"
    "--spark-event-logs-path" = "s3://${aws_s3_bucket.iceberg_data_bucket.id}/spark-logs/"
    
    # Iceberg AWS integration configuration
    "--conf"                 = "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.warehouse=s3://${aws_s3_bucket.iceberg_data_bucket.id}/${var.iceberg_data_prefix} --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory --conf spark.sql.catalogImplementation=hive"
    
    # Add Iceberg connector JARs
    "--extra-jars"           = "s3://${aws_s3_bucket.iceberg_data_bucket.id}/jars/iceberg-spark-runtime-3.4_2.12-1.4.2.jar,s3://${aws_s3_bucket.iceberg_data_bucket.id}/jars/iceberg-aws-1.4.2.jar,s3://${aws_s3_bucket.iceberg_data_bucket.id}/jars/bundle-2.20.18.jar,s3://${aws_s3_bucket.iceberg_data_bucket.id}/jars/apache-client-2.20.18.jar,s3://${aws_s3_bucket.iceberg_data_bucket.id}/jars/url-connection-client-2.20.18.jar"
  }
  
  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 60
  
  execution_property {
    max_concurrent_runs = 1
  }
  
  depends_on = [aws_s3_object.glue_views_normal_script]
}

# AWS Glue Trigger for the normal views job
resource "aws_glue_trigger" "views_normal_trigger" {
  name          = "normal-views-trigger"
  type          = "CONDITIONAL"
  description   = "Trigger to create normal views after data ingestion completes"
  
  predicate {
    conditions {
      job_name = aws_glue_job.csv_to_iceberg_job.name
      state    = "SUCCEEDED"
    }
  }
  
  actions {
    job_name = aws_glue_job.views_normal_job.name
  }
}
