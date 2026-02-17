# Glue Job JAR Dependencies

This folder contains the JAR files required for AWS Glue jobs to work with Apache Iceberg tables.

## Contents

### Individual JAR Files

1. **apache-client-2.20.18.jar** (73 KB)
   - AWS SDK Apache HTTP client

2. **bundle-2.20.18.jar** (442 MB)
   - AWS SDK bundle with all AWS service clients

3. **iceberg-aws-1.4.2.jar** (188 KB)
   - Apache Iceberg AWS integration

4. **iceberg-spark-runtime-3.4_2.12-1.4.2.jar** (18 MB)
   - Apache Iceberg Spark runtime for Spark 3.4 and Scala 2.12

5. **url-connection-client-2.20.18.jar** (32 KB)
   - AWS SDK URL connection client

### Zip Archive

**glue-jars.zip** (~480 MB)
- Contains all the above JAR files in a single archive
- Use this for easy deployment to S3 or sharing

## Source

These JARs were downloaded from:
```
s3://sdh-staging-data-storage-bucket/jars/
```

## Usage in Glue Jobs

These JARs are referenced in Terraform configuration:

```hcl
default_arguments = {
  "--extra-jars" = "s3://your-bucket/jars/bundle-2.20.18.jar,s3://your-bucket/jars/iceberg-spark-runtime-3.4_2.12-1.4.2.jar,..."
}
```

## Uploading to S3

To upload these JARs to your S3 bucket:

```bash
# Upload individual files
aws s3 sync jars/ s3://your-bucket/jars/ --exclude "*.zip" --exclude "README.md"

# Or upload the zip and extract
aws s3 cp jars/glue-jars.zip s3://your-bucket/jars/
```

## Version Information

- **AWS SDK Version**: 2.20.18
- **Apache Iceberg Version**: 1.4.2
- **Spark Version**: 3.4
- **Scala Version**: 2.12

## Notes

- These JARs are required for Iceberg table operations in AWS Glue
- The bundle JAR is large (442 MB) but contains all necessary AWS SDK dependencies
- Make sure your Glue job has access to the S3 location where these JARs are stored

---

**Last Updated**: 2026-02-16
