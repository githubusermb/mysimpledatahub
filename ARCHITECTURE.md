# MySimpleDataHub Architecture

## High-Level Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          AWS Data Lake Architecture                          │
└─────────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────────┐
│  DATA INGESTION LAYER                                                         │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│  ┌─────────────────┐                                                         │
│  │  CSV Files      │  Upload via AWS CLI / Console / Lambda Trigger          │
│  │  (MDRM Data)    │                                                         │
│  └────────┬────────┘                                                         │
│           │                                                                   │
│           ▼                                                                   │
│  ┌─────────────────────────────────────────────────────────────────┐        │
│  │  S3 Raw Data Bucket                                              │        │
│  │  └── collections-data/                                           │        │
│  │      └── ingest_ts=<timestamp>/                                  │        │
│  │          └── mdrm_data_<timestamp>.csv                           │        │
│  │                                                                   │        │
│  │  Columns: seriesid, aod, rssdid, submissionts, key, value        │        │
│  └─────────────────────────────────────────────────────────────────┘        │
│           │                                                                   │
│           │ S3 Event Notification (Optional)                                 │
│           ▼                                                                   │
│  ┌─────────────────────────────────────────────────────────────────┐        │
│  │  Lambda Function (Optional)                                      │        │
│  │  trigger-glue-job-lambda                                         │        │
│  │  └── Triggers Glue Job on S3 upload                              │        │
│  └─────────────────────────────────────────────────────────────────┘        │
└──────────────────────────────────────────────────────────────────────────────┘
           │
           │ Trigger
           ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  DATA PROCESSING LAYER                                                        │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│  ┌─────────────────────────────────────────────────────────────────┐        │
│  │  AWS Glue ETL Job #1: csv-to-iceberg-ingestion                  │        │
│  │  ┌────────────────────────────────────────────────────────────┐ │        │
│  │  │  • Read CSV from S3 (with validation)                      │ │        │
│  │  │  • Extract ingest_ts from S3 path                          │ │        │
│  │  │  • Add ingest_timestamp column                             │ │        │
│  │  │  • Convert to Iceberg format                               │ │        │
│  │  │  • Partition by seriesid & ingest_timestamp                │ │        │
│  │  │  • Write to S3 Iceberg bucket                              │ │        │
│  │  │  • Register with Lake Formation                            │ │        │
│  │  └────────────────────────────────────────────────────────────┘ │        │
│  └─────────────────────────────────────────────────────────────────┘        │
│           │                                                                   │
│           │ On Success Trigger                                               │
│           ▼                                                                   │
│  ┌─────────────────────────────────────────────────────────────────┐        │
│  │  AWS Glue ETL Job #2: create-views-dual-engine                  │        │
│  │  ┌────────────────────────────────────────────────────────────┐ │        │
│  │  │  • Read collections_data_staging table                     │ │        │
│  │  │  • Get distinct seriesid values                            │ │        │
│  │  │  • For each seriesid:                                      │ │        │
│  │  │    1. CREATE PROTECTED MULTI DIALECT VIEW (Spark)          │ │        │
│  │  │    2. ALTER VIEW ADD DIALECT (Athena API)                  │ │        │
│  │  │    3. Verify in both engines                               │ │        │
│  │  │  • Generate pivoted views (key → columns)                  │ │        │
│  │  └────────────────────────────────────────────────────────────┘ │        │
│  └─────────────────────────────────────────────────────────────────┘        │
│           │                                                                   │
│           ▼                                                                   │
│  ┌─────────────────────────────────────────────────────────────────┐        │
│  │  S3 Iceberg Data Bucket                                          │        │
│  │  └── iceberg-data/                                               │        │
│  │      └── collections_data_staging/                               │        │
│  │          ├── metadata/                                           │        │
│  │          │   ├── v1.metadata.json                                │        │
│  │          │   └── snap-*.avro                                     │        │
│  │          └── data/                                               │        │
│  │              └── seriesid=<id>/ingest_timestamp=<ts>/            │        │
│  │                  └── *.parquet                                   │        │
│  └─────────────────────────────────────────────────────────────────┘        │
└──────────────────────────────────────────────────────────────────────────────┘
           │
           │ Catalog Registration
           ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  METADATA & GOVERNANCE LAYER                                                  │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│  ┌─────────────────────────────────────────────────────────────────┐        │
│  │  AWS Glue Data Catalog                                           │        │
│  │  ┌────────────────────────────────────────────────────────────┐ │        │
│  │  │  Database: iceberg_db                                      │ │        │
│  │  │                                                             │ │        │
│  │  │  Tables:                                                    │ │        │
│  │  │  ├── collections_data_staging (Iceberg Table)              │ │        │
│  │  │  │   └── Partitioned by: seriesid, ingest_timestamp        │ │        │
│  │  │  │                                                          │ │        │
│  │  │  └── Views (Multi-Dialect):                                │ │        │
│  │  │      ├── fry9c_report_view                                 │ │        │
│  │  │      ├── fry15_report_view                                 │ │        │
│  │  │      └── <seriesid>_report_view                            │ │        │
│  │  │          └── Dual-engine support (Spark + Athena)          │ │        │
│  │  └────────────────────────────────────────────────────────────┘ │        │
│  └─────────────────────────────────────────────────────────────────┘        │
│           │                                                                   │
│           │ Permissions Management                                            │
│           ▼                                                                   │
│  ┌─────────────────────────────────────────────────────────────────┐        │
│  │  AWS Lake Formation                                              │        │
│  │  ┌────────────────────────────────────────────────────────────┐ │        │
│  │  │  • Data location registration                              │ │        │
│  │  │  • Table-level permissions                                 │ │        │
│  │  │  • View-level permissions                                  │ │        │
│  │  │  • Column-level security (optional)                        │ │        │
│  │  │  • Row-level security (optional)                           │ │        │
│  │  └────────────────────────────────────────────────────────────┘ │        │
│  └─────────────────────────────────────────────────────────────────┘        │
└──────────────────────────────────────────────────────────────────────────────┘
           │
           │ Query Access
           ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  QUERY & ANALYTICS LAYER                                                      │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│  ┌──────────────────────────────┐    ┌──────────────────────────────┐       │
│  │  Amazon Athena               │    │  AWS Glue Spark              │       │
│  │  ┌────────────────────────┐  │    │  ┌────────────────────────┐ │       │
│  │  │ • SQL Query Engine     │  │    │  │ • PySpark Jobs         │ │       │
│  │  │ • Presto/Trino based   │  │    │  │ • Interactive Notebooks│ │       │
│  │  │ • Serverless           │  │    │  │ • Batch Processing     │ │       │
│  │  │                        │  │    │  │                        │ │       │
│  │  │ Query Format:          │  │    │  │ Query Format:          │ │       │
│  │  │ SELECT * FROM          │  │    │  │ spark.sql(             │ │       │
│  │  │   iceberg_db.          │  │    │  │   "SELECT * FROM       │ │       │
│  │  │   fry9c_report_view    │  │    │  │    glue_catalog.       │ │       │
│  │  │                        │  │    │  │    iceberg_db.         │ │       │
│  │  │ Uses: Athena Dialect   │  │    │  │    fry9c_report_view") │ │       │
│  │  │                        │  │    │  │                        │ │       │
│  │  │                        │  │    │  │ Uses: Spark Dialect    │ │       │
│  │  └────────────────────────┘  │    │  └────────────────────────┘ │       │
│  └──────────────────────────────┘    └──────────────────────────────┘       │
│           │                                      │                            │
│           └──────────────┬───────────────────────┘                            │
│                          │                                                    │
│                          ▼                                                    │
│  ┌─────────────────────────────────────────────────────────────────┐        │
│  │  Query Results                                                   │        │
│  │  └── S3 Athena Results Bucket                                    │        │
│  └─────────────────────────────────────────────────────────────────┘        │
└──────────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────────┐
│  INFRASTRUCTURE AS CODE                                                       │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│  ┌─────────────────────────────────────────────────────────────────┐        │
│  │  Terraform Configuration                                         │        │
│  │  ├── main.tf                    (Core infrastructure)            │        │
│  │  ├── lakeformation.tf           (Permissions)                    │        │
│  │  ├── views_dual_engine_job.tf   (View creation job)              │        │
│  │  ├── variables.tf               (Configuration)                  │        │
│  │  └── outputs.tf                 (Resource outputs)               │        │
│  └─────────────────────────────────────────────────────────────────┘        │
└──────────────────────────────────────────────────────────────────────────────┘
```

## Data Flow Sequence

```
1. CSV Upload
   └─> S3 Raw Data Bucket (collections-data/ingest_ts=<timestamp>/)

2. Ingestion Trigger
   └─> Lambda (optional) OR Manual trigger
       └─> Glue Job: csv-to-iceberg-ingestion

3. Data Transformation
   └─> Read CSV with validation
   └─> Extract ingest_ts from S3 path
   └─> Add ingest_timestamp column
   └─> Convert to Iceberg format
   └─> Partition by seriesid & ingest_timestamp
   └─> Write to S3 Iceberg bucket
   └─> Register with Glue Catalog
   └─> Register with Lake Formation

4. View Creation (Auto-triggered)
   └─> Glue Job: create-views-dual-engine
       └─> Read collections_data_staging table
       └─> Get distinct seriesid values
       └─> For each seriesid:
           ├─> CREATE PROTECTED MULTI DIALECT VIEW (Spark SQL)
           ├─> ALTER VIEW ADD DIALECT (Athena API)
           └─> Verify in both engines

5. Query Access
   ├─> Athena: SELECT * FROM iceberg_db.<seriesid>_report_view
   └─> Spark: spark.sql("SELECT * FROM glue_catalog.iceberg_db.<seriesid>_report_view")
```

## Component Details

### S3 Buckets

| Bucket | Purpose | Contents |
|--------|---------|----------|
| **raw-data-bucket** | Source CSV files | `collections-data/ingest_ts=<timestamp>/mdrm_data_<timestamp>.csv` |
| **iceberg-data-bucket** | Iceberg table storage | `iceberg-data/collections_data_staging/` with metadata and data folders |
| **athena-results** | Query results | Athena query outputs |

### Glue Jobs

| Job Name | Type | Purpose | Trigger |
|----------|------|---------|---------|
| **csv-to-iceberg-ingestion** | ETL | Convert CSV to Iceberg | Manual / Lambda / Scheduled |
| **create-views-dual-engine** | ETL | Create multi-dialect views | On ingestion success |

### IAM Roles

| Role | Purpose | Key Permissions |
|------|---------|-----------------|
| **GlueServiceRole** | Glue job execution | S3, Glue Catalog, Lake Formation, Athena |
| **LambdaGlueJobTriggerRole** | Lambda trigger | Glue StartJobRun, S3 read |

### Lake Formation

- **Data Location Registration**: S3 Iceberg bucket
- **Database Permissions**: iceberg_db
- **Table Permissions**: collections_data_staging, <seriesid>_report_view
- **Governed Tables**: GOVERNED=true for multi-dialect views

## Technology Stack

```
┌─────────────────────────────────────────────────────────────┐
│  Storage Layer                                               │
│  • Amazon S3 (Raw CSV + Iceberg Parquet)                     │
│  • Apache Iceberg Format (ACID, Time Travel, Schema Evolve) │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  Processing Layer                                            │
│  • AWS Glue ETL (PySpark 3.4)                                │
│  • Iceberg Spark Runtime 3.4                                 │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  Metadata Layer                                              │
│  • AWS Glue Data Catalog (Hive Metastore compatible)        │
│  • AWS Lake Formation (Permissions & Governance)            │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  Query Layer                                                 │
│  • Amazon Athena (Presto/Trino SQL)                          │
│  • AWS Glue Spark (PySpark SQL)                              │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  Infrastructure Layer                                        │
│  • Terraform (Infrastructure as Code)                        │
│  • AWS Lambda (Event-driven triggers)                        │
└─────────────────────────────────────────────────────────────┘
```

## Key Features

✅ **Dual-Engine Views**: Single view works in both Athena and Glue Spark
✅ **Iceberg Format**: ACID transactions, time travel, schema evolution
✅ **Partitioning**: Optimized queries by seriesid and ingest_timestamp
✅ **Lake Formation**: Centralized permissions and governance
✅ **Infrastructure as Code**: Fully automated deployment with Terraform
✅ **Event-Driven**: Optional Lambda trigger on S3 upload
✅ **Automated Workflow**: View creation triggered on ingestion success

---

**Version**: 1.0  
**Last Updated**: February 11, 2026  
**Status**: Production Ready ✅
