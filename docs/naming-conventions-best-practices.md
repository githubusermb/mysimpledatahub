# Naming Conventions Best Practices for Iceberg Tables and Dynamic Views

## Table of Contents
1. [General Principles](#general-principles)
2. [Database Naming](#database-naming)
3. [Iceberg Table Naming](#iceberg-table-naming)
4. [Dynamic View Naming](#dynamic-view-naming)
5. [Column Naming](#column-naming)
6. [Partition Column Naming](#partition-column-naming)
7. [S3 Path Naming](#s3-path-naming)
8. [Real-World Examples](#real-world-examples)
9. [Anti-Patterns to Avoid](#anti-patterns-to-avoid)
10. [Migration Strategy](#migration-strategy)

---

## General Principles

### Core Rules

```
✅ DO:
  • Use lowercase with underscores (snake_case)
  • Be descriptive but concise
  • Use consistent patterns across all objects
  • Include business context in names
  • Make names self-documenting
  • Consider query readability

❌ DON'T:
  • Use mixed case (camelCase, PascalCase)
  • Use special characters (-, @, #, $, %, etc.)
  • Use reserved keywords (table, select, from, etc.)
  • Use abbreviations unless widely understood
  • Make names too long (>64 characters)
  • Use spaces or hyphens
```

### Naming Philosophy

```
Good Name = Business Context + Data Type + Granularity

Examples:
  ✅ customer_transactions_daily
  ✅ product_inventory_snapshot
  ✅ sales_orders_raw
  ✅ financial_reports_aggregated

❌ data_table
❌ tbl1
❌ temp_view
❌ MyTable
```

---

## Database Naming

### Pattern: `<domain>_<environment>_<purpose>`

```
Examples:
  ✅ finance_prod_lakehouse
  ✅ customer_dev_analytics
  ✅ regulatory_prod_reporting
  ✅ operations_staging_etl

For single-purpose databases:
  ✅ iceberg_db
  ✅ analytics_db
  ✅ reporting_db
  ✅ data_lake
```

### Environment Suffixes (Optional)

```
• _dev    - Development environment
• _test   - Testing environment
• _stage  - Staging environment
• _prod   - Production environment (or no suffix)

Examples:
  ✅ regulatory_reporting_dev
  ✅ regulatory_reporting_prod
  ✅ regulatory_reporting (implies prod)
```

### Best Practices

```
✅ Keep database names short (< 30 characters)
✅ Use domain-driven naming (align with business units)
✅ Avoid version numbers in database names
✅ Use consistent naming across environments
✅ Document database purpose in Glue catalog description

Example:
  Database: regulatory_reporting
  Description: "FDIC regulatory reporting data for FR Y-9C and FR Y-15 forms"
```

---

## Iceberg Table Naming

### Pattern: `<entity>_<data_type>_<granularity>`

```
Components:
  • entity: Business entity (customer, transaction, product)
  • data_type: Type of data (raw, processed, aggregated, snapshot)
  • granularity: Time granularity (daily, hourly, monthly) - optional

Examples:
  ✅ customer_transactions_raw
  ✅ product_inventory_snapshot
  ✅ sales_orders_processed
  ✅ financial_metrics_daily
  ✅ regulatory_submissions_raw
```

### Data Type Suffixes

```
• _raw          - Unprocessed source data
• _processed    - Cleaned and transformed data
• _aggregated   - Summarized/rolled-up data
• _snapshot     - Point-in-time snapshot
• _historical   - Historical/archived data
• _staging      - Temporary staging data
• _curated      - Business-ready data

Examples:
  ✅ mdrm_data_raw
  ✅ mdrm_data_processed
  ✅ mdrm_data_curated
```

### Granularity Suffixes

```
• _realtime     - Real-time streaming data
• _hourly       - Hourly aggregations
• _daily        - Daily aggregations
• _weekly       - Weekly aggregations
• _monthly      - Monthly aggregations
• _quarterly    - Quarterly aggregations
• _yearly       - Yearly aggregations

Examples:
  ✅ sales_transactions_hourly
  ✅ customer_metrics_daily
  ✅ revenue_summary_monthly
```

### Domain-Specific Patterns

#### Financial/Regulatory Data
```
Pattern: <form_type>_<data_category>_<status>

Examples:
  ✅ fry9c_call_report_raw
  ✅ fry15_holding_company_processed
  ✅ ffiec_regulatory_submissions
  ✅ mdrm_entity_data
```

#### Transactional Data
```
Pattern: <business_process>_<entity>_<type>

Examples:
  ✅ order_fulfillment_events
  ✅ payment_transactions_raw
  ✅ inventory_movements_processed
```

#### Analytics Data
```
Pattern: <subject_area>_<metric_type>_<granularity>

Examples:
  ✅ customer_behavior_metrics_daily
  ✅ product_performance_kpis_monthly
  ✅ sales_funnel_conversions_hourly
```

### Best Practices

```
✅ Use singular nouns for entity names (customer, not customers)
✅ Include business context (not just technical details)
✅ Keep table names under 50 characters
✅ Use consistent suffixes across all tables
✅ Avoid redundant prefixes (tbl_, iceberg_, etc.)
✅ Document table purpose in Glue catalog

Example:
  Table: regulatory_submission_data
  Description: "Raw MDRM regulatory submission data in narrow format"
  Tags: {domain: regulatory, format: iceberg, layer: raw}
```

---

## Dynamic View Naming

### Pattern: `<base_table>_view_<discriminator>`

```
Components:
  • base_table: Source table name (or abbreviated)
  • view: Literal "view" keyword
  • discriminator: Unique identifier (seriesid, category, region)

Examples:
  ✅ entity_view_fry9c
  ✅ entity_view_fry15
  ✅ customer_view_premium
  ✅ sales_view_north_region
  ✅ product_view_electronics
```

### Alternative Pattern: `<seriesid>_report_view` (Recommended for Regulatory Data)

```
Examples:
  ✅ fry9c_report_view
  ✅ fry15_report_view
  ✅ ffiec031_report_view
  ✅ ffiec041_report_view
```

### Alternative Pattern: `<entity>_<discriminator>_<view_type>`

```
Examples:
  ✅ mdrm_fry9c_pivoted
  ✅ mdrm_fry15_pivoted
  ✅ customer_premium_summary
  ✅ sales_regional_aggregated
  ✅ product_category_metrics
```

### View Type Suffixes

```
• _view         - Generic view
• _pivoted      - Pivoted/transposed view
• _summary      - Summarized/aggregated view
• _detail       - Detailed/expanded view
• _materialized - Materialized view (if supported)
• _latest       - Latest/current state view
• _historical   - Historical/time-series view

Examples:
  ✅ entity_fry9c_pivoted
  ✅ customer_metrics_summary
  ✅ transaction_detail_view
  ✅ inventory_latest_snapshot
```

### Discriminator Patterns

#### By Category/Type
```
Examples:
  ✅ product_view_electronics
  ✅ product_view_clothing
  ✅ customer_view_enterprise
  ✅ customer_view_retail
```

#### By Region/Geography
```
Examples:
  ✅ sales_view_north_america
  ✅ sales_view_europe
  ✅ sales_view_asia_pacific
```

#### By Time Period
```
Examples:
  ✅ metrics_view_current_quarter
  ✅ metrics_view_ytd
  ✅ metrics_view_trailing_12m
```

#### By Business Unit
```
Examples:
  ✅ revenue_view_retail_banking
  ✅ revenue_view_investment_banking
  ✅ revenue_view_wealth_management
```

### Best Practices

```
✅ Make discriminator meaningful and business-relevant
✅ Use consistent discriminator format across all views
✅ Keep view names under 60 characters
✅ Include "view" in the name for clarity (or use _report_view for reports)
✅ Match discriminator to partition key when possible
✅ Document view purpose and filter criteria

Example:
  View: fry9c_report_view
  Description: "Pivoted view of FR Y-9C regulatory data with key-value pairs as columns"
  Filter: "WHERE seriesid = 'fry9c'"
  Base Table: collections_data_tbl
```

### Naming for Multi-Dialect Views

```
Pattern: Same as regular views (dialect is transparent)

✅ fry9c_report_view
   └─> Works in both Athena and Spark
   └─> No need for _athena or _spark suffix

❌ fry9c_report_view_athena
❌ fry9c_report_view_spark
   └─> Avoid engine-specific suffixes
```

---

## Column Naming

### General Principles

```
✅ Use lowercase with underscores
✅ Use descriptive names
✅ Avoid abbreviations unless standard
✅ Use consistent naming patterns
✅ Include units in name when relevant

Examples:
  ✅ customer_id
  ✅ order_date
  ✅ total_amount_usd
  ✅ quantity_units
  ✅ created_timestamp
  ✅ is_active
  ✅ has_discount
```

### Standard Column Patterns

#### Identifiers
```
Pattern: <entity>_id

Examples:
  ✅ customer_id
  ✅ order_id
  ✅ product_id
  ✅ transaction_id
  ✅ rssdid (industry standard)
```

#### Timestamps
```
Pattern: <event>_timestamp or <event>_date

Examples:
  ✅ created_timestamp
  ✅ updated_timestamp
  ✅ ingested_timestamp
  ✅ submission_timestamp
  ✅ order_date
  ✅ effective_date
```

#### Amounts/Metrics
```
Pattern: <metric>_<unit> or <metric>_amount

Examples:
  ✅ total_amount_usd
  ✅ quantity_units
  ✅ weight_kg
  ✅ duration_seconds
  ✅ revenue_amount
```

#### Boolean Flags
```
Pattern: is_<condition> or has_<attribute>

Examples:
  ✅ is_active
  ✅ is_deleted
  ✅ has_discount
  ✅ has_children
  ✅ is_verified
```

#### Codes/Categories
```
Pattern: <entity>_code or <entity>_type

Examples:
  ✅ country_code
  ✅ currency_code
  ✅ product_type
  ✅ transaction_type
  ✅ status_code
```

### Domain-Specific Columns

#### Financial/Regulatory
```
Examples:
  ✅ seriesid          (series identifier)
  ✅ aod               (as-of-date)
  ✅ rssdid            (RSSD identifier)
  ✅ submissionts      (submission timestamp)
  ✅ reporting_period
  ✅ fiscal_quarter
```

#### Audit Columns
```
Standard audit trail columns:
  ✅ created_by
  ✅ created_timestamp
  ✅ updated_by
  ✅ updated_timestamp
  ✅ deleted_by
  ✅ deleted_timestamp
  ✅ version_number
```

### Reserved Column Names to Avoid

```
❌ Avoid SQL reserved keywords:
  • select, from, where, group, order, table, view
  • insert, update, delete, create, drop, alter
  • join, union, intersect, except

❌ Avoid Iceberg reserved columns:
  • _file
  • _pos
  • _deleted
  • _spec_id
  • _partition

❌ Avoid ambiguous names:
  • data, value, field, column, row
  • temp, tmp, test
  • id (too generic - use entity_id)
```

---

## Partition Column Naming

### Best Practices

```
✅ Use descriptive partition column names
✅ Include time granularity in name
✅ Use consistent naming across tables
✅ Consider query patterns
✅ Document partition strategy

Examples:
  ✅ year
  ✅ month
  ✅ day
  ✅ event_date
  ✅ ingestion_date
  ✅ reporting_period
  ✅ region_code
  ✅ product_category
```

### Time-Based Partitions

```
Pattern: <time_unit> or <event>_<time_unit>

Examples:
  ✅ year, month, day
  ✅ event_date
  ✅ ingestion_date
  ✅ reporting_date
  ✅ transaction_date
  ✅ created_date
```

### Category-Based Partitions

```
Pattern: <entity>_<attribute>

Examples:
  ✅ seriesid
  ✅ region_code
  ✅ product_category
  ✅ customer_segment
  ✅ business_unit
```

### Composite Partitions

```
Order matters! Most selective first:

✅ Good ordering:
  PARTITIONED BY (seriesid, ingestion_date)
  PARTITIONED BY (region_code, year, month)
  PARTITIONED BY (product_category, event_date)

❌ Poor ordering:
  PARTITIONED BY (year, seriesid)  # year is less selective
  PARTITIONED BY (month, region_code)  # month is less selective
```

### Partition Value Naming

```
✅ Use consistent format for partition values:
  • Dates: YYYY-MM-DD or YYYYMMDD
  • Timestamps: Unix epoch or ISO 8601
  • Codes: Lowercase with underscores

Examples:
  ✅ seriesid=fry9c
  ✅ year=2024
  ✅ month=03
  ✅ event_date=2024-03-31
  ✅ region=north_america

❌ Avoid:
  ❌ seriesid=FRY9C (mixed case)
  ❌ date=03/31/2024 (slashes)
  ❌ region=North America (spaces)
```

---

## S3 Path Naming

### Pattern: `s3://<bucket>/<environment>/<domain>/<table>/<partitions>/`

```
Examples:
  ✅ s3://data-lake-prod/raw/regulatory/entity_data/seriesid=fry9c/ingestion_date=2024-03-31/
  ✅ s3://analytics-bucket/processed/sales/transactions/year=2024/month=03/day=31/
  ✅ s3://iceberg-data/curated/customer/metrics/region=us/date=2024-03-31/
```

### Bucket Naming

```
Pattern: <purpose>-<environment>-<region>-<account_id>

Examples:
  ✅ data-lake-prod-us-east-1-123456789012
  ✅ analytics-dev-us-west-2-123456789012
  ✅ iceberg-staging-eu-west-1-123456789012

Simplified (for single account/region):
  ✅ data-lake-prod
  ✅ analytics-dev
  ✅ iceberg-staging
```

### Folder Structure

```
Recommended hierarchy:

s3://bucket/
├── raw/                    # Raw/source data
│   ├── regulatory/
│   │   └── entity_data/
│   ├── transactions/
│   └── customers/
├── processed/              # Processed/cleaned data
│   ├── regulatory/
│   ├── transactions/
│   └── customers/
├── curated/                # Business-ready data
│   ├── reports/
│   ├── metrics/
│   └── analytics/
├── metadata/               # Iceberg metadata
└── scripts/                # ETL scripts
```

### Iceberg-Specific Paths

```
Iceberg table structure:

s3://bucket/iceberg-data/entity_data/
├── metadata/
│   ├── v1.metadata.json
│   ├── v2.metadata.json
│   ├── snap-1234567890.avro
│   └── version-hint.text
└── data/
    ├── seriesid=fry9c/
    │   └── ingestion_date=2024-03-31/
    │       ├── 00000-0-data-001.parquet
    │       └── 00001-0-data-002.parquet
    └── seriesid=fry15/
        └── ingestion_date=2024-03-31/
            └── 00000-0-data-001.parquet
```

### Best Practices

```
✅ Use hyphens in bucket names (not underscores)
✅ Use underscores in folder names (not hyphens)
✅ Keep paths under 1024 characters
✅ Use lowercase for all paths
✅ Include environment in bucket name
✅ Separate metadata from data
✅ Use Hive-style partitioning (key=value)

❌ Avoid:
  ❌ Mixed case in paths
  ❌ Special characters (except - and _)
  ❌ Spaces in folder names
  ❌ Deep nesting (> 10 levels)
```

---

## Real-World Examples

### Example 1: Financial Regulatory Reporting

```
Database:
  ✅ regulatory_reporting

Tables:
  ✅ collections_data_tbl
  ✅ mdrm_submission_data_processed
  ✅ call_report_metrics_daily

Views:
  ✅ fry9c_report_view
  ✅ fry15_report_view
  ✅ call_report_summary_current

Columns:
  ✅ seriesid
  ✅ aod (as-of-date)
  ✅ rssdid
  ✅ submission_timestamp
  ✅ ingestion_timestamp
  ✅ reporting_period

Partitions:
  ✅ PARTITIONED BY (seriesid, ingestion_date)

S3 Path:
  ✅ s3://regulatory-data-prod/raw/mdrm/collections_data_tbl/seriesid=fry9c/ingestion_date=2024-03-31/
```

### Example 2: E-Commerce Analytics

```
Database:
  ✅ ecommerce_analytics

Tables:
  ✅ customer_orders_raw
  ✅ customer_orders_processed
  ✅ product_sales_aggregated_daily

Views:
  ✅ orders_view_electronics
  ✅ orders_view_clothing
  ✅ customer_metrics_summary

Columns:
  ✅ order_id
  ✅ customer_id
  ✅ product_id
  ✅ order_date
  ✅ total_amount_usd
  ✅ quantity_units
  ✅ is_shipped
  ✅ created_timestamp

Partitions:
  ✅ PARTITIONED BY (product_category, order_date)

S3 Path:
  ✅ s3://ecommerce-data-prod/processed/orders/product_category=electronics/order_date=2024-03-31/
```

### Example 3: IoT Sensor Data

```
Database:
  ✅ iot_telemetry

Tables:
  ✅ sensor_readings_raw
  ✅ sensor_readings_processed
  ✅ sensor_metrics_hourly

Views:
  ✅ readings_view_temperature
  ✅ readings_view_pressure
  ✅ sensor_health_summary

Columns:
  ✅ sensor_id
  ✅ device_id
  ✅ reading_timestamp
  ✅ metric_type
  ✅ metric_value
  ✅ unit_of_measure
  ✅ is_anomaly

Partitions:
  ✅ PARTITIONED BY (device_id, year, month, day, hour)

S3 Path:
  ✅ s3://iot-data-prod/raw/sensors/device_id=sensor001/year=2024/month=03/day=31/hour=14/
```

---

## Anti-Patterns to Avoid

### ❌ Poor Naming Examples

```
❌ Database Names:
  • db1, database, mydb
  • TestDB, DevDatabase
  • data-lake (hyphens)

❌ Table Names:
  • tbl_data, table1, temp
  • MyTable, DataTable
  • data-table (hyphens)
  • customer_transactions_raw_v2 (version in name)

❌ View Names:
  • view1, temp_view, vw_data
  • MyView, DataView
  • entity-view-fry9c (hyphens)

❌ Column Names:
  • col1, field, data, value
  • CustomerID (mixed case)
  • customer-id (hyphens)
  • id (too generic)

❌ Partition Names:
  • p1, partition, part
  • Date (mixed case)
  • event-date (hyphens)
```

### ❌ Common Mistakes

```
1. Inconsistent Naming
   ❌ customer_orders, CustomerTransactions, product-inventory
   ✅ customer_orders, customer_transactions, product_inventory

2. Overly Abbreviated
   ❌ cust_ord_tbl, prod_inv_vw
   ✅ customer_orders, product_inventory_view

3. Too Generic
   ❌ data_table, main_view, temp_table
   ✅ customer_transactions, sales_summary_view, staging_orders

4. Mixed Conventions
   ❌ customerOrders, product_inventory, Sales-Data
   ✅ customer_orders, product_inventory, sales_data

5. Including Metadata in Names
   ❌ customer_orders_iceberg, sales_view_athena
   ✅ customer_orders, sales_view

6. Version Numbers
   ❌ customer_orders_v2, sales_view_2024
   ✅ customer_orders (use table versioning instead)
```

---

## Migration Strategy

### Renaming Existing Objects

```
Step 1: Create new object with correct name
  CREATE TABLE customer_orders_new AS SELECT * FROM CustomerOrders;

Step 2: Verify data integrity
  SELECT COUNT(*) FROM customer_orders_new;
  SELECT COUNT(*) FROM CustomerOrders;

Step 3: Update dependent views/queries
  CREATE OR REPLACE VIEW sales_summary AS 
  SELECT * FROM customer_orders_new;

Step 4: Drop old object
  DROP TABLE CustomerOrders;

Step 5: Rename new object (if needed)
  ALTER TABLE customer_orders_new RENAME TO customer_orders;
```

### Gradual Migration Approach

```
Phase 1: New Objects Only
  • Apply naming conventions to all new tables/views
  • Document naming standards

Phase 2: High-Impact Objects
  • Rename frequently-used tables/views
  • Update critical queries/jobs

Phase 3: Remaining Objects
  • Rename remaining objects
  • Deprecate old names

Phase 4: Cleanup
  • Remove deprecated objects
  • Update all documentation
```

### Backward Compatibility

```
Option 1: Create Aliases (Views)
  CREATE VIEW CustomerOrders AS SELECT * FROM customer_orders;
  -- Allows old queries to work during transition

Option 2: Update Queries Gradually
  -- Update queries one by one
  -- Track progress in migration log

Option 3: Use Both Names Temporarily
  -- Keep both objects during transition period
  -- Set deprecation date for old name
```

---

## Summary Checklist

### ✅ Naming Convention Checklist

```
Database:
  ☐ Lowercase with underscores
  ☐ Descriptive and business-relevant
  ☐ Under 30 characters
  ☐ Includes environment (if needed)

Table:
  ☐ Lowercase with underscores
  ☐ Includes entity + data type + granularity
  ☐ Under 50 characters
  ☐ No redundant prefixes (tbl_, iceberg_)
  ☐ Documented in Glue catalog

View:
  ☐ Lowercase with underscores
  ☐ Includes "view" in name
  ☐ Meaningful discriminator
  ☐ Under 60 characters
  ☐ Documented with filter criteria

Column:
  ☐ Lowercase with underscores
  ☐ Descriptive and clear
  ☐ Consistent patterns (_id, _timestamp, is_, has_)
  ☐ Avoids reserved keywords
  ☐ Includes units when relevant

Partition:
  ☐ Descriptive partition column names
  ☐ Consistent format for values
  ☐ Optimal ordering (most selective first)
  ☐ Hive-style partitioning (key=value)

S3 Path:
  ☐ Hyphens in bucket names
  ☐ Underscores in folder names
  ☐ Lowercase throughout
  ☐ Logical hierarchy
  ☐ Under 1024 characters
```

---

## References

### Industry Standards

- **AWS Glue Best Practices**: https://docs.aws.amazon.com/glue/latest/dg/best-practices.html
- **Apache Iceberg Naming**: https://iceberg.apache.org/docs/latest/
- **Hive Naming Conventions**: https://cwiki.apache.org/confluence/display/Hive/
- **SQL Style Guide**: https://www.sqlstyle.guide/

### Internal Documentation

- See: `ARCHITECTURE.md` for system architecture
- See: `ENTITY-DIAGRAMS.md` for entity relationships
- See: `README.md` for implementation details

---

**Version**: 1.0  
**Last Updated**: February 11, 2026  
**Status**: Best Practices Guide ✅
