# Materialized Views vs Iceberg Tables in AWS Glue Catalog

## Overview

This guide explains the key differences between materialized views and Iceberg tables in the AWS Glue Data Catalog, helping you choose the right approach for your data architecture.

---

## Quick Comparison

| Feature | Iceberg Tables | Materialized Views | Regular Views |
|---------|---------------|-------------------|---------------|
| **Storage** | Physical data files | Physical data files | No storage (virtual) |
| **Query Performance** | Fast (direct read) | Fast (pre-computed) | Slower (computed on query) |
| **Data Freshness** | Real-time (on write) | Stale (needs refresh) | Real-time (always current) |
| **Storage Cost** | High (stores all data) | High (stores results) | None (no data stored) |
| **Maintenance** | Automatic (Iceberg) | Manual refresh needed | None |
| **ACID Support** | Yes (full ACID) | Limited | N/A (no writes) |
| **Time Travel** | Yes | No | No |
| **Schema Evolution** | Yes | Limited | Inherits from base |
| **Use Case** | Source of truth | Pre-aggregated reports | Simple transformations |

---

## Iceberg Tables

### What Are They?

Iceberg tables are **physical tables** that store actual data in Parquet/ORC/Avro files on S3, with metadata managed by Apache Iceberg format.

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  Iceberg Table: collections_data_staging                     │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  Glue Catalog Metadata                                       │
│  ├── Table Schema                                            │
│  ├── Partition Spec                                          │
│  └── Table Properties                                        │
│                                                              │
│  S3 Storage                                                  │
│  ├── metadata/                                               │
│  │   ├── v1.metadata.json    (schema, partitions)           │
│  │   ├── v2.metadata.json    (updated metadata)             │
│  │   └── snap-*.avro         (snapshots)                    │
│  └── data/                                                   │
│      ├── seriesid=fry9c/                                     │
│      │   └── ingest_timestamp=1770609249/                    │
│      │       └── 00000-0-data.parquet  (actual data)        │
│      └── seriesid=fry15/                                     │
│          └── ingest_timestamp=1770609249/                    │
│              └── 00000-0-data.parquet  (actual data)        │
└─────────────────────────────────────────────────────────────┘
```

### Key Features

#### 1. ACID Transactions
```sql
-- Multiple concurrent writes are safe
INSERT INTO collections_data_staging VALUES (...);  -- Transaction 1
INSERT INTO collections_data_staging VALUES (...);  -- Transaction 2
-- Both succeed without conflicts
```

#### 2. Time Travel
```sql
-- Query data as it was at a specific time
SELECT * FROM collections_data_staging 
FOR SYSTEM_TIME AS OF '2024-01-01 00:00:00';

-- Query a specific snapshot
SELECT * FROM collections_data_staging 
FOR SYSTEM_VERSION AS OF 12345;
```

#### 3. Schema Evolution
```sql
-- Add columns without rewriting data
ALTER TABLE collections_data_staging 
ADD COLUMN new_field STRING;

-- Rename columns
ALTER TABLE collections_data_staging 
RENAME COLUMN old_name TO new_name;
```

#### 4. Partition Evolution
```python
# Change partitioning without rewriting data
table.update_spec() \
    .remove_field("old_partition") \
    .add_field("new_partition") \
    .commit()
```

#### 5. Hidden Partitioning
```sql
-- Partition by date, but users don't need to specify it
SELECT * FROM collections_data_staging 
WHERE event_date = '2024-01-01';
-- Iceberg automatically prunes partitions
```

### Advantages

✅ **Real-time Data**: Data is immediately available after write  
✅ **ACID Guarantees**: Safe concurrent reads and writes  
✅ **Time Travel**: Query historical data  
✅ **Schema Evolution**: Add/modify columns without rewrites  
✅ **Efficient Updates**: Row-level updates and deletes  
✅ **Partition Evolution**: Change partitioning strategy  
✅ **Snapshot Isolation**: Consistent reads during writes  
✅ **Metadata Management**: Efficient metadata operations  

### Disadvantages

❌ **Storage Cost**: Stores all raw data  
❌ **Query Complexity**: Complex queries still need computation  
❌ **Initial Setup**: Requires Iceberg configuration  
❌ **Learning Curve**: Need to understand Iceberg concepts  

### Best Use Cases

1. **Source of Truth Tables**
   - Raw data ingestion
   - Master data tables
   - Transaction logs

2. **Frequently Updated Data**
   - Real-time streaming data
   - CDC (Change Data Capture) tables
   - Operational data stores

3. **Large Historical Datasets**
   - Need time travel capabilities
   - Require schema evolution
   - Benefit from partition evolution

### Example: Our Use Case

```python
# collections_data_staging - Iceberg Table
# Stores raw MDRM regulatory data in narrow format

CREATE TABLE glue_catalog.iceberg_db.collections_data_staging
USING iceberg
PARTITIONED BY (seriesid, ingest_timestamp)
TBLPROPERTIES (
    'format-version'='2',
    'table_type'='ICEBERG'
)
AS SELECT 
    seriesid,
    aod,
    rssdid,
    submissionts,
    key,
    value,
    ingest_timestamp
FROM source_data;
```

**Why Iceberg?**
- Need to append new data regularly (daily/weekly submissions)
- Require partition by seriesid for efficient queries
- Want time travel to see historical submissions
- Need ACID guarantees for concurrent ingestion

---

## Materialized Views

### What Are They?

Materialized views are **pre-computed query results** stored as physical data, similar to tables but derived from other tables.

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  Materialized View: fry9c_summary_mv                         │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  Glue Catalog Metadata                                       │
│  ├── View Definition (SQL query)                             │
│  ├── Base Tables (collections_data_staging)                  │
│  ├── Last Refresh Time                                       │
│  └── Refresh Schedule                                        │
│                                                              │
│  S3 Storage (Materialized Data)                              │
│  └── data/                                                   │
│      └── 00000-0-data.parquet  (pre-computed results)       │
│                                                              │
│  Base Table: collections_data_staging                        │
│  └── Source data (may be newer than materialized view)      │
└─────────────────────────────────────────────────────────────┘
```

### Key Features

#### 1. Pre-Computed Results
```sql
-- Create materialized view with aggregation
CREATE MATERIALIZED VIEW fry9c_summary_mv AS
SELECT 
    seriesid,
    aod,
    rssdid,
    COUNT(*) as record_count,
    MAX(submissionts) as latest_submission
FROM collections_data_staging
WHERE seriesid = 'fry9c'
GROUP BY seriesid, aod, rssdid;

-- Query is fast (reads pre-computed results)
SELECT * FROM fry9c_summary_mv;
```

#### 2. Manual Refresh Required
```sql
-- Data becomes stale over time
-- Must refresh to get latest data
REFRESH MATERIALIZED VIEW fry9c_summary_mv;
```

#### 3. Storage Optimization
```sql
-- Stores only aggregated results, not raw data
-- Much smaller than base table
```

### Advantages

✅ **Fast Queries**: Pre-computed results = instant queries  
✅ **Reduced Computation**: Complex aggregations done once  
✅ **Storage Efficient**: Stores only results, not raw data  
✅ **Query Simplification**: Hide complex joins/aggregations  
✅ **Performance Predictable**: Consistent query times  

### Disadvantages

❌ **Stale Data**: Not real-time, needs refresh  
❌ **Refresh Overhead**: Refresh can be expensive  
❌ **Storage Duplication**: Stores derived data  
❌ **Maintenance Required**: Need refresh schedule  
❌ **Limited ACID**: No transaction support  
❌ **No Time Travel**: Can't query historical states  
❌ **Limited in AWS**: Not fully supported in Athena/Glue  

### Best Use Cases

1. **Expensive Aggregations**
   - Complex GROUP BY queries
   - Multi-table joins
   - Window functions

2. **Reporting Dashboards**
   - Daily/weekly reports
   - KPI calculations
   - Summary statistics

3. **Data Warehouse Patterns**
   - Star schema fact tables
   - Pre-aggregated dimensions
   - OLAP cubes

### Example: Hypothetical Use Case

```sql
-- Materialized view for daily summary report
CREATE MATERIALIZED VIEW daily_regulatory_summary_mv AS
SELECT 
    seriesid,
    DATE(aod) as report_date,
    COUNT(DISTINCT rssdid) as institution_count,
    COUNT(*) as total_records,
    SUM(CASE WHEN key LIKE 'RCON%' THEN 1 ELSE 0 END) as rcon_fields,
    MAX(submissionts) as latest_submission
FROM collections_data_staging
GROUP BY seriesid, DATE(aod);

-- Refresh nightly
REFRESH MATERIALIZED VIEW daily_regulatory_summary_mv;

-- Dashboard queries are instant
SELECT * FROM daily_regulatory_summary_mv 
WHERE report_date >= CURRENT_DATE - INTERVAL '30' DAY;
```

**Why Materialized View?**
- Dashboard needs fast response times
- Aggregations are expensive on raw data
- Data only needs to be fresh once per day
- Summary data is much smaller than raw data

---

## Regular Views (For Comparison)

### What Are They?

Regular views are **virtual tables** with no physical storage - just saved SQL queries.

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  Regular View: fry9c_report_view                             │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  Glue Catalog Metadata                                       │
│  ├── View Definition (SQL query)                             │
│  └── Base Tables (collections_data_staging)                  │
│                                                              │
│  No Storage (Virtual)                                        │
│  └── Query executed every time view is accessed             │
│                                                              │
│  Base Table: collections_data_staging                        │
│  └── Source data (always current)                           │
└─────────────────────────────────────────────────────────────┘
```

### Key Features

```sql
-- Create regular view (no data stored)
CREATE VIEW fry9c_report_view AS
SELECT 
    seriesid,
    aod,
    rssdid,
    submissionts,
    MAX(CASE WHEN key = 'RCON2170' THEN value END) AS RCON2170,
    MAX(CASE WHEN key = 'RCON0010' THEN value END) AS RCON0010
FROM collections_data_staging
WHERE seriesid = 'fry9c'
GROUP BY seriesid, aod, rssdid, submissionts;

-- Query executes the view SQL every time
SELECT * FROM fry9c_report_view;
```

### Advantages

✅ **Always Current**: Real-time data  
✅ **No Storage Cost**: No physical data  
✅ **No Maintenance**: No refresh needed  
✅ **Simple to Create**: Just save a query  
✅ **Inherits Base Table Features**: ACID, time travel, etc.  

### Disadvantages

❌ **Slower Queries**: Computed on every access  
❌ **Repeated Computation**: Same work done multiple times  
❌ **No Performance Guarantee**: Depends on base table  

---

## Decision Matrix

### When to Use Iceberg Tables

```
Use Iceberg Tables When:
├── You need to store raw/source data
├── Data is frequently updated (inserts/updates/deletes)
├── You need ACID guarantees
├── Time travel is important
├── Schema will evolve over time
├── You need partition evolution
└── Data is the source of truth

Examples:
✅ collections_data_staging (raw MDRM data)
✅ Transaction logs
✅ Master data tables
✅ Streaming data ingestion
```

### When to Use Materialized Views

```
Use Materialized Views When:
├── Query performance is critical
├── Aggregations are expensive
├── Data can be slightly stale
├── You have a refresh schedule
├── Results are much smaller than source
└── Used for reporting/dashboards

Examples:
✅ Daily summary reports
✅ Pre-aggregated KPIs
✅ Complex join results
✅ OLAP cube alternatives

Note: Limited support in AWS Glue/Athena
Consider using Iceberg tables with scheduled ETL instead
```

### When to Use Regular Views

```
Use Regular Views When:
├── Need real-time data
├── Transformation is simple
├── Query performance is acceptable
├── No storage budget for duplication
└── Simplify complex queries

Examples:
✅ fry9c_report_view (pivot transformation)
✅ fry15_report_view (pivot transformation)
✅ Filtered subsets of data
✅ Column renaming/selection
```

---

## Our Architecture Choice

### Current Implementation

```
collections_data_staging (Iceberg Table)
    ↓
fry9c_report_view (Regular Multi-Dialect View)
fry15_report_view (Regular Multi-Dialect View)
```

### Why This Design?

#### 1. Iceberg Table for Raw Data
```
✅ Need to append new submissions regularly
✅ Require ACID for concurrent ingestion
✅ Want time travel for historical analysis
✅ Need efficient partitioning by seriesid
✅ Schema may evolve (new fields added)
```

#### 2. Regular Views for Reports
```
✅ Pivot transformation is relatively fast
✅ Need real-time data (latest submissions)
✅ No storage duplication needed
✅ Multi-dialect support (Athena + Spark)
✅ Simple to maintain
```

### Alternative: Materialized Views

If we used materialized views instead:

```sql
-- Hypothetical materialized view approach
CREATE MATERIALIZED VIEW fry9c_report_mv AS
SELECT 
    seriesid,
    aod,
    rssdid,
    submissionts,
    MAX(CASE WHEN key = 'RCON2170' THEN value END) AS RCON2170,
    -- ... 150+ columns
FROM collections_data_staging
WHERE seriesid = 'fry9c'
GROUP BY seriesid, aod, rssdid, submissionts;

-- Would need to refresh after each ingestion
REFRESH MATERIALIZED VIEW fry9c_report_mv;
```

**Why We Didn't Choose This:**
- ❌ Adds refresh complexity
- ❌ Data would be stale between refreshes
- ❌ Duplicates storage (raw + pivoted)
- ❌ Limited support in Athena
- ❌ Pivot is fast enough with regular views

---

## Hybrid Approach: Iceberg + Scheduled ETL

### Best of Both Worlds

Instead of materialized views, use Iceberg tables with scheduled ETL:

```
collections_data_staging (Iceberg - Raw)
    ↓ Glue ETL Job (Scheduled)
fry9c_report_table (Iceberg - Pivoted)
fry15_report_table (Iceberg - Pivoted)
```

### Advantages Over Materialized Views

```
✅ Full ACID support
✅ Time travel on pivoted data
✅ Schema evolution
✅ Better AWS integration
✅ Incremental updates possible
✅ Partition evolution
✅ Snapshot isolation
```

### Implementation

```python
# Glue ETL Job: Refresh Pivoted Tables
from pyspark.sql.functions import col, max as spark_max

# Read source data
source_df = spark.table("glue_catalog.iceberg_db.collections_data_staging")

# Filter for fry9c
fry9c_df = source_df.filter(col("seriesid") == "fry9c")

# Pivot transformation
pivoted_df = fry9c_df.groupBy("seriesid", "aod", "rssdid", "submissionts") \
    .pivot("key") \
    .agg(spark_max("value"))

# Write to Iceberg table (overwrites or merges)
pivoted_df.writeTo("glue_catalog.iceberg_db.fry9c_report_table") \
    .using("iceberg") \
    .createOrReplace()

# Schedule: Run after each ingestion
```

### When to Use This Approach

```
Use Iceberg + ETL When:
├── Need both performance AND real-time option
├── Want ACID on derived data
├── Need time travel on aggregations
├── Have complex transformations
├── Want incremental updates
└── Need full AWS Glue/Athena support
```

---

## Performance Comparison

### Query Performance

| Scenario | Iceberg Table | Materialized View | Regular View |
|----------|--------------|-------------------|--------------|
| Simple SELECT | ⚡ Fast | ⚡ Fast | ⚡ Fast |
| Filtered SELECT | ⚡ Fast (partitions) | ⚡ Fast | 🐌 Slower |
| Aggregation | 🐌 Slower | ⚡ Fast | 🐌 Slower |
| Complex JOIN | 🐌 Slower | ⚡ Fast | 🐌 Slower |
| Pivot | 🐌 Slower | ⚡ Fast | 🐌 Slower |

### Storage Costs

```
Iceberg Table:        $$$ (stores all data)
Materialized View:    $$ (stores results)
Regular View:         $ (no storage)
```

### Maintenance Overhead

```
Iceberg Table:        Low (automatic)
Materialized View:    High (manual refresh)
Regular View:         None
```

---

## Summary

### Quick Decision Guide

```
Need to store raw data?
└─> Use Iceberg Table

Need fast aggregations?
├─> Data can be stale? → Materialized View (or Iceberg + ETL)
└─> Need real-time? → Regular View

Need simple transformation?
└─> Use Regular View

Need ACID + Performance?
└─> Use Iceberg Table + Scheduled ETL
```

### Our Recommendation

For the MDRM regulatory reporting use case:

1. **Raw Data**: Iceberg Table (`collections_data_staging`)
   - Source of truth
   - ACID guarantees
   - Time travel
   - Partition by seriesid

2. **Report Views**: Regular Multi-Dialect Views (`fry9c_report_view`)
   - Real-time data
   - No storage duplication
   - Works in Athena + Spark
   - Simple maintenance

3. **Future Enhancement**: Consider Iceberg tables for pivoted data if:
   - Query performance becomes an issue
   - Need time travel on pivoted data
   - Want to cache expensive transformations

---

**Version**: 1.0  
**Last Updated**: February 11, 2026  
**Status**: Reference Guide ✅
