create an aws data ingestion pipeline that ingests csv file under ingest_ts=<ts> folder in raw data s3 bucket. csv file should have header entity1,entity2,entity3,entity4, key, value. use same value for entity1 to entity4 columns with distinct key and value data for first 1000 records. Change data for entity4 and value columns for next 1000 records. Repeat the same for next 1000 records for different entity1, key, value. Create glue pyspark job to infer schema from csv file and create iceberg table using inferred schema with data in parquet if it does not exist. Then load data from csv file to iceberg table. Provide terraform code for all infrastructure resources.



create glue job to create dynamic view for each distinct entity1 value  in entity_data table. All distinct values in key column for the specifci entity1 should be transposed as columns and data for transposed columns should be from value column of entity_data table. View should also include columns - entity1, entity2, entity3, entity4 alongwith transposed columns. Provide an example of this view to verify understanding.



create a copy of generate_smaple_csv.py that has header seriesid,aod,rssdid,submissionts,key,value. Generate first 1000 records with seriesid as FRY9C,aod as 20230131, rssdid as 1234567, key as each distinct value from comma separated list in mdrm/output_fry9c.txt, value as random integer value. Generate next 1000 records with seriesid as FRY9C,aod as 20231231, rssdid as 2345678, key as each distinct value from comma separated list in mdrm/output_fry9c.txt, value as random integer value. Generate next 1000 records with seriesid as FRY15,aod as 20241231, rssdid as 2345678, key as each distinct value from comma separated list in mdrm/output_fry15.txt, value as random integer value.


STATSDW report item view - 
SELECT
    DISTINCT CAST("items"."filing_id" AS VARCHAR(250)) AS "filing_id",
    CAST("items"."series_id" AS VARCHAR(20)) AS "series_id",
    CAST("schedule"."schedule_id" AS VARCHAR(250)) AS "schedule_id",
    CAST("items"."rssd_id" AS VARCHAR(22)) AS "rssd_id",
    "items"."as_of_date" AS "as_of_date",
    CAST("items"."item_mdrm" AS VARCHAR(8)) AS "item_mdrm",
    CAST("items"."item_description" AS VARCHAR(4000)) AS "item_description",
    CAST("items"."item_value" AS VARCHAR(4000)) AS "item_value",
    CAST("items"."context_level1_mdrm" AS VARCHAR(8)) AS "context_level1_mdrm",
    CAST("items"."context_level1_desc" AS VARCHAR(4000)) AS "context_level1_desc",
    CAST("items"."context_level1_value" AS VARCHAR(4000)) AS "context_level1_value",
    CAST("items"."context_level2_mdrm" AS VARCHAR(8)) AS "context_level2_mdrm",
    CAST("items"."context_level2_desc" AS VARCHAR(4000)) AS "context_level2_desc",
    CAST("items"."context_level2_value" AS VARCHAR(4000)) AS "context_level2_value",
    CAST("items"."context_level3_mdrm" AS VARCHAR(8)) AS "context_level3_mdrm",
    CAST("items"."context_level3_desc" AS VARCHAR(4000)) AS "context_level3_desc",
    CAST("items"."context_level3_value" AS VARCHAR(4000)) AS "context_level3_value",
    CAST("items"."submission_datetime" AS TIMESTAMP) AS "submission_datetime",
    CAST("items"."filing_status" AS VARCHAR(30)) AS "filing_status",
    CAST("items"."estimated_flag" AS VARCHAR(250)) AS "estimated_flag",
    CAST("items"."estimated_by" AS VARCHAR(250)) AS "estimated_by"