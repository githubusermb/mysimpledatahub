create an aws data ingestion pipeline that ingests csv file under ingest_ts=<ts> folder in raw data s3 bucket. csv file should have header entity1,entity2,entity3,entity4, key, value. use same value for entity1 to entity4 columns with distinct key and value data for first 1000 records. Change data for entity4 and value columns for next 1000 records. Repeat the same for next 1000 records for different entity1, key, value. Create glue pyspark job to infer schema from csv file and create iceberg table using inferred schema with data in parquet if it does not exist. Then load data from csv file to iceberg table. Provide terraform code for all infrastructure resources.



create glue job to create dynamic view for each distinct entity1 value  in entity_data table. All distinct values in key column for the specifci entity1 should be transposed as columns and data for transposed columns should be from value column of entity_data table. View should also include columns - entity1, entity2, entity3, entity4 alongwith transposed columns. Provide an example of this view to verify understanding.

update terraform and if needed, glue jobs to create tablename with _tbl as prefix ex:- collections_data_tbl.

rename glue_create_normal_views.py to glue_create_normal_views.py and update references to this script for new name.

update glue_create_normal_views.py to create 2 dynamic views - collections_data_vw which is same as collections_data_tbl, cdp_data_vw that gives data for specific seriesid. seriesid filter should be externalized in glue job. 

create another glue job that creates pivoted views for distinct seriesid in collections_data_tbl. value of context level mdrm and value should be combined with item_mdrm value following pattern <context_level1_mdrm>=<context_level1_value>:<context_level2_mdrm>=<context_level2_value>:<context_level3_mdrm>=<context_level3_value>:<item_value>. if no context info exist then only item_value should be in the record for that item_mdrm. other columns in view should be seriesid,aod,rssdid,submission_ts, ingest_ts. name the glue job as glue_create_series_wide_views.py.


provide an example of a privoted view for collections_data csv file with each distinct value of item_mdrm as column. privoted view should not desc columns. how should context_level mdrm and value columns be handled in thsi pivoted view

how will pivoted view looks like if each distinct item_mdrm is transposed to column. value of context level mdrm and value should be combined with item_mdrm value following pattern <context_level1_mdrm>=<context_level1_value>:<context_level2_mdrm>=<context_level2_value>:<context_level3_mdrm>=<context_level3_value>:<item_value>. if no context info exist then only item_value should be in the record for that item_mdrm. 

how will pivoted view looks like if each distinct item_mdrm is transposed to column. value of context level mdrm and value should be combined with item_mdrm value following pattern <context_level1_mdrm>=<context_level1_value>:<context_level2_mdrm>=<context_level2_value>:<context_level3_mdrm>=<context_level3_value>:<item_value>. if no context info exist then only item_value should be in the record for that item_mdrm. shows an example of pivoted view for fr2004a series

create a copy of generate_smaple_csv.py that has header seriesid,aod,rssdid,submissionts,key,value. Generate first 1000 records with seriesid as FRY9C,aod as 20230131, rssdid as 1234567, key as each distinct value from comma separated list in mdrm/output_fry9c.txt, value as random integer value. Generate next 1000 records with seriesid as FRY9C,aod as 20231231, rssdid as 2345678, key as each distinct value from comma separated list in mdrm/output_fry9c.txt, value as random integer value. Generate next 1000 records with seriesid as FRY15,aod as 20241231, rssdid as 2345678, key as each distinct value from comma separated list in mdrm/output_fry15.txt, value as random integer value.


create a copy of generate_smaple_csv.py that has header seriesid,aod,rssdid,submission_ts,item_mdrm,item_desc,item_value,context_level1_mdrm,context_level1_desc,context_level1_value,context_level2_mdrm,context_level2_desc,context_level2_value,context_level3_mdrm,context_level3_desc,context_level3_value. Generate first 1000 records with seriesid as FRY9C,aod as 20230131, rssdid as 1234567, item_mdrm,item_desc,context_level1_mdrm,context_level1_desc,context_level1_value as each distinct value from  mdrm/fry9c_mdrm_map_output.csv, item_value as random integer value with same submission_ts for all new records. Generate next 1000 records with seriesid as FRY9C,aod as 20231231, rssdid as 2345678, item_mdrm,item_desc,context_level1_mdrm,context_level1_desc,context_level1_value as each distinct value from  mdrm/fry9c_mdrm_map_output.csv, item_value as random integer value with same submission_ts for all new records. Generate next 1000 records with seriesid as FRY15,aod as 20241231, rssdid as 2345678, item_mdrm,item_desc,context_level1_mdrm,context_level1_desc,context_level1_value as each distinct value from  mdrm/fry15_mdrm_map_output.csv, item_value as random integer value with same submission_ts for all new records.Generate next 1000 records with seriesid as FR2004A,aod as 20241231, rssdid as 2345678, item_mdrm,item_desc,context_level1_mdrm,context_level1_desc,context_level1_value as each distinct value from  mdrm/fr2004a_mdrm_map_output.csv, item_value as random integer value with same submission_ts for all new records.


aws s3 cp your-data.csv s3://your-bucket/collections-data/


aws s3 cp "data/ingest_ts=1770609249/" "s3://sdh-raw-data-ingestion-bucket/collections-data/ingest_ts=1770609249/" --recursive


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

