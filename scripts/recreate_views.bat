@echo off
REM Script to recreate views with correct Glue 5.0 syntax
REM This script:
REM 1. Drops old views with dialect errors
REM 2. Uploads updated script to S3
REM 3. Runs Glue job to recreate views
REM 4. Verifies views are in catalog

setlocal enabledelayedexpansion

REM Configuration
set DATABASE_NAME=iceberg_db
set VIEW_PREFIX=entity_view
set GLUE_JOB_NAME=create-dynamic-entity-views
set S3_BUCKET=iceberg-data-storage-bucket
set SCRIPT_PATH=scripts\glue_create_dynamic_views.py

echo ==========================================
echo Recreating Views with Glue 5.0 Syntax
echo ==========================================
echo.

REM Step 1: List existing views
echo Step 1: Checking for existing views...
aws glue get-tables --database-name %DATABASE_NAME% --query "TableList[?starts_with(Name, '%VIEW_PREFIX%')].Name" --output text > temp_views.txt

set /p VIEWS=<temp_views.txt
if "%VIEWS%"=="" (
  echo No existing views found.
) else (
  echo Found views:
  type temp_views.txt
  echo.
  
  REM Step 2: Drop existing views
  echo Step 2: Dropping existing views...
  for /f "tokens=*" %%v in (temp_views.txt) do (
    echo   Dropping: %%v
    aws glue delete-table --database-name %DATABASE_NAME% --name %%v 2>nul
  )
  echo All views dropped.
)

del temp_views.txt 2>nul
echo.

REM Step 3: Upload updated script
echo Step 3: Uploading updated script to S3...
if exist "%SCRIPT_PATH%" (
  aws s3 cp %SCRIPT_PATH% s3://%S3_BUCKET%/scripts/
  echo Script uploaded successfully.
) else (
  echo ERROR: Script not found at %SCRIPT_PATH%
  exit /b 1
)

echo.

REM Step 4: Run Glue job
echo Step 4: Starting Glue job...
aws glue start-job-run --job-name %GLUE_JOB_NAME% --query "JobRunId" --output text > temp_jobid.txt
set /p JOB_RUN_ID=<temp_jobid.txt
del temp_jobid.txt

echo Job started with ID: %JOB_RUN_ID%
echo Waiting for job to complete...

REM Wait for job to complete
:wait_loop
aws glue get-job-run --job-name %GLUE_JOB_NAME% --run-id %JOB_RUN_ID% --query "JobRun.JobRunState" --output text > temp_state.txt
set /p JOB_STATE=<temp_state.txt
del temp_state.txt

if "%JOB_STATE%"=="SUCCEEDED" (
  echo Job completed successfully!
  goto verify
)

if "%JOB_STATE%"=="FAILED" goto failed
if "%JOB_STATE%"=="STOPPED" goto failed
if "%JOB_STATE%"=="ERROR" goto failed

echo Job state: %JOB_STATE% (waiting...)
timeout /t 10 /nobreak >nul
goto wait_loop

:failed
echo Job failed with state: %JOB_STATE%
echo Check logs for details:
echo aws logs tail /aws-glue/jobs/error --follow
exit /b 1

:verify
echo.

REM Step 5: Verify views
echo Step 5: Verifying views in Glue Data Catalog...
aws glue get-tables --database-name %DATABASE_NAME% --query "TableList[?starts_with(Name, '%VIEW_PREFIX%')].[Name,TableType]" --output table

echo.

REM Step 6: Get first view for testing
aws glue get-tables --database-name %DATABASE_NAME% --query "TableList[?starts_with(Name, '%VIEW_PREFIX%')].Name | [0]" --output text > temp_first.txt
set /p FIRST_VIEW=<temp_first.txt
del temp_first.txt

if not "%FIRST_VIEW%"=="" (
  echo ==========================================
  echo SUCCESS! Views are ready to use.
  echo ==========================================
  echo.
  echo Test in Athena with this query:
  echo.
  echo   SELECT * FROM %DATABASE_NAME%.%FIRST_VIEW% LIMIT 10;
  echo.
  echo Or check view details:
  echo.
  echo   aws glue get-table --database-name %DATABASE_NAME% --name %FIRST_VIEW%
  echo.
)

echo Done!
