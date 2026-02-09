@echo off
REM Test Views in Spark - Quick Test Script (Windows)
REM This script runs the Glue job to test views in Spark

setlocal enabledelayedexpansion

echo ==========================================
echo Testing Views in Glue Spark
echo ==========================================
echo.

REM Configuration
set JOB_NAME=test-entity-views-spark
set DATABASE_NAME=iceberg_db
set VIEW_PREFIX=entity_view

REM Check if views exist
echo Step 1: Checking if views exist...
for /f "delims=" %%i in ('aws glue get-tables --database-name %DATABASE_NAME% --query "TableList[?TableType=='VIRTUAL_VIEW' ^&^& starts_with(Name, '%VIEW_PREFIX%')].Name" --output text') do set VIEWS=%%i

if "%VIEWS%"=="" (
  echo X No views found with prefix '%VIEW_PREFIX%' in database '%DATABASE_NAME%'
  echo.
  echo Please run the view creation job first:
  echo   aws glue start-job-run --job-name create-dynamic-entity-views
  exit /b 1
)

echo + Found views to test
echo   Views: %VIEWS%
echo.

REM Run the test job
echo Step 2: Starting test job...
for /f "tokens=2 delims=:" %%a in ('aws glue start-job-run --job-name %JOB_NAME% ^| findstr "JobRunId"') do (
  set JOB_RUN_ID=%%a
  set JOB_RUN_ID=!JOB_RUN_ID:"=!
  set JOB_RUN_ID=!JOB_RUN_ID:,=!
  set JOB_RUN_ID=!JOB_RUN_ID: =!
)

if "%JOB_RUN_ID%"=="" (
  echo X Failed to start job
  exit /b 1
)

echo + Job started successfully
echo   Job Run ID: %JOB_RUN_ID%
echo.

REM Wait for job to complete
echo Step 3: Waiting for job to complete...
echo (This usually takes 2-3 minutes)
echo.

set MAX_WAIT=300
set WAIT_TIME=0
set SLEEP_INTERVAL=10

:wait_loop
if %WAIT_TIME% geq %MAX_WAIT% goto timeout

for /f "delims=" %%i in ('aws glue get-job-run --job-name %JOB_NAME% --run-id %JOB_RUN_ID% --query "JobRun.JobRunState" --output text') do set STATUS=%%i

if "%STATUS%"=="SUCCEEDED" (
  echo + Job completed successfully!
  echo.
  goto get_results
)

if "%STATUS%"=="FAILED" goto failed
if "%STATUS%"=="STOPPED" goto failed
if "%STATUS%"=="TIMEOUT" goto failed

echo   Status: %STATUS% (waited %WAIT_TIME%s)
timeout /t %SLEEP_INTERVAL% /nobreak >nul
set /a WAIT_TIME=%WAIT_TIME%+%SLEEP_INTERVAL%
goto wait_loop

:failed
echo X Job %STATUS%
echo.
for /f "delims=" %%i in ('aws glue get-job-run --job-name %JOB_NAME% --run-id %JOB_RUN_ID% --query "JobRun.ErrorMessage" --output text') do set ERROR=%%i
if not "%ERROR%"=="None" (
  echo Error: %ERROR%
)
echo.
echo Check logs for details:
echo   aws logs tail /aws-glue/jobs/output --follow
exit /b 1

:timeout
echo X Job timed out after %MAX_WAIT%s
exit /b 1

:get_results
REM Get job results from logs
echo Step 4: Fetching test results...
echo.

REM Wait a bit for logs to be available
timeout /t 5 /nobreak >nul

echo View logs:
echo   aws logs tail /aws-glue/jobs/output --follow
echo.

echo ==========================================
echo Test Complete
echo ==========================================
echo.
echo To view full logs:
echo   aws logs tail /aws-glue/jobs/output --follow
echo.
echo To view in AWS Console:
echo   https://console.aws.amazon.com/glue/home#/v2/etl-jobs/view/%JOB_NAME%
echo.

endlocal
