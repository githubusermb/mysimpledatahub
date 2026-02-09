# PowerShell script to drop all views in iceberg_db
# Run this before recreating views with the Glue job

$DATABASE_NAME = "iceberg_db"

Write-Host "Fetching all views from database: $DATABASE_NAME" -ForegroundColor Cyan

# Get all views
$views = aws glue get-tables `
    --database-name $DATABASE_NAME `
    --query "TableList[?TableType=='VIRTUAL_VIEW'].Name" `
    --output json | ConvertFrom-Json

if ($views.Count -eq 0) {
    Write-Host "No views found in database $DATABASE_NAME" -ForegroundColor Yellow
    exit 0
}

Write-Host "Found $($views.Count) views:" -ForegroundColor Green
$views | ForEach-Object { Write-Host "  - $_" }

Write-Host "`nDropping all views..." -ForegroundColor Cyan

foreach ($view in $views) {
    Write-Host "Dropping view: $view" -ForegroundColor Yellow
    aws glue delete-table --database-name $DATABASE_NAME --table-name $view
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "  ✓ Successfully dropped $view" -ForegroundColor Green
    } else {
        Write-Host "  ✗ Failed to drop $view" -ForegroundColor Red
    }
}

Write-Host "`nAll views dropped!" -ForegroundColor Green
Write-Host "Now run: cd terraform && terraform apply" -ForegroundColor Cyan
