# PowerShell script to verify view format and test in Athena
param(
    [string]$ViewName = "entity_view_entity1_set2",
    [string]$DatabaseName = "iceberg_db"
)

Write-Host "=== Verifying View Format ===" -ForegroundColor Cyan
Write-Host ""

# Step 1: Get the view metadata
Write-Host "Step 1: Fetching view metadata from Glue..." -ForegroundColor Yellow
$table = aws glue get-table `
    --database-name $DatabaseName `
    --name $ViewName `
    --output json | ConvertFrom-Json

if (-not $table) {
    Write-Host "ERROR: Could not fetch table" -ForegroundColor Red
    exit 1
}

# Step 2: Check StorageDescriptor columns
Write-Host "Step 2: Checking StorageDescriptor.Columns..." -ForegroundColor Yellow
$columns = $table.Table.StorageDescriptor.Columns
Write-Host "  Total columns: $($columns.Count)" -ForegroundColor Green
Write-Host "  First column:"
Write-Host "    Name: $($columns[0].Name)"
Write-Host "    Type: $($columns[0].Type)"
Write-Host "    Type is string: $($columns[0].Type -is [string])"
Write-Host ""

# Step 3: Decode and validate Presto view
Write-Host "Step 3: Decoding Presto view..." -ForegroundColor Yellow
$viewText = $table.Table.ViewOriginalText
if ($viewText -match '/\* Presto View: (.*) \*/') {
    $base64 = $Matches[1]
    try {
        $decoded = [System.Text.Encoding]::UTF8.GetString([System.Convert]::FromBase64String($base64))
        $prestoView = $decoded | ConvertFrom-Json
        
        Write-Host "  Presto view decoded successfully!" -ForegroundColor Green
        Write-Host "  Total columns in Presto view: $($prestoView.columns.Count)"
        Write-Host "  First column in Presto view:"
        Write-Host "    name: $($prestoView.columns[0].name)"
        Write-Host "    type: $($prestoView.columns[0].type)"
        Write-Host "    type is string: $($prestoView.columns[0].type -is [string])"
        Write-Host ""
        
        # Check for any non-string types
        $badColumns = $prestoView.columns | Where-Object { $_.type -isnot [string] }
        if ($badColumns.Count -gt 0) {
            Write-Host "  WARNING: Found $($badColumns.Count) columns with non-string types!" -ForegroundColor Red
            $badColumns | Select-Object -First 5 | ForEach-Object {
                Write-Host "    Column: $($_.name), Type: $($_.type), Type class: $($_.type.GetType().Name)"
            }
        } else {
            Write-Host "  All column types are strings ✓" -ForegroundColor Green
        }
    } catch {
        Write-Host "  ERROR: Failed to decode Presto view: $_" -ForegroundColor Red
    }
} else {
    Write-Host "  ERROR: Could not extract Presto view from ViewOriginalText" -ForegroundColor Red
}

Write-Host ""
Write-Host "=== Testing in Athena ===" -ForegroundColor Cyan
Write-Host "Run this query in Athena to test:" -ForegroundColor Yellow
Write-Host "  SELECT * FROM $DatabaseName.$ViewName LIMIT 10;" -ForegroundColor White
Write-Host ""
Write-Host "If you get an error, please share the EXACT error message." -ForegroundColor Yellow
