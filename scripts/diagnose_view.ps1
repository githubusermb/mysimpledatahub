# PowerShell script to diagnose view column issues
param(
    [Parameter(Mandatory=$true)]
    [string]$ViewName = "entity_view_FRY9C",
    
    [string]$DatabaseName = "iceberg_db"
)

Write-Host "Diagnosing view: $DatabaseName.$ViewName" -ForegroundColor Cyan
Write-Host ""

# Get the table metadata
Write-Host "Fetching table metadata..." -ForegroundColor Yellow
$table = aws glue get-table `
    --database-name $DatabaseName `
    --name $ViewName `
    --output json | ConvertFrom-Json

if (-not $table) {
    Write-Host "ERROR: Could not fetch table metadata" -ForegroundColor Red
    exit 1
}

# Check StorageDescriptor Columns
Write-Host "StorageDescriptor.Columns:" -ForegroundColor Green
$columns = $table.Table.StorageDescriptor.Columns
Write-Host "  Total columns: $($columns.Count)"
Write-Host "  First 3 columns:"
for ($i = 0; $i -lt [Math]::Min(3, $columns.Count); $i++) {
    $col = $columns[$i]
    Write-Host "    [$i] Name: $($col.Name)"
    Write-Host "        Type: $($col.Type)"
    Write-Host "        Type class: $($col.Type.GetType().Name)"
    Write-Host ""
}

# Decode Presto view
Write-Host "Decoding Presto view..." -ForegroundColor Yellow
$viewText = $table.Table.ViewOriginalText
if ($viewText -match '/\* Presto View: (.*) \*/') {
    $base64 = $Matches[1]
    $decoded = [System.Text.Encoding]::UTF8.GetString([System.Convert]::FromBase64String($base64))
    $prestoView = $decoded | ConvertFrom-Json
    
    Write-Host "Presto View Columns:" -ForegroundColor Green
    Write-Host "  Total columns: $($prestoView.columns.Count)"
    Write-Host "  First 3 columns:"
    for ($i = 0; $i -lt [Math]::Min(3, $prestoView.columns.Count); $i++) {
        $col = $prestoView.columns[$i]
        Write-Host "    [$i] name: $($col.name)"
        Write-Host "        type: $($col.type)"
        Write-Host "        type class: $($col.type.GetType().Name)"
        Write-Host ""
    }
} else {
    Write-Host "ERROR: Could not extract Presto view from ViewOriginalText" -ForegroundColor Red
}

# Test query in Athena
Write-Host "Testing query in Athena..." -ForegroundColor Yellow
Write-Host "Run this command to test:"
Write-Host "  SELECT * FROM $DatabaseName.$ViewName LIMIT 10;" -ForegroundColor Cyan
