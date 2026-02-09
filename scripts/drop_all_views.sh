#!/bin/bash
# Bash script to drop all views in iceberg_db
# Run this before recreating views with the Glue job

DATABASE_NAME="iceberg_db"

echo "Fetching all views from database: $DATABASE_NAME"

# Get all views
views=$(aws glue get-tables \
    --database-name $DATABASE_NAME \
    --query "TableList[?TableType=='VIRTUAL_VIEW'].Name" \
    --output text)

if [ -z "$views" ]; then
    echo "No views found in database $DATABASE_NAME"
    exit 0
fi

echo "Found views:"
echo "$views" | tr '\t' '\n' | sed 's/^/  - /'

echo ""
echo "Dropping all views..."

for view in $views; do
    echo "Dropping view: $view"
    aws glue delete-table --database-name $DATABASE_NAME --table-name $view
    
    if [ $? -eq 0 ]; then
        echo "  ✓ Successfully dropped $view"
    else
        echo "  ✗ Failed to drop $view"
    fi
done

echo ""
echo "All views dropped!"
echo "Now run: cd terraform && terraform apply"
