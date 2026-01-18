#!/bin/bash

# dbt execution script for Ethiopian Medical Business Data Warehouse

set -e

echo "=== dbt Data Warehouse Pipeline ==="

# Check if dbt is available
if ! command -v dbt &> /dev/null; then
    echo "Error: dbt is not installed or not in PATH"
    echo "Please install dbt-postgres: pip install dbt-postgres"
    exit 1
fi

# Change to dbt project directory
cd medical_warehouse

echo "Current directory: $(pwd)"

# Step 1: Load raw data to PostgreSQL
echo "Step 1: Loading raw data to PostgreSQL..."
cd ..
python src/load_to_postgres.py
cd medical_warehouse

# Step 2: Run dbt models
echo "Step 2: Running dbt transformations..."
dbt run

# Step 3: Run data quality tests
echo "Step 3: Running data quality tests..."
dbt test

# Step 4: Generate documentation
echo "Step 4: Generating documentation..."
dbt docs generate

echo "=== Pipeline Complete ==="
echo "View documentation: dbt docs serve (from medical_warehouse directory)"
echo "Database: medical_warehouse, schema: analytics"
