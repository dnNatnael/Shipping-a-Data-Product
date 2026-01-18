#!/bin/bash

# Dagster Pipeline Startup Script

set -e

echo "=== Ethiopian Medical Business Data Pipeline - Dagster ==="

# Check if Dagster is installed
if ! command -v dagster &> /dev/null; then
    echo "❌ Dagster not found. Installing..."
    pip install dagster dagster-webserver
fi

# Check if pipeline.py exists
if [ ! -f "pipeline.py" ]; then
    echo "❌ pipeline.py not found in current directory"
    exit 1
fi

# Create dagster home directory if it doesn't exist
mkdir -p dagster_home

# Check environment variables
echo "🔍 Checking environment configuration..."

if [ -z "$DB_HOST" ]; then
    echo "⚠️  DB_HOST not set, using default: localhost"
    export DB_HOST=localhost
fi

if [ -z "$DB_NAME" ]; then
    echo "⚠️  DB_NAME not set, using default: medical_warehouse"
    export DB_NAME=medical_warehouse
fi

if [ -z "$DB_USER" ]; then
    echo "⚠️  DB_USER not set, using default: postgres"
    export DB_USER=postgres
fi

if [ -z "$DB_PASSWORD" ]; then
    echo "⚠️  DB_PASSWORD not set, using default: password"
    export DB_PASSWORD=password
fi

# Check database connection
echo "🗄️  Testing database connection..."
python -c "
from src.database import test_connection
if test_connection():
    print('✅ Database connection successful')
else:
    print('❌ Database connection failed')
    print('Please ensure PostgreSQL is running and accessible')
    exit(1)
"

if [ $? -ne 0 ]; then
    echo "❌ Database check failed"
    exit 1
fi

# Check if required directories exist
echo "📁 Checking required directories..."

if [ ! -d "data/raw" ]; then
    echo "📁 Creating data/raw directory..."
    mkdir -p data/raw
fi

if [ ! -d "logs" ]; then
    echo "📁 Creating logs directory..."
    mkdir -p logs
fi

if [ ! -d "results" ]; then
    echo "📁 Creating results directory..."
    mkdir -p results
fi

# Check if dbt project is built
echo "🔧 Checking dbt project..."
if [ -d "medical_warehouse" ]; then
    echo "✅ dbt project found"
    
    # Check if dbt models exist
    if [ -f "medical_warehouse/models/marts/fct_messages.sql" ]; then
        echo "✅ dbt models found"
    else
        echo "⚠️  dbt models not found. Run dbt run first."
    fi
else
    echo "❌ dbt project not found"
fi

# Start Dagster development server
echo ""
echo "🚀 Starting Dagster development server..."
echo "📊 Web UI: http://localhost:3000"
echo "📚 GraphQL: http://localhost:3000/graphql"
echo "🔍 API: http://localhost:3000/graphql_playground"
echo ""
echo "📋 Available Jobs:"
echo "   • ethiopian_medical_pipeline (Complete pipeline)"
echo "   • scrape_and_load_pipeline (Scraping + Loading)"
echo "   • transformation_pipeline (dbt only)"
echo "   • enrichment_pipeline (YOLO only)"
echo ""
echo "⏰ Schedules:"
echo "   • daily_schedule: Daily at 2 AM UTC"
echo "   • weekly_schedule: Weekly Sunday at 3 AM UTC"
echo ""
echo "Press Ctrl+C to stop the server"
echo ""

# Start Dagster dev server
dagster dev -f pipeline.py --config-file dagster.yaml
