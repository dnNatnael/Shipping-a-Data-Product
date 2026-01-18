#!/bin/bash

# FastAPI Server Startup Script

set -e

echo "=== Ethiopian Medical Business Analytics API ==="

# Check if database is accessible
echo "Checking database connection..."
python -c "
from api.database import test_connection
if test_connection():
    print('✅ Database connection successful')
else:
    print('❌ Database connection failed')
    print('Please ensure PostgreSQL is running and dbt models are built')
    exit(1)
"

if [ $? -ne 0 ]; then
    echo "❌ Database check failed"
    exit 1
fi

# Start the API server
echo "Starting FastAPI server..."
echo "📊 API Documentation: http://localhost:8000/docs"
echo "📚 Alternative Docs: http://localhost:8000/redoc"
echo "🏥 Health Check: http://localhost:8000/health"
echo ""
echo "Press Ctrl+C to stop the server"
echo ""

cd api
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
