# Dagster Pipeline Orchestration

This document describes the Dagster-based pipeline orchestration for the Ethiopian Medical Business Data Platform, providing automated, observable, and schedulable data pipeline execution.

## Overview

Dagster transforms our collection of scripts into a production-grade pipeline with proper dependency management, execution monitoring, failure handling, and automated scheduling.

## Pipeline Architecture

### **Complete Pipeline Flow**
```
scrape_telegram_data → load_raw_to_postgres → run_dbt_transformations → run_yolo_enrichment
```

### **Pipeline Operations**

#### **1. scrape_telegram_data**
- **Purpose**: Extract messages and images from Ethiopian medical Telegram channels
- **Input**: Telegram API credentials and channel configuration
- **Output**: Path to scraped data directory
- **Error Handling**: API rate limiting, network issues, authentication failures
- **Asset Tracking**: `telegram_data` asset with metadata

#### **2. load_raw_to_postgres**
- **Purpose**: Load JSON data from data lake to PostgreSQL database
- **Input**: Path to scraped data directory
- **Output**: Number of records loaded
- **Error Handling**: Data validation, duplicate handling, connection issues
- **Asset Tracking**: `raw_database` asset with loading statistics

#### **3. run_dbt_transformations**
- **Purpose**: Execute dbt transformations to build dimensional data warehouse
- **Input**: Number of records loaded (for validation)
- **Output**: dbt execution results and statistics
- **Error Handling**: Model failures, test failures, timeout handling
- **Asset Tracking**: `data_warehouse` asset with transformation metadata

#### **4. run_yolo_enrichment**
- **Purpose**: Run YOLO object detection on images to enrich data
- **Input**: dbt transformation results
- **Output**: YOLO enrichment results and statistics
- **Error Handling**: Model loading failures, image processing errors
- **Asset Tracking**: `image_enrichment` asset with detection metrics

## Available Jobs

### **1. ethiopian_medical_pipeline**
**Complete end-to-end pipeline**
- **Description**: Full pipeline from scraping to enrichment
- **Use Case**: Daily automated execution
- **Execution Time**: ~30-60 minutes depending on data volume
- **Dependencies**: All operations in sequence

### **2. scrape_and_load_pipeline**
**Lightweight scraping and loading**
- **Description**: Only scraping and database loading
- **Use Case**: Testing, incremental updates
- **Execution Time**: ~10-20 minutes
- **Dependencies**: scrape → load

### **3. transformation_pipeline**
**dbt transformations only**
- **Description**: Run dbt models on existing data
- **Use Case**: Schema updates, model changes
- **Execution Time**: ~5-10 minutes
- **Dependencies**: None (assumes data exists)

### **4. enrichment_pipeline**
**YOLO enrichment only**
- **Description**: Object detection on existing data
- **Use Case**: Image analysis, model updates
- **Execution Time**: ~15-30 minutes
- **Dependencies**: None (assumes warehouse exists)

## Scheduling

### **Daily Schedule**
```yaml
daily_schedule:
  cron: "0 2 * * *"  # Daily at 2 AM UTC
  job: ethiopian_medical_pipeline
  timezone: UTC
```

### **Weekly Schedule**
```yaml
weekly_schedule:
  cron: "0 3 * * 0"  # Weekly Sunday at 3 AM UTC
  job: ethiopian_medical_pipeline
  timezone: UTC
```

## Setup and Usage

### **1. Install Dependencies**
```bash
pip install dagster dagster-webserver
```

### **2. Start Dagster Development Server**
```bash
./run_dagster.sh
```

### **3. Access Dagster UI**
- **Web UI**: http://localhost:3000
- **GraphQL**: http://localhost:3000/graphql
- **GraphQL Playground**: http://localhost:3000/graphql_playground

### **4. Run Pipeline Manually**
```bash
# Via UI
1. Navigate to http://localhost:3000
2. Select "ethiopian_medical_pipeline"
3. Click "Launch Run"

# Via CLI
dagster job execute -f pipeline.py ethiopian_medical_pipeline
```

## Monitoring and Alerting

### **Pipeline Monitoring**
```bash
python monitor_pipeline.py
```

### **Alert Types**
- **Critical**: Pipeline failures
- **Error**: High failure rates (>10%)
- **Warning**: Long execution times, low data volume, stale data
- **Info**: Successful completions

### **Alert Configuration**
Alerts are configured in `pipeline_alerts.json`:
```json
{
  "alert_type": "pipeline_failure",
  "threshold": 1.0,
  "comparison": "gt",
  "message_template": "Pipeline failed with status: {status}",
  "severity": "critical"
}
```

### **Monitoring Metrics**
- **Pipeline Health**: Success/failure rates
- **Execution Performance**: Run times, bottlenecks
- **Data Quality**: Volume, freshness, completeness
- **Resource Usage**: Memory, CPU, database connections

## Configuration

### **Dagster Configuration** (`dagster.yaml`)
```yaml
python_modules:
  - pipeline

storage:
  root: dagster_home

webserver:
  host: 0.0.0.0
  port: 3000
  graphql_playground: true

scheduler:
  module: dagster.daemon.run

execution:
  multiprocess:
    max_concurrent: 4
```

### **Environment Variables**
```bash
DB_HOST=localhost
DB_PORT=5432
DB_NAME=medical_warehouse
DB_USER=postgres
DB_PASSWORD=password
RAW_DATA_PATH=data/raw
LOGS_PATH=logs
```

## Production Deployment

### **Docker Deployment**
```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
EXPOSE 3000

CMD ["dagster", "dev", "-f", "pipeline.py", "--host", "0.0.0.0"]
```

### **Systemd Service**
```ini
[Unit]
Description=Dagster Ethiopian Medical Pipeline
After=postgresql.service

[Service]
Type=simple
User=dagster
WorkingDirectory=/opt/dagster
ExecStart=/opt/dagster/venv/bin/dagster dev -f pipeline.py
Restart=always

[Install]
WantedBy=multi-user.target
```

### **Production Considerations**
- **Database Connection Pooling**: Configure for concurrent operations
- **Resource Limits**: Set appropriate memory and CPU limits
- **Log Rotation**: Implement log rotation for long-running processes
- **Backup Strategy**: Regular backups of Dagster storage and database
- **Security**: Restrict UI access, implement authentication

## Troubleshooting

### **Common Issues**

#### **Database Connection Errors**
```bash
# Check PostgreSQL status
sudo systemctl status postgresql

# Test connection
python -c "from src.database import test_connection; print(test_connection())"
```

#### **dbt Model Failures**
```bash
# Check dbt project
cd medical_warehouse
dbt parse
dbt build --select <failing_model>
```

#### **YOLO Model Loading**
```bash
# Check model files
ls -la ~/.ultralytics/models/

# Test YOLO separately
python -c "from ultralytics import YOLO; print('YOLO available')"
```

#### **Dagster UI Not Loading**
```bash
# Check port availability
netstat -tlnp | grep :3000

# Clear Dagster cache
rm -rf dagster_home
```

### **Debug Mode**
```bash
# Enable debug logging
export DAGSTER_LOG_LEVEL=DEBUG

# Run with verbose output
dagster dev -f pipeline.py --log-level DEBUG
```

## Performance Optimization

### **Execution Optimization**
- **Parallel Processing**: Configure max_concurrent for multi-process execution
- **Resource Management**: Monitor memory usage during YOLO processing
- **Database Indexing**: Ensure proper indexes on frequently queried columns
- **Caching**: Implement result caching for expensive operations

### **Pipeline Optimization**
- **Incremental Loading**: Only process new/updated data
- **Selective Execution**: Run specific jobs based on data changes
- **Resource Allocation**: Allocate more resources to YOLO enrichment
- **Batch Processing**: Process images in batches to manage memory

## Integration Points

### **External Systems**
- **PostgreSQL**: Data warehouse storage
- **Telegram API**: Data source
- **YOLO Models**: Image processing
- **dbt**: Data transformation
- **FastAPI**: API layer

### **Data Flow**
```
Telegram Channels → Data Lake → PostgreSQL → dbt → YOLO → Analytics API
      ↓              ↓           ↓      ↓      ↓
   Dagster → Dagster → Dagster → Dagster → Dagster → Dashboard
```

## Business Value

### **Operational Excellence**
- **Automation**: Fully automated pipeline execution
- **Reliability**: Error handling and retry logic
- **Observability**: Comprehensive monitoring and alerting
- **Scalability**: Parallel execution and resource management

### **Data Quality**
- **Consistency**: Standardized execution environment
- **Freshness**: Automated daily updates
- **Completeness**: End-to-end data validation
- **Traceability**: Complete audit trail

### **Cost Efficiency**
- **Resource Optimization**: Efficient resource usage
- **Automation**: Reduced manual intervention
- **Scheduling**: Optimal execution timing
- **Monitoring**: Early issue detection

## Next Steps

### **Enhancement Opportunities**
- **Dynamic Scheduling**: Adjust frequency based on data volume
- **Smart Retries**: Intelligent retry logic for transient failures
- **Performance Tuning**: Optimize execution for large datasets
- **Advanced Monitoring**: Custom metrics and dashboards
- **Integration Testing**: Automated end-to-end testing

### **Production Readiness**
- **Security**: Authentication and authorization
- **High Availability**: Redundancy and failover
- **Disaster Recovery**: Backup and recovery procedures
- **Compliance**: Audit logging and data governance

The Dagster orchestration transforms our data pipeline from a collection of scripts into a production-ready, observable, and maintainable system that can reliably deliver insights from Ethiopian medical business data.
