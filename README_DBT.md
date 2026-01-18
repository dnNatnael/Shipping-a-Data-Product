# dbt Data Warehouse for Ethiopian Medical Business Data

This dbt project transforms raw Telegram data into a clean, structured data warehouse using dimensional modeling principles.

## Project Structure

```
medical_warehouse/
├── models/
│   ├── staging/           # Raw data cleaning and standardization
│   │   ├── stg_telegram_messages.sql
│   │   ├── sources.yml
│   │   └── schema.yml
│   └── marts/            # Dimensional model (star schema)
│       ├── dim_channels.sql
│       ├── dim_dates.sql
│       ├── fct_messages.sql
│       └── schema.yml
├── tests/                # Custom data quality tests
│   ├── assert_no_future_messages.sql
│   ├── assert_positive_views.sql
│   ├── assert_valid_message_length.sql
│   └── assert_no_duplicate_messages.sql
├── dbt_project.yml       # dbt project configuration
└── dbt_docs.yml         # Model documentation
```

## Star Schema Design

### Dimension Tables

#### `dim_channels`
- **Purpose**: Channel-level analytics and classification
- **Key**: `channel_key` (surrogate)
- **Attributes**: Channel name, type, activity metrics, media statistics
- **Business Value**: Channel performance analysis, content strategy insights

#### `dim_dates`
- **Purpose**: Time-based analysis and trend identification
- **Key**: `date_key` (surrogate)
- **Attributes**: Calendar dimensions, weekend flags, year/quarter/month groupings
- **Business Value**: Temporal trend analysis, seasonal patterns

### Fact Table

#### `fct_messages`
- **Purpose**: Central analytics table with all message metrics
- **Grain**: One row per message
- **Foreign Keys**: `channel_key`, `date_key`
- **Measures**: View counts, forwards, message length, media flags
- **Business Value**: Core analytics for business questions

## Data Transformation Pipeline

### 1. Staging Layer (`stg_telegram_messages`)
- **Input**: Raw JSON data from `raw.telegram_messages`
- **Transformations**:
  - Data type casting (dates, integers, booleans)
  - Column standardization (lowercase, trimming)
  - Invalid record filtering (nulls, future dates)
  - Calculated fields (message_length, has_image)
  - Date component extraction

### 2. Dimension Layer
- **`dim_channels`**: Channel aggregation and classification
- **`dim_dates`**: Calendar dimension with temporal attributes

### 3. Fact Layer
- **`fct_messages`**: Join staging data with dimension keys

## Data Quality Tests

### Built-in Tests
- **Unique**: Primary keys (`channel_key`, `date_key`)
- **Not Null**: Critical columns
- **Relationships**: Foreign key validation

### Custom Tests
- **`assert_no_future_messages`**: Prevents future-dated messages
- **`assert_positive_views`**: Ensures view counts are non-negative
- **`assert_valid_message_length`**: Validates message length within Telegram limits
- **`assert_no_duplicate_messages`**: Prevents duplicate message IDs

## Business Questions Answered

### Top Products Analysis
```sql
-- Top 10 most frequently mentioned products
SELECT 
    dc.channel_name,
    COUNT(*) as message_count,
    SUM(fm.view_count) as total_views
FROM analytics.fct_messages fm
JOIN analytics.dim_channels dc ON fm.channel_key = dc.channel_key
GROUP BY dc.channel_name
ORDER BY message_count DESC
LIMIT 10;
```

### Price/Availability Analysis
```sql
-- Channel-specific product availability patterns
SELECT 
    dc.channel_name,
    dd.month_name,
    COUNT(*) as posts_count,
    AVG(fm.view_count) as avg_engagement
FROM analytics.fct_messages fm
JOIN analytics.dim_channels dc ON fm.channel_key = dc.channel_key
JOIN analytics.dim_dates dd ON fm.date_key = dd.date_key
WHERE fm.has_image = true
GROUP BY dc.channel_name, dd.month_name
ORDER BY posts_count DESC;
```

### Visual Content Analysis
```sql
-- Channels with most visual content
SELECT 
    dc.channel_name,
    dc.total_posts,
    dc.posts_with_images,
    dc.image_percentage
FROM analytics.dim_channels dc
ORDER BY dc.image_percentage DESC;
```

### Trend Analysis
```sql
-- Daily and weekly posting trends
SELECT 
    dd.full_date,
    dd.day_name,
    COUNT(*) as message_count,
    SUM(fm.view_count) as total_views
FROM analytics.fct_messages fm
JOIN analytics.dim_dates dd ON fm.date_key = dd.date_key
GROUP BY dd.full_date, dd.day_name
ORDER BY dd.full_date;
```

## Running dbt

### Install Dependencies
```bash
pip install dbt-postgres
```

### Setup Database
```bash
# Create PostgreSQL database
createdb medical_warehouse

# Load raw data
python src/load_to_postgres.py
```

### Run dbt
```bash
cd medical_warehouse

# Run all models
dbt run

# Run tests
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

## Configuration

### Database Connection
Edit `.dbt/profiles.yml` to match your PostgreSQL configuration:
```yaml
medical_warehouse:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      port: 5432
      user: postgres
      password: your_password
      dbname: medical_warehouse
      schema: analytics
```

## Data Quality Assurance

### Validation Rules
- No future-dated messages
- Non-negative view counts
- Valid message lengths (0-4000 chars)
- Unique message IDs per channel
- Proper foreign key relationships

### Monitoring
- Test results in `dbt test` output
- Documentation at `http://localhost:8080` after `dbt docs serve`
- Log files in `logs/` directory

## Performance Considerations

### Materialization
- Staging models: Tables (for data quality)
- Mart models: Tables (for analytics performance)

### Indexing Strategy
- Primary keys on all dimension tables
- Foreign key indexes on fact table
- Date-based indexes for temporal queries

## Next Steps

1. **Object Detection Integration**: Enrich image data with YOLO model
2. **Analytical API**: Build FastAPI endpoints for insights
3. **Scheduled Refresh**: Automate data pipeline execution
4. **Performance Optimization**: Add incremental loading strategies
