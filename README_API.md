# Ethiopian Medical Business Analytics API

FastAPI application that exposes the Ethiopian medical business data warehouse through REST endpoints for comprehensive analytics and insights.

## Overview

The API provides access to analyzed Telegram data from Ethiopian medical channels, enabling business intelligence through structured endpoints that answer key questions about product trends, channel performance, and content strategies.

## Features

### **🔍 Four Core Analytical Endpoints**

1. **Top Products Analysis** - Identify trending medical terms and products
2. **Channel Activity Tracking** - Monitor posting patterns and engagement
3. **Message Search** - Full-text search across message content
4. **Visual Content Statistics** - Image analysis and engagement metrics

### **🛠️ Technical Features**

- **FastAPI Framework**: Modern, high-performance API with automatic documentation
- **Pydantic Validation**: Comprehensive request/response schema validation
- **SQLAlchemy Integration**: Efficient database connectivity and query execution
- **Error Handling**: Robust error management with proper HTTP status codes
- **CORS Support**: Cross-origin resource sharing for web applications
- **Auto Documentation**: Interactive OpenAPI/Swagger documentation

## Setup

### Install Dependencies
```bash
pip install fastapi uvicorn pydantic python-multipart
```

### Database Prerequisites
```bash
# Ensure PostgreSQL is running
# Build dbt models
cd medical_warehouse
dbt run

# Load YOLO results (optional)
python src/load_yolo_results.py
```

## Usage

### Start the API Server
```bash
./run_api.sh
```

The server will start on `http://localhost:8000`

### Access Documentation
- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc
- **Health Check**: http://localhost:8000/health

## API Endpoints

### 1. Top Products Analysis
**GET** `/api/reports/top-products`

Returns the most frequently mentioned medical products and terms across all channels.

**Parameters:**
- `limit` (int, default=10): Maximum results to return (1-100)
- `min_mentions` (int, default=1): Minimum mentions required
- `date_from` (string, optional): Start date (YYYY-MM-DD)
- `date_to` (string, optional): End date (YYYY-MM-DD)

**Example Request:**
```bash
curl "http://localhost:8000/api/reports/top-products?limit=5&min_mentions=3"
```

**Example Response:**
```json
{
  "success": true,
  "data": [
    {
      "term": "paracetamol",
      "mention_count": 45,
      "total_views": 12500,
      "avg_views": 278.5,
      "channels": ["chemed", "tikvahpharma"]
    }
  ],
  "total_analyzed": 1250,
  "query_params": {"limit": 5, "min_mentions": 3}
}
```

### 2. Channel Activity Analysis
**GET** `/api/channels/{channel_name}/activity`

Returns comprehensive activity and engagement metrics for a specific channel.

**Parameters:**
- `channel_name` (path): Channel name to analyze
- `days` (int, default=30): Number of days to analyze (1-365)
- `include_top_terms` (bool, default=true): Include top mentioned terms

**Example Request:**
```bash
curl "http://localhost:8000/api/channels/chemed/activity?days=7"
```

**Example Response:**
```json
{
  "success": true,
  "channel_info": {
    "channel_name": "chemed",
    "channel_type": "Medical",
    "total_messages": 2500,
    "avg_daily_posts": 3.2,
    "total_views": 125000,
    "avg_views_per_post": 50.0,
    "image_percentage": 35.5
  },
  "daily_activity": [
    {
      "date": "2024-01-15",
      "message_count": 5,
      "total_views": 250,
      "avg_views": 50.0,
      "messages_with_images": 2
    }
  ],
  "top_terms": [
    {
      "term": "antibiotic",
      "mention_count": 12,
      "total_views": 600,
      "avg_views": 50.0,
      "channels": ["chemed"]
    }
  ]
}
```

### 3. Message Search
**GET** `/api/search/messages`

Searches for messages containing specific keywords with filtering options.

**Parameters:**
- `query` (string, required): Search query
- `limit` (int, default=20): Maximum results (1-100)
- `channel` (string, optional): Filter by channel
- `date_from` (string, optional): Start date (YYYY-MM-DD)
- `date_to` (string, optional): End date (YYYY-MM-DD)

**Example Request:**
```bash
curl "http://localhost:8000/api/search/messages?query=amoxicillin&limit=5"
```

**Example Response:**
```json
{
  "success": true,
  "messages": [
    {
      "message_id": 12345,
      "channel_name": "chemed",
      "message_date": "2024-01-15T10:30:00",
      "message_text": "New stock of Amoxicillin 500mg available",
      "view_count": 150,
      "forward_count": 8,
      "has_image": true,
      "message_length": 45
    }
  ],
  "total_found": 23,
  "query_params": {"query": "amoxicillin", "limit": 5}
}
```

### 4. Visual Content Statistics
**GET** `/api/reports/visual-content`

Returns comprehensive statistics about image usage and YOLO detection results.

**Parameters:**
- `include_details` (bool, default=true): Include detailed detection stats
- `min_confidence` (float, default=0.1): Minimum confidence threshold (0-1)

**Example Request:**
```bash
curl "http://localhost:8000/api/reports/visual-content?min_confidence=0.3"
```

**Example Response:**
```json
{
  "success": true,
  "channel_stats": [
    {
      "channel_name": "chemed",
      "total_messages": 2500,
      "messages_with_images": 890,
      "image_percentage": 35.6,
      "promotional_posts": 125,
      "product_display_posts": 450,
      "lifestyle_posts": 85,
      "avg_confidence": 0.7234
    }
  ],
  "summary": {
    "total_images_analyzed": 2340,
    "avg_confidence_score": 0.6856,
    "top_detected_objects": [
      {"object": "bottle", "count": 450, "avg_confidence": 0.8234}
    ]
  },
  "category_distribution": {
    "promotional": 280,
    "product_display": 890,
    "lifestyle": 156,
    "other": 114
  }
}
```

## Data Models

### Request/Response Schemas

The API uses Pydantic models for comprehensive validation:

**ProductMention:**
- `term`: Product or term mentioned
- `mention_count`: Number of mentions
- `total_views`: Total views for messages containing term
- `avg_views`: Average views per message
- `channels`: List of channels where term was mentioned

**ChannelStats:**
- `channel_name`: Channel identifier
- `channel_type`: Channel classification
- `total_messages`: Total messages in channel
- `avg_daily_posts`: Average posts per day
- `total_views`: Total views across all messages
- `image_percentage`: Percentage of posts with images

**MessageResult:**
- `message_id`: Unique message identifier
- `channel_name`: Source channel
- `message_date`: Posting timestamp
- `message_text`: Message content
- `view_count`: View engagement
- `has_image`: Visual content presence

## Error Handling

The API provides comprehensive error handling:

### HTTP Status Codes
- `200`: Success
- `400`: Bad Request (validation errors)
- `404`: Not Found (channel not found)
- `500`: Internal Server Error
- `503`: Service Unavailable (database issues)

### Error Response Format
```json
{
  "success": false,
  "error_code": "VALIDATION_ERROR",
  "error_detail": "Invalid parameter value",
  "timestamp": "2024-01-15T10:30:00"
}
```

## Performance Considerations

### Database Optimization
- Uses indexed columns for efficient queries
- Implements pagination for large result sets
- Connection pooling for concurrent requests

### Caching Strategy
- Database query results cached where appropriate
- Static responses cached for performance
- ETags for conditional requests

### Rate Limiting
- No built-in rate limiting (add with middleware if needed)
- Database connection limits protect against overload

## Integration Examples

### Python Client
```python
import requests

# Get top products
response = requests.get(
    "http://localhost:8000/api/reports/top-products",
    params={"limit": 10, "min_mentions": 2}
)
data = response.json()

# Search messages
response = requests.get(
    "http://localhost:8000/api/search/messages",
    params={"query": "paracetamol", "limit": 5}
)
messages = response.json()["messages"]
```

### JavaScript Client
```javascript
// Fetch channel activity
const response = await fetch(
  '/api/channels/chemed/activity?days=30'
);
const data = await response.json();
console.log(data.channel_info);
```

### curl Examples
```bash
# Health check
curl http://localhost:8000/health

# Top products with date range
curl "http://localhost:8000/api/reports/top-products?date_from=2024-01-01&date_to=2024-01-31"

# Channel activity
curl "http://localhost:8000/api/channels/tikvahpharma/activity?days=14&include_top_terms=true"

# Message search with filters
curl "http://localhost:8000/api/search/messages?query=vaccine&channel=chemed&limit=10"

# Visual content stats
curl "http://localhost:8000/api/reports/visual-content?include_details=true&min_confidence=0.5"
```

## Development

### Running in Development Mode
```bash
cd api
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

### Running in Production
```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --workers 4
```

### Environment Variables
```bash
DB_HOST=localhost
DB_PORT=5432
DB_NAME=medical_warehouse
DB_USER=postgres
DB_PASSWORD=password
```

## Monitoring

### Health Endpoints
- `/health`: Basic health check
- `/`: API information and endpoints

### Logging
- Application logs sent to console
- Error logging for debugging
- Request logging for monitoring

### Metrics
Consider adding:
- Request/response time tracking
- Endpoint usage statistics
- Error rate monitoring
- Database query performance

## Security Considerations

### Current Implementation
- CORS enabled for all origins (development)
- Basic input validation
- SQL injection prevention through parameterized queries

### Production Hardening
- Implement authentication/authorization
- Add rate limiting middleware
- Restrict CORS origins
- Add API key authentication
- Implement HTTPS
- Add request logging for audit trails

## Business Value

### Strategic Insights
- **Product Trending**: Identify popular medications and treatments
- **Channel Performance**: Compare engagement across channels
- **Content Optimization**: Understand which content drives engagement
- **Market Intelligence**: Track competitor activity and strategies

### Operational Benefits
- **Real-time Analytics**: Up-to-date insights from live data
- **Scalable Architecture**: Handle multiple concurrent requests
- **Standardized Interface**: Consistent data access for applications
- **Integration Ready**: Easy integration with dashboards and tools

The API provides a powerful foundation for data-driven decision making in the Ethiopian medical business sector, enabling stakeholders to extract actionable insights from Telegram channel data efficiently and reliably.
