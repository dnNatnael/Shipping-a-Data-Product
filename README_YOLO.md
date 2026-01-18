# YOLO Object Detection for Ethiopian Medical Business Images

This implementation uses YOLOv8 to analyze images downloaded from Telegram channels and enrich the data warehouse with computer vision insights.

## Overview

The YOLO object detection pipeline adds analytical value by:
- Classifying image content (promotional, product_display, lifestyle, other)
- Detecting objects and people in medical product images
- Providing engagement insights based on visual content types
- Identifying channels with high visual content usage

## Setup

### Install Dependencies
```bash
pip install ultralytics torch torchvision opencv-python
```

### Directory Structure
```
data/raw/images/           # Downloaded Telegram images
├── chemed/
│   ├── 12345.jpg
│   └── ...
├── lobelia4cosmetics/
│   └── ...
└── tikvahpharma/
    └── ...

results/                   # YOLO detection results
├── yolo_detections.csv
├── yolo_detections_detailed.json
├── detection_statistics.json
└── image_analysis_report.txt
```

## Image Classification Scheme

Based on detected objects, images are classified into four categories:

### **promotional**
- **Criteria**: Contains person + product
- **Examples**: Someone holding/showing medication, person with medical products
- **Expected**: Higher engagement due to human presence

### **product_display**
- **Criteria**: Contains bottle/container, no person
- **Examples**: Product photos, packaging images, medical supplies
- **Expected**: Focus on product features

### **lifestyle**
- **Criteria**: Contains person, no product
- **Examples**: People in medical settings, lifestyle imagery
- **Expected**: Contextual content

### **other**
- **Criteria**: Neither detected objects nor people
- **Examples**: Text images, graphics, unclear content
- **Expected**: Miscellaneous content

## Usage

### Run Complete Pipeline
```bash
./run_yolo_pipeline.sh
```

### Individual Steps
```bash
# 1. Run object detection
python src/yolo_detect.py

# 2. Load results to database
python src/load_yolo_results.py

# 3. Transform with dbt
cd medical_warehouse
dbt run --select fct_image_detections

# 4. Analyze patterns
cd ..
python src/analyze_image_patterns.py
```

## Key Components

### **src/yolo_detect.py**
- Uses YOLOv8 nano model for efficiency
- Processes all downloaded images
- Extracts message_id and channel from file paths
- Classifies content based on detected objects
- Saves results to CSV and JSON formats

### **src/load_yolo_results.py**
- Loads CSV results to PostgreSQL
- Validates data integrity
- Creates raw.yolo_detections table
- Provides loading statistics

### **fct_image_detections.sql**
- Integrates YOLO results with message data
- Joins with dimension tables
- Adds calculated fields for analysis
- Creates enriched fact table for analytics

### **src/analyze_image_patterns.py**
- Analyzes engagement by image category
- Compares channel visual content usage
- Identifies model limitations
- Generates comprehensive reports

## Analysis Insights

### **Key Questions Answered**

1. **Do promotional posts get more views than product_display posts?**
   - Compares average views by image category
   - Analyzes engagement patterns
   - Identifies effective content strategies

2. **Which channels use more visual content?**
   - Calculates image percentage by channel
   - Identifies visual content leaders
   - Provides competitive insights

3. **What are the limitations of pre-trained models?**
   - Analyzes confidence score distributions
   - Identifies detection gaps
   - Documents domain-specific challenges

### **Sample Queries**

```sql
-- Promotional vs Product Performance
SELECT 
    calculated_category,
    COUNT(*) as post_count,
    AVG(view_count) as avg_views,
    AVG(forward_count) as avg_forwards
FROM analytics.fct_image_detections img
JOIN analytics.fct_messages fm ON img.message_id = fm.message_id
GROUP BY calculated_category
ORDER BY avg_views DESC;

-- Channel Visual Content Usage
SELECT 
    dc.channel_name,
    COUNT(img.message_id) as images_analyzed,
    ROUND(COUNT(img.message_id) * 100.0 / COUNT(*), 2) as image_percentage
FROM analytics.dim_channels dc
LEFT JOIN analytics.fct_messages fm ON dc.channel_key = fm.channel_key
LEFT JOIN analytics.fct_image_detections img ON fm.message_id = img.message_id
GROUP BY dc.channel_name
ORDER BY image_percentage DESC;
```

## Model Limitations

### **Pre-trained Model Challenges**
- **Generic Object Classes**: COCO dataset doesn't include Ethiopian medical products
- **Cultural Context**: Medical imagery may differ from training data
- **Text Recognition**: YOLO doesn't read text on packaging
- **Small Objects**: May miss detailed medical items
- **Domain Specificity**: Bottles/cups may not represent medications accurately

### **Mitigation Strategies**
- Use confidence thresholds for reliable detections
- Focus on presence/absence rather than specific product types
- Combine with text analysis for better classification
- Consider fine-tuning on labeled medical images

## Performance Considerations

### **Model Choice**
- **YOLOv8n (nano)**: Fast processing, suitable for large datasets
- **Confidence Threshold**: 0.25 balances precision and recall
- **Batch Processing**: Efficient handling of thousands of images

### **Resource Requirements**
- **GPU**: Optional but speeds up processing significantly
- **Memory**: ~2GB RAM for model + image processing
- **Storage**: ~50MB for model, additional for results

## Integration with Data Warehouse

### **Schema Integration**
```sql
-- New fact table for image detections
CREATE TABLE analytics.fct_image_detections (
    message_id BIGINT,
    channel_key INTEGER,
    date_key INTEGER,
    image_category VARCHAR(50),
    calculated_category VARCHAR(50),
    total_detections INTEGER,
    person_count INTEGER,
    product_count INTEGER,
    max_confidence DECIMAL(5,4),
    confidence_level VARCHAR(20),
    detection_density VARCHAR(20)
);
```

### **Foreign Key Relationships**
- `channel_key` → `dim_channels.channel_key`
- `date_key` → `dim_dates.date_key`
- `message_id` → `fct_messages.message_id`

## Business Value

### **Strategic Insights**
- **Content Strategy**: Identify most effective visual content types
- **Channel Analysis**: Compare visual content approaches
- **Engagement Optimization**: Understand visual content impact
- **Competitive Intelligence**: Benchmark visual strategies

### **Operational Benefits**
- **Automated Analysis**: Scale image analysis across thousands of posts
- **Consistent Classification**: Standardized content categorization
- **Trend Identification**: Track visual content patterns over time
- **Performance Metrics**: Quantify visual content effectiveness

## Next Steps

### **Model Enhancement**
- Fine-tune YOLO on Ethiopian medical product images
- Add custom classes for specific medical items
- Implement ensemble methods for better accuracy

### **Advanced Analytics**
- Visual content trend analysis
- Cross-channel visual strategy comparison
- Seasonal visual content patterns
- ROI analysis of visual content types

### **Integration**
- Real-time image processing pipeline
- Automated visual content recommendations
- Dashboard integration for visual insights
- Alert system for visual content anomalies
