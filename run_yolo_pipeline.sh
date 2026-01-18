#!/bin/bash

# YOLO Object Detection Pipeline for Ethiopian Medical Business Data

set -e

echo "=== YOLO Object Detection Pipeline ==="

# Check if required directories exist
if [ ! -d "data/raw/images" ]; then
    echo "❌ Error: Images directory not found. Run the scraper first."
    exit 1
fi

# Create results directory
mkdir -p results
mkdir -p logs

echo "Step 1: Running YOLO object detection..."
python src/yolo_detect.py

if [ $? -ne 0 ]; then
    echo "❌ YOLO detection failed"
    exit 1
fi

echo "Step 2: Loading YOLO results to PostgreSQL..."
python src/load_yolo_results.py

if [ $? -ne 0 ]; then
    echo "❌ Loading results failed"
    exit 1
fi

echo "Step 3: Running dbt transformations..."
cd medical_warehouse
dbt run --select fct_image_detections

if [ $? -ne 0 ]; then
    echo "❌ dbt transformation failed"
    exit 1
fi

echo "Step 4: Analyzing image content patterns..."
cd ..
python src/analyze_image_patterns.py

if [ $? -ne 0 ]; then
    echo "❌ Analysis failed"
    exit 1
fi

echo "=== YOLO Pipeline Complete ==="
echo "📊 Results saved to: results/"
echo "📈 Analysis report: results/image_analysis_report.txt"
echo "🔍 Detection data: results/yolo_detections.csv"
echo "📋 Statistics: results/detection_statistics.json"

# Display summary
if [ -f "results/detection_statistics.json" ]; then
    echo ""
    echo "📊 Quick Summary:"
    python -c "
import json
with open('results/detection_statistics.json', 'r') as f:
    stats = json.load(f)
    if 'total_images_processed' in stats:
        print(f'Images processed: {stats[\"total_images_processed\"]}')
    if 'image_categories' in stats:
        print('Categories:')
        for cat, count in stats['image_categories'].items():
            print(f'  {cat}: {count}')
"
fi
