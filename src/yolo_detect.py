#!/usr/bin/env python3
"""
YOLOv8 Object Detection for Ethiopian Medical Business Images
Analyzes downloaded Telegram images to detect objects and classify content
"""

import os
import json
import csv
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional
import pandas as pd
from ultralytics import YOLO
import cv2
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/yolo_detection.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class YOLODetector:
    def __init__(self, model_name: str = 'yolov8n.pt'):
        """Initialize YOLO detector"""
        self.model_name = model_name
        self.model = None
        self.raw_data_path = Path(os.getenv('RAW_DATA_PATH', 'data/raw'))
        self.images_path = self.raw_data_path / 'images'
        self.results_path = Path('results')
        self.confidence_threshold = 0.25
        
        # Create results directory
        self.results_path.mkdir(exist_ok=True)
        
        # Object categories for classification
        self.person_classes = {'person'}
        self.product_classes = {
            'bottle', 'cup', 'vase', 'wine glass', 'cup', 'fork', 'spoon', 'knife',
            'bowl', 'banana', 'apple', 'sandwich', 'orange', 'broccoli', 'carrot',
            'hot dog', 'pizza', 'donut', 'cake', 'book', 'cell phone', 'laptop',
            'remote', 'keyboard', 'mouse', 'microwave', 'oven', 'toaster', 'sink',
            'refrigerator', 'clock', 'vase', 'scissors', 'teddy bear', 'hair drier',
            'toothbrush'
        }
        
        # Medical-related classes (COCO dataset)
        self.medical_classes = {
            'bottle', 'cup', 'vase', 'wine glass'  # Could contain medicine
        }
    
    def load_model(self):
        """Load YOLO model"""
        try:
            logger.info(f"Loading YOLO model: {self.model_name}")
            self.model = YOLO(self.model_name)
            logger.info("Model loaded successfully")
        except Exception as e:
            logger.error(f"Failed to load YOLO model: {e}")
            raise
    
    def find_images(self) -> List[Path]:
        """Find all downloaded images"""
        images = []
        
        if not self.images_path.exists():
            logger.warning(f"Images directory not found: {self.images_path}")
            return images
        
        # Find all image files
        for ext in ['*.jpg', '*.jpeg', '*.png', '*.bmp']:
            images.extend(self.images_path.rglob(ext))
        
        logger.info(f"Found {len(images)} images to process")
        return images
    
    def extract_message_id_from_path(self, image_path: Path) -> Optional[int]:
        """Extract message ID from image path"""
        try:
            # Extract message_id from filename (e.g., "12345.jpg")
            filename = image_path.stem
            return int(filename)
        except (ValueError, AttributeError):
            logger.warning(f"Could not extract message ID from path: {image_path}")
            return None
    
    def extract_channel_from_path(self, image_path: Path) -> Optional[str]:
        """Extract channel name from image path"""
        try:
            # Path structure: data/raw/images/channel_name/message_id.jpg
            parts = image_path.parts
            if 'images' in parts:
                channel_idx = parts.index('images') + 1
                if channel_idx < len(parts):
                    return parts[channel_idx]
        except (IndexError, AttributeError):
            logger.warning(f"Could not extract channel from path: {image_path}")
        return None
    
    def classify_image_content(self, detections: List[Dict[str, Any]]) -> str:
        """
        Classify image based on detected objects:
        - promotional: Contains person + product
        - product_display: Contains bottle/container, no person
        - lifestyle: Contains person, no product
        - other: Neither detected
        """
        has_person = False
        has_product = False
        
        for detection in detections:
            class_name = detection['class_name']
            confidence = detection['confidence']
            
            # Only consider detections above threshold
            if confidence >= self.confidence_threshold:
                if class_name in self.person_classes:
                    has_person = True
                if class_name in self.product_classes:
                    has_product = True
        
        # Classification logic
        if has_person and has_product:
            return 'promotional'
        elif has_product and not has_person:
            return 'product_display'
        elif has_person and not has_product:
            return 'lifestyle'
        else:
            return 'other'
    
    def detect_objects(self, image_path: Path) -> List[Dict[str, Any]]:
        """Run YOLO detection on a single image"""
        try:
            # Run inference
            results = self.model(image_path, verbose=False)
            
            detections = []
            for result in results:
                boxes = result.boxes
                if boxes is not None:
                    for box in boxes:
                        # Extract detection information
                        confidence = float(box.conf[0])
                        class_id = int(box.cls[0])
                        class_name = self.model.names[class_id]
                        
                        # Bounding box coordinates
                        x1, y1, x2, y2 = box.xyxy[0].tolist()
                        
                        detection = {
                            'class_name': class_name,
                            'confidence': confidence,
                            'bbox_x1': x1,
                            'bbox_y1': y1,
                            'bbox_x2': x2,
                            'bbox_y2': y2,
                            'bbox_width': x2 - x1,
                            'bbox_height': y2 - y1,
                            'bbox_area': (x2 - x1) * (y2 - y1)
                        }
                        detections.append(detection)
            
            return detections
            
        except Exception as e:
            logger.error(f"Error processing image {image_path}: {e}")
            return []
    
    def process_single_image(self, image_path: Path) -> Optional[Dict[str, Any]]:
        """Process a single image and return detection results"""
        try:
            # Extract metadata
            message_id = self.extract_message_id_from_path(image_path)
            channel_name = self.extract_channel_from_path(image_path)
            
            if message_id is None or channel_name is None:
                logger.warning(f"Skipping image due to missing metadata: {image_path}")
                return None
            
            # Run object detection
            detections = self.detect_objects(image_path)
            
            # Classify image content
            image_category = self.classify_image_content(detections)
            
            # Create result record
            result = {
                'message_id': message_id,
                'channel_name': channel_name,
                'image_path': str(image_path),
                'image_category': image_category,
                'total_detections': len(detections),
                'processing_timestamp': datetime.now().isoformat(),
                'detections_json': json.dumps(detections) if detections else None
            }
            
            # Add detection-specific fields
            person_detections = [d for d in detections if d['class_name'] in self.person_classes]
            product_detections = [d for d in detections if d['class_name'] in self.product_classes]
            
            result.update({
                'person_count': len(person_detections),
                'product_count': len(product_detections),
                'max_confidence': max([d['confidence'] for d in detections]) if detections else 0.0,
                'avg_confidence': sum([d['confidence'] for d in detections]) / len(detections) if detections else 0.0
            })
            
            # Add top detection
            if detections:
                top_detection = max(detections, key=lambda x: x['confidence'])
                result.update({
                    'top_class': top_detection['class_name'],
                    'top_confidence': top_detection['confidence']
                })
            else:
                result.update({
                    'top_class': None,
                    'top_confidence': 0.0
                })
            
            return result
            
        except Exception as e:
            logger.error(f"Error processing image {image_path}: {e}")
            return None
    
    def process_all_images(self) -> List[Dict[str, Any]]:
        """Process all images in the dataset"""
        logger.info("Starting YOLO object detection processing")
        
        # Load model
        self.load_model()
        
        # Find all images
        images = self.find_images()
        
        if not images:
            logger.warning("No images found to process")
            return []
        
        results = []
        processed_count = 0
        
        for i, image_path in enumerate(images):
            logger.info(f"Processing image {i+1}/{len(images)}: {image_path}")
            
            result = self.process_single_image(image_path)
            if result:
                results.append(result)
                processed_count += 1
            
            # Log progress every 10 images
            if (i + 1) % 10 == 0:
                logger.info(f"Processed {i+1}/{len(images)} images")
        
        logger.info(f"Completed processing. Successfully processed {processed_count}/{len(images)} images")
        return results
    
    def save_results_to_csv(self, results: List[Dict[str, Any]], filename: str = 'yolo_detections.csv'):
        """Save detection results to CSV file"""
        if not results:
            logger.warning("No results to save")
            return
        
        # Convert to DataFrame
        df = pd.DataFrame(results)
        
        # Remove detections_json column for CSV (save separately if needed)
        csv_df = df.drop('detections_json', axis=1, errors='ignore')
        
        # Save to CSV
        csv_path = self.results_path / filename
        csv_df.to_csv(csv_path, index=False)
        
        logger.info(f"Results saved to {csv_path}")
        logger.info(f"Total records: {len(csv_df)}")
        
        return csv_path
    
    def save_detailed_results(self, results: List[Dict[str, Any]], filename: str = 'yolo_detections_detailed.json'):
        """Save detailed results including detection JSON"""
        if not results:
            logger.warning("No detailed results to save")
            return
        
        # Save detailed JSON
        json_path = self.results_path / filename
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Detailed results saved to {json_path}")
        return json_path
    
    def generate_summary_statistics(self, results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Generate summary statistics from detection results"""
        if not results:
            return {}
        
        df = pd.DataFrame(results)
        
        stats = {
            'total_images_processed': len(results),
            'image_categories': df['image_category'].value_counts().to_dict(),
            'channels_processed': df['channel_name'].nunique(),
            'images_per_channel': df['channel_name'].value_counts().to_dict(),
            'avg_detections_per_image': df['total_detections'].mean(),
            'max_detections_per_image': df['total_detections'].max(),
            'avg_confidence': df['max_confidence'].mean(),
            'top_detected_classes': df['top_class'].value_counts().head(10).to_dict(),
            'person_detection_rate': (df['person_count'] > 0).mean() * 100,
            'product_detection_rate': (df['product_count'] > 0).mean() * 100
        }
        
        return stats
    
    def run_detection_pipeline(self):
        """Run the complete detection pipeline"""
        logger.info("Starting YOLO detection pipeline")
        
        try:
            # Process all images
            results = self.process_all_images()
            
            if not results:
                logger.warning("No images were processed successfully")
                return
            
            # Save results
            csv_path = self.save_results_to_csv(results)
            json_path = self.save_detailed_results(results)
            
            # Generate statistics
            stats = self.generate_summary_statistics(results)
            
            # Save statistics
            stats_path = self.results_path / 'detection_statistics.json'
            with open(stats_path, 'w', encoding='utf-8') as f:
                json.dump(stats, f, indent=2, ensure_ascii=False)
            
            # Log summary
            logger.info("=== Detection Pipeline Summary ===")
            logger.info(f"Total images processed: {stats.get('total_images_processed', 0)}")
            logger.info(f"Image categories: {stats.get('image_categories', {})}")
            logger.info(f"Channels processed: {stats.get('channels_processed', 0)}")
            logger.info(f"Average detections per image: {stats.get('avg_detections_per_image', 0):.2f}")
            logger.info(f"Person detection rate: {stats.get('person_detection_rate', 0):.1f}%")
            logger.info(f"Product detection rate: {stats.get('product_detection_rate', 0):.1f}%")
            
            return {
                'results': results,
                'csv_path': csv_path,
                'json_path': json_path,
                'statistics': stats
            }
            
        except Exception as e:
            logger.error(f"Error in detection pipeline: {e}")
            raise

def main():
    """Main function"""
    detector = YOLODetector()
    
    try:
        pipeline_results = detector.run_detection_pipeline()
        
        if pipeline_results:
            print("\n=== YOLO Detection Complete ===")
            print(f"Results saved to: {pipeline_results['csv_path']}")
            print(f"Statistics saved to: results/detection_statistics.json")
            print(f"Images processed: {pipeline_results['statistics']['total_images_processed']}")
        
    except Exception as e:
        logger.error(f"Detection pipeline failed: {e}")
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
