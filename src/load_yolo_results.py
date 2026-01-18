#!/usr/bin/env python3
"""
Load YOLO detection results into PostgreSQL database
"""

import os
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/yolo_loading.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class YOLOResultsLoader:
    def __init__(self):
        """Initialize YOLO results loader"""
        self.db_host = os.getenv('DB_HOST', 'localhost')
        self.db_port = os.getenv('DB_PORT', '5432')
        self.db_name = os.getenv('DB_NAME', 'medical_warehouse')
        self.db_user = os.getenv('DB_USER', 'postgres')
        self.db_password = os.getenv('DB_PASSWORD', 'password')
        
        self.results_path = Path('results')
        
        # Create database connection
        self.connection_string = f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
        self.engine = create_engine(self.connection_string)
    
    def create_yolo_results_table(self):
        """Create table for YOLO detection results"""
        logger.info("Creating YOLO results table...")
        
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS raw.yolo_detections (
            message_id BIGINT,
            channel_name VARCHAR(255),
            image_path VARCHAR(500),
            image_category VARCHAR(50),
            total_detections INTEGER,
            person_count INTEGER,
            product_count INTEGER,
            max_confidence DECIMAL(5,4),
            avg_confidence DECIMAL(5,4),
            top_class VARCHAR(100),
            top_confidence DECIMAL(5,4),
            processing_timestamp TIMESTAMP,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (message_id, channel_name)
        );
        """
        
        try:
            with self.engine.connect() as conn:
                conn.execute(text(create_table_sql))
                conn.commit()
            logger.info("YOLO results table created successfully")
        except SQLAlchemyError as e:
            logger.error(f"Error creating YOLO results table: {e}")
            raise
    
    def load_csv_results(self, csv_file: str = 'yolo_detections.csv') -> pd.DataFrame:
        """Load YOLO results from CSV file"""
        csv_path = self.results_path / csv_file
        
        if not csv_path.exists():
            logger.error(f"CSV file not found: {csv_path}")
            return pd.DataFrame()
        
        try:
            df = pd.read_csv(csv_path)
            logger.info(f"Loaded {len(df)} records from {csv_path}")
            return df
        except Exception as e:
            logger.error(f"Error loading CSV file: {e}")
            return pd.DataFrame()
    
    def validate_yolo_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate and clean YOLO detection data"""
        if df.empty:
            return df
        
        # Required columns
        required_columns = [
            'message_id', 'channel_name', 'image_category', 
            'total_detections', 'person_count', 'product_count',
            'max_confidence', 'avg_confidence'
        ]
        
        # Check for required columns
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.error(f"Missing required columns: {missing_columns}")
            return pd.DataFrame()
        
        # Filter invalid records
        valid_df = df[
            (df['message_id'].notna()) & 
            (df['channel_name'].notna()) & 
            (df['image_category'].notna()) &
            (df['total_detections'] >= 0) &
            (df['person_count'] >= 0) &
            (df['product_count'] >= 0)
        ].copy()
        
        # Convert data types
        valid_df['message_id'] = valid_df['message_id'].astype('int64')
        valid_df['total_detections'] = valid_df['total_detections'].astype('int')
        valid_df['person_count'] = valid_df['person_count'].astype('int')
        valid_df['product_count'] = valid_df['product_count'].astype('int')
        
        # Parse timestamp
        if 'processing_timestamp' in valid_df.columns:
            valid_df['processing_timestamp'] = pd.to_datetime(valid_df['processing_timestamp'])
        
        logger.info(f"Validated {len(valid_df)} records out of {len(df)} total")
        return valid_df
    
    def load_to_database(self, df: pd.DataFrame) -> int:
        """Load YOLO results to PostgreSQL database"""
        if df.empty:
            logger.warning("No data to load")
            return 0
        
        try:
            # Select columns for database
            db_columns = [
                'message_id', 'channel_name', 'image_path', 'image_category',
                'total_detections', 'person_count', 'product_count',
                'max_confidence', 'avg_confidence', 'top_class', 'top_confidence',
                'processing_timestamp'
            ]
            
            # Filter to available columns
            available_columns = [col for col in db_columns if col in df.columns]
            db_df = df[available_columns].copy()
            
            # Use upsert to handle duplicates
            rows_inserted = db_df.to_sql(
                'yolo_detections',
                self.engine,
                schema='raw',
                if_exists='append',
                index=False,
                method='multi'
            )
            
            logger.info(f"Loaded {rows_inserted} YOLO detection records to database")
            return rows_inserted
            
        except SQLAlchemyError as e:
            logger.error(f"Error loading YOLO results to database: {e}")
            raise
    
    def get_loading_statistics(self) -> Dict[str, Any]:
        """Get statistics about loaded YOLO data"""
        stats_sql = """
        SELECT 
            COUNT(*) as total_detections,
            COUNT(DISTINCT channel_name) as unique_channels,
            COUNT(DISTINCT image_category) as unique_categories,
            AVG(total_detections) as avg_detections_per_image,
            AVG(person_count) as avg_persons_per_image,
            AVG(product_count) as avg_products_per_image,
            COUNT(CASE WHEN person_count > 0 THEN 1 END) as images_with_persons,
            COUNT(CASE WHEN product_count > 0 THEN 1 END) as images_with_products,
            image_category,
            COUNT(*) as category_count
        FROM raw.yolo_detections
        GROUP BY ROLLUP(image_category);
        """
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(stats_sql)).fetchall()
                
                # Convert to dictionary
                stats = {
                    'total_records': [],
                    'category_breakdown': []
                }
                
                for row in result:
                    if row.image_category is None:
                        stats['total_records'] = {
                            'total_detections': row.total_detections,
                            'unique_channels': row.unique_channels,
                            'unique_categories': row.unique_categories,
                            'avg_detections_per_image': float(row.avg_detections_per_image) if row.avg_detections_per_image else 0,
                            'avg_persons_per_image': float(row.avg_persons_per_image) if row.avg_persons_per_image else 0,
                            'avg_products_per_image': float(row.avg_products_per_image) if row.avg_products_per_image else 0,
                            'images_with_persons': row.images_with_persons,
                            'images_with_products': row.images_with_products
                        }
                    else:
                        stats['category_breakdown'].append({
                            'category': row.image_category,
                            'count': row.category_count
                        })
                
                return stats
                
        except SQLAlchemyError as e:
            logger.error(f"Error getting loading statistics: {e}")
            return {}
    
    def run_loading_pipeline(self, csv_file: str = 'yolo_detections.csv'):
        """Run the complete YOLO results loading pipeline"""
        logger.info("Starting YOLO results loading pipeline")
        
        try:
            # Create table
            self.create_yolo_results_table()
            
            # Load CSV data
            df = self.load_csv_results(csv_file)
            
            if df.empty:
                logger.warning("No data found in CSV file")
                return
            
            # Validate data
            valid_df = self.validate_yolo_data(df)
            
            if valid_df.empty:
                logger.error("No valid data after validation")
                return
            
            # Load to database
            rows_loaded = self.load_to_database(valid_df)
            
            # Get statistics
            stats = self.get_loading_statistics()
            
            # Log summary
            logger.info("=== YOLO Loading Summary ===")
            if stats.get('total_records'):
                total_stats = stats['total_records']
                logger.info(f"Total detection records: {total_stats['total_detections']}")
                logger.info(f"Unique channels: {total_stats['unique_channels']}")
                logger.info(f"Unique categories: {total_stats['unique_categories']}")
                logger.info(f"Images with persons: {total_stats['images_with_persons']}")
                logger.info(f"Images with products: {total_stats['images_with_products']}")
            
            if stats.get('category_breakdown'):
                logger.info("Category breakdown:")
                for cat in stats['category_breakdown']:
                    logger.info(f"  {cat['category']}: {cat['count']}")
            
            return {
                'rows_loaded': rows_loaded,
                'statistics': stats
            }
            
        except Exception as e:
            logger.error(f"Error in YOLO loading pipeline: {e}")
            raise

def main():
    """Main function"""
    loader = YOLOResultsLoader()
    
    try:
        results = loader.run_loading_pipeline()
        
        if results:
            print("\n=== YOLO Results Loading Complete ===")
            print(f"Records loaded: {results['rows_loaded']}")
            if results['statistics'].get('total_records'):
                stats = results['statistics']['total_records']
                print(f"Total images: {stats['total_detections']}")
                print(f"Images with persons: {stats['images_with_persons']}")
                print(f"Images with products: {stats['images_with_products']}")
        
    except Exception as e:
        logger.error(f"YOLO loading pipeline failed: {e}")
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
