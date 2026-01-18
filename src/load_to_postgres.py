#!/usr/bin/env python3
"""
Load raw Telegram data from JSON files to PostgreSQL database
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
        logging.FileHandler('logs/data_loading.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class PostgresDataLoader:
    def __init__(self):
        """Initialize PostgreSQL data loader"""
        self.db_host = os.getenv('DB_HOST', 'localhost')
        self.db_port = os.getenv('DB_PORT', '5432')
        self.db_name = os.getenv('DB_NAME', 'medical_warehouse')
        self.db_user = os.getenv('DB_USER', 'postgres')
        self.db_password = os.getenv('DB_PASSWORD', 'password')
        
        self.raw_data_path = Path(os.getenv('RAW_DATA_PATH', 'data/raw'))
        
        # Create database connection
        self.connection_string = f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
        self.engine = create_engine(self.connection_string)
        
    def create_raw_schema(self):
        """Create raw schema and telegram_messages table"""
        logger.info("Creating raw schema and tables...")
        
        create_schema_sql = """
        CREATE SCHEMA IF NOT EXISTS raw;
        """
        
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS raw.telegram_messages (
            message_id BIGINT,
            channel_name VARCHAR(255),
            message_date TIMESTAMP,
            message_text TEXT,
            has_media BOOLEAN,
            image_path VARCHAR(500),
            views INTEGER DEFAULT 0,
            forwards INTEGER DEFAULT 0,
            scraped_at TIMESTAMP,
            file_path VARCHAR(500),
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (message_id, channel_name)
        );
        """
        
        try:
            with self.engine.connect() as conn:
                conn.execute(text(create_schema_sql))
                conn.execute(text(create_table_sql))
                conn.commit()
            logger.info("Raw schema and tables created successfully")
        except SQLAlchemyError as e:
            logger.error(f"Error creating schema: {e}")
            raise
    
    def find_json_files(self) -> List[Path]:
        """Find all JSON files in the telegram_messages directory"""
        json_files = []
        messages_dir = self.raw_data_path / 'telegram_messages'
        
        if not messages_dir.exists():
            logger.error(f"Messages directory not found: {messages_dir}")
            return json_files
        
        # Recursively find all JSON files
        for json_file in messages_dir.rglob('*.json'):
            json_files.append(json_file)
        
        logger.info(f"Found {len(json_files)} JSON files to process")
        return json_files
    
    def load_json_file(self, file_path: Path) -> List[Dict[str, Any]]:
        """Load and parse JSON file"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Add file path to each record for tracking
            for record in data:
                record['file_path'] = str(file_path)
            
            return data
        except Exception as e:
            logger.error(f"Error loading JSON file {file_path}: {e}")
            return []
    
    def validate_message(self, message: Dict[str, Any]) -> bool:
        """Validate message record"""
        required_fields = ['message_id', 'channel_name', 'message_date']
        
        for field in required_fields:
            if field not in message or message[field] is None:
                return False
        
        # Validate message_id is positive
        try:
            if int(message['message_id']) <= 0:
                return False
        except (ValueError, TypeError):
            return False
        
        return True
    
    def clean_and_transform_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and transform message data"""
        cleaned = message.copy()
        
        # Convert message_id to integer
        try:
            cleaned['message_id'] = int(cleaned['message_id'])
        except (ValueError, TypeError):
            cleaned['message_id'] = None
        
        # Parse message_date
        try:
            if isinstance(cleaned['message_date'], str):
                # Handle ISO format with potential Z suffix
                date_str = cleaned['message_date'].replace('Z', '+00:00')
                cleaned['message_date'] = datetime.fromisoformat(date_str)
        except (ValueError, TypeError):
            cleaned['message_date'] = None
        
        # Clean numeric fields
        for field in ['views', 'forwards']:
            try:
                cleaned[field] = int(cleaned.get(field, 0)) if cleaned.get(field) else 0
            except (ValueError, TypeError):
                cleaned[field] = 0
        
        # Clean boolean fields
        cleaned['has_media'] = bool(cleaned.get('has_media', False))
        
        # Clean text fields
        for field in ['channel_name', 'message_text', 'image_path', 'file_path']:
            if field in cleaned and cleaned[field] is not None:
                cleaned[field] = str(cleaned[field]).strip()
            else:
                cleaned[field] = None
        
        return cleaned
    
    def load_messages_to_db(self, messages: List[Dict[str, Any]]) -> int:
        """Load messages to PostgreSQL database"""
        if not messages:
            return 0
        
        # Clean and transform messages
        cleaned_messages = []
        for message in messages:
            if self.validate_message(message):
                cleaned = self.clean_and_transform_message(message)
                cleaned_messages.append(cleaned)
        
        if not cleaned_messages:
            logger.warning("No valid messages to load")
            return 0
        
        # Convert to DataFrame
        df = pd.DataFrame(cleaned_messages)
        
        # Select only required columns
        required_columns = [
            'message_id', 'channel_name', 'message_date', 'message_text',
            'has_media', 'image_path', 'views', 'forwards', 'scraped_at', 'file_path'
        ]
        
        df = df[required_columns]
        
        try:
            # Use upsert to handle duplicates
            rows_inserted = df.to_sql(
                'telegram_messages',
                self.engine,
                schema='raw',
                if_exists='append',
                index=False,
                method='multi'
            )
            
            logger.info(f"Loaded {rows_inserted} messages to database")
            return rows_inserted
            
        except SQLAlchemyError as e:
            logger.error(f"Error loading messages to database: {e}")
            raise
    
    def load_all_data(self):
        """Load all JSON files to PostgreSQL"""
        logger.info("Starting data loading process...")
        
        try:
            # Create schema and tables
            self.create_raw_schema()
            
            # Find all JSON files
            json_files = self.find_json_files()
            
            total_messages_loaded = 0
            
            for file_path in json_files:
                logger.info(f"Processing file: {file_path}")
                
                # Load JSON data
                messages = self.load_json_file(file_path)
                
                if messages:
                    # Load to database
                    messages_loaded = self.load_messages_to_db(messages)
                    total_messages_loaded += messages_loaded
                else:
                    logger.warning(f"No messages found in {file_path}")
            
            logger.info(f"Data loading completed. Total messages loaded: {total_messages_loaded}")
            return total_messages_loaded
            
        except Exception as e:
            logger.error(f"Error during data loading: {e}")
            raise
    
    def get_loading_stats(self):
        """Get statistics about loaded data"""
        stats_sql = """
        SELECT 
            COUNT(*) as total_messages,
            COUNT(DISTINCT channel_name) as unique_channels,
            MIN(message_date) as earliest_message,
            MAX(message_date) as latest_message,
            COUNT(CASE WHEN has_media = true THEN 1 END) as messages_with_media
        FROM raw.telegram_messages;
        """
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(stats_sql)).fetchone()
                return dict(result._mapping)
        except SQLAlchemyError as e:
            logger.error(f"Error getting loading stats: {e}")
            return {}

def main():
    """Main function"""
    loader = PostgresDataLoader()
    
    try:
        total_loaded = loader.load_all_data()
        stats = loader.get_loading_stats()
        
        print("\n=== Data Loading Summary ===")
        print(f"Total messages loaded: {total_loaded}")
        print(f"Unique channels: {stats.get('unique_channels', 0)}")
        print(f"Date range: {stats.get('earliest_message')} to {stats.get('latest_message')}")
        print(f"Messages with media: {stats.get('messages_with_media', 0)}")
        
    except Exception as e:
        logger.error(f"Data loading failed: {e}")
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
