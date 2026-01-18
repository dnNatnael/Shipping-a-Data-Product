"""
Configuration settings for Telegram scraper
"""

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

class Config:
    # Telegram API Settings
    API_ID = int(os.getenv('API_ID', '0'))
    API_HASH = os.getenv('API_HASH', '')
    PHONE_NUMBER = os.getenv('PHONE_NUMBER', '')
    
    # Channels to scrape
    CHANNELS = [
        'chemed',
        'lobelia4cosmetics', 
        'tikvahpharma'
    ]
    
    # Additional channels from et.tgstat.com/medicine (to be added manually)
    ADDITIONAL_CHANNELS = [
        # Add more channel names here as needed
    ]
    
    # Data Storage Paths
    PROJECT_ROOT = Path(__file__).parent.parent
    RAW_DATA_PATH = PROJECT_ROOT / os.getenv('RAW_DATA_PATH', 'data/raw')
    LOGS_PATH = PROJECT_ROOT / os.getenv('LOGS_PATH', 'logs')
    
    # Scraping Settings
    DEFAULT_MESSAGE_LIMIT = 1000
    REQUEST_DELAY = 1  # seconds between requests
    MAX_RETRIES = 3
    
    # Logging Settings
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
