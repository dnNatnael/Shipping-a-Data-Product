#!/usr/bin/env python3
"""
Telegram Scraper for Ethiopian Medical Business Data
Extracts messages and images from public Telegram channels
"""

import os
import json
import asyncio
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv

from telethon import TelegramClient
from telethon.tl.types import MessageMediaPhoto
from telethon.errors import FloodWait, ChatAdminRequiredError, ChannelPrivateError

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/telegram_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class TelegramScraper:
    def __init__(self):
        """Initialize the Telegram scraper with API credentials"""
        self.api_id = int(os.getenv('API_ID', '0'))
        self.api_hash = os.getenv('API_HASH', '')
        self.phone_number = os.getenv('PHONE_NUMBER', '')
        self.raw_data_path = Path(os.getenv('RAW_DATA_PATH', 'data/raw'))
        self.logs_path = Path(os.getenv('LOGS_PATH', 'logs'))
        
        # Create directories if they don't exist
        self.raw_data_path.mkdir(parents=True, exist_ok=True)
        self.logs_path.mkdir(parents=True, exist_ok=True)
        (self.raw_data_path / 'telegram_messages').mkdir(parents=True, exist_ok=True)
        (self.raw_data_path / 'images').mkdir(parents=True, exist_ok=True)
        
        # Initialize Telegram client
        self.client = TelegramClient('session_name', self.api_id, self.api_hash)
        
        # Channels to scrape
        self.channels = [
            'chemed',
            'lobelia4cosmetics', 
            'tikvahpharma'
        ]
        
        # Rate limiting
        self.request_delay = 1  # seconds between requests
        
    async def connect(self):
        """Connect to Telegram API"""
        try:
            await self.client.connect()
            if not await self.client.is_user_authorized():
                await self.client.send_code_request(self.phone_number)
                code = input('Enter the code you received: ')
                await self.client.sign_in(self.phone_number, code)
            logger.info("Successfully connected to Telegram")
        except Exception as e:
            logger.error(f"Failed to connect to Telegram: {e}")
            raise
    
    async def disconnect(self):
        """Disconnect from Telegram API"""
        await self.client.disconnect()
        logger.info("Disconnected from Telegram")
    
    def get_date_partition(self, message_date: datetime) -> str:
        """Get date partition path for message storage"""
        return message_date.strftime('%Y-%m-%d')
    
    async def download_image(self, message, channel_name: str, message_id: int) -> Optional[str]:
        """Download image from message if present"""
        try:
            if message.media and isinstance(message.media, MessageMediaPhoto):
                # Create channel-specific directory
                channel_dir = self.raw_data_path / 'images' / channel_name
                channel_dir.mkdir(parents=True, exist_ok=True)
                
                # Download image
                image_path = channel_dir / f"{message_id}.jpg"
                await self.client.download_media(message.media, str(image_path))
                logger.info(f"Downloaded image for message {message_id} from {channel_name}")
                return str(image_path)
        except Exception as e:
            logger.error(f"Failed to download image for message {message_id}: {e}")
        return None
    
    def extract_message_data(self, message, channel_name: str) -> Dict[str, Any]:
        """Extract relevant data from a Telegram message"""
        return {
            'message_id': message.id,
            'channel_name': channel_name,
            'message_date': message.date.isoformat(),
            'message_text': message.text or '',
            'has_media': bool(message.media),
            'views': getattr(message, 'views', 0),
            'forwards': getattr(message, 'forwards', 0),
            'scraped_at': datetime.now().isoformat()
        }
    
    async def scrape_channel_messages(self, channel_name: str, limit: int = 1000):
        """Scrape messages from a specific channel"""
        logger.info(f"Starting to scrape channel: {channel_name}")
        
        try:
            # Get channel entity
            channel = await self.client.get_entity(channel_name)
            
            messages_data = []
            message_count = 0
            
            # Iterate through messages
            async for message in self.client.iter_messages(channel, limit=limit):
                try:
                    # Extract message data
                    message_data = self.extract_message_data(message, channel_name)
                    
                    # Download image if present
                    if message_data['has_media']:
                        image_path = await self.download_image(message, channel_name, message.id)
                        if image_path:
                            message_data['image_path'] = image_path
                    
                    messages_data.append(message_data)
                    message_count += 1
                    
                    # Log progress every 100 messages
                    if message_count % 100 == 0:
                        logger.info(f"Scraped {message_count} messages from {channel_name}")
                    
                    # Rate limiting
                    await asyncio.sleep(self.request_delay)
                    
                except FloodWait as e:
                    logger.warning(f"Rate limited. Waiting {e.seconds} seconds...")
                    await asyncio.sleep(e.seconds)
                except Exception as e:
                    logger.error(f"Error processing message {message.id}: {e}")
                    continue
            
            # Save messages to JSON file
            await self.save_channel_messages(channel_name, messages_data)
            
            logger.info(f"Completed scraping {channel_name}. Total messages: {len(messages_data)}")
            return messages_data
            
        except ChatAdminRequiredError:
            logger.error(f"Admin access required for channel {channel_name}")
        except ChannelPrivateError:
            logger.error(f"Channel {channel_name} is private or not accessible")
        except Exception as e:
            logger.error(f"Error scraping channel {channel_name}: {e}")
        
        return []
    
    async def save_channel_messages(self, channel_name: str, messages: List[Dict[str, Any]]):
        """Save messages to partitioned JSON files"""
        if not messages:
            return
        
        # Group messages by date
        messages_by_date = {}
        for message in messages:
            message_date = datetime.fromisoformat(message['message_date'].replace('Z', '+00:00'))
            date_partition = self.get_date_partition(message_date)
            
            if date_partition not in messages_by_date:
                messages_by_date[date_partition] = []
            messages_by_date[date_partition].append(message)
        
        # Save each date's messages to separate file
        for date_partition, date_messages in messages_by_date.items():
            date_dir = self.raw_data_path / 'telegram_messages' / date_partition
            date_dir.mkdir(parents=True, exist_ok=True)
            
            file_path = date_dir / f"{channel_name}.json"
            
            # Load existing messages if file exists
            existing_messages = []
            if file_path.exists():
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        existing_messages = json.load(f)
                except Exception as e:
                    logger.warning(f"Error reading existing file {file_path}: {e}")
            
            # Merge new messages with existing ones (avoid duplicates)
            existing_ids = {msg['message_id'] for msg in existing_messages}
            new_messages = [msg for msg in date_messages if msg['message_id'] not in existing_ids]
            
            if new_messages:
                all_messages = existing_messages + new_messages
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(all_messages, f, ensure_ascii=False, indent=2)
                logger.info(f"Saved {len(new_messages)} new messages to {file_path}")
    
    async def scrape_all_channels(self, limit_per_channel: int = 1000):
        """Scrape all configured channels"""
        logger.info("Starting to scrape all channels")
        
        try:
            await self.connect()
            
            for channel_name in self.channels:
                try:
                    await self.scrape_channel_messages(channel_name, limit_per_channel)
                except Exception as e:
                    logger.error(f"Failed to scrape channel {channel_name}: {e}")
                    continue
            
            logger.info("Completed scraping all channels")
            
        except Exception as e:
            logger.error(f"Error during scraping: {e}")
        finally:
            await self.disconnect()

async def main():
    """Main function to run the scraper"""
    scraper = TelegramScraper()
    await scraper.scrape_all_channels(limit_per_channel=1000)

if __name__ == "__main__":
    asyncio.run(main())
