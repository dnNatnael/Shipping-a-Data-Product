"""
Utility functions for the Telegram scraper
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any

logger = logging.getLogger(__name__)

def ensure_directory(path: Path) -> None:
    """Ensure directory exists, create if it doesn't"""
    path.mkdir(parents=True, exist_ok=True)

def load_json_file(file_path: Path) -> List[Dict[str, Any]]:
    """Load and parse JSON file"""
    try:
        if file_path.exists():
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
    except Exception as e:
        logger.error(f"Error loading JSON file {file_path}: {e}")
    return []

def save_json_file(data: List[Dict[str, Any]], file_path: Path) -> bool:
    """Save data to JSON file"""
    try:
        ensure_directory(file_path.parent)
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return True
    except Exception as e:
        logger.error(f"Error saving JSON file {file_path}: {e}")
        return False

def get_date_partition(message_date: datetime) -> str:
    """Get date partition path for message storage"""
    return message_date.strftime('%Y-%m-%d')

def validate_message_data(message_data: Dict[str, Any]) -> bool:
    """Validate that required message fields are present"""
    required_fields = ['message_id', 'channel_name', 'message_date', 'message_text']
    return all(field in message_data for field in required_fields)

def deduplicate_messages(messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Remove duplicate messages based on message_id and channel_name"""
    seen = set()
    unique_messages = []
    
    for message in messages:
        key = (message['message_id'], message['channel_name'])
        if key not in seen:
            seen.add(key)
            unique_messages.append(message)
    
    return unique_messages

def format_file_size(size_bytes: int) -> str:
    """Format file size in human readable format"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f} TB"
