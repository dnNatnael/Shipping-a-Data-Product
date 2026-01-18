#!/usr/bin/env python3
"""
Entry point script for running the Telegram scraper
"""

import asyncio
import argparse
import sys
from pathlib import Path

# Add src directory to path
sys.path.append(str(Path(__file__).parent / 'src'))

from scraper import TelegramScraper
from config import Config

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Scrape Telegram channels for Ethiopian medical business data')
    parser.add_argument('--limit', type=int, default=Config.DEFAULT_MESSAGE_LIMIT,
                       help=f'Number of messages to scrape per channel (default: {Config.DEFAULT_MESSAGE_LIMIT})')
    parser.add_argument('--channels', nargs='+', default=Config.CHANNELS,
                       help='List of channel names to scrape')
    parser.add_argument('--verbose', action='store_true',
                       help='Enable verbose logging')
    return parser.parse_args()

async def main():
    """Main function"""
    args = parse_arguments()
    
    # Update logging level if verbose
    if args.verbose:
        import logging
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Initialize scraper
    scraper = TelegramScraper()
    
    # Override channels if specified
    if args.channels:
        scraper.channels = args.channels
    
    print(f"Starting Telegram scraper...")
    print(f"Channels: {scraper.channels}")
    print(f"Message limit per channel: {args.limit}")
    print(f"Data will be saved to: {scraper.raw_data_path}")
    
    try:
        await scraper.scrape_all_channels(limit_per_channel=args.limit)
        print("Scraping completed successfully!")
    except KeyboardInterrupt:
        print("\nScraping interrupted by user")
    except Exception as e:
        print(f"Error during scraping: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
