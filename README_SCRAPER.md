# Telegram Scraper for Ethiopian Medical Business Data

This scraper extracts messages and images from public Telegram channels related to medical and pharmaceutical businesses in Ethiopia.

## Setup

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Get Telegram API Credentials
1. Visit [https://my.telegram.org](https://my.telegram.org)
2. Sign in with your phone number
3. Go to "API development tools"
4. Create a new application
5. Copy the `API ID` and `API Hash`

### 3. Configure Environment Variables
Edit the `.env` file and add your credentials:
```bash
API_ID=your_api_id_here
API_HASH=your_api_hash_here
PHONE_NUMBER=your_phone_number_with_country_code
```

### 4. Run the Scraper
```bash
# Run with default settings
python run_scraper.py

# Custom message limit
python run_scraper.py --limit 500

# Specific channels
python run_scraper.py --channels chemed lobelia4cosmetics

# Verbose logging
python run_scraper.py --verbose
```

## Data Structure

### Raw Messages
Messages are stored in partitioned JSON files:
```
data/raw/telegram_messages/YYYY-MM-DD/channel_name.json
```

### Images
Images are downloaded and organized by channel:
```
data/raw/images/channel_name/message_id.jpg
```

### Logs
Scraping activity is logged to:
```
logs/telegram_scraper.log
```

## Data Fields

Each message contains:
- `message_id`: Unique identifier
- `channel_name`: Source channel
- `message_date`: Timestamp
- `message_text`: Full text content
- `has_media`: Whether media is present
- `image_path`: Path to downloaded image (if applicable)
- `views`: Number of views
- `forwards`: Number of forwards
- `scraped_at`: When the message was scraped

## Channels

Default channels:
- `chemed` - Medical products
- `lobelia4cosmetics` - Cosmetics and health products  
- `tikvahpharma` - Pharmaceuticals

## Rate Limiting

The scraper includes built-in rate limiting to avoid being blocked by Telegram:
- 1 second delay between requests
- Automatic handling of FloodWait errors
- Retry logic for failed requests

## Error Handling

The scraper handles common errors:
- Rate limiting (FloodWait)
- Private/inaccessible channels
- Network issues
- Media download failures

## Next Steps

After scraping, the data will be processed by the ELT pipeline:
1. Raw data is loaded into the data lake
2. Data is transformed using dbt
3. Cleaned data is stored in PostgreSQL
4. Object detection enriches image data
5. Analytical API exposes insights
