"""
Dagster pipeline for Ethiopian Medical Business Data Platform
Orchestrates the complete data pipeline from scraping to enrichment
"""

import os
import sys
import subprocess
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional

from dagster import (
    job, op, Out, In, graph,
    AssetMaterialization, ExpectationResult,
    OpExecutionContext, DagsterRunConfig,
    materialize, MetadataValue
)

# Add src directory to path for imports
sys.path.append(str(Path(__file__).parent / 'src'))

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Op 1: Scrape Telegram Data
@op(
    description="Scrape Telegram messages and images from Ethiopian medical channels",
    required_resource_keys={"log"},
    out=Out(description="Path to scraped data directory"),
)
def scrape_telegram_data(context: OpExecutionContext) -> str:
    """
    Run the Telegram scraper to collect messages and images from Ethiopian medical channels.
    
    This operation:
    - Connects to Telegram API
    - Scrapes messages from configured channels
    - Downloads images with organized folder structure
    - Saves data in partitioned JSON format
    """
    try:
        context.log.info("Starting Telegram data scraping...")
        
        # Import and run scraper
        from scraper import TelegramScraper
        
        scraper = TelegramScraper()
        
        # Run scraping with error handling
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            # Scrape with a reasonable limit for daily runs
            limit_per_channel = 500  # Adjust based on needs
            loop.run_until_complete(scraper.scrape_all_channels(limit_per_channel=limit_per_channel))
        finally:
            loop.close()
        
        # Get data path
        raw_data_path = Path(os.getenv('RAW_DATA_PATH', 'data/raw'))
        
        # Log results
        context.log.info(f"Scraping completed. Data saved to: {raw_data_path}")
        
        # Create asset materialization
        context.log_event(
            AssetMaterialization(
                asset_key="telegram_data",
                description="Telegram messages scraped successfully",
                metadata={
                    "data_path": str(raw_data_path),
                    "timestamp": datetime.now().isoformat(),
                    "channels_scraped": scraper.channels
                }
            )
        )
        
        return str(raw_data_path)
        
    except Exception as e:
        context.log.error(f"Telegram scraping failed: {e}")
        raise

# Op 2: Load Raw Data to PostgreSQL
@op(
    description="Load scraped JSON data into PostgreSQL database",
    ins={"data_path": In(description="Path to scraped data")},
    out=Out(description="Number of records loaded"),
)
def load_raw_to_postgres(context: OpExecutionContext, data_path: str) -> int:
    """
    Load raw JSON data from the data lake into PostgreSQL database.
    
    This operation:
    - Reads JSON files from data lake
    - Validates and cleans data
    - Loads to raw.telegram_messages table
    - Handles duplicates and errors
    """
    try:
        context.log.info(f"Loading raw data from: {data_path}")
        
        # Import and run loader
        from load_to_postgres import PostgresDataLoader
        
        loader = PostgresDataLoader()
        
        # Load data
        total_loaded = loader.load_all_data()
        
        # Get loading statistics
        stats = loader.get_loading_stats()
        
        context.log.info(f"Data loading completed. Records loaded: {total_loaded}")
        context.log.info(f"Loading stats: {stats}")
        
        # Create asset materialization
        context.log_event(
            AssetMaterialization(
                asset_key="raw_database",
                description="Raw data loaded to PostgreSQL",
                metadata={
                    "records_loaded": total_loaded,
                    "unique_channels": stats.get('unique_channels', 0),
                    "date_range": f"{stats.get('earliest_message')} to {stats.get('latest_message')}",
                    "messages_with_media": stats.get('messages_with_media', 0),
                    "timestamp": datetime.now().isoformat()
                }
            )
        )
        
        return total_loaded
        
    except Exception as e:
        context.log.error(f"Data loading failed: {e}")
        raise

# Op 3: Run dbt Transformations
@op(
    description="Execute dbt transformations to build the data warehouse",
    ins={"records_loaded": In(description="Number of records loaded to verify")},
    out=Out(description="dbt run results"),
)
def run_dbt_transformations(context: OpExecutionContext, records_loaded: int) -> Dict[str, Any]:
    """
    Run dbt transformations to build the dimensional data warehouse.
    
    This operation:
    - Runs dbt models in proper order
    - Builds staging and marts layers
    - Validates data quality tests
    - Creates star schema tables
    """
    try:
        context.log.info(f"Running dbt transformations for {records_loaded} records...")
        
        # Change to medical_warehouse directory
        dbt_dir = Path(__file__).parent / 'medical_warehouse'
        original_dir = os.getcwd()
        
        try:
            os.chdir(dbt_dir)
            context.log.info(f"Changed to directory: {dbt_dir}")
            
            # Run dbt commands
            commands = [
                ['dbt', 'run'],
                ['dbt', 'test']
            ]
            
            results = {}
            
            for cmd in commands:
                context.log.info(f"Running: {' '.join(cmd)}")
                
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=300  # 5 minute timeout
                )
                
                cmd_name = ' '.join(cmd)
                results[cmd_name] = {
                    'return_code': result.returncode,
                    'stdout': result.stdout,
                    'stderr': result.stderr
                }
                
                if result.returncode != 0:
                    context.log.error(f"dbt command failed: {cmd_name}")
                    context.log.error(f"stderr: {result.stderr}")
                    raise Exception(f"dbt {cmd_name} failed with return code {result.returncode}")
                
                context.log.info(f"dbt {cmd_name} completed successfully")
            
            # Parse results for summary
            run_output = results.get('dbt run', {}).get('stdout', '')
            test_output = results.get('dbt test', {}).get('stdout', '')
            
            # Extract key metrics
            summary = {
                'run_success': results['dbt run']['return_code'] == 0,
                'test_success': results['dbt test']['return_code'] == 0,
                'run_output': run_output,
                'test_output': test_output,
                'timestamp': datetime.now().isoformat()
            }
            
            context.log.info("dbt transformations completed successfully")
            
            # Create asset materialization
            context.log_event(
                AssetMaterialization(
                    asset_key="data_warehouse",
                    description="dbt transformations completed",
                    metadata=summary
                )
            )
            
            return summary
            
        finally:
            os.chdir(original_dir)
        
    except subprocess.TimeoutExpired:
        context.log.error("dbt operation timed out")
        raise
    except Exception as e:
        context.log.error(f"dbt transformations failed: {e}")
        raise

# Op 4: Run YOLO Enrichment
@op(
    description="Run YOLO object detection on downloaded images",
    ins={"dbt_results": In(description="dbt transformation results")},
    out=Out(description="YOLO enrichment results"),
)
def run_yolo_enrichment(context: OpExecutionContext, dbt_results: Dict[str, Any]) -> Dict[str, Any]:
    """
    Run YOLO object detection on downloaded images to enrich the data warehouse.
    
    This operation:
    - Processes downloaded images with YOLOv8
    - Classifies image content (promotional, product_display, etc.)
    - Loads results to database
    - Generates analytical insights
    """
    try:
        context.log.info("Starting YOLO enrichment...")
        
        # Check if images directory exists
        images_path = Path(os.getenv('RAW_DATA_PATH', 'data/raw')) / 'images'
        
        if not images_path.exists():
            context.log.warning(f"Images directory not found: {images_path}")
            context.log.info("Skipping YOLO enrichment - no images found")
            return {"status": "skipped", "reason": "no_images"}
        
        # Count images
        image_files = list(images_path.rglob('*.jpg')) + list(images_path.rglob('*.jpeg')) + list(images_path.rglob('*.png'))
        
        if not image_files:
            context.log.warning("No image files found")
            return {"status": "skipped", "reason": "no_image_files"}
        
        context.log.info(f"Found {len(image_files)} images to process")
        
        # Import and run YOLO detection
        from yolo_detect import YOLODetector
        
        detector = YOLODetector()
        
        # Run detection pipeline
        pipeline_results = detector.run_detection_pipeline()
        
        if not pipeline_results:
            context.log.warning("YOLO pipeline returned no results")
            return {"status": "failed", "reason": "no_results"}
        
        # Load results to database
        from load_yolo_results import YOLOResultsLoader
        
        loader = YOLOResultsLoader()
        loading_results = loader.run_loading_pipeline()
        
        # Analyze patterns
        from analyze_image_patterns import ImagePatternAnalyzer
        
        analyzer = ImagePatternAnalyzer()
        analysis_results = analyzer.generate_comprehensive_analysis()
        
        # Compile results
        enrichment_results = {
            "status": "success",
            "images_processed": pipeline_results['statistics'].get('total_images_processed', 0),
            "detection_results": pipeline_results['statistics'],
            "loading_results": loading_results.get('statistics', {}),
            "analysis_results": analysis_results.get('summary', {}),
            "timestamp": datetime.now().isoformat()
        }
        
        context.log.info("YOLO enrichment completed successfully")
        context.log.info(f"Processed {enrichment_results['images_processed']} images")
        
        # Create asset materialization
        context.log_event(
            AssetMaterialization(
                asset_key="image_enrichment",
                description="YOLO object detection completed",
                metadata={
                    "images_processed": enrichment_results['images_processed'],
                    "categories_detected": len(enrichment_results['detection_results'].get('image_categories', {})),
                    "timestamp": enrichment_results['timestamp']
                }
            )
        )
        
        return enrichment_results
        
    except Exception as e:
        context.log.error(f"YOLO enrichment failed: {e}")
        raise

# Define the complete pipeline job
@job(
    description="Complete Ethiopian Medical Business Data Pipeline",
    tags=["etl", "telegram", "medical", "dagster"],
)
def ethiopian_medical_pipeline():
    """
    Complete data pipeline for Ethiopian medical business analytics.
    
    Pipeline flow:
    1. Scrape Telegram data from medical channels
    2. Load raw data to PostgreSQL database
    3. Run dbt transformations to build data warehouse
    4. Enrich with YOLO object detection on images
    """
    
    # Define the pipeline graph
    scraped_data = scrape_telegram_data()
    loaded_records = load_raw_to_postgres(scraped_data)
    dbt_results = run_dbt_transformations(loaded_records)
    enrichment_results = run_yolo_enrichment(dbt_results)
    
    return enrichment_results

# Alternative job for just scraping and loading (useful for testing)
@job(
    description="Scrape and load pipeline (without transformations)",
    tags=["etl", "telegram", "scraping"],
)
def scrape_and_load_pipeline():
    """
    Lightweight pipeline for scraping and loading only.
    Useful for testing or when transformations are handled separately.
    """
    scraped_data = scrape_telegram_data()
    loaded_records = load_raw_to_postgres(scraped_data)
    return loaded_records

# Alternative job for transformations only
@job(
    description="dbt transformations pipeline",
    tags=["dbt", "transformations", "warehouse"],
)
def transformation_pipeline():
    """
    Pipeline for running dbt transformations only.
    Assumes raw data is already loaded.
    """
    dbt_results = run_dbt_transformations(1000)  # Default record count
    return dbt_results

# Alternative job for YOLO enrichment only
@job(
    description="YOLO enrichment pipeline",
    tags=["yolo", "images", "enrichment"],
)
def enrichment_pipeline():
    """
    Pipeline for YOLO object detection only.
    Assumes data warehouse is built.
    """
    dummy_results = {"run_success": True, "test_success": True}
    enrichment_results = run_yolo_enrichment(dummy_results)
    return enrichment_results

# Asset definitions for tracking
@asset(key="telegram_data", description="Scraped Telegram messages and images")
def telegram_data_asset():
    """Asset representing scraped Telegram data"""
    pass

@asset(key="raw_database", description="Raw data in PostgreSQL")
def raw_database_asset():
    """Asset representing raw database state"""
    pass

@asset(key="data_warehouse", description="Transformed data warehouse")
def data_warehouse_asset():
    """Asset representing the dimensional data warehouse"""
    pass

@asset(key="image_enrichment", description="YOLO object detection results")
def image_enrichment_asset():
    """Asset representing image enrichment results"""
    pass

# Schedule for daily execution
from dagster import ScheduleDefinition

daily_schedule = ScheduleDefinition(
    job=ethiopian_medical_pipeline,
    cron_schedule="0 2 * * *",  # Run daily at 2 AM
    execution_timezone="UTC",
    description="Daily run of Ethiopian medical data pipeline",
)

# Weekly schedule for full pipeline
weekly_schedule = ScheduleDefinition(
    job=ethiopian_medical_pipeline,
    cron_schedule="0 3 * * 0",  # Run weekly on Sunday at 3 AM
    execution_timezone="UTC",
    description="Weekly full pipeline run with YOLO enrichment",
)

if __name__ == "__main__":
    # For local testing
    import asyncio
    
    print("Testing pipeline locally...")
    
    # Test individual ops
    from dagster import build_op_job
    
    # You can test individual ops like this:
    # scrape_job = build_op_job(scrape_telegram_data)
    # scrape_job.execute_in_process()
    
    print("Pipeline definition complete. Run with: dagster dev -f pipeline.py")
