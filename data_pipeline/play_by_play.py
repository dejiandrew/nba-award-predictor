#!/usr/bin/env python3
import os
import subprocess
import logging
from datetime import datetime
import sys
import shutil
from google.cloud import storage

# Configure logging
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"nba_pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

# Configuration constants
KAGGLE_DATASET = "wyattowalsh/basketball"
DATASET_PATH = "data"
GCS_BUCKET_NAME = "nba_award_predictor"
GCS_BLOB_NAME = "nba_data/play_by_play_raw.csv"
GCS_CREDENTIALS_PATH = "cis-5450-final-project-485661e2f371.json"

def setup_directories():
    """Create necessary directories"""
    os.makedirs(DATASET_PATH, exist_ok=True)
    logger.info(f"Created dataset directory: {DATASET_PATH}")

def download_play_by_play_csv():
    """Download only the play_by_play CSV file directly from Kaggle"""
    # The Kaggle API has a -f flag to download a specific file
    csv_path = os.path.join(DATASET_PATH, "play_by_play.csv")
    
    # Try to download just the CSV file directly
    try:
        logger.info("Attempting to download only the play_by_play CSV file from Kaggle")
        cmd = f"kaggle datasets download {KAGGLE_DATASET} -f csv/play_by_play.csv --path {DATASET_PATH} --unzip --force"
        logger.info(f"Running command: {cmd}")
        
        result = subprocess.run(
            cmd,
            shell=True,
            check=True,
            capture_output=True,
            text=True
        )
        
        logger.info(f"CSV file downloaded to {DATASET_PATH}")
        logger.debug(f"Command output: {result.stdout}")
        
        # Check if file exists
        if os.path.exists(csv_path):
            return csv_path
        else:
            logger.warning("File not found at expected location, will try alternative download method")
            raise FileNotFoundError(f"CSV file not found at {csv_path}")
            
    except Exception as e:
        logger.warning(f"Direct CSV download failed: {str(e)}")
        
        # Fallback: Download the dataset and use the CSV from it
        try:
            logger.info("Trying to download the full dataset and extract CSV")
            cmd = f"kaggle datasets download {KAGGLE_DATASET} --path {DATASET_PATH} --unzip --force"
            logger.info(f"Running command: {cmd}")
            
            result = subprocess.run(
                cmd,
                shell=True,
                check=True,
                capture_output=True,
                text=True
            )
            
            logger.info("Dataset downloaded, checking for play_by_play CSV")
            
            # Check for CSV file in expected location
            csv_path = os.path.join(DATASET_PATH, "csv", "play_by_play.csv")
            if os.path.exists(csv_path):
                logger.info(f"Found CSV file at {csv_path}")
                return csv_path
            else:
                logger.error(f"CSV file not found at {csv_path}")
                raise FileNotFoundError(f"CSV file not found at {csv_path}")
                
        except Exception as e:
            logger.error(f"Error in fallback download: {str(e)}")
            raise

def upload_file_to_gcs(file_path, bucket_name, blob_name, credentials_path=None):
    """Upload file directly to GCS without loading into memory"""
    logger.info(f"Uploading file to GCS bucket: {bucket_name}")
    
    # Set credentials if provided
    if credentials_path:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
    
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        
        # Get file size for logging
        file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
        logger.info(f"Starting upload of {file_size_mb:.2f} MB file to gs://{bucket_name}/{blob_name}")
        
        # Upload the file to GCS
        blob.upload_from_filename(file_path)
        
        logger.info(f"Successfully uploaded to gs://{bucket_name}/{blob_name}")
        return True
        
    except Exception as e:
        logger.error(f"Error uploading to GCS: {str(e)}")
        raise

def thorough_cleanup():
    """Clean up ALL downloaded files including SQLite database"""
    logger.info("Starting thorough cleanup of ALL downloaded files")
    
    try:
        # List all expected large files to specifically check for
        large_files = [
            os.path.join(DATASET_PATH, "nba.sqlite"),
            os.path.join(DATASET_PATH, "PlayerStatistics.csv")
        ]
        
        # Check and remove each large file individually
        for file_path in large_files:
            if os.path.exists(file_path):
                file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
                logger.info(f"Removing large file: {file_path} ({file_size_mb:.2f} MB)")
                os.remove(file_path)
                logger.info(f"Successfully removed: {file_path}")
            
        # Check for the csv directory
        csv_dir = os.path.join(DATASET_PATH, "csv")
        if os.path.exists(csv_dir):
            logger.info(f"Removing CSV directory: {csv_dir}")
            shutil.rmtree(csv_dir)
            logger.info("CSV directory removed")
        
        # Finally, remove the entire data directory to ensure complete cleanup
        if os.path.exists(DATASET_PATH):
            logger.info(f"Removing entire data directory: {DATASET_PATH}")
            shutil.rmtree(DATASET_PATH)
            logger.info("Data directory completely removed")
            
        # Calculate total disk usage before and after (if possible)
        try:
            result = subprocess.run(
                "df -h / | grep -v Filesystem",
                shell=True,
                check=True,
                capture_output=True,
                text=True
            )
            logger.info(f"Current disk usage: {result.stdout.strip()}")
        except Exception:
            pass  # Ignore if df command fails
            
    except Exception as e:
        logger.error(f"Error during thorough cleanup: {str(e)}")
        logger.warning("Continuing despite cleanup issues")

def main():
    """Main function that orchestrates the pipeline"""
    start_time = datetime.now()
    logger.info(f"Starting NBA play-by-play direct upload pipeline at {start_time}")
    
    try:
        # Setup directories
        setup_directories()
        
        # Download play_by_play CSV file
        csv_path = download_play_by_play_csv()
        logger.info(f"Successfully found play_by_play CSV at: {csv_path}")
        
        # Upload to GCS
        upload_file_to_gcs(
            file_path=csv_path,
            bucket_name=GCS_BUCKET_NAME,
            blob_name=GCS_BLOB_NAME,
            credentials_path=GCS_CREDENTIALS_PATH
        )
        
        # Thorough cleanup of ALL downloaded files
        thorough_cleanup()
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds() / 60  # in minutes
        logger.info(f"Pipeline completed successfully in {duration:.2f} minutes")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        
        # Attempt cleanup even after failure
        logger.info("Attempting cleanup after failure")
        thorough_cleanup()
        
        raise

if __name__ == "__main__":
    main()
