import os
import subprocess
import pandas as pd
import logging
from datetime import datetime
import sys
import time
import glob
import argparse
import psutil
import shutil 
from kaggle.api.kaggle_api_extended import KaggleApi
from google.cloud import storage 
import tempfile

# Configure logging
log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"pipeline_{datetime.now().strftime('%Y%m%d')}.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

# Updated Kaggle dataset 
KAGGLE_DATASET = "eoinamoore/historical-nba-data-and-player-box-scores"
DATASET_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")

# GCS configuration
GCS_BUCKET_NAME = "nba_award_predictor"
GCS_PREFIX = "nba_data/"

# Default chunk size for processing large files
DEFAULT_CHUNK_SIZE = 1000

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='NBA Data Pipeline')
    parser.add_argument('--update-mode', choices=['replace', 'append', 'skip'], default='replace',
                      help='How to handle existing tables: replace (drop and recreate), append (add new data), skip (ignore if exists)')
    parser.add_argument('--chunk-size', type=int, default=DEFAULT_CHUNK_SIZE,
                      help=f'Number of rows to process at once for large files (default: {DEFAULT_CHUNK_SIZE})')
    parser.add_argument('--no-download', action='store_true',
                      help='Skip downloading the dataset (use existing files)')
    parser.add_argument('--file-size-threshold', type=int, default=10,
                      help='Size in MB above which files are processed in chunks (default: 10)')
    parser.add_argument('--cleanup', action='store_true', default=True,
                      help='Cleanup downloaded files after processing (default: True)')
    parser.add_argument('--no-cleanup', action='store_true',
                      help='Skip cleanup of downloaded files (keeps files on disk)')
    parser.add_argument('--export-gcs', action='store_true', default=True,
                      help='Export data to Google Cloud Storage (default: True)')
    parser.add_argument('--no-export-gcs', action='store_true',
                      help='Skip exporting data to Google Cloud Storage')
    parser.add_argument('--gcs-credentials-file', type=str, default=None,
                      help='Path to GCS credentials JSON file')
    parser.add_argument('--compress-gcs', action='store_true', default=False,
                      help='Compress files when uploading to GCS (uses gzip)')
    return parser.parse_args()

def setup_kaggle_api():
    """Authenticate with Kaggle API"""
    logger.info("Authenticating with Kaggle API")
    api = KaggleApi()
    api.authenticate()
    return api

def download_dataset(api, skip_download=False):
    """Download the latest version of the dataset using the Kaggle API"""
    logger.info(f"Creating dataset directory: {DATASET_PATH}")
    os.makedirs(DATASET_PATH, exist_ok=True)
    
    if skip_download:
        logger.info("Skipping dataset download as requested")
        return
    
    logger.info(f"Downloading dataset from Kaggle: {KAGGLE_DATASET}")
    try:
        # Use Kaggle API to download the dataset
        api.dataset_download_files(
            dataset=KAGGLE_DATASET,
            path=DATASET_PATH,
            unzip=True,
            force=True
        )
        
        logger.info(f"Dataset downloaded to {DATASET_PATH}")
    except Exception as e:
        logger.error(f"Error downloading dataset: {str(e)}")
        raise

def get_table_name_from_file(file_path):
    """Extract table name from file path (removing .csv extension)"""
    file_name = os.path.basename(file_path)
    table_name = os.path.splitext(file_name)[0].lower()
    return table_name

def export_csv_files_to_gcs(gcs_bucket_name, prefix="", credentials_file=None, compress=False):
    """
    Export all CSV files from the dataset directory to a GCS bucket
    
    Parameters:
        gcs_bucket_name (str): Name of the GCS bucket where CSV files will be stored
        prefix (str, optional): Prefix for the CSV files in the bucket (e.g., "data/")
        credentials_file (str, optional): Path to GCP service account credentials JSON file
        compress (bool, optional): Whether to compress files with gzip before uploading
    """
    # Find all CSV files in the dataset directory
    csv_files = glob.glob(os.path.join(DATASET_PATH, "*.csv"))
    
    if not csv_files:
        logger.warning("No CSV files found in dataset directory for GCS export")
        return
        
    logger.info(f"Found {len(csv_files)} CSV files for GCS export")
    
    # Set credentials environment variable if provided
    if credentials_file:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_file
    
    # Initialize GCS client
    storage_client = storage.Client()
    bucket = storage_client.bucket(gcs_bucket_name)
    
    # Check if bucket exists
    if not bucket.exists():
        logger.error(f"GCS bucket {gcs_bucket_name} does not exist")
        return
    
    # Process each file
    for csv_file in csv_files:
        table_name = get_table_name_from_file(csv_file)
        try:
            logger.info(f"Exporting file to GCS: {csv_file}")
            
            # Determine if we need to compress
            if compress:
                destination_blob_name = f"{prefix}{table_name}.csv.gz"
                
                # Create a temporary file with gzip compression
                with tempfile.NamedTemporaryFile(suffix='.csv.gz', delete=False) as temp_file:
                    temp_filename = temp_file.name
                
                # Read and compress the file
                df = pd.read_csv(csv_file, low_memory=False)
                df.to_csv(temp_filename, index=False, compression='gzip')
                
                logger.info(f"Compressed {len(df)} rows from {table_name}")
                
                # Upload the compressed file
                blob = bucket.blob(destination_blob_name)
                blob.upload_from_filename(temp_filename)
                
                # Clean up temporary file
                os.remove(temp_filename)
            else:
                # Upload the file directly without compression
                destination_blob_name = f"{prefix}{table_name}.csv"
                
                # Upload the file to GCS
                blob = bucket.blob(destination_blob_name)
                blob.upload_from_filename(csv_file)
            
            logger.info(f"File successfully uploaded to gs://{gcs_bucket_name}/{destination_blob_name}")
            
        except Exception as e:
            logger.error(f"Error uploading file {csv_file} to GCS: {str(e)}")
            # Continue with the next file
            continue

def cleanup_downloaded_data():
    """Cleanup downloaded data files after successful processing"""
    logger.info("Starting cleanup of downloaded data files")
    
    try:
        # Calculate total size before cleanup
        total_size_before = 0
        for root, dirs, files in os.walk(DATASET_PATH):
            for file in files:
                file_path = os.path.join(root, file)
                total_size_before += os.path.getsize(file_path)
        
        total_size_before_mb = total_size_before / (1024 * 1024)
        logger.info(f"Total size before cleanup: {total_size_before_mb:.2f} MB")
        
        # Check if SQLite file exists and delete it
        sqlite_file = os.path.join(DATASET_PATH, "nba.sqlite")
        if os.path.exists(sqlite_file):
            logger.info(f"Deleting SQLite file: {sqlite_file}")
            os.remove(sqlite_file)
            logger.info("SQLite file removed")
        
        # Delete all CSV files in the directory
        csv_files = glob.glob(os.path.join(DATASET_PATH, "*.csv"))
        for csv_file in csv_files:
            logger.info(f"Deleting CSV file: {csv_file}")
            os.remove(csv_file)
        
        # Remove subdirectories if they exist
        for item in os.listdir(DATASET_PATH):
            item_path = os.path.join(DATASET_PATH, item)
            if os.path.isdir(item_path):
                logger.info(f"Removing subdirectory: {item_path}")
                shutil.rmtree(item_path)
        
        # Calculate space saved
        logger.info(f"Cleanup complete. Approximately {total_size_before_mb:.2f}MB of space freed")
        
    except Exception as e:
        logger.error(f"Error during cleanup: {str(e)}")
        # Continue with pipeline even if cleanup fails
        logger.warning("Continuing with pipeline despite cleanup failure")

def main():
    """Main pipeline function"""
    args = parse_arguments()
    
    start_time = datetime.now()
    logger.info(f"Starting NBA data pipeline at {start_time}")
    logger.info(f"Update mode: {args.update_mode}")
    logger.info(f"Chunk size: {args.chunk_size}")
    logger.info(f"Cleanup after processing: {not args.no_cleanup}")
    logger.info(f"Export to GCS: {not args.no_export_gcs}")
    if not args.no_export_gcs:
        logger.info(f"GCS bucket: {GCS_BUCKET_NAME}")
        logger.info(f"GCS prefix: {GCS_PREFIX}")
        logger.info(f"GCS compression: {args.compress_gcs}")
    
    try:
        # Set up Kaggle API
        api = setup_kaggle_api()
        
        # Download latest dataset (unless --no-download is specified)
        download_dataset(api, args.no_download)
        
        # Export to GCS before cleanup (if enabled)
        if not args.no_export_gcs:
            logger.info("Exporting data to Google Cloud Storage...")
            export_csv_files_to_gcs(
                gcs_bucket_name=GCS_BUCKET_NAME,
                prefix=GCS_PREFIX,
                credentials_file="cis-5450-final-project-485661e2f371.json",
                compress=args.compress_gcs
            )
            logger.info("GCS export complete")
        
        # Clean up downloaded data if not disabled
        if not args.no_cleanup:
            logger.info("Data processing complete. Starting cleanup...")
            cleanup_downloaded_data()
        else:
            logger.info("Cleanup disabled. Downloaded files will remain on disk.")
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds() / 60  # in minutes
        logger.info(f"Pipeline completed successfully in {duration:.2f} minutes")
    except Exception as e:
        logger.error(f"Error in pipeline: {str(e)}")
        raise

if __name__ == "__main__":
    main()
