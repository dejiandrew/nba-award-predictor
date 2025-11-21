import os
import subprocess
import pandas as pd
import logging
from datetime import datetime
from kaggle.api.kaggle_api_extended import KaggleApi
import sys
import time
import shutil
import tempfile
from google.cloud import storage

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

# Configure these variables with your Digital Ocean database details
KAGGLE_DATASET = "wyattowalsh/basketball"  
DATASET_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")

# Table to exclude from import
EXCLUDED_TABLES = ["play_by_play"]

def setup_kaggle_api():
    """Authenticate with Kaggle API"""
    logger.info("Authenticating with Kaggle API")
    api = KaggleApi()
    api.authenticate()
    return api

def download_dataset(api):
    """Download the latest version of the dataset using the kaggle CLI"""
    logger.info(f"Creating dataset directory: {DATASET_PATH}")
    os.makedirs(DATASET_PATH, exist_ok=True)
    
    logger.info(f"Downloading dataset from Kaggle: {KAGGLE_DATASET}")
    try:
        # Use subprocess to call kaggle CLI
        cmd = f"kaggle datasets download {KAGGLE_DATASET} --path {DATASET_PATH} --unzip --force"
        logger.info(f"Running command: {cmd}")
        
        result = subprocess.run(
            cmd, 
            shell=True, 
            check=True,
            capture_output=True,
            text=True
        )
        
        logger.info(f"Dataset downloaded to {DATASET_PATH}")
        logger.debug(f"Command output: {result.stdout}")
    except subprocess.CalledProcessError as e:
        logger.error(f"Error downloading dataset: {e.stderr}")
        raise

def export_sqlite_tables_to_gcs(
    sqlite_file, 
    gcs_bucket_name, 
    prefix="", 
    excluded_tables=None,
    credentials_file=None
):
    """
    Extract tables from SQLite file and upload each as a CSV to a GCS bucket
    
    Parameters:
        sqlite_file (str): Path to the SQLite database file
        gcs_bucket_name (str): Name of the GCS bucket where CSV files will be stored
        prefix (str, optional): Prefix for the CSV files in the bucket (e.g., "data/")
        excluded_tables (list, optional): List of table names to exclude from processing
        credentials_file (str, optional): Path to GCP service account credentials JSON file
    """
    import sqlite3
    
    if excluded_tables is None:
        excluded_tables = []
    
    # Set credentials environment variable if provided
    if credentials_file:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_file
    
    logger.info(f"Processing SQLite file for GCS export: {sqlite_file}")
    
    # Use sqlite3 command to list all tables
    result = subprocess.run(
        ["sqlite3", sqlite_file, ".tables"], 
        capture_output=True, 
        text=True
    )
    tables = result.stdout.strip().split()
    
    # Filter out excluded tables
    tables_to_export = [table for table in tables if table not in excluded_tables]
    excluded_count = len(tables) - len(tables_to_export)
    
    logger.info(f"Found {len(tables)} tables in SQLite database")
    logger.info(f"Excluding {excluded_count} tables: {excluded_tables}")
    logger.info(f"Will export {len(tables_to_export)} tables: {tables_to_export}")
    
    # Create SQLite connection
    sqlite_conn = sqlite3.connect(sqlite_file)
    
    # Initialize GCS client
    storage_client = storage.Client()
    bucket = storage_client.bucket(gcs_bucket_name)
    
    # Process each table
    for table in tables_to_export:
        try:
            logger.info(f"Processing table for GCS: {table}")
            
            # Read data from SQLite
            query = f"SELECT * FROM {table}"
            df = pd.read_sql_query(query, sqlite_conn)
            
            logger.info(f"Exporting {len(df)} rows from table {table}")
            
            # Create a temporary file to store the CSV
            with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as temp_file:
                temp_filename = temp_file.name
                
                # Write DataFrame to CSV
                df.to_csv(temp_filename, index=False)
            
            # Define the destination blob name in GCS
            destination_blob_name = f"{prefix}{table}.csv"
            
            # Upload the file to GCS
            blob = bucket.blob(destination_blob_name)
            blob.upload_from_filename(temp_filename)
            
            # Clean up the temporary file
            os.remove(temp_filename)
            
            logger.info(f"Table {table} successfully uploaded to gs://{gcs_bucket_name}/{destination_blob_name}")
            
        except Exception as e:
            logger.error(f"Error processing table {table} for GCS: {str(e)}")
    
    # Close SQLite connection
    sqlite_conn.close()
    
    # Log tables that were skipped
    for table in excluded_tables:
        if table in tables:
            logger.info(f"Skipped table for GCS: {table} (in exclusion list)")


def cleanup_downloaded_data():
    """Cleanup downloaded data files after successful processing"""
    logger.info("Starting cleanup of downloaded data files")
    
    try:
        # List of large files to specifically delete
        large_files = [
            os.path.join(DATASET_PATH, "csv", "play_by_play.csv"),
            os.path.join(DATASET_PATH, "nba.sqlite"),
            os.path.join(DATASET_PATH, "PlayerStatistics.csv")
        ]
        
        # Delete the specific large files first
        for file_path in large_files:
            if os.path.exists(file_path):
                logger.info(f"Deleting large file: {file_path}")
                os.remove(file_path)
                logger.info(f"Successfully deleted: {file_path}")
            else:
                logger.info(f"File not found, skipping: {file_path}")
        
        # Calculate space saved
        saved_space = 0
        for file_path in large_files:
            if not os.path.exists(file_path):
                # Estimate based on known sizes from previous runs
                if "play_by_play.csv" in file_path:
                    saved_space += 2.2  # GB
                elif "nba.sqlite" in file_path:
                    saved_space += 2.2  # GB
                elif "PlayerStatistics.csv" in file_path:
                    saved_space += 0.303  # GB
        
        logger.info(f"Cleanup complete. Estimated {saved_space:.2f}GB of space freed")
        
        # Option to remove CSV directory entirely if it exists
        csv_dir = os.path.join(DATASET_PATH, "csv")
        if os.path.exists(csv_dir):
            logger.info(f"Removing CSV directory: {csv_dir}")
            shutil.rmtree(csv_dir)
            logger.info("CSV directory removed")
            
    except Exception as e:
        logger.error(f"Error during cleanup: {str(e)}")
        # Continue with pipeline even if cleanup fails
        logger.warning("Continuing with pipeline despite cleanup failure")

def main():
    """Main pipeline function"""
    start_time = datetime.now()
    logger.info(f"Starting NBA data pipeline at {start_time}")
    
    try:
        # Set up Kaggle API
        api = setup_kaggle_api()
        
        # Download latest dataset
        download_dataset(api)
        
        # Process SQLite file
        sqlite_file = os.path.join(DATASET_PATH, "nba.sqlite")

        # Export to GCS bucket
        logger.info("Exporting tables to Google Cloud Storage...")
        export_sqlite_tables_to_gcs(
            sqlite_file=sqlite_file,
            gcs_bucket_name="nba_award_predictor",
            prefix="nba_data/",
            excluded_tables=EXCLUDED_TABLES,
            credentials_file="cis-5450-final-project-485661e2f371.json"
        )
        logger.info("GCS export complete")
        
        # Add cleanup step after successful processing
        logger.info("Data processing complete. Starting cleanup...")
        cleanup_downloaded_data()
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds() / 60  # in minutes
        logger.info(f"Pipeline completed successfully in {duration:.2f} minutes")
    except Exception as e:
        logger.error(f"Error in pipeline: {str(e)}")
        raise

if __name__ == "__main__":
    main()
