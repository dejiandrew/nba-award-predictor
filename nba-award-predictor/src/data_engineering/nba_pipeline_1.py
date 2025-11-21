import os
import subprocess
import pandas as pd
from sqlalchemy import create_engine
import logging
from datetime import datetime
from kaggle.api.kaggle_api_extended import KaggleApi
import sys
import time
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

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
DB_HOST = os.getenv("NBA_DB_HOST")
DB_PORT = os.getenv("NBA_DB_PORT")
DB_NAME = os.getenv("NBA_DB_NAME")
DB_USER = os.getenv("NBA_DB_USER")
DB_PASSWORD = os.getenv("NBA_DB_PASSWORD")

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

def setup_database_connection():
    """Create a connection to the PostgreSQL database"""
    logger.info("Connecting to PostgreSQL database")
    connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    
    # Try to connect with retries
    max_retries = 3
    for attempt in range(max_retries):
        try:
            engine = create_engine(connection_string)
            # Test the connection
            with engine.connect() as conn:
                pass
            logger.info("Successfully connected to the database")
            return engine
        except Exception as e:
            logger.warning(f"Connection attempt {attempt+1} failed: {str(e)}")
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt  # Exponential backoff
                logger.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                logger.error("All connection attempts failed")
                raise

def load_sqlite_tables_to_postgres(sqlite_file, pg_engine):
    """Load all tables from SQLite file to PostgreSQL, except excluded tables"""
    logger.info(f"Processing SQLite file: {sqlite_file}")
    
    # Use sqlite3 command to list all tables
    result = subprocess.run(
        ["sqlite3", sqlite_file, ".tables"], 
        capture_output=True, 
        text=True
    )
    tables = result.stdout.strip().split()
    
    # Filter out excluded tables
    tables_to_load = [table for table in tables if table not in EXCLUDED_TABLES]
    excluded_count = len(tables) - len(tables_to_load)
    
    logger.info(f"Found {len(tables)} tables in SQLite database")
    logger.info(f"Excluding {excluded_count} tables: {EXCLUDED_TABLES}")
    logger.info(f"Will load {len(tables_to_load)} tables: {tables_to_load}")
    
    # Create SQLite connection
    sqlite_conn = f"sqlite:///{sqlite_file}"
    sqlite_engine = create_engine(sqlite_conn)
    
    # Load each table
    for table in tables_to_load:
        try:
            logger.info(f"Processing table: {table}")
            # Read data from SQLite
            df = pd.read_sql_table(table, sqlite_engine)
            
            # Write to PostgreSQL - using replace to update the entire table
            logger.info(f"Loading {len(df)} rows to table {table}")
            df.to_sql(
                name=table,
                con=pg_engine,
                if_exists='replace',  # Options: 'fail', 'replace', or 'append'
                index=False,
                chunksize=5000  # Load in chunks to avoid memory issues
            )
            logger.info(f"Table {table} successfully loaded with {len(df)} rows")
        except Exception as e:
            logger.error(f"Error processing table {table}: {str(e)}")
    
    # Log tables that were skipped
    for table in EXCLUDED_TABLES:
        if table in tables:
            logger.info(f"Skipped table: {table} (in exclusion list)")

def main():
    """Main pipeline function"""
    start_time = datetime.now()
    logger.info(f"Starting NBA data pipeline at {start_time}")
    
    try:
        # Set up Kaggle API
        api = setup_kaggle_api()
        
        # Download latest dataset
        download_dataset(api)
        
        # Connect to PostgreSQL
        pg_engine = setup_database_connection()
        
        # Process SQLite file
        sqlite_file = os.path.join(DATASET_PATH, "nba.sqlite")
        if os.path.exists(sqlite_file):
            load_sqlite_tables_to_postgres(sqlite_file, pg_engine)
        else:
            logger.error(f"SQLite file not found at {sqlite_file}")
            raise FileNotFoundError(f"SQLite file not found at {sqlite_file}")
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds() / 60  # in minutes
        logger.info(f"Pipeline completed successfully in {duration:.2f} minutes")
    except Exception as e:
        logger.error(f"Error in pipeline: {str(e)}")
        raise

if __name__ == "__main__":
    main()