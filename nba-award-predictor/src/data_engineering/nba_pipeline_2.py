import os
import subprocess
import pandas as pd
from sqlalchemy import create_engine, text, inspect, MetaData, Table, Column, String, Integer, Float, Boolean, DateTime
from sqlalchemy.ext.declarative import declarative_base
import logging
from datetime import datetime
import sys
import time
import glob
import argparse
import psutil
from kaggle.api.kaggle_api_extended import KaggleApi
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

# Updated Kaggle dataset 
KAGGLE_DATASET = "eoinamoore/historical-nba-data-and-player-box-scores"
DATASET_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")

# Configure these variables with your Digital Ocean database details
DB_HOST = os.getenv("NBA_DB_HOST")
DB_PORT = os.getenv("NBA_DB_PORT")
DB_NAME = os.getenv("NBA_DB_NAME")
DB_USER = os.getenv("NBA_DB_USER")
DB_PASSWORD = os.getenv("NBA_DB_PASSWORD")

# Default chunk size for processing large files
DEFAULT_CHUNK_SIZE = 1000

# Database schema mapping - define expected column types for each table
# This is especially important for columns that might have mixed types
TABLE_SCHEMAS = {
    'playerstatistics': {
        'firstname': String,
        'lastname': String,
        'personid': Float,
        'gameid': Float,
        'gamedate': String,  # Using String to handle ISO date format safely
        'playerteamcity': String,
        'playerteamname': String,
        'opponentteamcity': String,
        'opponentteamname': String,
        'gametype': String,  # This was causing the error - needs to be string
        'gamelabel': String,
        'gamesublabel': String,
        'seriesgamenumber': String,  # Could be NULL or numbers, safer as string
        'win': Float,
        'home': Float,
        'numminutes': Float,
        'points': Float,
        'assists': Float,
        'blocks': Float,
        'steals': Float,
        'fieldgoalsmade': Float,
        'fieldgoalsattempted': Float,
        'fieldgoalspercentage': Float,
        'threepointersmade': Float,
        'threepointersattempted': Float,
        'threepointerspercentage': Float,
        'freethrowsmade': Float,
        'freethrowsattempted': Float,
        'freethrowspercentage': Float,
        'reboundsoffensive': Float,
        'reboundsdefensive': Float,
        'reboundstotal': Float,
        'foulspersonal': Float,
        'turnovers': Float,
        'plusminuspoints': Float
    }
    # Add other tables as needed
}

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

def get_table_name_from_file(file_path):
    """Extract table name from file path (removing .csv extension)"""
    file_name = os.path.basename(file_path)
    table_name = os.path.splitext(file_name)[0].lower()
    return table_name

def table_exists(engine, table_name):
    """Check if a table already exists in the database"""
    inspector = inspect(engine)
    return table_name in inspector.get_table_names()

def get_row_count(engine, table_name):
    """Get the number of rows in an existing table"""
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
        return result.scalar()

def is_large_file(file_path, threshold_mb=10):
    """Check if a file is larger than the threshold size"""
    size_mb = os.path.getsize(file_path) / (1024 * 1024)
    return size_mb > threshold_mb

def get_optimal_chunk_size(file_size_mb, available_memory_mb):
    """Calculate optimal chunk size based on file size and available memory"""
    # Use at most 50% of available memory, with a minimum of 500 rows
    # and a maximum of 5000 rows
    memory_factor = 0.5  # Use 50% of available memory
    estimated_rows_per_mb = 1000  # Estimate 1000 rows per MB (adjust based on your data)
    
    # Calculate based on available memory
    rows_based_on_memory = int((available_memory_mb * memory_factor) / (file_size_mb / (file_size_mb * estimated_rows_per_mb)))
    
    # Ensure within reasonable bounds
    return max(500, min(5000, rows_based_on_memory))

def create_table_with_schema(engine, table_name, update_mode='replace'):
    """Create a table with the predefined schema if it exists in TABLE_SCHEMAS"""
    if table_name not in TABLE_SCHEMAS:
        logger.info(f"No predefined schema for {table_name}, will use pandas auto-detection")
        return False
    
    if update_mode == 'skip' and table_exists(engine, table_name):
        logger.info(f"Table {table_name} already exists - skipping as requested")
        return True
    
    if update_mode == 'append' and table_exists(engine, table_name):
        logger.info(f"Table {table_name} already exists - will append new data")
        return False  # Continue with processing, don't need to create table
    
    # Get the schema for this table
    schema = TABLE_SCHEMAS[table_name]
    
    # Create a new metadata and table definition
    metadata = MetaData()
    
    # Define the columns based on our schema
    columns = []
    for column_name, column_type in schema.items():
        columns.append(Column(column_name, column_type))
    
    # Create the table definition
    table = Table(table_name, metadata, *columns)
    
    # Create the table in the database
    if update_mode == 'replace' and table_exists(engine, table_name):
        # Drop the existing table if replace mode
        with engine.connect() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
            conn.commit()
        
    # Create the table
    metadata.create_all(engine)
    logger.info(f"Created table {table_name} with predefined schema")
    return True

def sample_and_detect_schema(csv_file):
    """Sample a CSV file and detect the schema"""
    # Read a small sample to detect schema
    sample_df = pd.read_csv(csv_file, nrows=1000, low_memory=False)
    
    table_name = get_table_name_from_file(csv_file)
    schema = {}
    
    # Examine each column
    for column in sample_df.columns:
        col_name = column.lower().replace(' ', '_').replace('-', '_')
        
        # Check column data type
        dtype = sample_df[column].dtype
        
        if pd.api.types.is_integer_dtype(dtype):
            schema[col_name] = Integer
        elif pd.api.types.is_float_dtype(dtype):
            schema[col_name] = Float
        elif pd.api.types.is_bool_dtype(dtype):
            schema[col_name] = Boolean
        elif pd.api.types.is_datetime64_dtype(dtype):
            schema[col_name] = DateTime
        else:
            # Default to string for object, categorical, or mixed types
            schema[col_name] = String
    
    return schema

def process_file(csv_file, pg_engine, update_mode, chunk_size, file_size_threshold):
    """Process a CSV file and load it to PostgreSQL"""
    table_name = get_table_name_from_file(csv_file)
    logger.info(f"Processing CSV file: {csv_file} for table: {table_name}")
    
    # Check if table exists
    if table_exists(pg_engine, table_name):
        if update_mode == 'skip':
            logger.info(f"Table {table_name} already exists - skipping as requested")
            return
        elif update_mode == 'append':
            logger.info(f"Table {table_name} already exists - will append new data")
            existing_rows = get_row_count(pg_engine, table_name)
            logger.info(f"Existing rows in {table_name}: {existing_rows}")
    
    try:
        # If we have a predefined schema, create the table
        table_created = False
        if table_name in TABLE_SCHEMAS:
            table_created = create_table_with_schema(pg_engine, table_name, update_mode)
        
        # Check if this is a large file that needs chunking
        if is_large_file(csv_file, file_size_threshold):
            # Get available memory
            available_memory_mb = psutil.virtual_memory().available / (1024 * 1024)
            file_size_mb = os.path.getsize(csv_file) / (1024 * 1024)
            
            # Adjust chunk size based on available memory
            optimal_chunk_size = get_optimal_chunk_size(file_size_mb, available_memory_mb)
            actual_chunk_size = min(chunk_size, optimal_chunk_size)
            
            logger.info(f"Loading large file {table_name} ({file_size_mb:.2f} MB) in chunks of {actual_chunk_size} rows")
            
            # Read and load in chunks
            for chunk_num, chunk in enumerate(pd.read_csv(
                csv_file, 
                chunksize=actual_chunk_size, 
                low_memory=False,
                dtype=str  # Read everything as string initially to avoid type conversion issues
            )):
                logger.info(f"Loading chunk {chunk_num+1} with {len(chunk)} rows for table {table_name}")
                
                # Clean column names (remove spaces, special chars)
                chunk.columns = [col.lower().replace(' ', '_').replace('-', '_') for col in chunk.columns]
                
                # Convert columns to appropriate types based on schema if table has not been created yet
                if not table_created and chunk_num == 0 and table_name in TABLE_SCHEMAS:
                    for col, dtype in TABLE_SCHEMAS[table_name].items():
                        if col in chunk.columns:
                            # Convert to appropriate type based on schema
                            if dtype == Integer:
                                # Handle NaN values
                                chunk[col] = pd.to_numeric(chunk[col], errors='coerce').fillna(0).astype('int64')
                            elif dtype == Float:
                                chunk[col] = pd.to_numeric(chunk[col], errors='coerce')
                            elif dtype == Boolean:
                                chunk[col] = chunk[col].astype('bool')
                            # String and DateTime remain as strings
                
                # For the first chunk with 'replace' mode, replace the table
                # For subsequent chunks or 'append' mode, append to the table
                if chunk_num == 0 and update_mode == 'replace' and not table_created:
                    if_exists = 'replace'
                else:
                    if_exists = 'append'
                
                # Save to database
                chunk.to_sql(
                    name=table_name,
                    con=pg_engine,
                    if_exists=if_exists,
                    index=False,
                    # Only specify dtype if the table was not created explicitly
                    dtype=None if table_created else {col: String for col in chunk.columns}
                )
                
                # Free memory explicitly
                del chunk
                
                # Log memory usage
                memory_usage = psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024)
                logger.debug(f"Current memory usage: {memory_usage:.2f} MB")
                
            logger.info(f"Large table {table_name} loaded successfully")
        else:
            # For smaller files, load the entire file at once
            logger.info(f"Loading small file {table_name} in one operation")
            df = pd.read_csv(csv_file, low_memory=False, dtype=str)
            
            # Clean column names
            df.columns = [col.lower().replace(' ', '_').replace('-', '_') for col in df.columns]
            
            # Convert columns to appropriate types based on schema
            if table_name in TABLE_SCHEMAS and not table_created:
                for col, dtype in TABLE_SCHEMAS[table_name].items():
                    if col in df.columns:
                        if dtype == Integer:
                            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('int64')
                        elif dtype == Float:
                            df[col] = pd.to_numeric(df[col], errors='coerce')
                        elif dtype == Boolean:
                            df[col] = df[col].astype('bool')
                        # String and DateTime remain as strings
            
            logger.info(f"Loading {len(df)} rows to table {table_name}")
            
            # Determine if we should replace or append
            if_exists = 'replace' if update_mode == 'replace' and not table_created else 'append'
            
            df.to_sql(
                name=table_name,
                con=pg_engine,
                if_exists=if_exists,
                index=False,
                # Only specify dtype if the table was not created explicitly
                dtype=None if table_created else {col: String for col in df.columns}
            )
            logger.info(f"Table {table_name} successfully loaded with {len(df)} rows")
            
            # Free memory
            del df
    except Exception as e:
        logger.error(f"Error processing file {csv_file}: {str(e)}")
        raise

def load_all_csv_files_to_postgres(pg_engine, update_mode, chunk_size, file_size_threshold):
    """Load all CSV files from the dataset directory to PostgreSQL"""
    # Find all CSV files in the dataset directory
    csv_files = glob.glob(os.path.join(DATASET_PATH, "*.csv"))
    
    logger.info(f"Found {len(csv_files)} CSV files in dataset directory")
    
    # Process each file
    for csv_file in csv_files:
        try:
            process_file(csv_file, pg_engine, update_mode, chunk_size, file_size_threshold)
        except Exception as e:
            logger.error(f"Failed to load {csv_file}: {str(e)}")
            # Continue with the next file even if this one fails
            continue

def main():
    """Main pipeline function"""
    args = parse_arguments()
    
    start_time = datetime.now()
    logger.info(f"Starting NBA data pipeline at {start_time}")
    logger.info(f"Update mode: {args.update_mode}")
    logger.info(f"Chunk size: {args.chunk_size}")
    
    try:
        # Set up Kaggle API
        api = setup_kaggle_api()
        
        # Download latest dataset (unless --no-download is specified)
        download_dataset(api, args.no_download)
        
        # Connect to PostgreSQL
        pg_engine = setup_database_connection()
        
        # Load all CSV files to PostgreSQL
        load_all_csv_files_to_postgres(
            pg_engine, 
            args.update_mode, 
            args.chunk_size, 
            args.file_size_threshold
        )
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds() / 60  # in minutes
        logger.info(f"Pipeline completed successfully in {duration:.2f} minutes")
    except Exception as e:
        logger.error(f"Error in pipeline: {str(e)}")
        raise

if __name__ == "__main__":
    main()