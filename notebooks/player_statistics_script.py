# First, clear any existing large DataFrames from memory
try:
    del player_statistics_df
    import gc
    gc.collect()
    print("Memory cleared from previous DataFrame")
except NameError:
    print("No existing DataFrame found in memory")

import pandas as pd
import unicodedata
import re
import duckdb
import wget
from google.cloud import storage
import os

def remove_accents(text):
    """
    Remove accent marks from input text while preserving the base characters.
    Also handles special characters like Đ/đ.
    """
    if not isinstance(text, str):
        return text
        
    special_chars = {
        'Đ': 'D', 'đ': 'd', 'Ł': 'L', 'ł': 'l', 'Ø': 'O', 'ø': 'o',
        'Ŧ': 'T', 'ŧ': 't', 'Æ': 'AE', 'æ': 'ae', 'Œ': 'OE', 'œ': 'oe',
        'ß': 'ss'
    }
    
    for char, replacement in special_chars.items():
        text = text.replace(char, replacement)
    
    normalized_text = unicodedata.normalize('NFKD', text)
    result = ''.join(c for c in normalized_text if not unicodedata.category(c).startswith('Mn'))
    
    return result

# Download files
print("Downloading files...")
filename = 'playerstatistics.csv'
url = f'https://storage.googleapis.com/nba_award_predictor/nba_data/{filename}'
wget.download(url)
print(f"\nDownloaded {filename}")

filename = 'name_mappings.csv'
url = f'https://storage.googleapis.com/nba_award_predictor/nba_data/{filename}'
wget.download(url)
print(f"\nDownloaded {filename}")

filename = 'nba_player_lookup.csv'
url = f'https://storage.googleapis.com/nba_award_predictor/nba_data/{filename}'
wget.download(url)
print(f"\nDownloaded {filename}")

# Read in the smaller datasets fully
name_mapping_df = pd.read_csv('name_mappings.csv')
nba_player_lookup_df = pd.read_csv('nba_player_lookup.csv')

# Clean player names in lookup table
nba_player_lookup_df["player_name"] = nba_player_lookup_df["player_name"].apply(remove_accents)

# Register these dataframes with DuckDB
duckdb.register('name_mapping_df', name_mapping_df)
duckdb.register('nba_player_lookup_df', nba_player_lookup_df)

# Define the output file
output_file = 'player-statistics.csv'

# Process and save in chunks
chunk_size = 100000
first_chunk = True
processed_rows = 0

print(f"Processing data in chunks of {chunk_size} rows...")

for chunk_num, chunk in enumerate(pd.read_csv('playerstatistics.csv', chunksize=chunk_size, low_memory=False)):
    # Add full_name column
    chunk['full_name'] = chunk['firstName'] + ' ' + chunk['lastName']
    
    # Register current chunk with DuckDB
    duckdb.register('player_statistics_chunk', chunk)
    
    query = """
WITH CTE AS (
        SELECT * FROM player_statistics_chunk
        LEFT JOIN name_mapping_df
        ON player_statistics_chunk.full_name = name_mapping_df.in_table_name
    )
    ,CTE2 AS (
        SELECT *,
        CASE WHEN nba_lookup_name IS NULL THEN full_name
        ELSE nba_lookup_name
        END AS player_full_name
        FROM CTE
    )
    
    ,CTE3 AS (
    SELECT 
        CTE2.*,
        nba_player_lookup_df.player_id
    FROM CTE2
    LEFT JOIN nba_player_lookup_df
    ON CTE2.player_full_name = nba_player_lookup_df.player_name
    )

    SELECT
    firstName
    ,lastName
    ,full_name
    ,CAST(player_id_1 AS INT) AS player_id
    --,personId
    ,gameId
    ,gameDate
    ,playerteamCity
    ,playerteamName
    ,opponentteamCity
    ,opponentteamName
    ,gameType
    ,gameLabel
    ,gameSubLabel
    ,seriesGameNumber
    ,win
    ,home
    ,numMinutes
    ,points
    ,assists
    ,blocks
    ,steals
    ,fieldGoalsAttempted
    ,fieldGoalsMade
    ,fieldGoalsPercentage
    ,threePointersAttempted
    ,threePointersMade
    ,threePointersPercentage
    ,freeThrowsAttempted
    ,freeThrowsMade
    ,freeThrowsPercentage
    ,reboundsDefensive
    ,reboundsOffensive
    ,reboundsTotal
    ,foulsPersonal
    ,turnovers
    ,plusMinusPoints
    --,in_table_name
    --,nba_lookup_name
    --,player_id
    --,Unnamed: 3
    --,player_full_name
    FROM CTE3
    """
    
    # Execute query for this chunk
    result_chunk = duckdb.query(query).df()

    # Drop any rows where player_id is null then convert player_id column from float to int. 
    result_chunk = result_chunk.dropna(subset=['player_id'])
    result_chunk["player_id"] = result_chunk["player_id"].astype('Int64')
    
    # Write to CSV (first chunk with header, subsequent chunks without)
    if first_chunk:
        result_chunk.to_csv(output_file, index=False, mode='w')
        first_chunk = False
    else:
        result_chunk.to_csv(output_file, index=False, mode='a', header=False)
    
    # Update progress
    processed_rows += len(result_chunk)
    print(f"Processed chunk {chunk_num+1} - Total rows: {processed_rows}")
    
    # Clean up to free memory
    duckdb.unregister('player_statistics_chunk')
    del chunk
    del result_chunk

print(f"All chunks processed. Total rows: {processed_rows}")
print(f"Results saved to {output_file}")

# Upload to GCS
print("Uploading file to Google Cloud Storage...")

# Path to your credentials file
credentials_path = 'cis-5450-final-project-485661e2f371.json'

try:
    # Set up the client with your credentials
    storage_client = storage.Client.from_service_account_json(credentials_path)
    
    # Specify your bucket name
    bucket_name = 'nba_award_predictor'
    bucket = storage_client.bucket(bucket_name)
    
    # Define blob (file in GCS) and upload from the local file
    blob = bucket.blob('nba_data/player-statistics.csv')
    blob.cache_control = "max-age=0"
    blob.upload_from_filename(output_file)
    
    print(f"File successfully uploaded to gs://{bucket_name}/nba_data/player-statistics.csv")
    
    # Get file size for confirmation
    file_size_mb = os.path.getsize(output_file) / (1024 * 1024)
    print(f"Uploaded file size: {file_size_mb:.2f} MB")
    
except Exception as e:
    print(f"Error uploading to GCS: {e}")
    print("You may need to update the credentials file.")

# URL of the final CSV file
filename = 'player-statistics.csv'
url = f'https://storage.googleapis.com/nba_award_predictor/nba_data/{filename}'
wget.download(url)
# Read in the player-statistics csv (comment this out if deploying on remote server, to save RAM)
#player_statistics_df = pd.read_csv(filename)

os.remove("player-statistics.csv")
os.remove("playerstatistics.csv")
os.remove("name_mappings.csv")
os.remove("nba_player_lookup.csv")
os.remove("player-statistics (1).csv")

print("Process complete!")