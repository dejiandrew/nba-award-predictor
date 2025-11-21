import pandas as pd
import unicodedata
import re
import duckdb
from io import StringIO
from google.cloud import storage
import wget
import os

def remove_accents(text):
    """
    Remove accent marks from input text while preserving the base characters.
    Also handles special characters like Đ/đ.
    
    Example:
    "Nikola Đurišić" -> "Nikola Durisic"
    """
    # First, handle special characters that need specific replacements
    special_chars = {
        'Đ': 'D', 'đ': 'd',  # Serbian/Croatian D with stroke
        'Ł': 'L', 'ł': 'l',  # Polish L with stroke
        'Ø': 'O', 'ø': 'o',  # Danish/Norwegian O with stroke
        'Ŧ': 'T', 'ŧ': 't',  # Sami T with stroke
        'Æ': 'AE', 'æ': 'ae',  # Æ/æ ligature
        'Œ': 'OE', 'œ': 'oe',  # Œ/œ ligature
        'ß': 'ss',  # German eszett
    }
    
    for char, replacement in special_chars.items():
        text = text.replace(char, replacement)
    
    # Normalize the text to decompose characters into base character and accent mark
    normalized_text = unicodedata.normalize('NFKD', text)
    
    # Filter out the non-spacing marks (accent marks)
    result = ''.join(c for c in normalized_text if not unicodedata.category(c).startswith('Mn'))
    
    return result

# URL of the CSV file
filename = 'playeroftheweek.csv'
url = f'https://storage.googleapis.com/nba_award_predictor/nba_data/{filename}'
wget.download(url)
# Read in the playeroftheweek csv
playeroftheweek_df = pd.read_csv(filename)

# Clean each player's full name
playeroftheweek_df["player"] = playeroftheweek_df["player"].apply(remove_accents)

# Bring in name mapping table for names to help match all names to the format seen in the NBA API
filename = 'name_mappings.csv?authuser=4'
url = f'https://storage.googleapis.com/nba_award_predictor/nba_data/{filename}'
wget.download(url)
# Read in the name_mappings csv
name_mapping_df = pd.read_csv('name_mappings.csv')

# Standardize the names in playeroftheweek_df
query = """
WITH CTE AS (
SELECT * FROM playeroftheweek_df
LEFT JOIN name_mapping_df a
ON playeroftheweek_df.player = a.in_table_name
LEFT JOIN name_mapping_df b
ON playeroftheweek_df.player = b.nba_lookup_name
)

SELECT season
,CASE WHEN nba_lookup_name IS NOT NULL THEN nba_lookup_name ELSE player END AS player
,conference
,date
,team
,pos
,height
,weight
,age
,"Pre-Draft Team"
,"Draft Yr"
,yos
FROM CTE
"""
playeroftheweek_df = duckdb.query(query).df()

# Bring in nba player lookup table to map the cleaned names to player IDs. Same player IDs from the NBA API.
filename = 'nba_player_lookup.csv'
url = f'https://storage.googleapis.com/nba_award_predictor/nba_data/{filename}'
wget.download(url)
# Read in the nba_player_lookup csv
nba_player_lookup_df = pd.read_csv(filename)

# Clean each player's full name
nba_player_lookup_df["player_name"] = nba_player_lookup_df["player_name"].apply(remove_accents)

query = """
WITH CTE AS (
SELECT * FROM playeroftheweek_df
LEFT JOIN name_mapping_df a
ON playeroftheweek_df.player = a.in_table_name
LEFT JOIN name_mapping_df b
ON playeroftheweek_df.player = b.nba_lookup_name
)
, CTE2 AS (
SELECT
season
,CASE WHEN nba_lookup_name IS NOT NULL THEN nba_lookup_name ELSE player END AS player
,conference
,date
,team
,pos
,height
,weight
,age
,"Pre-Draft Team"
,"Draft Yr"
,yos
FROM CTE
)

SELECT nba_player_lookup_df.player_id, CTE2.*
FROM CTE2
LEFT JOIN nba_player_lookup_df
ON CTE2.player = nba_player_lookup_df.player_name
ORDER BY DATE DESC, conference
"""

player_of_the_week_df = duckdb.query(query).df()

player_of_the_week_df.to_csv('player-of-the-week.csv',index=False)

# Path to your credentials file
credentials_path = 'cis-5450-final-project-485661e2f371.json'

# Set up the client with your credentials
storage_client = storage.Client.from_service_account_json(credentials_path)

# Specify your bucket name
bucket_name = 'nba_award_predictor'
bucket = storage_client.bucket(bucket_name)

# Define blob (file in GCS) and upload from the local file
blob = bucket.blob('nba_data/player-of-the-week.csv')
blob.cache_control = "max-age=0"
blob.upload_from_filename('player-of-the-week.csv')

os.remove("name_mappings.csv")
os.remove("nba_player_lookup.csv")
os.remove("player-of-the-week.csv")
os.remove("playeroftheweek.csv")

print(f"File uploaded to gs://{bucket_name}/nba_data/player-of-the-week.csv")
