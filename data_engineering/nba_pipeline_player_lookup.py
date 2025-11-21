"""
Simplified NBA Player Lookup Table GCS Export Script

This standalone script exports NBA player lookup tables to Google Cloud Storage.
It's a simplified version that can be run independently from the main pipeline.
"""

import os
import sys
import logging
import pandas as pd
import tempfile
from datetime import datetime
from google.cloud import storage
from nba_api.stats.static import players
import duckdb

# GCS Configuration - Using the same as in the provided script
GCS_BUCKET_NAME = "nba_award_predictor"
GCS_PREFIX = "nba_data/"
CREDENTIALS_FILE = "cis-5450-final-project-485661e2f371.json"

# Setup logging to console for this simplified script
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

def get_player_data(active_only=False):
    """Get player data from the NBA API"""
    player_type = "active" if active_only else "all"
    print(f"Fetching {player_type} NBA players...")
    
    # Get the player data
    if active_only:
        player_data = players.get_active_players()
    else:
        player_data = players.get_players()
    
    # Convert to DataFrame
    df_raw = pd.DataFrame(player_data)
    
    # Rename columns for clarity
    df_raw = df_raw.rename(columns={'id': 'player_id', 'full_name': 'player_name'})

    query = """
    
    WITH SpecificNames AS (
    SELECT 2399 AS player_id, 'Mike Dunleavy Sr.' AS player_full_name
    UNION ALL
    SELECT 76616, 'Mike Dunleavy Jr.'
    UNION ALL
    SELECT 121, 'Patrick Ewing Sr.'
    UNION ALL
    SELECT 201607, 'Patrick Ewing Jr.'
    UNION ALL
    SELECT 779, 'Glen Rice Sr.'
    UNION ALL
    SELECT 203318, 'Glen Rice Jr.'
    UNION ALL
    SELECT 77144, 'Eddie L. Johnson'
    UNION ALL
    SELECT 698, 'Eddie A. Johnson'
    UNION ALL
    SELECT 77156, 'Larry O. Johnson'
    UNION ALL
    SELECT 913, 'Larry D. Johnson'
    UNION ALL
    SELECT 200848, 'Steven A. Smith'
    UNION ALL
    SELECT 120, 'Steven D. Smith'
    UNION ALL
    SELECT 2229, 'Mike L. James'
    UNION ALL
    SELECT 1628455, 'Mike P. James'

    )

    SELECT df_raw.player_id
    ,CASE
        WHEN df_raw.player_id = SpecificNames.player_id THEN SpecificNames.player_full_name
        ELSE df_raw.player_name
    END AS player_name
    ,df_raw.first_name
    ,df_raw.last_name
    ,df_raw.is_active
     
    FROM df_raw
    LEFT JOIN SpecificNames
    ON df_raw.player_id = SpecificNames.player_id
    
    """
    
    df = duckdb.query(query).df()

    return df

def export_to_gcs(df, filename):
    """Export DataFrame to Google Cloud Storage"""
    # Set credentials
    if os.path.exists(CREDENTIALS_FILE):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_FILE
    
    # Create a temporary file
    with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as temp_file:
        temp_path = temp_file.name
        df.to_csv(temp_path, index=False)
    
    # Upload to GCS
    print(f"Uploading to gs://{GCS_BUCKET_NAME}/{GCS_PREFIX}{filename}...")
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(f"{GCS_PREFIX}{filename}")
    blob.upload_from_filename(temp_path)
    
    # Clean up
    os.remove(temp_path)
    print(f"✓ Uploaded {len(df)} records")

def main():
    """Main function"""
    print("\n=== NBA Player Lookup Table GCS Export ===\n")
    
    try:
        # Get all players
        all_players = get_player_data(active_only=False)
        print(f"Found {len(all_players)} total NBA players")
        
        # Get active players
        active_players = get_player_data(active_only=True)
        print(f"Found {len(active_players)} active NBA players")
        
        # Create simple lookup (just ID and name)
        simple_lookup = all_players[['player_id', 'player_name']].copy()
        
        # Export to GCS
        export_to_gcs(all_players, "nba_player_lookup.csv")
        
        print("\n✓ Export completed successfully!\n")
        print(f"Files exported to gs://{GCS_BUCKET_NAME}/{GCS_PREFIX}:")
        print(f"  - nba_player_lookup.csv ({len(all_players)} records)")
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
