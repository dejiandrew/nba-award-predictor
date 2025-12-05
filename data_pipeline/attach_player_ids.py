import pandas as pd
import duckdb
import wget
import unicodedata
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


def attach_player_ids(input_df, column: str):
    '''
    This function takes in a DataFrame containing NBA player names but not player_id column.
    It outputs a new DataFrame with a player_id column.
    input_df = A pandas dataframe
    column = string representation of the column name in input_df containing the NBA player names
    '''
    column = str(column)

    ##### Step 1: Remove all accents from the player's names #####
    
    input_df[column] = input_df[column].apply(remove_accents)
    #input_df.column = input_df.column.apply(remove_accents)

    ##### Step 2: Left join the df to the nba_mappings lookup table #####
    # Bring in name mapping table for names to help match all names to the format seen in the NBA API
    filename = 'name_mappings.csv'
    url = f'https://storage.googleapis.com/nba_award_predictor/nba_data/{filename}'
    wget.download(url)
    # Read in the name_mappings csv
    name_mapping_df = pd.read_csv('name_mappings.csv')

    #Perform left join
    query = f"""
    WITH CTE AS (
    SELECT * FROM input_df
    LEFT JOIN name_mapping_df a
    ON input_df.{column} = a.in_table_name
    LEFT JOIN name_mapping_df b
    ON input_df.{column} = b.nba_lookup_name
    )
    SELECT *
    ,CASE WHEN nba_lookup_name IS NOT NULL THEN nba_lookup_name ELSE {column} END AS player_name_to_use
    FROM CTE
    """
    input_df = duckdb.query(query).df()

    ##### Step 3: Left join the result from Step 2 to nba_player_lookup table #####
    # Bring in nba player lookup table to map the cleaned names to player IDs. Same player IDs from the NBA API.
    filename = 'nba_player_lookup.csv'
    url = f'https://storage.googleapis.com/nba_award_predictor/nba_data/{filename}'
    wget.download(url)
    # Read in the nba_player_lookup csv
    nba_player_lookup_df = pd.read_csv(filename)
    # Clean each player's full name
    nba_player_lookup_df["player_name"] = nba_player_lookup_df["player_name"].apply(remove_accents)
    
    #Perform left join
    query = f"""
    WITH CTE AS (
    SELECT
    nba_player_lookup_df.player_id
    ,input_df.*
    FROM input_df
    LEFT JOIN nba_player_lookup_df
    ON input_df.{column} = nba_player_lookup_df.player_name
    )
    SELECT *
    ,CASE
        WHEN player_id IS NOT NULL THEN player_id
        WHEN player_id IS NULL THEN player_id_1
        ELSE player_id_3
    END AS player_id_to_use
    FROM CTE
    
    """
    output_df = duckdb.query(query).df()
    #Drop unneeded columns and rearrange
    output_df = output_df.drop(columns=[f'{column}','player_id','in_table_name','nba_lookup_name','player_id_1','in_table_name_1','nba_lookup_name_2','player_id_3'])
    output_df = output_df.rename(columns={'player_name_to_use':'player_name', 'player_id_to_use':'player_id'})
    cols = output_df.columns.tolist()
    cols.pop(cols.index('player_name'))
    cols.pop(cols.index('player_id'))
    cols.insert(0, 'player_id')
    cols.insert(1, 'player_name')
    output_df = output_df[cols]

    #Delete lookup tables
    os.remove("name_mappings.csv")
    os.remove("nba_player_lookup.csv")

    return output_df
