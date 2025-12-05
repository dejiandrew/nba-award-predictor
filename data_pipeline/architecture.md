**NBA Award Predictor: Data Pipeline**

# Pipeline Architecture

Complete data flow from source to machine-learning-ready features:

# Key Components

## 1\. Trigger Mechanism

**Cloud Run Job - Daily at 5am EST**

The pipeline executes automatically every day at 5:00 AM EST via a Google Cloud Run Job. This ensures the data remains current and the feature sets reflect the latest NBA games and statistics.

## 2\. Data Sources

**Kaggle - wyattowalsh/basketball:** Comprehensive SQLite database containing historical NBA data including games, teams, and play-by-play information

**Kaggle - eoinamoore/historical-nba:** Detailed player box scores and historical statistics

**RealGM - NBA Awards:** Full list of Player of the Week winners & All-NBA selections

**Wikipedia - List of NBA All Stars:** Full list of all NBA All-Star teams

**NBA API**: baseline data source for player_id values

## 3\. Processing Stages

### Stage 1: Data Extraction

Both Kaggle datasets are downloaded and their CSV files are extracted. The SQLite database from the first dataset is converted to CSV format for consistency. All raw files are uploaded to Google Cloud Storage for persistent storage and downstream processing.

### Stage 2: Data Cleaning & Standardization

This stage addresses the fundamental challenge of integrating data from diverse sources: Wikipedia (All-Star data), RealGM.com (Player of the Week, All-NBA teams), and Kaggle datasets (NBA API statistics). Each source uses different naming conventions, requiring a sophisticated player ID standardization system. See the "Player ID Standardization System" section below for complete technical details on the attach_player_ids() helper function that solves this challenge.

**Create Player Lookup:** Establishes the authoritative player identity system using the NBA API. Handles duplicate names by explicitly disambiguating players like Mike Dunleavy Sr./Jr., Patrick Ewing Sr./Jr., Eddie Johnson variants, etc. Produces nba_player_lookup.csv as the single source of truth for player IDs.

**Clean Player Info:** Normalizes international player names by removing accent marks (Đ→D, Ł→L, Ø→O, etc.) and applies manual name mappings to handle edge cases. This ensures consistent matching across heterogeneous data sources. Produces common-player-info.csv.

**Standardize Awards:** Processes Player of the Week, All-Star, MVP, and All-NBA team data from Wikipedia and RealGM.com. Applies the attach_player_ids() helper function to assign consistent NBA API player IDs to these external sources. This enables seamless joining with statistical data in later stages. Produces player-of-the-week.csv and related award files.

**Process Statistics:** Standardizes data types, handles null values, and produces player-statistics.csv.

### Stage 3: Feature Engineering

**Feature Engineering Components:**

**1\. Team Performance Metrics:** Calculates season-to-date win/loss records, home/away splits, win streaks, and weekly performance. All metrics use "prior" calculations (excluding current game) to prevent data leakage.

**2\. Rolling Averages:** Computes expanding statistics for points, assists, and plus-minus for each player-season. Uses shift() to ensure only historical data informs current predictions.

**3\. Breakout Scores:** Identifies exceptional performance by calculating z-scores: (current_performance - historical_mean) / historical_std. Combines multiple metrics with weights: 0.5×points + 0.3×assists + 0.2×plus-minus. High scores (>2.0) indicate players performing significantly above their averages.

**4\. Opponent Strength:** Measures quality of competition by tracking opponent win rates, wins against teams with >50% records, and wins against rosters containing All-NBA players. Recognizes that beating strong opponents matters more than raw win totals.

**5\. Weekly Aggregations:** Aggregates individual game statistics to the weekly level (Monday-Sunday) to match the Player of the Week award timeframe. Includes sum statistics (points, assists, blocks, steals), count statistics (games played, wins), and average statistics (opponent quality metrics).

## 4\. Pipeline Output

**Dataset for ML Modeling: features-overall-weekly.csv**

The pipeline produces a weekly-aggregated dataset (features-overall-weekly.csv) where each row represents one player's performance for one week. This dataset includes:

- Player and team identifiers
- Temporal features (season, week, date)
- Performance statistics (points, assists, rebounds, etc.)
- Team performance metrics (record, win streaks)
- Opponent quality indicators
- Historical comparison metrics (z-scores, breakout scores)
- Target variable: won_player_of_the_week (binary)

# Technical Spotlight

## Player ID Standardization System

A critical challenge in this project was integrating data from multiple sources that used different player naming conventions. Award data came from Wikipedia (All-Star selections) and RealGM.com (Player of the Week, All-NBA teams). Statistical data came from Kaggle datasets sourced from the official NBA API. Each source had its own approach to player names, creating a complex data integration problem.

**Core Challenge: Name Variations and Ambiguity**

Player names appeared differently across data sources for several reasons:

- Formatting differences: "J.R. Smith" vs. "JR Smith", "C.J. McCollum" vs. "CJ McCollum"
- Nickname variations: "Penny Hardaway" vs. "Anfernee Hardaway", "Tiny Archibald" vs. "Nate Archibald"
- Suffix inconsistencies: "Jimmy Butler" vs. "Jimmy Butler III", "Kenyon Martin Jr." vs. "KJ Martin"
- International name ordering: "Jianlian Yi" vs. "Yi Jianlian", "Yue Sun" vs. "Sun Yue"
- Unusual nicknames: "Fat Lever" for Lafayette Lever, "World B. Free" for World Free
- Identical names requiring disambiguation: Multiple players named "Mike James", "Eddie Johnson", "Steve Smith", etc.

**Solution: name_mappings.csv Lookup Table**

To solve these inconsistencies, we created a manually curated lookup table (name_mappings.csv) through extensive research on Basketball-Reference.com. This table contains three critical columns:

**in_table_name:** How the player name appears in raw data sources (Kaggle, Wikipedia, RealGM)

**nba_lookup_name:** The standardized name from the NBA API (nba_player_lookup.csv)

**player_id:** The unique NBA API player ID that serves as the definitive identifier

**Example Mappings:**

The table resolves complex cases that automated methods would miss:

- "Jimmy Butler" → "Jimmy Butler III" (player_id: 202710)
- "Penny Hardaway" → "Anfernee Hardaway" (player_id: 358)
- "Fat Lever" → "Lafayette Lever" (player_id: 77376)
- "Jianlian Yi" → "Yi Jianlian" (player_id: 201146)
- "Mike Dunleavy, Sr." → "Mike Dunleavy Sr." (player_id: 2399)
- "Eddie Johnson, Jr." → "Eddie L. Johnson" (player_id: 77144) - distinguishes from Eddie A. Johnson

Each entry in this lookup table was researched and added manually by cross-referencing Basketball-Reference.com to verify player identities and resolve ambiguities.

**The Implementation: attach_player_ids() Helper Function**

With the name_mappings.csv lookup table established, a reusable helper function was developed that takes any DataFrame containing player names and adds standardized player_id values from the NBA API.

**Function Workflow:**

**1\. Step 1 - Accent Removal:** Applies the remove_accents() function to normalize all player names to ASCII, handling international players with special characters (Đ, Ł, Ø, Æ, ß, etc.). This ensures consistent string matching regardless of character encoding.

**2\. Step 2 - Name Mapping Join:** Performs two LEFT JOINs with name_mappings.csv:  
• First join matches on in_table_name (catches names as they appear in raw source data)  
• Second join matches on nba_lookup_name (catches names already in NBA API format)  
• Uses CASE logic to prefer the standardized nba_lookup_name if found, otherwise keeps original name  
• This dual-join approach maximizes match rate while handling edge cases

**3\. Step 3 - Player ID Assignment:** LEFT JOINs the result with nba_player_lookup.csv (the authoritative NBA API-based lookup) to assign player_id values. Uses CASE logic with fallback hierarchy to handle scenarios where multiple ID columns exist from the joins, ensuring every player gets the correct unique identifier.

**4\. Step 4 - Cleanup and Standardization:** Removes redundant columns created during joins, renames final columns to standardized names (player_name, player_id), reorders columns to place player identifiers first for readability, and deletes temporary lookup files to conserve disk space.

**Impact and Reusability:**

This helper function was applied to all external data sources (Wikipedia All-Star data, RealGM Player of the Week, RealGM All-NBA teams) to establish a consistent player_id primary key across all datasets. The function's design makes it reusable for any future data sources that need player ID standardization - simply pass in a DataFrame and specify the column containing player names.

## Storage Architecture

**Why CSV in Google Cloud Storage?**

The pipeline stores all processed data as CSV files in GCS for several strategic reasons:

- Universal compatibility with Pandas, DuckDB, and other data tools
- Easy access from any environment (local, Colab, cloud instances) via simple downloads
- No schema evolution complications - columns can be added/removed easily
- Persistent storage survives pipeline execution environment termination

# Conclusion

This automated data pipeline represents a comprehensive solution for maintaining high-quality, ML-ready NBA data to predict Player of the Week awards.
