# NBA Award Predictor

CIS 5450: Big Data Analytics - Fall 2025  
Final Project

## Team Members
- Deji Andrew: Data Engineering & Infrastructure
- Abdullah Amer: Feature Engineering & EDA
- Josh Lim: Machine Learning Modeling
- Hao Hua: Visualization & Final Deliverable

## Project Overview
This project uses historical NBA performance data to predict Player of the Week awards. We apply machine learning techniques to identify the statistical profiles and patterns that lead to award recognition, focusing on:
- Player performance metrics (points, assists, rebounds, etc.)
- Team performance and standings
- Opponent strength metrics
- Historical performance trends and breakout detection

## Data Pipeline

### Automated ETL Process
Our data pipeline is orchestrated through `run_nba_pipeline.sh`, which executes the following scripts in sequence:

1. **`nba_pipeline_1.py`** - Downloads NBA dataset from Kaggle and exports core tables to Google Cloud Storage
2. **`nba_pipeline_2.py`** - Processes additional historical NBA data and box scores
3. **`nba_pipeline_player_lookup.py`** - Creates player lookup tables from the NBA API
4. **`common_player_info_script.py`** - Processes and standardizes player information
5. **`player_of_the_week_script.py`** - Cleans and maps Player of the Week award data
6. **`player_statistics_script.py`** - Processes large player statistics files in chunks to manage memory
7. **`play_by_play.py`** - Handles play-by-play data upload to GCS

All processed data is stored in Google Cloud Storage bucket: `nba_award_predictor`

### Key Data Sources
- **Kaggle Datasets**: 
  - `wyattowalsh/basketball` - Core NBA statistics
  - `eoinamoore/historical-nba-data-and-player-box-scores` - Historical data
- **NBA API**: Player lookup and common player information
- **Custom Mappings**: Name standardization and player ID mapping

## Feature Engineering

The `feature_engineering/overall_features.ipynb` notebook creates comprehensive features including:

### Team Performance Features
- Games played, wins, losses (season and weekly)
- Home/away records and win streaks
- Opponent win rate and strength metrics
- Wins vs teams with All-NBA players
- Team aggregate statistics (points, assists, blocks, steals)

### Player Performance Features
- Per-game statistics (points, assists, rebounds, blocks, steals, etc.)
- Shooting percentages (FG%, 3P%, FT%)
- Plus/minus metrics
- Historical season averages and standard deviations

### Advanced Features
- **Breakout Score**: Z-score based metric combining points (50%), assists (30%), and plus/minus (20%)
- Rolling weekly averages for performance tracking
- Award history flags (All-Star, MVP, All-NBA teams)

### Target Variable
- `won_player_of_the_week`: Binary indicator (1 = won Player of the Week award, 0 = did not win)

The final feature set contains 109 columns and ~930,000 player-game observations.

## Data Access

All processed data is available in Google Cloud Storage:
```
gs://nba_award_predictor/nba_data/
```

Key files:
- `features-overall.csv` - Complete feature set for modeling
- `player-statistics.csv` - Raw player game statistics
- `player-of-the-week.csv` - Player of the Week awards
- `nba_player_lookup.csv` - Player ID mappings
- `games.csv` - Game-level data

## Project Structure
```
nba-award-predictor/
├── data_engineering/          # ETL scripts and pipeline orchestration
│   ├── run_nba_pipeline.sh   # Main pipeline execution script
│   ├── nba_pipeline_1.py     # Kaggle data download and GCS export
│   ├── nba_pipeline_2.py     # Historical data processing
│   └── ...                    # Additional processing scripts
├── feature_engineering/       # Feature creation notebooks
│   └── overall_features.ipynb # Main feature engineering notebook
├── modeling/                  # Machine learning models (TBD)
└── notebooks/                 # Exploratory analysis (TBD)
```

## Requirements
See `requirements.txt` for Python dependencies. Key packages:
- pandas, numpy, duckdb
- google-cloud-storage
- kaggle, nba_api
