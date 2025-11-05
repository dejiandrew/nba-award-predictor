# NBA Award Predictor

CIS 5450: Big Data Analytics - Fall 2025  
Final Project

## Team Members
- Deji Andrew: Data Engineering & Infrastructure
- Josh/Abdullah/Hao: Feature Engineering & EDA
- Josh/Abdullah/Hao: Machine Learning Modeling
- Josh/Abdullah/Hao: Visualization & Final Deliverable

## Project Overview
Our project uses historical NBA performance data to predict various NBA awards, including:
- Most Valuable Player (MVP)
- Player of the Week/Month
- All-Star selections
- Defensive Player of the Year
- Most Improved Player
- Rookie of the Year

We'll apply machine learning techniques to identify the statistical profiles and patterns that lead to award recognition.

## Data Access

### Quick Start for Notebooks

Copy and paste these cells in your Jupyter notebook to access the NBA dataset files:

```python
# Cell 1: Install wget and pandas if needed
!pip install wget
!pip install pandas
```

```python
# Cell 2: Download the CSV file
import wget
wget.download('https://storage.googleapis.com/nba_award_predictor/nba_data/playerstatistics.csv')
```

```python
# Cell 3: Load the CSV into pandas
import pandas as pd
pd.read_csv('playerstatistics.csv')
```

### Available CSV Files

The dataset contains the following key files:
- `common_player_info.csv` - Basic player information
- `draft_combine_stats.csv` - Pre-draft measurements and stats
- `draft_history.csv` - Historical draft information
- `game.csv` - Individual game data
- `game_info.csv` - Game metadata
- `game_summary.csv` - Summarized game results
- `games.csv` - Complete game listings
- `inactive_players.csv` - Players not currently active
- `leagueschedule24_25.csv` - NBA schedule for 2024-25 season
- `leagueschedule25_26.csv` - NBA schedule for 2025-26 season
- `line_score.csv` - Per-quarter scoring
- `nba_player_lookup.csv` - Player ID reference table
- `officials.csv` - Game officials information
- `other_stats.csv` - Miscellaneous statistics
- `player.csv` - Player details
- `playeroftheweek.csv` - Player of the Week award data
- `players.csv` - Comprehensive player information
- `playerstatistics.csv` - Player performance statistics per game
- `team.csv` - Team information
- `team_details.csv` - Detailed team data
- `team_history.csv` - Historical team information
- `team_info_common.csv` - Common team metrics
- `teamhistories.csv` - Expanded team history
- `teamstatistics.csv` - Team performance statistics per game