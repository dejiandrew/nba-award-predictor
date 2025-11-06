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

All CSV files are stored in a Google Cloud Storage (GCS) bucket. You can access any file by replacing `playerstatistics.csv` with your desired CSV filename in the download URL.

Copy and paste these cells in your Jupyter notebook to access any NBA dataset file:

```python
# Cell 1: Install wget and pandas if needed
!pip install wget
!pip install pandas
```

```python
# Cell 2: Download the CSV file (replace playerstatistics.csv with any filename from the list below)
import wget
wget.download('https://storage.googleapis.com/nba_award_predictor/nba_data/playerstatistics.csv')
```

```python
# Cell 3: Load the CSV into pandas
import pandas as pd
pd.read_csv('playerstatistics.csv')
```

### Available CSV Files

All of the following files are available in the GCS bucket and can be accessed by replacing `playerstatistics.csv` with the desired filename in the URL pattern: `https://storage.googleapis.com/nba_award_predictor/nba_data/FILENAME.csv`

The dataset contains the following key files:
- `common-player-info.csv` - Basic player information
- `player-of-the-week.csv` - Player of the Week award data
- `player-statistics.csv` - Player performance statistics per game
