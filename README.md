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

## Database Access

### Quick Start for Notebooks

Copy and paste this code at the top of your Jupyter notebooks to access the NBA database:

```python
import pandas as pd
from sqlalchemy import create_engine

def get_nba_db():
    """Creates a connection to the NBA PostgreSQL database."""
    host = "[SEE DOCUMENTATION]"
    port = "[SEE DOCUMENTATION]"
    database = "[SEE DOCUMENTATION]"
    user = "[SEE DOCUMENTATION]"
    password = "[SEE DOCUMENTATION]"
    
    connection_string = f"[SEE DOCUMENTATION]"
    return create_engine(connection_string)

def query(sql):
    """Executes SQL query and returns results as a pandas DataFrame."""
    engine = get_nba_db()
    return pd.read_sql(sql, engine)

def list_tables():
    """Lists all tables available in the database."""
    tables = query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    return tables['table_name'].tolist()

# Example: View available tables
print(list_tables())

# Example: Preview the players table
players = query("SELECT * FROM players LIMIT 5")
display(players)
```

### Available Tables

The database contains the following key tables:
- `games` - Game information (date, teams, scores)
- `playerstatistics` - Player performance statistics per game
- `players` - Player information
- `teamstatistics` - Team statistics per game
- `teamhistories` - Historical team information
- `leagueschedule24_25` - NBA schedule for 2024-25 season
- `leagueschedule25_26` - NBA schedule for 2025-26 season

### Example Queries

**Basic player statistics:**
```python
# Get top scorers
top_scorers = query("""
    SELECT firstname, lastname, playerteamname, 
           AVG(points) as ppg, COUNT(*) as games
    FROM playerstatistics
    GROUP BY firstname, lastname, playerteamname
    HAVING COUNT(*) > 20
    ORDER BY ppg DESC
    LIMIT 10
""")

# Player comparison
lebron_vs_durant = query("""
    SELECT 
        CASE 
            WHEN firstname = 'LeBron' AND lastname = 'James' THEN 'LeBron James'
            WHEN firstname = 'Kevin' AND lastname = 'Durant' THEN 'Kevin Durant'
        END AS player,
        COUNT(*) as games,
        ROUND(AVG(points), 1) as ppg,
        ROUND(AVG(rebounds), 1) as rpg,
        ROUND(AVG(assists), 1) as apg
    FROM playerstatistics
    WHERE (firstname = 'LeBron' AND lastname = 'James') OR
          (firstname = 'Kevin' AND lastname = 'Durant')
    GROUP BY player
""")
```

**Team analysis:**
```python
# Team performance
team_stats = query("""
    SELECT 
        playerteamname as team,
        COUNT(*) as games,
        SUM(CASE WHEN win = 1 THEN 1 ELSE 0 END) as wins,
        ROUND(AVG(points), 1) as ppg
    FROM playerstatistics
    GROUP BY playerteamname
    ORDER BY wins DESC
""")
```

## Tech Stack
- **Database**: PostgreSQL (hosted on Digital Ocean)
- **Data Processing**: PySpark, Pandas
- **Machine Learning**: scikit-learn, Spark ML
- **Visualization**: Plotly, Seaborn, Matplotlib
- **Development**: Jupyter Notebooks, Python 3.9+

## Project Structure
```
├── data/               # Raw and processed data (gitignored)
├── notebooks/          # Jupyter notebooks for analysis
│   ├── 01-data-exploration.ipynb
│   ├── 02-feature-engineering.ipynb
│   ├── 03-modeling.ipynb
│   └── 04-evaluation.ipynb
├── src/                # Source code
│   ├── data_engineering/
│   │   └── pipeline.py
│   ├── features/
│   │   └── feature_builder.py
│   ├── models/
│   │   └── model_trainer.py
│   └── visualization/
│       └── plots.py
├── models/             # Saved models (gitignored)
├── outputs/            # Generated plots and results
├── docs/               # Project documentation
├── config/             # Configuration files
└── README.md           # Project overview
```

## Setup Instructions for Local Development

1. Clone the repository
   ```bash
   git clone https://github.com/dejiandrew/nba-award-predictor.git
   cd nba-award-predictor
   ```

2. Create and activate a virtual environment
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install dependencies
   ```bash
   pip install -r requirements.txt
   ```

4. Use the database connection code in your notebooks (see "Quick Start for Notebooks" above)

5. Start Jupyter Notebook
   ```bash
   jupyter notebook
   ```

## Project Timeline

- **Project Proposal**: Due November 3, 2025
- **Intermediate Check-in**: Due November 16, 2025
- **Final Submission**: Due December 6, 2025

## Data Source

We're using the historical NBA dataset from Kaggle, supplemented with award data:
- Main dataset: [NBA Historical Data and Player Box Scores](https://www.kaggle.com/datasets/eoinamoore/historical-nba-data-and-player-box-scores)
- Pipeline code is maintained in the `src/data_engineering` directory

## Contribution Guidelines

1. Create a new branch for each feature/task
2. Write clear commit messages
3. Update requirements.txt if you add dependencies
4. Document your code and add comments where necessary
5. Add appropriate logging for any data processing steps

## Contact

If you have any questions or issues with database access, contact Deji Andrew.