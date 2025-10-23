# NBA Awards Prediction

CIS 5450: Big Data Analytics - Fall 2025
Final Project

## Team Members
- Person 1: Data Engineering & Infrastructure
- Person 2: Feature Engineering & EDA
- Person 3: Machine Learning Modeling
- Person 4: Visualization & Final Deliverable

## Project Overview
Predicting NBA awards including Player of the Week, Player of the Month, MVP, All-Star selections, and more using machine learning.

## Tech Stack
- PostgreSQL
- PySpark
- scikit-learn
- Spark ML
- Pandas
- Plotly/Seaborn

## Setup Instructions
1. Clone the repository
2. Install dependencies: `pip install -r requirements.txt`
3. Configure database connection in `config/database.yml`
4. Run ETL pipeline: `python src/data_engineering/etl_pipeline.py`

## Project Structure
```
├── data/               # Raw and processed data (gitignored)
├── notebooks/          # Jupyter notebooks for analysis
├── src/                # Source code
│   ├── data_engineering/
│   ├── features/
│   ├── models/
│   └── visualization/
├── models/             # Saved models (gitignored)
├── outputs/            # Generated plots and results
├── docs/               # Project documentation
└── config/             # Configuration files
```

## Data Source
Kaggle NBA Database: https://www.kaggle.com/datasets/wyattowalsh/basketball

## Deliverables
- Project Proposal: November 3, 2025
- Intermediate Check-in: November 16, 2025
- Final Submission: December 6, 2025
