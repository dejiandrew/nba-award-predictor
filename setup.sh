#!/bin/bash

# setup_project.sh - Initial setup script for NBA Awards Prediction project

set -e  # Exit on any error

echo "🏀 Setting up NBA Awards Prediction project structure..."

# Create the entire folder structure
echo "📁 Creating directory structure..."
mkdir -p data/{raw,processed}
mkdir -p notebooks
mkdir -p src/{data_engineering,features,models,visualization}
mkdir -p models outputs docs config

# Create placeholder files so Git tracks the folders
echo "📝 Creating placeholder files..."
touch data/README.md
touch notebooks/.gitkeep
touch src/data_engineering/.gitkeep
touch src/features/.gitkeep
touch src/models/.gitkeep
touch src/visualization/.gitkeep

# Create basic files
echo "📄 Creating basic project files..."
touch README.md requirements.txt .gitignore

# Create a basic .gitignore
echo "🚫 Populating .gitignore..."
cat > .gitignore << 'EOF'
# Data files (too large for git)
data/raw/*
data/processed/*
!data/raw/.gitkeep
!data/processed/.gitkeep
*.csv
*.sqlite
*.db

# Model files
models/*
!models/.gitkeep
*.pkl
*.h5
*.pt

# Environment
.env
venv/
env/
__pycache__/
*.pyc

# Jupyter
.ipynb_checkpoints/
*.ipynb_checkpoints

# Config secrets
config/secrets.yml
config/database.yml

# OS files
.DS_Store
Thumbs.db

# IDE
.vscode/
.idea/
EOF

# Create a basic README
echo "📖 Creating README..."
cat > README.md << 'EOF'
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
EOF

# Create a basic requirements.txt
echo "📦 Creating requirements.txt..."
cat > requirements.txt << 'EOF'
# Core data processing
pandas==2.0.3
numpy==1.24.3

# Big Data
pyspark==3.4.1

# Machine Learning
scikit-learn==1.3.0
xgboost==1.7.6

# Database
psycopg2-binary==2.9.6
sqlalchemy==2.0.19

# Visualization
matplotlib==3.7.2
seaborn==0.12.2
plotly==5.15.0

# Jupyter
jupyter==1.0.0
ipykernel==6.25.0

# Model tracking (optional)
mlflow==2.5.0

# Utilities
python-dotenv==1.0.0
pyyaml==6.0.1
EOF

# Create data README
echo "📊 Creating data README..."
cat > data/README.md << 'EOF'
# Data Directory

## Raw Data
Place raw Kaggle NBA dataset files in `data/raw/`

This directory is gitignored to avoid committing large files.

## Processed Data
Cleaned and processed data will be stored in `data/processed/`

## Data Source
Kaggle NBA Database: https://www.kaggle.com/datasets/wyattowalsh/basketball
Updated daily with current season statistics.
EOF

# Create .gitkeep for models directory
touch models/.gitkeep

echo "✅ Project structure created successfully!"
echo ""
echo "📁 Directory structure:"
tree -L 2 -a 2>/dev/null || find . -maxdepth 2 -type d | sed 's|[^/]*/| |g'
echo ""
echo "🔧 Next steps:"
echo "1. Review and customize README.md"
echo "2. Add team member names to README.md"
echo "3. Commit and push to GitHub:"
echo "   git add ."
echo "   git commit -m 'Initial project structure'"
echo "   git push origin main"
echo ""
echo "🎉 Happy coding!"