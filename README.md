# NBA Player of the Week Award Predictor

**CIS 5450: Big Data Analytics - Fall 2025**

## Team Members: Deji Andrew, Abdullah Amer, Josh Lim, Hao Hua
---

## Project Overview

This project applies machine learning to predict NBA Player of the Week (POW) awards using historical performance data. We built an end-to-end pipeline that ingests data daily, engineers features, and trains models to identify the statistical patterns that lead to award recognition.

### Objective
Predict which players will win Player of the Week awards based on:
- Individual performance metrics (points, assists, rebounds, shooting efficiency)
- Team success and context (wins, standings)
- Opponent strength
- Historical performance trends and breakout performances

### Dataset
- **Size**: ~234,000 player-week observations after cleaning
- **Features**: 64 features
- **Target**: Binary classification (POW winner vs. non-winner)
- **Time Range**: Multiple NBA seasons

---

## Project Workflow

### 1. Data Pipeline (Automated on GCP)
Our production pipeline runs daily at 5:00 AM EST on **Google Cloud Platform** via Cloud Run:

**Data Sources:**
- Kaggle: Core NBA statistics and historical data
- NBA API: Official player information and lookups
- Custom mappings for player ID standardization

**Pipeline Steps:**
1. Ingest data from Kaggle and NBA API
2. Clean and standardize player information
3. Process player statistics and game data
4. Engineer features across multiple categories
5. Store processed data in Google Cloud Storage

All scripts are orchestrated through `run_pipeline.py` in the `data_pipeline/` directory.

### 2. Feature Engineering
Created comprehensive features including:
- Player performance metrics (points, assists, rebounds, efficiency)
- Team context (wins, standings, home/away splits)
- Opponent strength indicators
- Historical performance baselines and z-scores
- **Breakout Score**: Composite metric identifying exceptional performances

Key innovation: League-normalized z-scores to account for era adjustments and scoring trends.

### 3. Machine Learning Models
Evaluated multiple approaches to handle extreme class imbalance (~1:166 ratio):
- **Logistic Regression**: Interpretable baseline with L2 regularization
- **LightGBM**: Gradient boosting framework optimized for large datasets
- **Neural Network**: Deep learning approach with multiple hidden layers

All models use class weighting and threshold tuning to address the severe imbalance between POW winners and non-winners.

### 4. Key Challenges
- **Data integration**: Built comprehensive player lookup system across multiple sources
- **Class imbalance**: Addressed with class weighting
- **Memory constraints**: Implemented chunked processing for large datasets
- **Temporal integrity**: Ensured no data leakage with strict feature engineering discipline

---

### Cloud Deployment
Pipeline is deployed on GCP Cloud Run with automated daily execution. All processed data is stored in Google Cloud Storage bucket: `nba_award_predictor`

---

## Results

Our models successfully identify POW candidates while maintaining balance between precision and recall despite extreme class imbalance. Key evaluation metrics include precision, recall, F1 score, and top-K accuracy (whether the actual winner appears in the top-K predictions).

**Model Comparison:**
- **Logistic Regression**: Provides interpretable baseline and feature importance through coefficients
- **LightGBM**: Efficient gradient boosting with strong performance on large datasets
- **Neural Network**: Deep learning approach capturing complex non-linear patterns

---

## Course Topics Applied

This project applied concepts from **8 out of 13 course modules**:

### **Module 2: Data Acquisition and Transformation**
- Data acquisition from web sources (Kaggle API, NBA API)
- Combining dataframes across multiple sources
- Data cleaning and record linking with player ID standardization

### **Module 4: Scalable Data Processing**
- Efficient computing with chunked processing for large datasets (3GB+ files)
- Cloud-based data processing on GCP Cloud Run

### **Module 5: Big Data Infrastructure**
- Cloud deployment on Google Cloud Platform
- Google Cloud Storage for distributed data management

### **Module 6: Advanced Graph Analysis and Statistics**
- Statistical analysis with z-scores and normalization
- Correlation analysis for feature selection

### **Module 7: Human-Centered Data Science**
- Data visualization for exploratory analysis (matplotlib, seaborn, plotly)
- Ethical considerations in predictive modeling

### **Module 9: Supervised Machine Learning**
- Binary classification with multiple algorithms:
  - Logistic Regression
  - LightGBM (Gradient Boosting)
  - Neural Networks
- Model evaluation for imbalanced data (precision, recall, F1, PR-AUC)

### **Module 10: Advanced Machine Learning**
- Handling class imbalance (SMOTE, class weighting, threshold tuning)
- Hyperparameter optimization
- Feature engineering and selection
- Model comparison and evaluation

### **Module 13: Data Management and Archival**
- Distributed data storage on Google Cloud Storage
- Automated data pipeline for persistent archival

---
