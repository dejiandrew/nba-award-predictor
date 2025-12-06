# GCP Infrastructure Documentation

**Project**: NBA Player of the Week Award Predictor  
**Team**: Deji Andrew, Abdullah Amer, Josh Lim, Hao Hua  
---

## Overview

This document provides evidence of our production infrastructure deployed on Google Cloud Platform (GCP), including automated data pipeline execution via Cloud Run and data storage in Google Cloud Storage.

---

## 1. Cloud Run - Automated Pipeline

### Cloud Run Jobs Dashboard automated via Cloud Scheduler
**Description**: Shows our scheduled `nba-pipeline` job that runs daily at 5:00 AM EST.

<img width="680" height="313" alt="image" src="https://github.com/user-attachments/assets/fd8d99e8-df21-454a-8945-121ff99da1cf" />

---

## 2. Google Cloud Storage - Data Storage

### GCS Bucket Overview
**Description**: The `nba_award_predictor` bucket containing all processed NBA data.

<img width="677" height="313" alt="image" src="https://github.com/user-attachments/assets/8ee1e251-d439-46ae-84b6-a0e35b621d64" />

---

## 3. Infrastructure Diagram

```
┌─────────────────────────────────────────────────────────┐
│                     GCP Cloud Platform                   │
│                                                          │
│  ┌────────────────────────────────────────────┐         │
│  │        Cloud Run (Scheduled Job)            │         │
│  │  ┌──────────────────────────────────────┐  │         │
│  │  │   nba-pipeline                       │  │         │
│  │  │   - Schedule: Daily 5:00 AM EST      │  │         │
│  │  │   - Docker Container                 │  │         │
│  │  │   - Runs: run_pipeline.py            │  │         │
│  │  └──────────────────────────────────────┘  │         │
│  │                    │                        │         │
│  │                    │ Executes               │         │
│  │                    ▼                        │         │
│  │  ┌──────────────────────────────────────┐  │         │
│  │  │  Pipeline Scripts (9 stages):        │  │         │
│  │  │  1. Kaggle data ingestion            │  │         │
│  │  │  2. Historical data processing       │  │         │
│  │  │  3. Player lookup creation           │  │         │
│  │  │  4. Player info standardization      │  │         │
│  │  │  5. POW data cleaning                │  │         │
│  │  │  6. Statistics processing            │  │         │
│  │  │  7. Play-by-play upload              │  │         │
│  │  │  8. Feature engineering              │  │         │
│  │  │  9. Inference data prep              │  │         │
│  │  └──────────────────────────────────────┘  │         │
│  │                    │                        │         │
│  │                    │ Uploads                │         │
│  │                    ▼                        │         │
│  └────────────────────────────────────────────┘         │
│                      │                                   │
│                      ▼                                   │
│  ┌────────────────────────────────────────────┐         │
│  │   Google Cloud Storage (GCS)               │         │
│  │                                             │         │
│  │   Bucket: nba_award_predictor              │         │
│  │   ├── nba_data/                            │         │
│  │   │   ├── features-overall.csv             │         │
│  │   │   ├── features-overall-weekly.csv      │         │
│  │   │   ├── player-statistics.csv            │         │
│  │   │   ├── player-of-the-week.csv           │         │
│  │   │   ├── nba_player_lookup.csv            │         │
│  │   │   ├── games.csv                        │         │
│  │   │   ├── nba-all-stars.csv                │         │
│  │   │   ├── nba-mvp.csv                      │         │
│  │   │   └── all-nba-*.csv                    │         │
│  └────────────────────────────────────────────┘         │
│                                                          │
└─────────────────────────────────────────────────────────┘
                           │
                           │ Accessed by
                           ▼
                  ┌─────────────────┐
                  │  Final_Notebook  │
                  │  (Model Training)│
                  └─────────────────┘
```

---

## 4. Deployment Configuration Files

### Dockerfile
Location: `data_pipeline/Dockerfile`

```dockerfile
FROM python:3.9-slim

WORKDIR /app

# Install sqlite3 command-line tool
RUN apt-get update && \
    apt-get install -y sqlite3 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all Python scripts and credentials
COPY *.py ./
COPY cis-5450-final-project-*.json ./
COPY kaggle.json ./

# Create .kaggle directory and move kaggle.json there
RUN mkdir -p /root/.kaggle && \
    cp kaggle.json /root/.kaggle/kaggle.json && \
    chmod 600 /root/.kaggle/kaggle.json

# Set environment variable for GCP credentials
ENV GOOGLE_APPLICATION_CREDENTIALS=/app/cis-5450-final-project-*.json

# Run the orchestration script
CMD ["python", "run_pipeline.py"]
```
