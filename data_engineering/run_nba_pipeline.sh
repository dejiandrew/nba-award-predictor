#!/bin/bash
# File: run_nba_pipeline.sh
# Purpose: Automate the execution of NBA pipeline scripts

# Set the working directory to where your scripts and venv are located
# Replace with your actual directory path
cd ~/nba-pipeline/data_engineering

# Activate the virtual environment
source /root/nba-pipeline/venv/bin/activate

# Run the first pipeline script and wait for it to complete
echo "Starting nba_pipeline_1.py at $(date)"
python /root/nba-pipeline/data_engineering/nba_pipeline_1.py
echo "Completed nba_pipeline_1.py at $(date)"

# Run the second pipeline script
echo "Starting nba_pipeline_2.py at $(date)"
python /root/nba-pipeline/data_engineering/nba_pipeline_2.py
echo "Completed nba_pipeline_2.py at $(date)"

# Run the third pipeline script
echo "Starting nba_pipeline_player_lookup.py at $(date)"
python /root/nba-pipeline/data_engineering/nba_pipeline_pow.py
echo "Completed nba_pipeline_player_lookup.py at $(date)"

# Run the fourth pipeline script
echo "Starting common_player_info_script.py at $(date)"
python /root/nba-pipeline/data_engineering/common_player_info_script.py
echo "Completed common_player_info_script.py at $(date)"

# Run the fifth pipeline script
echo "Starting player_of_the_week_script.py at $(date)"
python /root/nba-pipeline/data_engineering/player_of_the_week_script.py
echo "Completed player_of_the_week_script.py at $(date)"

# Run the sixth pipeline script
echo "Starting player_statistics_script.py at $(date)"
python /root/nba-pipeline/data_engineering/player_statistics_script.py
echo "Completed player_statistics_script.py at $(date)"

# Run the seventh pipeline script
echo "Starting play_by_play.py at $(date)"
python /root/nba-pipeline/data_engineering/play_by_play.py
echo "Completed play_by_play.py at $(date)"

# Deactivate the virtual environment (optional but good practice)
deactivate

echo "NBA pipeline completed successfully at $(date)"
