import subprocess
import sys
from datetime import datetime

scripts = [
    'kaggle_core_data_ingestion.py',    
    'kaggle_historical_data_ingestion.py',
    'nba_pipeline_player_lookup.py',
    'common_player_info_script.py',
    'player_of_the_week_script.py',
    'player_statistics_script.py',
    'play_by_play.py',
    'overall_features.py',
    'overall_features_deji.py',
    'for_inference.py',
    'for_inference_deji.py'
]

def run_script(script_name):
    print(f"Starting {script_name} at {datetime.now()}")
    result = subprocess.run(['python', script_name], capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error in {script_name}:")
        print(result.stderr)
        sys.exit(1)
    print(f"Completed {script_name} at {datetime.now()}")
    print(result.stdout)

if __name__ == "__main__":
    print(f"NBA pipeline started at {datetime.now()}")
    for script in scripts:
        run_script(script)
    print(f"NBA pipeline completed successfully at {datetime.now()}")
