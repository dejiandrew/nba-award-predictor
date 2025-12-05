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
COPY cis-5450-final-project-485661e2f371.json ./
COPY kaggle.json ./

# Create .kaggle directory and move kaggle.json there
RUN mkdir -p /root/.kaggle && \
    cp kaggle.json /root/.kaggle/kaggle.json && \
    chmod 600 /root/.kaggle/kaggle.json

# Set environment variable for GCP credentials
ENV GOOGLE_APPLICATION_CREDENTIALS=/app/cis-5450-final-project-485661e2f371.json

# Run the orchestration script
CMD ["python", "run_pipeline.py"]
