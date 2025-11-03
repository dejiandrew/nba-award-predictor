import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Creates a connection to the NBA PostgreSQL database
def get_nba_db():
    host = os.getenv("NBA_DB_HOST")
    port = os.getenv("NBA_DB_PORT")
    database = os.getenv("NBA_DB_NAME")
    user = os.getenv("NBA_DB_USER")
    password = os.getenv("NBA_DB_PASSWORD")
    
    connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    return create_engine(connection_string)

# Executes SQL query against the NBA database and returns results as a pandas DataFrame.
def query(sql):
    engine = get_nba_db()
    return pd.read_sql(sql, engine)

# Function to display available tables in database
def list_tables():
    tables = query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    return tables['table_name'].tolist()