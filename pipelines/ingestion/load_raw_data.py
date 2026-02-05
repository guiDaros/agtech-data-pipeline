import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# Carrega variáveis do .env
load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

DB_CONNECTION_STR = (
    f"postgresql://{DB_USER}:{DB_PASSWORD}"
    f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

def load_csv_to_raw():
    file_path = os.path.join("data", "raw", "raw_sensors_preview.csv")
    
    print(f"Looking for data at: {file_path}")
    
    if not os.path.exists(file_path):
        print("File not found. Make sure you ran the simulator and moved the file to data/raw/")
        return

    try:
        print("Reading CSV file...")
        df = pd.read_csv(file_path)

        df['event_timestamp'] = pd.to_datetime(df['event_timestamp'])
        df['ingestion_timestamp'] = pd.to_datetime(df['ingestion_timestamp'])

        print("Connecting to PostgreSQL...")
        engine = create_engine(DB_CONNECTION_STR)

        print(f"Loading {len(df)} rows into 'raw_sensor_readings'...")
        df.to_sql(
            name='raw_sensor_readings',
            con=engine,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=1000
        )

        print("Data successfully loaded into RAW layer.")

    except Exception as e:
        print(f"Failed to load data: {e}")
        print("Check your .env configuration and database status.")

if __name__ == "__main__":
    load_csv_to_raw()
