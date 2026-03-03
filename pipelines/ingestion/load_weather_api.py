import os
import sys
import json
import logging
from datetime import datetime, timezone

import requests
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Configure structured logging for production environments
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

# Load environment variables explicitly
load_dotenv()

# Strict extraction for API configuration (no fallbacks)
API_KEY = os.getenv("OPENWEATHER_API_KEY")
LAT = os.getenv("LATITUDE")
LON = os.getenv("LONGITUDE")
LOCATION_NAME = os.getenv("LOCATION_NAME")

# Strict extraction for Database credentials (no fallbacks)
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASSWORD")

# Fail-fast validation for all required environment variables
required_env_vars = {
    "OPENWEATHER_API_KEY": API_KEY,
    "LATITUDE": LAT,
    "LONGITUDE": LON,
    "LOCATION_NAME": LOCATION_NAME,
    "DB_HOST": DB_HOST,
    "DB_PORT": DB_PORT,
    "DB_NAME": DB_NAME,
    "DB_USER": DB_USER,
    "DB_PASSWORD": DB_PASS
}

missing_vars = [key for key, value in required_env_vars.items() if not value]

if missing_vars:
    logger.critical(f"Missing required environment variables: {', '.join(missing_vars)}. Application will exit.")
    sys.exit(1)

def fetch_weather_data() -> dict:
    """
    Fetches the 5-day weather forecast (in 3-hour intervals) from the OpenWeather API.
    """
    url = f"https://api.openweathermap.org/data/2.5/forecast?lat={LAT}&lon={LON}&appid={API_KEY}&units=metric"
    logger.info(f"Fetching weather forecast data for location: {LOCATION_NAME}")
    
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        logger.error(f"HTTP error occurred while fetching API data. Details: {http_err}")
        sys.exit(1)
    except requests.exceptions.RequestException as req_err:
        logger.error(f"Network or timeout error occurred. Details: {req_err}")
        sys.exit(1)

def process_and_insert_data(data: dict) -> None:
    """
    Parses the JSON payload and ingests the required fields into the PostgreSQL raw table.
    Implements an UPSERT (ON CONFLICT) strategy ensuring data idempotency.
    """
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST, 
            port=DB_PORT, 
            dbname=DB_NAME, 
            user=DB_USER, 
            password=DB_PASS
        )
        cursor = conn.cursor()

        records_to_insert = []
        forecast_list = data.get('list', [])
        
        for item in forecast_list:
            forecast_time = datetime.fromtimestamp(item['dt'], tz=timezone.utc)
            temp = item['main']['temp']
            humidity = item['main']['humidity']
            rain_3h = item.get('rain', {}).get('3h', 0.0)
            weather_condition = item['weather'][0]['main']
            
            # Serialize the entire dictionary back into JSON format to preserve source structure 
            raw_payload = json.dumps(item)

            records_to_insert.append((
                LOCATION_NAME,
                forecast_time,
                temp,
                humidity,
                rain_3h,
                weather_condition,
                raw_payload
            ))

        insert_query = """
            INSERT INTO raw_weather_forecast 
            (location_name, forecast_timestamp, temperature_c, humidity_pct, rain_mm_3h, weather_condition, raw_payload)
            VALUES %s
            ON CONFLICT (location_name, forecast_timestamp) 
            DO UPDATE SET 
                temperature_c = EXCLUDED.temperature_c,
                humidity_pct = EXCLUDED.humidity_pct,
                rain_mm_3h = EXCLUDED.rain_mm_3h,
                weather_condition = EXCLUDED.weather_condition,
                raw_payload = EXCLUDED.raw_payload,
                ingestion_timestamp = CURRENT_TIMESTAMP;
        """

        logger.info(f"Initiating bulk ingestion of {len(records_to_insert)} forecast records.")
        execute_values(cursor, insert_query, records_to_insert)
        
        conn.commit()
        logger.info("Successfully ingested API data into 'raw_weather_forecast'. Transaction committed.")

    except psycopg2.Error as db_err:
        if conn:
            conn.rollback()
        logger.error(f"Database transaction failed. Rolling back changes. Details: {db_err}")
    except Exception as general_err:
        logger.error(f"An unexpected error occurred during data processing: {general_err}")
    finally:
        if conn is not None:
            cursor.close()
            conn.close()
            logger.info("Database connection closed properly.")

if __name__ == "__main__":
    weather_payload = fetch_weather_data()
    process_and_insert_data(weather_payload)