import logging
import psycopg2  # or mysql.connector, depending on DB
import pandas as pd
from transform import transform_weather_data
from extract import fetch_weather_data
import yaml
import os
import sys


# ---------------------------
# 1. Load Config
# ---------------------------

def load_config(path):
    with open(path, "r") as stream:
        return yaml.safe_load(stream)

config = load_config("config/config.yaml")

# Ensure logs directory exists
os.makedirs("logs", exist_ok=True)

# Define log format
log_format = "[%(asctime)s] %(levelname)s - %(message)s"

# Configure logging (file + console)
logging.basicConfig(
    level=logging.DEBUG,
    format=log_format,
    handlers=[
        logging.FileHandler("logs/etl.log"),
        logging.StreamHandler()
    ]
)


# ---------------------------
# 2. Connect to Database
# ---------------------------
def get_db_connection(config):
    """
    Establish connection to PostgreSQL/MySQL using credentials from config.
    Return connection + cursor.
    """
    conn = psycopg2.connect(
        host=config["database"]["host"],
        port=config["database"]["port"],
        user=config["database"]["user"],
        password=config["database"]["password"],
        dbname=config["database"]["dbname"]
    )
    return conn


# ---------------------------
# 3. Create Table (if not exists)
# ---------------------------
def create_weather_table(cursor):
    """
    Create a table with required columns if it doesn't exist already.
    Example columns: city, temp, humidity, pressure, weather_desc, wind_speed, timestamp
    """
    cursor.execute(""" 
                    CREATE TABLE IF NOT EXISTS weather_data (
                    id SERIAL PRIMARY KEY,
                    city VARCHAR(100),
                    country VARCHAR(50),
                    weather_main VARCHAR(50),
                    weather_desc VARCHAR(100),
                    temp FLOAT,
                    feels_like FLOAT,
                    temp_min FLOAT,
                    temp_max FLOAT,
                    pressure INT,
                    humidity INT,
                    wind_speed FLOAT,
                    wind_deg INT,
                    clouds_all INT,
                    date TIMESTAMP,
                    sunrise TIMESTAMP,
                    sunset TIMESTAMP
                );
                """)

# ---------------------------
# 4. Insert Data
# ---------------------------
def insert_weather_data(cursor, df):
    """
    Insert transformed weather DataFrame into the table.
    Loop through DataFrame rows and insert each record.
    Use parameterized query (%s placeholders).
    """
    for index, row in df.iterrows():
        cursor.execute("""
            INSERT INTO weather_data (
                city, country, temp, feels_like, temp_min, temp_max,
                pressure, humidity, weather_main, weather_desc,
                wind_speed, wind_deg, clouds_all, date, sunrise, sunset
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, (
            row['city'], row['country'], row['temp'], row['feels_like'],
            row['temp_min'], row['temp_max'], row['pressure'], row['humidity'],
            row['weather_main'], row['weather_desc'], row['wind_speed'],
            row['wind_deg'], row['clouds_all'], row['date'],
            row['sunrise'], row['sunset']
        ))




# ---------------------------
# 5. Orchestration
# ---------------------------
if __name__ == "__main__":
    try:
        # Step 1: Fetch + Transform
        raw_data = fetch_weather_data()
        if raw_data is None:
            logging.error("No data fetched. Exiting.")
            sys.exit(1)

        df = transform_weather_data(raw_data)
        if df.empty:
            logging.warning("Transformed DataFrame is empty. Nothing to load.")
            sys.exit(0)

        # Step 2: Connect to DB
        conn = get_db_connection(config)
        cursor = conn.cursor()

        # Step 3: Ensure table exists
        create_weather_table(cursor)

        # Step 4: Insert rows
        insert_weather_data(cursor, df)

        # Step 5: Commit
        conn.commit()
        logging.info("Weather data loaded successfully into DB.")

    except Exception as e:
        logging.exception(f"ETL job failed: {e}")
        sys.exit(1)

    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()
