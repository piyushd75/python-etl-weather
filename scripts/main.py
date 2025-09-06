import logging
import sys
import os
from extract import fetch_weather_data
from transform import transform_weather_data
from load import get_db_connection, create_weather_table, insert_weather_data, load_config

# Ensure logs directory exists
os.makedirs("logs", exist_ok=True)

# Logging setup
log_format = "[%(asctime)s] %(levelname)s - %(message)s"
logging.basicConfig(
    level=logging.DEBUG,
    format=log_format,
    handlers=[
        logging.FileHandler("logs/etl.log"),
        logging.StreamHandler()
    ]
)

if __name__ == "__main__":
    try:
        # Step 1: Load config
        config = load_config("config/config.yaml")

        # Step 2: Extract
        raw_data = fetch_weather_data()
        if raw_data is None:
            logging.error("No data fetched. Exiting.")
            sys.exit(1)

        # Step 3: Transform
        df = transform_weather_data(raw_data)
        if df.empty:
            logging.warning("Transformed DataFrame is empty. Nothing to load.")
            sys.exit(0)

        # Step 4: Connect to DB
        conn = get_db_connection(config)
        cursor = conn.cursor()

        # Step 5: Ensure table exists
        create_weather_table(cursor)

        # Step 6: Load
        insert_weather_data(cursor, df)

        # Step 7: Commit + Close
        conn.commit()
        logging.info("ETL job completed successfully!")

    except Exception as e:
        logging.exception(f"ETL job failed: {e}")
        sys.exit(1)

    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()
