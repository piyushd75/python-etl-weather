import os
import logging
import pandas as pd
from extract import fetch_weather_data  # adjust if in scripts/

# Pandas display settings
pd.set_option("display.max_columns", None)

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

def transform_weather_data(data):
    """
    Transform raw weather data into a structured pandas DataFrame.

    Args:
        data (dict): Raw JSON data from the weather API.

    Returns:
        pd.DataFrame: Transformed data in a DataFrame.
    """
    if not data:
        logging.error("No data provided for transformation.")
        return pd.DataFrame()

    # Validate required fields
    required_fields = ["name", "sys", "main", "weather"]
    for field in required_fields:
        if field not in data:
            logging.warning(f"Missing field: {field}")

    try:
        transformed_data = {
            "city": data.get("name"),
            "country": data.get("sys", {}).get("country"),
            "weather_main": data.get("weather", [{}])[0].get("main"),
            "weather_description": data.get("weather", [{}])[0].get("description"),
            "temp": data.get("main", {}).get("temp"),
            "feels_like": data.get("main", {}).get("feels_like"),
            "temp_min": data.get("main", {}).get("temp_min"),
            "temp_max": data.get("main", {}).get("temp_max"),
            "pressure": data.get("main", {}).get("pressure"),
            "humidity": data.get("main", {}).get("humidity"),
            "wind_speed": data.get("wind", {}).get("speed"),
            "wind_deg": data.get("wind", {}).get("deg"),
            "clouds_all": data.get("clouds", {}).get("all"),
            "date": pd.to_datetime(data.get("dt"), unit='s', errors="coerce"),
            "sunrise": pd.to_datetime(data.get("sys", {}).get("sunrise"), unit='s', errors="coerce"),
            "sunset": pd.to_datetime(data.get("sys", {}).get("sunset"), unit='s', errors="coerce")
        }

        df = pd.DataFrame([transformed_data])
        logging.info(f"Data transformed successfully for city={transformed_data['city']}")
        return df

    except Exception as e:
        logging.error(f"Error during transformation: {e}", exc_info=True)
        return pd.DataFrame()

if __name__ == "__main__":
    extracted_data = fetch_weather_data()
    if extracted_data:
        transformed_df = transform_weather_data(extracted_data)
        if not transformed_df.empty:
            print(transformed_df.head())
        else:
            logging.error("Transformation returned empty DataFrame.")
    else:
        logging.error("Extraction returned None.")
