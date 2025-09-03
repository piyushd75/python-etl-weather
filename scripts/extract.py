import os
import requests
import yaml
import logging

# Ensure logs directory exists
if not os.path.exists("logs"):
    os.makedirs("logs", exist_ok=True)
    
# Define log format
log_format = "[%(asctime)s] %(levelname)s - %(message)s"

# Configure logging
logging.basicConfig(
    filename="logs/etl.log",
    level=logging.DEBUG,
    format=log_format
)


def load_config(path):
    with open(path, "r") as stream:
        return yaml.safe_load(stream)

configuration = load_config("config/config.yaml")


base_url = configuration.get("api").get("base_url")
api_key = configuration.get("api").get("key")
city = configuration.get("api").get("city")
lat = configuration.get("api").get("lat")
lon = configuration.get("api").get("lon")


def fetch_weather_data():
    """
    Fetch current weather data from OpenWeather API.
    Returns:
        dict: Parsed JSON response if successful, else None.
    """
    
    if lat and lon:
        request_url = f"{base_url}?lat={lat}&lon={lon}&appid={api_key}&units=metric"
    else:
        request_url = f"{base_url}?q={city}&appid={api_key}&units=metric"
    print(request_url)
    requests_response = requests.get(request_url)
    if requests_response.status_code == 200:
        data = requests_response.json()
        logging.info(f"Data fetched successfully for city={city}, lat={lat}, lon={lon}")
        logging.debug(f"Response JSON: {data}")
    else:
        logging.error(f"Failed to fetch data: {requests_response.status_code}, Response: {requests_response.text}")
        data = None
        
    return data

if __name__ == "__main__":
    data = fetch_weather_data()
    if data:
        print("Sample weather data:", data.get("main"))
