import functions_framework
import pandas as pd
import pandas_gbq
import requests
import logging
import os
import sys
from google.cloud import bigquery
from datetime import datetime, timedelta

# SETUP LOGGING
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# CONFIG - GET FROM ENVIRONMENT VARIABLES
PROJECT_ID = os.environ.get("GCP_PROJECT_ID") 
DATASET_ID = "weather_data"
TABLE_ID = "delhi_daily"

@functions_framework.cloud_event
def main(cloud_event):
    logger.info("Job started...")
    
    if not PROJECT_ID:
        logger.error("GCP_PROJECT_ID environment variable is missing!")
        return "Error"

    # 1. DETERMINE DATE RANGE
    full_table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    client = bigquery.Client(project=PROJECT_ID)
    
    start_str = "2020-01-01" 
    
    try:
        # GETTING THE LATEST DATE FROM THE TABLE
        query = f"SELECT MAX(timestamp) as last_time FROM `{full_table_id}`"
        query_job = client.query(query)
        results = query_job.result()
        for row in results:
            if row.last_time:
                # UPDATING THE start_str ACCORDING TO last_time
                start_str = (row.last_time + timedelta(days=1)).strftime("%Y-%m-%d")
    except Exception as e:
        logger.warning(f"Table check failed: {e}")

    end_str = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    # CHECKING FOR IDEMPOTENCY
    if start_str > end_str:
        logger.info("Data up to date. Exiting.")
        return "OK"

    logger.info(f"Fetching data from {start_str} to {end_str}...")

    # 2. FETCH AIR QUALITY (PM2.5) FOR DELHI
    url_aq = "https://air-quality-api.open-meteo.com/v1/air-quality"
    params_aq = {
        "latitude": 28.6139, "longitude": 77.2090,
        "start_date": start_str, "end_date": end_str,
        "hourly": "pm2_5",
        "timezone": "auto"
    }
    resp_aq = requests.get(url_aq, params=params_aq)
    data_aq = resp_aq.json()
    
    if 'hourly' not in data_aq:
        logger.error("Air Quality API failed")
        return "Error"

    df_aq = pd.DataFrame({
        'timestamp': pd.to_datetime(data_aq['hourly']['time']),
        'PM2_5': data_aq['hourly']['pm2_5']
    })
    df_aq.set_index('timestamp', inplace=True)

    # 3. FETCH WEATHER (Temp, Wind) FOR DELHI
    url_weather = "https://archive-api.open-meteo.com/v1/archive"
    params_weather = {
        "latitude": 28.6139, "longitude": 77.2090,
        "start_date": start_str, "end_date": end_str,
        "hourly": "temperature_2m,relative_humidity_2m,wind_speed_10m,wind_direction_10m",
        "timezone": "auto"
    }
    resp_weather = requests.get(url_weather, params=params_weather)
    data_weather = resp_weather.json()

    df_weather = pd.DataFrame({
        'timestamp': pd.to_datetime(data_weather['hourly']['time']),
        'Temperature': data_weather['hourly']['temperature_2m'],
        'Humidity': data_weather['hourly']['relative_humidity_2m'],
        'Wind_Speed': data_weather['hourly']['wind_speed_10m'],
        'Wind_Direction': data_weather['hourly']['wind_direction_10m']
    })
    df_weather.set_index('timestamp', inplace=True)

    # 4. MERGE & UPLOAD
    full_df = df_aq.join(df_weather, how='inner')
    daily_df = full_df.resample('D').mean().reset_index()

    logger.info(f"Uploading {len(daily_df)} merged rows...")
    pandas_gbq.to_gbq(
        daily_df,
        destination_table=f"{DATASET_ID}.{TABLE_ID}",
        project_id=PROJECT_ID,
        if_exists="append"
    )
    
    logger.info("Success!")
    return "Success"
