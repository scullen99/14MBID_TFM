"""
Este script pertenece a la primera fase de ingestión de datos en el Data Lake de AWS.
Los datos recopilados se almacen en la capa "bronze" del Data Lake, que es la capa de datos crudos.
Posteriormente se realizarán transformaciones y limpieza de datos para almacenarlos en la capa "silver".

Script de AWS Glue para la ingesta de datos meteorológicos históricos de la API de Weatherstack.
Este script se encarga de recopilar datos meteorológicos históricos de la API de Weatherstack (https://weatherstack.com/) 
para cada provincia (ciudad) de España y almacenarlos en un bucket de S3 en formato JSON.

El script se ejecuta en un job de AWS Glue y se encarga de lo siguiente:
1. Descargar un archivo JSON con las provincias de España desde un bucket de S3.
2. Iterar sobre cada provincia y fecha para recopilar datos meteorológicos históricos de la API de Weatherstack.
3. Guardar los datos en un bucket de S3 en formato JSON.

El script utiliza AWS Secrets Manager para almacenar credenciales y configuraciones sensibles.

Fecha de ultima actualizacion: 14/04/2025
Autor: Sergio Esteban Tarrero
Layer: Bronze
"""

import os
import boto3
import requests
import json
import logging
import re
import sys
from datetime import datetime, timedelta
from unicodedata import normalize
from botocore.exceptions import BotoCoreError, NoCredentialsError, ClientError

# Configuración de logging para AWS Glue (CloudWatch + S3)
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Configuración cliente AWS
session = boto3.session.Session()
secrets_manager = session.client("secretsmanager", region_name="eu-west-1")
s3_client = session.client("s3", region_name="eu-west-1")

PROVINCES_FILE_PATH = "jsons/provincias.json"
LAYER: str = "bronze"

START_DATE = datetime(2023, 12, 31)
END_DATE = datetime(2024, 1, 1)

log_messages = []

def write_log_to_s3(message, success=True):
    logs_secret = get_secret("s3_bucket_logs_storage")
    log_bucket = logs_secret.get("s3_bucket")
    log_type = "success" if success else "failure"
    log_key = f"glue_logs/{log_type}/ingestion/ingestion_{datetime.now().strftime('%Y_%m_%d_%H_%M_%S')}.log"
    s3_client.put_object(Bucket=log_bucket, Key=log_key, Body=message)
    logging.info(f"📤 Log written to S3: s3://{log_bucket}/{log_key}")

def log_message(level, message):
    global log_messages
    formatted_message = f"{datetime.now()} - {level.upper()} - {message}"
    log_messages.append(formatted_message)

    getattr(logging, level.lower())(message)
    print(message)

def get_secret(secret_name):
    try:
        log_message("info", f"🔍 Retrieving secret: {secret_name}")
        response = secrets_manager.get_secret_value(SecretId=secret_name)
        secret = json.loads(response["SecretString"])
        log_message("info", f"✅ Secret '{secret_name}' retrieved successfully.")
        return secret
    except (BotoCoreError, NoCredentialsError, ClientError) as e:
        log_message("critical", f"❌ Error retrieving secret '{secret_name}': {e}")
        raise

def load_provinces_from_s3(bucket_name, file_key):
    try:
        log_message("info", f"🔍 Downloading file from S3: {bucket_name}/{file_key}")
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        content = response["Body"].read().decode("utf-8")
        log_message("info", "✅ Provinces file loaded successfully.")
        return json.loads(content)
    except ClientError as e:
        log_message("critical", f"❌ Error retrieving file {file_key} from bucket {bucket_name}: {e}")
        raise

def normalize_filename(location, date):
    location_normalized = normalize('NFKD', location).encode('ASCII', 'ignore').decode('ASCII')
    location_sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", location_normalized)
    location_sanitized = re.sub(r"_+", "_", location_sanitized).strip("_")
    date_str = date.strftime("%Y_%m_%d")
    return f"{location_sanitized}_{date_str}.json"

def check_file_exists_in_s3(bucket_name, file_key):
    try:
        s3_client.head_object(Bucket=bucket_name, Key=file_key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        else:
            log_message("error", f"❌ Error checking file in S3: {e}")
            raise

def fetch_weather_data(api_key, location, date, file_count, total_files):
    date_str = date.strftime("%Y-%m-%d")
    url = "http://api.weatherstack.com/historical"
    params = {
        "access_key": api_key,
        "query": location,
        "historical_date": date_str,
        "language": "es",
        "hourly": "1"
    }
    log_message("info", f"🌍 ({file_count}/{total_files}) Requesting data for {location} on {date_str}.")

    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        if "historical" in data and date_str in data["historical"]:
            log_message("info", f"✅ ({file_count}/{total_files}) Data received for {location} on {date_str}.")
            return {
                "historical": data["historical"][date_str],
                "location": data.get("location", {})
            }
        else:
            log_message("warning", f"⚠️ ({file_count}/{total_files}) No historical data for {location} on {date_str}.")
            return None
    else:
        log_message("error", f"❌ ({file_count}/{total_files}) API Error {response.status_code} fetching data for {location}.")
        return None

def save_to_s3(data, filename, file_count, total_files, province, autonomous_community, s3_bucket_name):
    s3_key = f"{filename}"

    location = data.get("location", {})
    weather = data.get("historical", {})

    country = location.get("country", "")
    timezone = location.get("timezone_id", "")
    if country != "Spain" or timezone != "Europe/Madrid":
        log_message("warning", f"({file_count}/{total_files}) [Skipping] {province} — not in Spain/Europe-Madrid (Got {country}/{timezone}).")
        return

    records = []
    for hour_data in weather["hourly"]:
        hour_data.update({
            "province": province,
            "autonomous_community": autonomous_community,
            "date": weather["date"],
            "max_temp": weather["maxtemp"],
            "min_temp": weather["mintemp"],
            "avg_temp": weather["avgtemp"],
            "sun_hour": weather.get("sunhour", None),
            "uv_index": weather.get("uv_index", None),
            "city": location.get("name", province),
            "country": country,
            "timezone": timezone,
            "latitude": location.get("lat", None),
            "longitude": location.get("lon", None),
            "layer": LAYER,
            "filename": filename
        })
        records.append(json.dumps(hour_data))

    jsonl_data = "\n".join(records)

    try:
        s3_client.put_object(Bucket=s3_bucket_name, Key=s3_key, Body=jsonl_data.encode("utf-8"))
        log_message("info", f"✅ ({file_count}/{total_files}) Data saved to S3: s3://{s3_bucket_name}/{s3_key}")
    except (BotoCoreError, NoCredentialsError, ClientError) as e:
        log_message("critical", f"❌ ({file_count}/{total_files}) Failed to upload to S3: {e}")
        raise

if __name__ == "__main__":
    success = True
    log_message("info", "🚀 Starting Glue Job execution...")

    try:
        weatherstack_secret = get_secret("weatherstack_api")
        api_key = weatherstack_secret.get("weatherstack_api_key")

        s3_resources_secret = get_secret("s3_bucket_resources_storage")
        s3_resources_bucket = s3_resources_secret.get("s3_bucket")

        s3_bronze_secret = get_secret("s3_bucket_bronze_ingestion")
        s3_bronze_bucket = s3_bronze_secret.get("s3_bucket")

        provinces = load_provinces_from_s3(s3_resources_bucket, PROVINCES_FILE_PATH)
        log_message("info", f"✅ Loaded {len(provinces)} provinces from S3.")

        start_date = START_DATE
        end_date = END_DATE

        total_days = (end_date - start_date).days + 1
        total_files = len(provinces) * total_days
        file_count = 1

        for province in provinces:
            province_name = province["label"]
            autonomous_community_code = province["parent_code"]
            current_date = start_date

            while current_date <= end_date:
                filename = normalize_filename(province_name, current_date)
                s3_key = f"{filename}"

                if check_file_exists_in_s3(s3_bronze_bucket, s3_key):
                    log_message("warning", f"⚠️ ({file_count}/{total_files}) File already exists in S3, skipping: {s3_key}")
                else:
                    try:
                        weather_data = fetch_weather_data(api_key, province_name, current_date, file_count, total_files)
                        if weather_data:
                            save_to_s3(weather_data, filename, file_count, total_files, province_name, autonomous_community_code, s3_bronze_bucket)
                    except Exception as e:
                        log_message("error", f"❌ ({file_count}/{total_files}) Error processing {province_name}: {e}")
                        success = False

                file_count += 1
                current_date += timedelta(days=1)

        log_message("info", "✅ Data collection completed successfully.")

    except Exception as e:
        log_message("critical", f"❌ Glue Job failed: {e}")
        success = False

    final_logs = "\n".join(log_messages)
    write_log_to_s3(final_logs, success=success)