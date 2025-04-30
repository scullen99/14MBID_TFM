"""
Este script pertenece a la fase de transformación de datos en el Data Lake de AWS.
Los datos transformados se almacenan en la capa "silver" del Data Lake, estructurados y enriquecidos.
Este proceso toma datos desde la capa bronze (JSON crudo), realiza transformaciones necesarias 
y los guarda como CSV.

Script de AWS Glue para la transformación de datos meteorológicos previamente ingeridos.
Las transformaciones incluyen: limpieza, renombrado de columnas y valores, enriquecimiento temporal
y creación de campos derivados.

El script se ejecuta en un job de AWS Glue y se encarga de:
1. Identificar los archivos recientes en la capa bronze.
2. Leer, transformar y fusionar los datos en un CSV por día.
3. Guardar el resultado en el bucket S3 de la capa silver.

Fecha de última actualización: 14/04/2025
Autor: Sergio Esteban Tarrero
Layer: Silver
"""

import boto3
import pandas as pd
import json
import io
import re
import sys
import logging
from datetime import datetime, timedelta, timezone
from botocore.exceptions import ClientError, BotoCoreError, NoCredentialsError

# Configuración de logging
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

log_messages = []

def log_message(level, message):
    formatted_message = f"{datetime.now()} - {level.upper()} - {message}"
    log_messages.append(formatted_message)
    getattr(logging, level.lower())(message)
    print(message)

def write_log_to_s3(message, success=True):
    logs_secret = get_secret("s3_bucket_logs_storage")
    log_bucket = logs_secret.get("s3_bucket")
    log_type = "success" if success else "failure"
    log_key = f"glue_logs/{log_type}/transformation/transformation_{datetime.now().strftime('%Y_%m_%d_%H_%M_%S')}.log"
    s3_client.put_object(Bucket=log_bucket, Key=log_key, Body=message)
    log_message("info", f"📤 Log written to S3: s3://{log_bucket}/{log_key}")

# Constantes y configuración
HOURS_AGO: int = 2
LAYER: str = "silver"
DROP_COLUMNS_PATH: str = "jsons/drop_columns.json"
RENAME_CITIES_PATH: str = "jsons/rename_cities.json"
RENAME_COLUMNS_PATH: str = "jsons/rename_columns.json"
RENAME_MONTHS_PATH: str = "jsons/rename_months.json"
MAP_HOURS_PATH: str = "jsons/map_hours.json"

session = boto3.session.Session()
s3_client = session.client("s3")
secrets_client = session.client("secretsmanager")

def get_secret(secret_name):
    try:
        log_message("info", f"🔐 Retrieving secret: {secret_name}")
        response = secrets_client.get_secret_value(SecretId=secret_name)
        secret = json.loads(response["SecretString"])
        return secret
    except (BotoCoreError, NoCredentialsError, ClientError) as e:
        log_message("critical", f"❌ Error retrieving secret '{secret_name}': {e}")
        raise

bronze_bucket = get_secret("s3_bucket_bronze_ingestion")["s3_bucket"]
silver_bucket = get_secret("s3_bucket_silver_transformation")["s3_bucket"]
resources_bucket = get_secret("s3_bucket_resources_storage")["s3_bucket"]

def get_drop_columns(bucket, key):
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        return json.loads(response["Body"].read().decode("utf-8"))
    except Exception as e:
        log_message("error", f"❌ Error reading drop columns file: {e}")
        return []

def rename_values(bucket, key):
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        return json.loads(response["Body"].read().decode("utf-8"))
    except Exception as e:
        log_message("error", f"❌ Error reading rename values file: {e}")
        return {}

def list_recent_files(bucket):
    file_list = []
    cutoff = datetime.now(timezone.utc) - timedelta(hours=HOURS_AGO)
    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket):
            for item in page.get("Contents", []):
                if item["LastModified"] >= cutoff:
                    log_message("info", f"📁 Found recent file: {item['Key']}")
                    file_list.append(item["Key"])
        log_message("info", f"✅ Total recent files: {len(file_list)}")
        return file_list
    except ClientError as e:
        log_message("error", f"❌ Error listing files: {e}")
        return []

def extract_date_from_filename(filename):
    match = re.search(r"\d{4}_\d{2}_\d{2}", filename)
    return match.group(0) if match else None

def read_json_from_s3(bucket, key):
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        content = obj["Body"].read().decode("utf-8")
        records = [json.loads(line) for line in content.strip().split("\n")]
        return pd.DataFrame(records)
    except Exception as e:
        log_message("error", f"❌ Error reading/parsing JSON {key}: {e}")
        return pd.DataFrame()

def transform_data(df):
    try:
        df["province"] = df["province"].astype(str).str.replace(",Spain", "", regex=False).str.strip()

        df.drop(columns=get_drop_columns(resources_bucket, DROP_COLUMNS_PATH), errors='ignore', inplace=True)

        df["province"] = df["province"].map(rename_values(resources_bucket, RENAME_CITIES_PATH))
        df["current_layer"] = LAYER
        df.columns = df.columns.str.lower()
        df.rename(columns=rename_values(resources_bucket, RENAME_COLUMNS_PATH), inplace=True)
        df["hour_day"] = df["hour_day"].astype(str).map(rename_values(resources_bucket, MAP_HOURS_PATH)).fillna(df["hour_day"])

        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df["year"] = df["date"].dt.year.astype(str)
        df["month"] = df["date"].dt.month.astype(str).str.zfill(2)
        df["day"] = df["date"].dt.day.astype(str).str.zfill(2)
        df["filename_silver_layer"] = f"data_{df['year'].iloc[0]}_{df['month'].iloc[0]}_{df['day'].iloc[0]}.csv"
        df["month"] = df["month"].map(rename_values(resources_bucket, RENAME_MONTHS_PATH))

        df["temperature_kelvin"] = df["temperature_celsius"] + 273.15
        df["temperature_fahrenheit"] = (df["temperature_celsius"] * 9/5) + 32
        df["temperature_range"] = df["maximum_temperature"] - df["minimum_temperature"]

        df["weather_descriptions"] = df["weather_descriptions"].astype(str).str.replace(r"\[|\]|'", "", regex=True)
        df["weather_descriptions_es"] = df["weather_descriptions_es"].astype(str).str.replace(r"\[|\]|'", "", regex=True)

        df["timestamp"] = pd.to_datetime(df["date"].astype(str) + " " + df["hour_day"], errors="coerce")
        df["timestamp_unix"] = df["timestamp"].astype(int) // 10**9

        df["day_of_week"] = df["date"].dt.day_name()
        df["week_of_year"] = df["date"].dt.isocalendar().week
        df["season"] = (df["date"].dt.month % 12 + 3) // 3
        df["semester"] = df["date"].dt.quarter
        df["quarter"] = df["date"].dt.quarter
        df["country"] = "Spain"
        df["process_tranformation_date"] = datetime.now().strftime("%Y-%m-%d")

        missing_coords = df[df["latitude"].isnull() | df["longitude"].isnull()]["province"].unique()
        if missing_coords.size > 0:
            log_message("warning", f"🚨 Missing coordinates for: {missing_coords}")
        else:
            log_message("info", "✅ All cities have coordinates.")

        return df
    except Exception as e:
        log_message("error", f"❌ Error in data transformation: {e}")
        return pd.DataFrame()

def merge_files_to_csv(date, files):
    data_frames = [read_json_from_s3(bronze_bucket, f) for f in files]
    data_frames = [df for df in data_frames if not df.empty]

    if not data_frames:
        log_message("warning", f"No data to merge for {date}")
        return

    df_merged = pd.concat(data_frames, ignore_index=True)
    df_transformed = transform_data(df_merged)
    csv_buffer = io.StringIO()
    df_transformed.to_csv(csv_buffer, index=False)

    output_key = f"data_{date}.csv"
    try:
        s3_client.put_object(Bucket=silver_bucket, Key=output_key, Body=csv_buffer.getvalue(), ContentType="text/csv")
        log_message("info", f"✅ CSV {output_key} saved to bucket {silver_bucket}")
    except Exception as e:
        log_message("error", f"❌ Error saving CSV to S3: {e}")

def process_all_dates():
    recent_files = list_recent_files(bronze_bucket)
    date_files_map = {}
    for file in recent_files:
        date = extract_date_from_filename(file)
        if date:
            date_files_map.setdefault(date, []).append(file)

    for date, files in date_files_map.items():
        log_message("info", f"🛠️ Processing date {date} with {len(files)} files")
        merge_files_to_csv(date, files)

if __name__ == "__main__":
    success = True
    log_message("info", "🚀 Starting transformation Glue Job...")
    try:
        process_all_dates()
    except Exception as e:
        log_message("critical", f"❌ Glue Job failed: {e}")
        success = False
    finally:
        final_logs = "\n".join(log_messages)
        write_log_to_s3(final_logs, success)