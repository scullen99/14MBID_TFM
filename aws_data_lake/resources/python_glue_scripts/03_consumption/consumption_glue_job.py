"""
Este script pertenece a la fase de consumo del Data Lake de AWS.
Los datos transformados en la capa Silver son optimizados y almacenados en la capa Gold 
para consultas eficientes en herramientas como AWS Athena y QuickSight.

# Descripción del proceso:

1. Obtención de los buckets de origen y destino:
   - Recupera los nombres de los buckets Silver y Gold desde AWS Secrets Manager.
   - Recupera el bucket de recursos, donde pueden almacenarse configuraciones adicionales.

2. Selección de archivos recientes:
   - Lista los archivos en el bucket Silver creados hace menos de 1 hora.
   - Filtra los archivos a procesar y los agrupa por fecha (YYYY_MM_DD).

3. Conversión de CSV a Parquet con particionamiento:
   - Carga los archivos CSV en DataFrames de pandas.
   - Aplica particionamiento por `year`, `month`, `day` y `province` para optimizar consultas en Athena.
   - Evita duplicados en AWS Glue:  
     - Crea columnas temporales (`_year`, `_month`, `_day`, `_province`) en los datos.  
     - Elimina `year`, `month`, `day`, `province` antes de guardar en Parquet.  
     - Athena reconoce `year`, `month`, `day`, `province` como particiones sin errores.  
   - Guarda los archivos en formato Parquet en el bucket Gold.

4. Normalización de datos:
   - Elimina tildes y caracteres especiales en la columna `province` para evitar errores en consultas SQL.
   - Añade `consumption_date` con la fecha y hora de transformación Silver → Gold.
   - Genera `gold_file`, el nombre del archivo Parquet basado en `year`, `month`, `day`, `province`.

5. Registro de logs:
   - Se registran mensajes en CloudWatch y en un bucket de S3 de logs.
   - Si el proceso falla, se genera un log de error con detalles del problema.

# Consideraciones:
- Este script no elimina archivos del bucket Silver.
- La estructura de particionado se optimiza para AWS Athena y QuickSight.
- Se usa `pyarrow` para la generación de archivos Parquet.
- Los logs son almacenados en un bucket de S3 definido en AWS Secrets Manager.

# Formato de almacenamiento en S3:
Los datos Parquet se guardan con particiones jerárquicas en el bucket Gold:
s3://datalake-gold-consumption-{region}-{account_id}/year=YYYY/month=MM/day=DD/province=province/gold_<YYYY>_<MM>_<DD>_<province>.parquet

Fecha de última actualización: 14/04/2025
Autor: Sergio Esteban Tarrero  
Layer: Gold
"""

import os
import sys
import json
import boto3
import logging
import pandas as pd
from datetime import datetime, timezone, timedelta
from botocore.exceptions import ClientError

HOURS_AGO: int = 2
PARTITION_COLUMN: str = "date"   # Columna para particionar en formato 'YYYY-MM-DD'
PARTITION_FORMAT: str = "%Y-%m-%d"  # Formato de fecha para particionar

# Layer actual
LAYER: str = "gold"

# Configuración de logging
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Inicializar clientes de AWS
session = boto3.session.Session()
s3_client = session.client("s3")
secrets_client = session.client("secretsmanager")

# Lista para almacenar logs en memoria
log_messages = []


# Función para registrar mensajes de log
def log_message(level, message):
    global log_messages
    formatted_message = f"{datetime.now()} - {level.upper()} - {message}"
    log_messages.append(formatted_message)

    if level.lower() == "info":
        logging.info(message)
        print(message)
    elif level.lower() == "warning":
        logging.warning(message)
        print(message)
    elif level.lower() == "error":
        logging.error(message)
        print(message)
    elif level.lower() == "critical":
        logging.critical(message)
        print(message)


# Función para guardar los logs en el bucket de logs
def write_log_to_s3(success=True):
    try:
        logs_secret = get_secret("s3_bucket_logs_storage")
        log_bucket = logs_secret.get("s3_bucket")

        log_type = "success" if success else "failure"
        log_key = f"glue_logs/{log_type}/consumption/consumption_{datetime.now().strftime('%Y_%m_%d_%H_%M_%S')}.log"

        log_message("info", f"📤 Writing log to S3: s3://{log_bucket}/{log_key}")

        log_content = "\n".join(log_messages)
        s3_client.put_object(Bucket=log_bucket, Key=log_key, Body=log_content)

        log_message("info", f"✅ Log successfully written to s3://{log_bucket}/{log_key}")

    except Exception as e:
        log_message("error", f"❌ Error writing log to S3: {e}")


# Función para obtener secretos desde AWS Secrets Manager
def get_secret(secret_name):
    try:
        log_message("info", f"🔍 Retrieving secret: {secret_name}")
        response = secrets_client.get_secret_value(SecretId=secret_name)
        secret = json.loads(response["SecretString"])
        log_message("info", f"✅ Secret '{secret_name}' retrieved successfully.")
        return secret
    except ClientError as e:
        log_message("critical", f"❌ Error retrieving secret '{secret_name}': {e}")
        raise


# Función para listar objetos en un bucket S3
def list_s3_objects(bucket_name, prefix=""):
    try:
        log_message("info", f"🔍 Listing objects in bucket: {bucket_name} with prefix: '{prefix}'")
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        objects = response.get("Contents", [])
        log_message("info", f"✅ Found {len(objects)} objects in bucket: {bucket_name}")
        return objects
    except ClientError as e:
        log_message("critical", f"❌ Error listing objects in bucket {bucket_name}: {e}")
        raise


# Función para leer un CSV desde S3 y devolver un DataFrame
def read_csv_from_s3(bucket_name, key):
    try:
        log_message("info", f"🔍 Reading CSV file from S3: s3://{bucket_name}/{key}")
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        df = pd.read_csv(response["Body"])

        # Eliminar columnas duplicadas si existen
        df = df.loc[:, ~df.columns.duplicated()]
        
        log_message("info", f"✅ Successfully read file: {key} ({df.shape[0]} rows)")
        return df
    except ClientError as e:
        log_message("error", f"❌ Error reading CSV file s3://{bucket_name}/{key}: {e}")
        return pd.DataFrame()

def map_month(x):
    try:
        # Si el valor ya está en formato de mes textual, devolverlo directamente
        if x.lower() in ["january", "february", "march", "april", "may", "june", "july", "august", "september", "october", "november", "december"]:
            return x.lower()
        # Si es un número, convertirlo al nombre del mes
        return datetime.strptime(str(x), "%m").strftime("%B").lower()
    except ValueError:
        # Si ocurre un error, retornar el valor original (evita fallos)
        return str(x)

def write_parquet_to_s3(df, bucket_name):
    try:
        df["province"] = df["province"].astype(str).str.replace(" ", "_")

        print(list(sorted(df["province"].unique())))
        print(len(list(sorted(df["province"].unique()))))

        # mapear los valores de la columna "day" de 1 a 9 a 01 a 09
        #df["day"] = df["day"].apply(lambda x: f"0{x}" if x < 10 else x)
        df["day"] = df["day"].apply(lambda x: f"0{x}" if isinstance(x, int) and x < 10 else str(x))

        # mapear los valores de la columna "month" de 1 a january, 2 a february, etc
        df["month"] = df["month"].apply(map_month)

        partition_columns = ["year", "month", "day", "province"]
        temp_columns = ["_year", "_month", "_day", "_province"]
        
        log_message("info", f"🔍 Converting DataFrame to Parquet with partitioning: {partition_columns}")

        # Asegurar que las columnas de partición sean strings
        for col in partition_columns:
            df[col] = df[col].astype(str)

        log_message("info", f"🔍 Partitioning DataFrame by columns: {partition_columns}")

        # Crear versiones temporales de las columnas de partición
        for original, temp in zip(partition_columns, temp_columns):
            df[temp] = df[original]

        log_message("info", f"🔍 Created temporary columns: {temp_columns}")

        # Quitar todas las tildes de los valores de la columna "province"
        df["_province"] = df["_province"].str.normalize("NFKD").str.encode("ascii", errors="ignore").str.decode("utf-8")
        log_message("info", f"🔍 Normalized '_province' column values, new values: {list(df['_province'].unique())}")

        # Renombrar los valores de la columna "layer" a "gold"
        df["current_layer"] = LAYER

        # Crear columna "consumption_date" con la fecha actual (silver -> gold)
        df["consumption_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_message("info", f"🔍 Added 'consumption_date' column with value {df['consumption_date'].iloc[0]}")

        # Crear columna con el nombre del fichero en la capa gold
        df["filename_gold_layer"] = df.apply(lambda x: f"gold_{x['_year']}_{x['_month']}_{x['_day']}_{x['_province']}.parquet", axis=1)
        log_message("info", f"🔍 Added 'filename_gold_layer' column with values")

        # Forzar el tipo de precipitación a "int64" para que coincida con Glue
        if "precipitation" in df.columns:
            if df["precipitation"].isnull().any():
                df["precipitation"] = df["precipitation"].fillna(0)
                log_message("warning", "⚠️ La columna 'precipitation' tenía valores nulos. Se reemplazaron por 0.")
                
            df["precipitation"] = df["precipitation"].astype("int64")
            log_message("info", "🔍 Columna 'precipitation' convertida a int64")

        # Eliminar las columnas originales de partición para evitar duplicados en Glue
        df = df.drop(columns=partition_columns, errors="ignore")
        log_message("info", f"🔍 Dropped original partition columns: {partition_columns}")

        # Ordenar columnas por orden alfabético
        df = df.reindex(sorted(df.columns), axis=1)
        log_message("info", f"🔍 Sorted DataFrame columns alphabetically")

        unique_partitions = df[temp_columns].drop_duplicates()
        log_message("info", f"🔍 Found {len(unique_partitions)} unique partitions")

        # Verificar si hay particiones antes de iterar
        if unique_partitions.empty:
            log_message("error", "❌ No partitions found. Check data integrity!")
            return

        for _, row in unique_partitions.iterrows():
            year, month, day, province = row["_year"], row["_month"], row["_day"], row["_province"]

            # Filtrar los datos para la partición actual (usando columnas temporales)
            partition_df = df[
                (df["_year"] == year) &
                (df["_month"] == month) &
                (df["_day"] == day) &
                (df["_province"] == province)
            ]

            # Crear la ruta de almacenamiento en S3 con particionamiento
            partition_key = f"year={year}/month={month}/day={day}/province={province}/gold_{year}_{month}_{day}_{province}.parquet"

            log_message("info", f"🔍 Writing Parquet to S3: s3://{bucket_name}/{partition_key}")

            # Convertir a Parquet usando `pyarrow` para mejor compatibilidad con Athena
            parquet_buffer = partition_df.to_parquet(index=False, engine="pyarrow", compression="snappy")

            # Subir a S3
            s3_client.put_object(Bucket=bucket_name, Key=partition_key, Body=parquet_buffer)

            log_message("info", f"✅ Parquet written to s3://{bucket_name}/{partition_key} ({len(partition_df)} rows)")

    except ClientError as e:
        log_message("error", f"❌ Error writing Parquet to S3: {e}")
        raise

# Función principal del trabajo de consumo (Silver -> Gold)
def main():
    log_message("info", "🚀 Starting Gold consumption job...")
    try:
        # Obtener nombres de buckets desde Secrets Manager
        silver_secret = get_secret("s3_bucket_silver_transformation")
        gold_secret = get_secret("s3_bucket_gold_consumption")
        #resources_secret = get_secret("s3_bucket_resources_storage")

        silver_bucket = silver_secret["s3_bucket"]
        gold_bucket = gold_secret["s3_bucket"]
        #resources_bucket = resources_secret["s3_bucket"]

        # Listar objetos en el bucket Silver
        objects = list_s3_objects(silver_bucket)

        # Filtrar archivos creados hace menos de "X" horas
        now = datetime.now(timezone.utc)
        hours_ago = now - timedelta(hours=HOURS_AGO)
        recent_objects = [obj for obj in objects if obj.get("LastModified", now) >= hours_ago]

        log_message("info", f"🔍 Found {len(recent_objects)} files modified after {hours_ago.isoformat()}")

        if not recent_objects:
            log_message("info", "ℹ️ No recent files found in the Silver bucket. Exiting.")
            return

        # Procesar archivos recientes y convertirlos a Parquet con particionamiento
        for obj in recent_objects:
            key = obj["Key"]
            df = read_csv_from_s3(silver_bucket, key)

            if df.empty:
                log_message("warning", f"⚠️ Skipping empty DataFrame from {key}")
                continue

            # Convertir a Parquet con particionamiento en la capa Gold
            write_parquet_to_s3(df, gold_bucket)

        log_message("info", "🚀 Gold consumption job completed successfully.")
        write_log_to_s3(success=True)

    except Exception as e:
        log_message("critical", f"❌ Gold consumption job failed: {e}")
        write_log_to_s3(success=False)
        raise


if __name__ == "__main__":
    success = True
    try:
        main()
    except Exception as e:
        success = False