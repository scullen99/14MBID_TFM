import json
import os
import boto3
import http.client
import urllib.parse
import datetime

# Inicializar clientes de AWS
s3_client = boto3.client("s3")

AWS_REGION = os.getenv("AWS_REGION")
AWS_ACCOUNT_ID = os.getenv("AWS_ACCOUNT_ID")

# Lista de buckets a analizar
BUCKETS = [
    f"datalake-bronze-ingest-{AWS_REGION}-{AWS_ACCOUNT_ID}",
    f"datalake-silver-transformation-{AWS_REGION}-{AWS_ACCOUNT_ID}",
    f"datalake-gold-consumption-{AWS_REGION}-{AWS_ACCOUNT_ID}"
]

# Obtener la URL del webhook de Slack desde las variables de entorno
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL_NOTIFY_S3_BUCKETS")


def get_bucket_size_and_files(bucket_name):
    """
    Obtiene el número de objetos y el tamaño total de un bucket S3.
    """
    total_size_bytes = 0
    total_files = 0

    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket_name)

    for page in pages:
        if "Contents" in page:
            for obj in page["Contents"]:
                total_size_bytes += obj["Size"]
                total_files += 1

    # Convertir bytes a MB y GB
    total_size_mb = round(total_size_bytes / (1024 * 1024), 2)
    total_size_gb = round(total_size_mb / 1024, 2)

    return total_files, total_size_mb, total_size_gb


def send_slack_notification(bucket_info):
    """
    Envía una notificación a Slack con la información de los buckets.
    """
    current_datetime = (datetime.datetime.now() + datetime.timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")

    attachments = []
    for bucket in bucket_info:
        attachments.append({
            "color": "#626F47",
            "title": f"S3 Bucket: {bucket['name'][:-22] + '*' * 22}",
            "fields": [
                {"title": "Fecha y Hora:", "value": current_datetime, "short": True},
                {"title": "Tamaño (GB):", "value": f"{bucket['size_gb']} GB", "short": True},
                {"title": "Nº archivos:", "value": f"{bucket['files']}", "short": True},
                {"title": "Tamaño (MB):", "value": f"{bucket['size_mb']} MB", "short": True}
            ],
            "footer": "AWS Lambda Notification"
        })

    message = {"attachments": attachments}

    # Enviar mensaje a Slack
    parsed_url = urllib.parse.urlparse(SLACK_WEBHOOK_URL)
    conn = http.client.HTTPSConnection(parsed_url.netloc)
    headers = {"Content-Type": "application/json"}
    conn.request("POST", parsed_url.path, body=json.dumps(message), headers=headers)
    response = conn.getresponse()

    return response.status


def lambda_handler(event, context):
    """
    Función principal que recopila información de los buckets y envía notificación a Slack.
    """
    bucket_info = []

    for bucket in BUCKETS:
        try:
            total_files, total_size_mb, total_size_gb = get_bucket_size_and_files(bucket)
            bucket_info.append({
                "name": bucket,
                "files": total_files,
                "size_mb": total_size_mb,
                "size_gb": total_size_gb
            })
        except Exception as e:
            print(f"Error obteniendo datos del bucket {bucket}: {str(e)}")

    if bucket_info:
        send_slack_notification(bucket_info)

    return {
        "statusCode": 200,
        "body": json.dumps({"message": "Notificación enviada a Slack", "buckets": bucket_info})
    }