import boto3
import os
from botocore.exceptions import ClientError

# Configura tu región si no la tienes por defecto en ~/.aws/config
s3_client = boto3.client('s3', region_name='eu-west-1')

# Configuración de rutas
base_local_path = "/home/sergio/Escritorio/AWS_Datalake/upload_resources_to_bucket"
bucket_name = "datalake-resources-storage-eu-west-1"

upload_map = {
    "csv": "csv/",
    "jsons": "jsons/",
    "python": "python_glue_scripts/"
}

def file_exists_in_s3(bucket, key):
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            raise

def upload_file_if_needed(local_path, s3_key):
    if file_exists_in_s3(bucket_name, s3_key):
        print(f"[SKIPPING] Ya existe en S3: {s3_key}, se omite.")
    else:
        print(f"⬆️ Subiendo: {s3_key}")
        s3_client.upload_file(local_path, bucket_name, s3_key)
        print(f"✅ Subido correctamente: {s3_key}")

def process_folder(local_folder, s3_prefix):
    full_local_path = os.path.join(base_local_path, local_folder)
    for root, dirs, files in os.walk(full_local_path):
        for file in files:
            local_file_path = os.path.join(root, file)
            relative_path = os.path.relpath(local_file_path, full_local_path)
            s3_key = os.path.join(s3_prefix, relative_path).replace("\\", "/")
            upload_file_if_needed(local_file_path, s3_key)

def main():
    for folder, s3_prefix in upload_map.items():
        print(f"\n📂 Procesando carpeta local: {folder} → s3://{bucket_name}/{s3_prefix}")
        process_folder(folder, s3_prefix)

if __name__ == "__main__":
    main()