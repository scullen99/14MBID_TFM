# AWS Datalake
## Sergio Esteban Tarrero
### VIU - 14MBID_10_Z_2024-25_Trabajo Fin de Máster (Big Data y Ciencia de Datos)

Este repositorio contiene el proyecto **AWS Datalake**, un sistema completo de ingestión, procesamiento y consumo de datos meteorológicos basado en la infraestructura de Amazon Web Services (AWS). La automatización de recursos y despliegue se gestiona mediante plantillas de **AWS CloudFormation**.

---

## ¿Qué recursos o servicios se utilizan?

A continuación, se resumen los principales servicios de AWS utilizados y su propósito en el proyecto:

- **S3**: Almacenamiento de datos en distintas capas (bronze, silver, gold) y recursos estáticos (scripts, configuraciones, logs).
- **AWS Glue**: Servicio de ETL para realizar la ingestión, transformación y preparación de los datos.
- **Lambda**: Automatización y monitorización de procesos a través de funciones sin servidor.
- **Secrets Manager**: Almacenamiento seguro de secretos como nombres de buckets y claves de APIs.
- **Step Functions**: Orquestación de flujos de trabajo ETL de manera escalable y estructurada.
- **EventBridge**: Automatización de ejecuciones mediante eventos o programaciones temporales.
- **CloudFormation**: Infraestructura como código (IaC) para el despliegue automatizado de todos los recursos anteriores.

---

## Estructura de Carpetas

```
AWS_Datalake/
├── .git/                        # Control de versiones con Git
├── aws_data_lake/
│   ├── cloudformation/          # Plantillas CloudFormation por servicio
│   │   ├── eventbridge/         # Configuraciones para EventBridge
│   │   ├── glue_data_catalog/   # Definición de catálogos de Glue
│   │   ├── glue_jobs/           # Definición de Glue Jobs
│   │   ├── lambda/              # Creación de funciones Lambda
│   │   ├── s3/                  # Buckets de almacenamiento
│   │   ├── secrets_manager/     # Manejo de secretos
│   │   └── step_functions/      # Definición de Step Functions
│   └── resources/               # Recursos usados por el datalake
│       ├── jsons/               # Archivos JSON de configuración
│       ├── python_dependencies/ # Librerías Python empaquetadas
│       ├── python_glue_scripts/ # Scripts de AWS Glue
│       └── python_lambda_scripts/ # Códigos para funciones Lambda
├── upload_resources_to_bucket/  # Utilidad para cargar recursos a S3
│   ├── csv/                     # CSVs para procesamiento
│   ├── jsons/                   # JSONs auxiliares
│   └── python/                  # Scripts y zips de dependencias
```

---

## Descripción de Carpetas y Archivos Principales

### 1. `.git/`

Contiene la configuración y objetos de Git para el control de versiones del proyecto.

### 2. `aws_data_lake/`

Carpeta principal que estructura todo el desarrollo de infraestructura como código y recursos.

- **`cloudformation/`**: Archivos YAML que definen todos los recursos de AWS.
- **`resources/`**: Ficheros de apoyo (JSONs, scripts de Glue y Lambda, dependencias Python).

### 3. `upload_resources_to_bucket/`

Scripts auxiliares para subir recursos al bucket de S3.

### 4. Archivo destacado

- `upload_resourcers_bucket.py`: Script que automatiza la carga de recursos a S3.

---

## Requisitos

- Cuenta AWS con permisos suficientes.
- AWS CLI configurado.
- Python 3.8+
- Librerías Boto3, botocore y similares.

---

## Instalación y despliegue rápido

```bash
# Clonar el repositorio
$ git clone <repositorio>

# Subir recursos estáticos
$ cd upload_resources_to_bucket
$ python upload_resourcers_bucket.py

# Desplegar infraestructura
$ aws cloudformation deploy --template-file aws_data_lake/cloudformation/s3/s3_buckets.yaml --stack-name s3-buckets
$ aws cloudformation deploy --template-file aws_data_lake/cloudformation/glue_jobs/glue_jobs.yaml --stack-name glue-jobs
...
```

> **Nota:** El orden de despliegue es importante: primero S3, luego Secrets Manager, luego Glue Jobs.