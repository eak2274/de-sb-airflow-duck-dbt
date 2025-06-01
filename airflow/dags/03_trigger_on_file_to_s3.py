from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging
from custom_file_sensor import CustomFileSensor  # Импорт кастомного сенсора
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import os
import logging
import boto3
from botocore.exceptions import ClientError

# Инициализируем логгер
logger = logging.getLogger(__name__)

# Путь к директории, за которой следим
FILES_DIR = "/opt/airflow/files"
FILE_PATTERN = os.path.join(FILES_DIR, "*")  # любой файл в этой директории

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 4, 5),
    "retries": 1,
}

def upload_file_to_s3(**context):
    """
    Функция для загрузки найденного файла в S3.
    Получает путь к файлу из XCom после срабатывания FileSensor.
    """
    ti = context["task_instance"]
    file_path = ti.xcom_pull(task_ids="wait_for_file", key="file_path")

    logger.info(f"XCom value from wait_for_file: {file_path}")

    if not file_path:
        logger.warning("No file path received from FileSensor, skipping upload")
        return

    s3_client = boto3.client(
        's3',
        endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        verify=False
    )

    bucket_name = Variable.get("s3_bucket_name", default_var="my-first-bucket")

    try:
        s3_client.head_bucket(Bucket=bucket_name)
        logger.info(f"Bucket {bucket_name} already exists")
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            s3_client.create_bucket(Bucket=bucket_name)
            logger.info(f"Bucket {bucket_name} created")
        else:
            logger.error(f"Error checking/creating bucket: {e}")
            raise

    object_name = os.path.relpath(file_path, FILES_DIR).replace(os.sep, "/")

    try:
        s3_client.head_object(Bucket=bucket_name, Key=object_name)
        logger.info(f"Object {object_name} already exists in {bucket_name}, skipping upload")
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            s3_client.upload_file(Filename=file_path, Bucket=bucket_name, Key=object_name)
            logger.info(f"File {file_path} uploaded to {bucket_name}/{object_name}")
            # os.remove(file_path)  # Опционально: раскомментируйте для удаления файла после загрузки
        else:
            logger.error(f"Error checking object {object_name}: {e}")
            raise

# Определение DAG
with DAG(
    dag_id="03_trigger_on_file_to_s3",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["test", "xcom", "filesensor"],
) as dag:

    wait_for_file = CustomFileSensor(  # Используем кастомный сенсор вместо стандартного FileSensor
        task_id="wait_for_file",
        filepath=FILE_PATTERN,
        poke_interval=10,
        timeout=60 * 5,
        mode="poke",
    )

    upload_to_s3 = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_file_to_s3,
        provide_context=True,
    )

    wait_for_file >> upload_to_s3
