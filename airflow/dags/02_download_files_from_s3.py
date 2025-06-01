from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.variable import Variable
from datetime import datetime
import os
import boto3
from botocore.exceptions import ClientError

def download_files_from_s3():
    # Инициализация клиента S3
    s3_client = boto3.client(
        's3',
        endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        verify=False
    )
    
    # Получаем bucket из переменной Airflow (по умолчанию "my-first-bucket")
    bucket_name = Variable.get("s3_bucket_name", default_var="my-first-bucket")
    
    # Путь к локальному каталогу
    files_dir = "/opt/airflow/files"
    
    # Проверяем, существует ли локальный каталог, и создаём его, если нет
    os.makedirs(files_dir, exist_ok=True)
    
    try:
        # Получаем список объектов в бакете
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        if 'Contents' not in response:
            print(f"Бакет {bucket_name} пуст или не существует")
            return
        
        # Перебираем все объекты в бакете
        for obj in response['Contents']:
            object_key = obj['Key']
            # Формируем локальный путь для сохранения файла
            local_file_path = os.path.join(files_dir, object_key.replace("/", os.sep))
            
            # Создаём подкаталоги, если они есть в object_key
            os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
            
            try:
                # Скачиваем файл из S3
                s3_client.download_file(
                    Bucket=bucket_name,
                    Key=object_key,
                    Filename=local_file_path
                )
                print(f"Файл {object_key} скачан в {local_file_path}")
                
                # Опционально: удаляем файл из S3 после успешного скачивания
                # s3_client.delete_object(Bucket=bucket_name, Key=object_key)
                # print(f"Файл {object_key} удалён из {bucket_name}")
                
            except ClientError as e:
                print(f"Ошибка при скачивании {object_key}: {e}")
                raise
                
    except ClientError as e:
        print(f"Ошибка при получении списка объектов из {bucket_name}: {e}")
        raise

with DAG(
    dag_id='02_download_files_from_s3',
    start_date=datetime(2025, 5, 30),
    schedule_interval=None,
    catchup=False,
) as dag:
    download_task = PythonOperator(
        task_id='download_files_from_s3',
        python_callable=download_files_from_s3,
    )

    download_task