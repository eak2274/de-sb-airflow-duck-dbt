from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
from minio import Minio
from minio.error import S3Error

def upload_files_to_minio():
    # Настройки MinIO
    minio_client = Minio(
        endpoint="minio:9000",  # Имя сервиса в docker-compose
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False  # Отключаем HTTPS для локального MinIO
    )
    bucket_name = "my-first-bucket"
    
    # Путь к каталогу files
    files_dir = "/opt/airflow/files"
    
    # Создаем бакет, если он не существует
    try:
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
            print(f"Бакет {bucket_name} создан")
        else:
            print(f"Бакет {bucket_name} уже существует")
    except S3Error as e:
        print(f"Ошибка при создании бакета: {e}")
        raise

    # Перечисляем все файлы в /opt/airflow/files (включая подкаталоги)
    for root, dirs, files in os.walk(files_dir):
        for file_name in files:
            file_path = os.path.join(root, file_name)
            # Формируем объектное имя в MinIO (сохраняем структуру подкаталогов)
            object_name = os.path.relpath(file_path, files_dir).replace(os.sep, "/")
            
            try:
                # Загружаем файл в MinIO
                minio_client.fput_object(
                    bucket_name=bucket_name,
                    object_name=object_name,
                    file_path=file_path
                )
                print(f"Файл {file_path} загружен в {bucket_name}/{object_name}")
                
                # Удаляем файл после успешной загрузки (для "перемещения")
                # os.remove(file_path)
                # print(f"Файл {file_path} удален из {files_dir}")
            except S3Error as e:
                print(f"Ошибка при загрузке {file_path}: {e}")
                raise

with DAG(
    dag_id='01_upload_files_to_minio',
    start_date=datetime(2025, 5, 30),
    schedule_interval=None,
    catchup=False,
) as dag:
    upload_task = PythonOperator(
        task_id='upload_files_to_minio',
        python_callable=upload_files_to_minio,
    )

    upload_task