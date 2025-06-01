from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.variable import Variable
from datetime import datetime
import os
import boto3
from botocore.exceptions import ClientError

def upload_files_to_s3():
    # Получаем конфигурацию подключения из Airflow Connection
    # s3_conn = BaseHook.get_connection("s3_connection")
    # s3_client = boto3.client(
    #     's3',
    #     endpoint_url=s3_conn.host,  # Например, "http://minio:9000" или "https://s3.amazonaws.com"
    #     aws_access_key_id=s3_conn.login,
    #     aws_secret_access_key=s3_conn.password,
    #     verify=False if s3_conn.extra_dejson.get("secure", "False").lower() == "false" else True  # SSL отключение, если secure=False
    # )
    s3_client = boto3.client(
        's3',
        endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        verify=False
    )
    
    # Получаем bucket из переменной Airflow (по умолчанию "my-first-bucket")
    bucket_name = Variable.get("s3_bucket_name", default_var="my-first-bucket")
    
    # Путь к каталогу files
    files_dir = "/opt/airflow/files"
    
    # Создаем бакет, если он не существует
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"Бакет {bucket_name} уже существует")
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            s3_client.create_bucket(Bucket=bucket_name)
            print(f"Бакет {bucket_name} создан")
        else:
            print(f"Ошибка при проверке/создании бакета: {e}")
            raise

    # Перечисляем все файлы в /opt/airflow/files (включая подкаталоги)
    for root, dirs, files in os.walk(files_dir):
        for file_name in files:
            file_path = os.path.join(root, file_name)
            # Формируем объектное имя в S3 (сохраняем структуру подкаталогов)
            object_name = os.path.relpath(file_path, files_dir).replace(os.sep, "/")
            
            try:
                # Загружаем файл в S3
                s3_client.upload_file(
                    Filename=file_path,
                    Bucket=bucket_name,
                    Key=object_name
                )
                print(f"Файл {file_path} загружен в {bucket_name}/{object_name}")
                
                # Удаляем файл после успешной загрузки (для "перемещения")
                # os.remove(file_path)
                # print(f"Файл {file_path} удален из {files_dir}")
            except ClientError as e:
                print(f"Ошибка при загрузке {file_path}: {e}")
                raise

with DAG(
    dag_id='01a_upload_files_to_s3',
    start_date=datetime(2025, 5, 30),
    schedule_interval=None,
    catchup=False,
) as dag:
    upload_task = PythonOperator(
        task_id='upload_files_to_s3',
        python_callable=upload_files_to_s3,
    )

    upload_task