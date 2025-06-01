# Базовый образ
FROM apache/airflow:2.10.5

# Обновление pip до последней версии
RUN pip install --upgrade pip

# Установка дополнительных Python-пакетов
RUN pip install --no-cache-dir \
    minio==7.2.15 \
    boto3==1.38.27 \
    # Убираем pendulum, так как он уже включен в Airflow
    # pendulum==2.1.2 \
    duckdb

# Настройка рабочей директории
WORKDIR /opt/airflow

# Копирование пользовательских файлов (если есть кастомные скрипты или плагины)
# COPY ./plugins /opt/airflow/plugins  # Разкомментируй, если нужно копировать плагины

# Установка пользователя
USER ${AIRFLOW_UID:-50000}:${AIRFLOW_GID:-50000}