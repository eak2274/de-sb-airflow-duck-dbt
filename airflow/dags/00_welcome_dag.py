from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import requests


def print_welcome():
    print('Welcome to Airflow!')


def print_welcome_with_args(name_1, name_2):
    print(f"We'd like to welcome our dear guests, {name_1} and {name_2}!")


def print_date():
    print('Today is {}'.format(datetime.today().date()))


def print_random_quote():
    response = requests.get('https://zenquotes.io/api/random')
    quote = response.json()[0]['q']
    print('Quote of the day: "{}"'.format(quote))


default_args = {
    "owner": "eak74",
    "retries": 5,
    "retry_delay": timedelta(minutes=1)
}


dag = DAG(
    '00_welcome_dag',
    default_args=default_args,
    schedule_interval='0 23 * * *',
    start_date=datetime(2024, 10, 19, 15),
    catchup=False
)


print_welcome_task = PythonOperator(
    task_id='print_welcome',
    python_callable=print_welcome,
    dag=dag
)

print_welcome_with_args_task = PythonOperator(
    task_id='print_welcome_with_args',
    python_callable=print_welcome_with_args,
    op_kwargs={"name_1": "Johnny", "name_2": "Molly"},
    dag=dag
)


print_date_task = PythonOperator(
    task_id='print_date',
    python_callable=print_date,
    dag=dag
)

print_random_quote = PythonOperator(
    task_id='print_random_quote',
    python_callable=print_random_quote,
    dag=dag
)


# Set the dependencies between the tasks
print_welcome_task >> print_date_task >> print_random_quote
print_welcome_task >> print_welcome_with_args_task
