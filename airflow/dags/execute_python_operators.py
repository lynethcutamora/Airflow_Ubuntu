import time

from datetime import datetime, timedelta
from airflow import DAG

from airflow.operators.python import PythonOperator

default_args = {
    'owner' : 'kutamuralyn'
}

def greet_hello(name):
    print("Hello, {name}!".format(name=name))
    
def greet_hello_with_city(name, city):
    print("Hello, {name} from {city}!".format(name=name, city=city))
    
with DAG(
    dag_id='execute_python_operators',
    description='Python operators in DAGs with parameters',
    default_args=default_args,
    start_date=datetime(2025, 9, 1),
    schedule='@daily',
    tags=['parameters', 'python_operator']   
) as dag:
    task1 = PythonOperator(
        task_id='greet_hello_task',
        python_callable=greet_hello,
        op_kwargs={'name':'Alice'}
    ),
    task2 = PythonOperator(
        task_id='greet_hello_with_city_task',
        python_callable=greet_hello_with_city,
        op_kwargs={'name':'Lyneth', 'city':'Cebu City'}
    )
    
task1 >> task2