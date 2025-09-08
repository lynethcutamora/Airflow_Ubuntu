from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'kutamuralyn',
    'depends_on_past': False,
    'email_on_failure': False
}

with DAG(
    dag_id="hello_dag",
    start_date=datetime(2025, 1, 1),
    default_args=default_args,
    schedule='@daily',
    tags=['beginner','bash','hello world'],
    catchup=False,
) as dag:
    task = BashOperator(
        task_id='Hello_world_task',
        bash_command='echo "Hello World once again!"',
    )

task