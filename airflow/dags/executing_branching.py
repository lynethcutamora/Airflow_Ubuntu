from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from random import choice

default_args = {
    'owner': 'Kutamuralyn',
}


def has_driving_license():
    return choice([True,False])

def branch(ti):
    if ti.xcom_pull(task_ids = 'has_driving_license'):
        return 'eligible_to_drive' 
    else:
        return 'not_eligible_to_drive' 
    
def eligible_to_drive():
    print("You can drive, you have license!")
        
def not_eligible_to_drive():
    print("I'm afraid you are out of luck, you need license to drive")
    
with DAG(
    dag_id='executing_branching',
    description='Running branchin pipelines',
    default_args=default_args,
    start_date=datetime(2025,9,4),
    schedule='@once',
    tags=['branching', 'pipeline', 'python', 'BranchPythonOperator','conditions']
) as dag:
    taskA = PythonOperator(
        task_id='has_driving_license',
        python_callable = has_driving_license,
    )
    taskB = BranchPythonOperator(
        task_id='branch',
        python_callable=branch
    )
    taskC = PythonOperator(
        task_id='eligible_to_drive',
        python_callable = eligible_to_drive
    )
    taskD = PythonOperator(
        task_id='not_eligible_to_drive',
        python_callable=not_eligible_to_drive
    )
    
taskA>>taskB>>[taskC, taskD]
    