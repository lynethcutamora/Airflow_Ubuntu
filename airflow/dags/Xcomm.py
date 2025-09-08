from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'kutamuralyn',
}

def increment_by1 (value):
    print("Value: {value}!".format(value=value))
    
    return value + 1

def multiply_by100(ti):
    value = ti.xcom_pull(task_ids='increment_by1_task')
    print("Value: {value}!".format(value=value))
    
    return value * 100

def subtract_9(ti):
    value = ti.xcom_pull(task_ids='multiply_by100_task')
    print("Value: {value}!".format(value=value))
    
    return value - 9

def print_value(ti):
    value = ti.xcom_pull(task_ids='subtract_9_task')
    
    print("Value: {value}!".format(value=value))
    

with DAG(
    dag_id='Xcom_dag',
    description='XCom in PythonOperator',
    default_args=default_args,
    start_date=datetime(2025, 9, 1),
    schedule='@daily',
    tags=['xcom','python_operator','cross_task_communication','python' ]
) as dag:
    increment_by1 = PythonOperator(
        task_id='increment_by1_task',
        python_callable=increment_by1,
        op_kwargs={'value':1}
    ),
    multiply_by100 = PythonOperator(
        task_id='multiply_by100_task',
        python_callable=multiply_by100
    )
    subtract_9 = PythonOperator(
        task_id='subtract_9_task',
        python_callable=subtract_9
    )
    print_value = PythonOperator(
        task_id='print_value_task',
        python_callable=print_value
    )
    
increment_by1 >> multiply_by100 >> subtract_9 >> print_value
