import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'kutamuralyn',
}

def read_csv_file():
    df = pd.read_csv('/home/kutamuralyn/airflow/insurance.csv')
    print (df)
    
    return df.to_json()


def remove_null_values(**kwargs):
    ti = kwargs['ti']
    
    json_data = ti.xcom_pull(task_ids='read_csv_file_task')
    df = pd.read_json(json_data)
    df = df.dropna()
    print(df)
    
    return df.to_json()

def group_by_smoker(ti):
    json_data = ti.xcom_pull(task_ids='remove_null_values_task')
    df = pd.read_json(json_data)
    smoker_df = df.groupby('smoker').agg({
        'age':'mean',
        'bmi':'mean',
        'charges':'mean'
    }).reset_index()
    
    smoker_df.to_csv(
        '/home/kutamuralyn/airflow/outputs/grouped_by_smoker.csv',index=False
    )

def group_by_region(ti):
    json_data = ti.xcom_pull(task_ids='remove_null_values_task')
    df = pd.read_json(json_data)
    region_df = df.groupby('region').agg({
        'age':'mean',
        'bmi':'mean',
        'charges':'mean'
    }).reset_index()
    
    region_df.to_csv(
        '/home/kutamuralyn/airflow/outputs/grouped_by_region.csv',index=False
    )



with DAG(
    dag_id='executing_python_pipeline',
    description='Executing a Python Pipeline in Airflow',
    default_args=default_args,
    start_date=datetime(2025, 9, 1),
    schedule='@once',
    tags=['python', 'pipeline', 'pandas']
) as dag:
    read_csv = PythonOperator(
        task_id='read_csv_file_task',
        python_callable=read_csv_file
    )
    remove_null_values = PythonOperator(
        task_id='remove_null_values_task',
        python_callable = remove_null_values
    )
    group_by_smoker = PythonOperator(
        task_id='group_by_smoker_task',
        python_callable=group_by_smoker
    )
    group_by_region = PythonOperator(
        task_id='group_by_region_task',
        python_callable=group_by_region
    )
    
read_csv >> remove_null_values >> [group_by_smoker, group_by_region]

