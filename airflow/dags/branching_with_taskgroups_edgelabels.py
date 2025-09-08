from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
import pandas as pd
from airflow.utils.edgemodifier import Label

default_args = {
    'owner': 'Kutamuralyn',
}

DATASET_PATH = '/home/kutamuralyn/airflow/insurance.csv'
OUTPUT_PATH = '/home/kutamuralyn/airflow/outputs/{0}.csv'

def read_csv_file(ti):
    df = pd.read_csv(DATASET_PATH)
    print (df)
    
    ti.xcom_push(key='my_csv', value=df.to_json())


def remove_null_values(ti):
    json_data = ti.xcom_pull(
        task_ids='reading_and_reprocessing.read_csv_file_task',
        key='my_csv'
    )
    
    df = pd.read_json(json_data)
    df = df.dropna()
    print(df)
    
    ti.xcom_push(key='my_cleaned_csv', value=df.to_json())


def determine_branch():
    transform_action = Variable.get('transform_action', default_var=None)
    
    print(transform_action)
    
    if transform_action.startswith('filter'):
        return "filtering.{0}".format(transform_action)
    elif transform_action == 'group_by_region_smoker':
        return "grouping.{0}".format(transform_action)
    
    
def filter_by_southwest(ti):
    json_data = ti.xcom_pull(task_ids='reading_and_reprocessing.remove_null_values_task', key='my_cleaned_csv')
    df = pd.read_json(json_data)
    
    region_df = df[df['region']=='southwest']
    
    region_df.to_csv(
        OUTPUT_PATH.format('southwest'), index=False
    )
    
    
def filter_by_southeast(ti):
    json_data = ti.xcom_pull(task_ids='reading_and_reprocessing.remove_null_values_task', key='my_cleaned_csv')
    df = pd.read_json(json_data)
    
    region_df = df[df['region']=='southeast']
    
    region_df.to_csv(
        OUTPUT_PATH.format('southeast'), index=False
    )

def filter_by_northwest(ti):
    json_data = ti.xcom_pull(task_ids='reading_and_reprocessing.remove_null_values_task', key='my_cleaned_csv')
    df = pd.read_json(json_data)
    
    region_df = df[df['region']=='northwest']
    
    region_df.to_csv(
        OUTPUT_PATH.format('northwest'), index=False
    )

def group_by_region_smoker(ti):
    json_data = ti.xcom_pull(task_ids='reading_and_reprocessing.remove_null_values_task', key='my_cleaned_csv')
    df = pd.read_json(json_data)
    
    region_df = df.groupby(['region','smoker']).agg({
        'age':'mean',
        'bmi':'mean',
        'charges':'mean'
    }).reset_index()
    
    region_df.to_csv(
        OUTPUT_PATH.format('group_by_region_smoker'), index=False
    )
    
    smoker_df = df.groupby('smoker').agg({
        'age':'mean',
        'bmi':'mean',
        'charges':'mean'
    }).reset_index()
    
    smoker_df.to_csv(
        OUTPUT_PATH.format('group_by_smoker'),index=False
    )
    
with DAG(
    dag_id='branching_with_taskgroups_edgelabels',
    description='Running branching pipelines with Airflow Variables',
    default_args=default_args,
    start_date=datetime(2025,9,4),
    schedule='@once',
    tags=['branching', 'pipeline', 'python', 'BranchPythonOperator','conditions','variables']
) as dag:
    
    with TaskGroup('reading_and_reprocessing') as reading_and_reprocessing:
            
        read_csv = PythonOperator(
            task_id='read_csv_file_task',
            python_callable=read_csv_file
        )
        remove_null_values = PythonOperator(
            task_id='remove_null_values_task',
            python_callable = remove_null_values
        )
        
        read_csv >> remove_null_values
        
    determine_branch = BranchPythonOperator(
        task_id='determine_branch_task',
        python_callable=determine_branch
    )
    
    with TaskGroup('filtering') as filtering:
        
        filter_by_southwest = PythonOperator(
            task_id='filter_by_southwest',
            python_callable=filter_by_southwest
        )
        filter_by_southeast = PythonOperator(
            task_id='filter_by_southeast',
            python_callable=filter_by_southeast
        )
        filter_by_northwest = PythonOperator(
            task_id='filter_by_northwest',
            python_callable=filter_by_northwest
        )

    with TaskGroup('grouping') as grouping:

        group_by_region_smoker = PythonOperator(
            task_id='group_by_region_smoker',
            python_callable=group_by_region_smoker
        )

reading_and_reprocessing >> Label('preprocessed data') >> determine_branch >> Label('branch on condition')>>[filtering,grouping]
