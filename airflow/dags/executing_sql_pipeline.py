from airflow import DAG
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'kutamuralyn',
}

with DAG(
    dag_id='executing_sql_pipeline',
    description='Executing a SQL Pipeline in Airflow',
    default_args=default_args,
    start_date=datetime(2025, 9, 1),
    schedule='@once',
    tags=['sql', 'sqlite', 'pipeline']
) as dag:
    create_table = SqliteOperator(
        task_id='create_table_task',
        sql= r"""
            CREATE TABLE IF NOT EXISTS users(
                id INTEGER PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                age INTEGER NOT NULL,
                city VARCHAR(50) NULL,
                is_active BOOLEAN DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """,
        sqlite_conn_id='my_sqlite_conn',
    )
    insert_values1 =  SqliteOperator(
        task_id='insert_values1_task',
        sql=r"""
            INSERT INTO users (name, age, is_active) VALUES
            ('Alice', 30, 1),
            ('Bob', 25, 1),
            ('Charlie', 35, 0);
        """,
        sqlite_conn_id='my_sqlite_conn',
    )
    insert_values2 = SqliteOperator(
        task_id='insert_values2_task',
        sql=r"""
            INSERT INTO users (name, age) VALUES
            ('David', 28),
            ('Eva', 22);
        """,
        sqlite_conn_id='my_sqlite_conn',
    )    
    delete_values = SqliteOperator(
        task_id='delete_values_task',
        sql=r"""
            DELETE FROM users WHERE name='Bob';
            """,
        sqlite_conn_id='my_sqlite_conn',
    )
    update_values = SqliteOperator(
        task_id='update_values_task',
        sql=r"""
            UPDATE users SET city='Seattle';
        """,
        sqlite_conn_id='my_sqlite_conn',
    )
    
    
    display_result = SqliteOperator(
        task_id='display_result_task',
        sql=r"""
            SELECT * FROM users;
        """,
        sqlite_conn_id='my_sqlite_conn',
        do_xcom_push=True
    )
    
create_table>>[insert_values1, insert_values2]>>delete_values>>update_values>>display_result