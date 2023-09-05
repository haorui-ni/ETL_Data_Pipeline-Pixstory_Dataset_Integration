from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.transfers.snowflake_to_snowflake import SnowflakeToSnowflakeOperator
from datetime import datetime

default_args = {
    'owner': 'your_name',
    'start_date': datetime(2023, 8, 31),  
    'retries': 1,
}

dag = DAG(
    'data_processing_dag',
    default_args=default_args,
    schedule_interval=None,  
)


# Create custom PythonOperator for each step

preprocess_task = PythonOperator(
    task_id='load_pix_data',
    python_callable=None,  # Set to None
    op_args=["load_hs.py"],
    dag=dag,
)

