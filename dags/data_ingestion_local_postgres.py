import os
import logging
import pandas as pd


from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
import airflow


dataset_file = "yellow_tripdata_2021-01.parquet"
dataset_csv_file = "yellow_tripdata_2021-01.csv"
dataset_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{dataset_file}"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")


default_args = {
    "email" : ["franklinmutwiri41@gmail.com"],
    "email_on_failure" :False,
    "email_on_retry" : False,
    "retries" : 5,
    "retry_delay" : timedelta(minutes=3),
    "depends_on_past" : True,
    "wait_for_downstream" : True
}

def convert_parquet_to_csv():
    csv_df = pd.read_parquet(f"{path_to_local_home}/{dataset_file}", engine='pyarrow')
    csv_df.to_csv(f"{path_to_local_home}/{dataset_csv_file}", index=False )


def copy_data_postgres_task():
    pass

with DAG(
    dag_id ="ingest_to_postgres",
    default_args=default_args,
    schedule_interval="@daily",
    start_date= datetime(2023,6,29),
    max_active_runs=3

) as dag:
    
    download_parquet_file = BashOperator(
        task_id ="download_parquet_file",
        bash_command= f"curl -sSL {dataset_url} > {path_to_local_home}/{dataset_file}"
    )

    convert_parquet_to_csv = PythonOperator(
        task_id = "convert_parquet_to_csv",
        python_callable= convert_parquet_to_csv
    )

    copy_data_postgres_task = PythonOperator(
        task_id = "copy_data_postgres_task",
        python_callable=copy_data_postgres_task
    )

