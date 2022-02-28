import os
import logging

from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq


PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "opt/airflow/")


def format_to_parquet(src_file, dest_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, dest_file)


def upload_to_gcs(bucket, object_name, local_file):
    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    "start_date": datetime(2019,1,1),
    "depends_on_past": False,
    "retries": 1,
}

URL_PREFIX = 'https://s3.amazonaws.com/nyc-tlc/trip+data' 
YELLOW_TAXI_URL_TEMPLATE = URL_PREFIX + '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
YELLOW_TAXI_CSV_FILE_TEMPLATE = AIRFLOW_HOME + '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
YELLOW_TAXI_PARQUET_FILE_TEMPLATE = AIRFLOW_HOME + '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
YELLOW_TAXI_GCS_PATH_TEMPLATE = "raw/yellow_tripdata/{{ execution_date.strftime(\'%Y\') }}/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"

with DAG(
    dag_id="yellow_taxi_data",
    schedule_interval="0 6 2 * *",
    # start_date=datetime(2019, 1, 1),
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    # Download task from online source   
    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSLf {YELLOW_TAXI_URL_TEMPLATE} > {YELLOW_TAXI_CSV_FILE_TEMPLATE}"
    )

    # Reformat csv file to parquet format
    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": YELLOW_TAXI_CSV_FILE_TEMPLATE,
            "dest_file": YELLOW_TAXI_PARQUET_FILE_TEMPLATE
        },
    )

    # Upload data to Google Cloud bucket
    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": YELLOW_TAXI_GCS_PATH_TEMPLATE,
            "local_file": YELLOW_TAXI_PARQUET_FILE_TEMPLATE,
        },
    )

    rm_task = BashOperator(
        task_id="rm_task",
        bash_command=f"rm {YELLOW_TAXI_CSV_FILE_TEMPLATE} {YELLOW_TAXI_PARQUET_FILE_TEMPLATE}"
    )

    download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> rm_task
