import datetime
import json
import pendulum
import os
from airflow import DAG
from airflow.decorators import task

from airflow.models import Variable
from google.cloud import storage
from google.cloud import bigquery
from google.oauth2 import service_account
from airflow.operators.python import PythonOperator
with DAG(
    dag_id="upload_data_to_gcs",
    schedule=None, 
    start_date=pendulum.datetime(2023, 4, 15),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
) as dag:

    def load_file_from_local_to_gcs():
        keyfile = "/opt/airflow/dags/gcs-and-bigquery-admin.json"
        service_account_info = json.load(open(keyfile))
        credentials = service_account.Credentials.from_service_account_info(service_account_info)
        project_id = "prompt-lab-383408"

        storage_client = storage.Client(
            project=project_id,
            credentials=credentials,
        )
        bucket = storage_client.bucket("aam-sirinya")
        blob = bucket.blob("data/shop_data.csv")

        blob.upload_from_filename("/opt/airflow/dags/shop_data.csv")
        print("File shop_data.csv  uploaded to data/shop_data.csv")


    def load_data_from_gcs_to_bigquery(**context):
        pass
    load_file_from_local_to_gcs_task = PythonOperator(task_id="upload_data_to_gcs",python_callable=load_file_from_local_to_gcs)
    load_data_from_gcs_to_bigquery = PythonOperator(task_id="upload_data_to_bigquery",python_callable=load_data_from_gcs_to_bigquery)
    
load_file_from_local_to_gcs_task >> load_data_from_gcs_to_bigquery