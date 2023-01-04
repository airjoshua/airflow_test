import os
from datetime import (
    datetime,
    timedelta
)
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import storage
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

from dotenv import dotenv_values, load_dotenv
#load_dotenv()

#from helper_functions.google_cloud_constants import GoogleCloud

UNPROCESSED_FILES_PATH = '/Users/airjoshua/airflow/csv_files' #GoogleCloud.FILE_PATH.value
PROJECT_ID = '635596282989'

DAILY_SALES_BUCKET = 'pay-less-example-bucket'


os.environ['AIRFLOW_VAR_GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/airjoshua/pay-less-example-d0c4616306d2.json' #'/Users/airjoshua/.config/gcloud/application_default_credentials.json'
#os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/airjoshua/pay-less-example-d0c4616306d2.json' #'/Users/airjoshua/.config/gcloud/application_default_credentials.json'
#os.environ["AIRFLOW_VAR_GOOGLE_CLOUD_PROJECT"] = 'pay-less-example'#'635596282989'#'pay-less-example'
#os.environ["AIRFLOW_CONN_GOOGLE_CLOUD_PROJECT"] = 'pay-less-example'
#os.environ["GOOGLE_CLOUD_PROJECT"] = 'pay-less-example'
#os.environ["AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT"]='{"conn_type": "google-cloud-platform", "key_path": "/Users/airjoshua/.config/gcloud/application_default_credentials.json", "scope": "https://www.googleapis.com/auth/cloud-platform", "project": "pay-less-example", "num_retries": 5}'
#os.environ["AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT"] = "google-cloud-platform://?key_path=/Users/airjoshua/.config/gcloud/application_default_credentials.json&scope=https%3A%2F%2Fwww.googleapis.com%2Fauth%2Fcloud-platform&project=pay-less-example&num_retries=5"
# /Users/airjoshua/airflow/pay-less-example-65454db9e51f.json
#os.environ["AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT"] = '{"conn_type": "google-cloud-platform", "key_path": "/Users/airjoshua/.config/gcloud/application_default_credentials.json", "scope": "https://www.googleapis.com/auth/cloud-platform", "project": "pay-less-example", "num_retries": 5}'
#credentials, project_id = google.auth.default()
#google.auth.load_credentials_from_file('/Users/airjoshua/.config/gcloud/application_default_credentials.json')
#os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/airjoshua/.config/gcloud/application_default_credentials.json'
#os.environ['AIRFLOW_CONN_GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/airjoshua/application_default_credentials.json'
#os.environ['AIRFLOW__LOGGING__GOOGLE_KEY_PATH'] = '/Users/airjoshua/.config/gcloud/application_default_credentials.json'
import google.auth
credentials, project_id = google.auth.default()

def get_unprocessed_csv_files():
    csv_file_path = Path(UNPROCESSED_FILES_PATH)
    return list(csv_file_path.glob("*.csv"))


def move_processed_files_to_processed_files_folder(unprocessed_csv_files):
    for file in unprocessed_csv_files:
        file.replace(file.parent / "processed_files" / file.name)


UNPROCESSED_CSV_FILES = list(get_unprocessed_csv_files())

def upload_blobs(bucket_name, file_list):
    storage_client = storage.Client(project='pay-less-example', credentials=credentials)
    bucket = storage_client.bucket(bucket_name)
    for file in file_list:
        destination_blob_name = f"gs://{bucket_name}/{file.name}"
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(f"{file.parent}/{file.name}")
        print(f"File {file} uploaded to {destination_blob_name}.")
#upload_blobs(bucket_name='pay-less-example-bucket', file_list=UNPROCESSED_CSV_FILES)

default_args = {
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
        dag_id="upload_files_to_google_cloud_storage",
        default_args=default_args,
        description="Uploading from Pay-Less to Google Cloud Storage",
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=["Pay-Less"]
) as dag:
    #upload_files_to_gcs = PythonOperator(
    #    task_id='upload_files_to_gcs',
    ##    python_callable=upload_blobs,
    #    op_kwargs={'bucket_name': 'pay-less-example-bucket',
    #               'file_list': UNPROCESSED_CSV_FILES}
    #),
    upload_files_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_files_to_gcs",
        gcp_conn_id='google_cloud_connection',
        src=UNPROCESSED_CSV_FILES,
        dst="files/",
        bucket=DAILY_SALES_BUCKET)

    move_files = PythonOperator(
        task_id='move_files',
        python_callable=move_processed_files_to_processed_files_folder,
        op_kwargs={'unprocessed_csv_files':  UNPROCESSED_CSV_FILES}
    )

    upload_files_to_gcs >> move_files

