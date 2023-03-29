import os
from typing import List
from datetime import datetime
from dataclasses import dataclass

from airflow import DAG
from datetime import timedelta

# Importing specific Airflow libraries


from airflow.operators.python import PythonOperator

from stock_data_transform import transform_stock_data

# Importing custom-defined functions
from helper_functions import (
    extract_sp500_data_to_csv,
    upload_data_to_gcs_from_local,
    ingest_from_gcs_to_bquery,
)

project_id = os.environ.get("GCP_PROJECT_ID")
bucket_name  = os.environ.get("GCP_GCS_BUCKET")
file_name = "sp_500_data"
dataset_name = f"{file_name}"
table_name = f"{dataset_name}_table"

source_file_path_local = f"{file_name}.csv"
destination_blob_path = f"input-data/{file_name}.csv"
gcs_input_data_path = (
    "gs://dtc_data_lake_dataengineering-378316/input-data/sp_500_data.csv"
)
gcs_output_data_path = "gs://dtc_data_lake_dataengineering-378316/transformed-data/"
csv_uri = "gs://dtc_data_lake_dataengineering-378316/transformed-data/*.csv"


@dataclass
class ETLConfig:
    owner: str
    start_date: datetime
    email: List[str]
    email_on_failure: bool
    email_on_retry: bool
    depends_on_past: bool
    retries: int
    retry_delay: timedelta


# Defining the default configuration for the DAG
default_config = ETLConfig(
    owner="me",
    start_date=datetime.now(),
    email=[""],
    email_on_failure=False,
    email_on_retry=False,
    depends_on_past=False,
    retries=2,
    retry_delay=timedelta(minutes=1),
)


# Defining the DAG using the `dag` decorator
with DAG(
    dag_id="SP_500_DATA_PIPELINE_v1",  # Unique identifier for the DAG
    schedule_interval="@daily",  # How often to run the DAG
    default_args=default_config.__dict__,  # Default arguments for the DAG
    catchup=False,  # Whether or not to catch up on missed runs
    tags=["sp500-data"],
) as dag:

    extract_data_task = PythonOperator(
        task_id="extract_data_task",
        python_callable=extract_sp500_data_to_csv,
        op_kwargs={"file_name": f"{file_name}"},
    )

    upload_to_gcs_task = PythonOperator(
        task_id="ingest_to_gcs",
        python_callable=upload_data_to_gcs_from_local,
        op_kwargs={
            "bucket_name": bucket_name,
            "source_file_path_local": f"{source_file_path_local}",
            "destination_blob_path": f"{destination_blob_path}",
        },
    )

    transform_data_task = PythonOperator(
        task_id="transform_data_task",
        python_callable=transform_stock_data,
        op_kwargs={
            "gcs_input_data_path": f"{gcs_input_data_path}",
            "gcs_output_data_path": f"{gcs_output_data_path}",
        },
    )

    ingest_data_into_bigquery = PythonOperator(
        task_id="ingest_data_into_bigquery",
        python_callable=ingest_from_gcs_to_bquery,
        op_kwargs={
            "dataset_name": f"{dataset_name}",
            "table_name": f"{table_name}",
            "csv_uri": f"{csv_uri}",
        },
    )

    (
        extract_data_task
        >> upload_to_gcs_task
        >> transform_data_task
        >> ingest_data_into_bigquery
    )
