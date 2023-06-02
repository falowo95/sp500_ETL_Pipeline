"""
SP 500 Data Processing DAG

This module defines the DAG for the SP 500 data processing pipeline.
"""
import os
from typing import List
from datetime import datetime, timedelta
from dataclasses import dataclass

from airflow import DAG
from airflow.operators.python import PythonOperator

from stock_data_transform import transform_stock_data
from helper_functions import (
    extract_sp500_data_to_csv,
    upload_data_to_gcs_from_local,
    ingest_from_gcs_to_bquery,
)


@dataclass
class ETLConfig:
    """
    Configuration class for ETL process.
    """

    owner: str
    start_date: datetime
    email: List[str]
    email_on_failure: bool
    email_on_retry: bool
    depends_on_past: bool
    retries: int
    retry_delay: timedelta


def define_dag() -> DAG:
    """
    Define the DAG for the SP 500 data pipeline.

    Returns:
        DAG: The defined DAG object.
    """
    project_id = os.environ.get("GCP_PROJECT_ID")
    bucket_name = os.environ.get("GCP_GCS_BUCKET")
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

    with DAG(
        dag_id="SP_500_DATA_PIPELINE_v1",
        schedule_interval="@daily",
        default_args=default_config.__dict__,
        catchup=False,
        tags=["sp500-data"],
    ) as dag:
        extract_data_task = PythonOperator(
            task_id="extract_data_task",
            python_callable=extract_sp500_data_to_csv,
            op_kwargs={"file_name": file_name},
        )

        upload_to_gcs_task = PythonOperator(
            task_id="ingest_to_gcs",
            python_callable=upload_data_to_gcs_from_local,
            op_kwargs={
                "bucket_name": bucket_name,
                "source_file_path_local": source_file_path_local,
                "destination_blob_path": destination_blob_path,
            },
        )

        transform_data_task = PythonOperator(
            task_id="transform_data_task",
            python_callable=transform_stock_data,
            op_kwargs={
                "gcs_input_data_path": gcs_input_data_path,
                "gcs_output_data_path": gcs_output_data_path,
            },
        )

        ingest_data_into_bigquery = PythonOperator(
            task_id="ingest_data_into_bigquery",
            python_callable=ingest_from_gcs_to_bquery,
            op_kwargs={
                "dataset_name": dataset_name,
                "table_name": table_name,
                "csv_uri": csv_uri,
            },
        )

        (
            extract_data_task
            >> upload_to_gcs_task
            >> transform_data_task
            >> ingest_data_into_bigquery
        )

    return dag


dag: DAG = define_dag()
