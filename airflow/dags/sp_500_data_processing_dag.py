"""
SP 500 Data Processing DAG

This module defines the DAG for the SP 500 data processing pipeline.
"""

import os
from typing import List
from datetime import datetime, timedelta
from dataclasses import dataclass
from helper_functions import (
    extract_sp500_data_to_csv,
    upload_data_to_gcs_from_local,
    ingest_from_gcs_to_bquery,
)
from stock_data_transform import transform_stock_data
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import DAG as DAGType
from config import ETLConfig


def define_dag() -> DAGType:
    """
    Define the DAG for the SP 500 data pipeline.

    Returns:
        DAG: The defined DAG object.
    """
    config = ETLConfig()

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
            op_kwargs={
                "file_name": config.file_name,
                "tiingo_api_key": config.tiingo_api_key,
                "start_date": config.start_date,
                "end_date": config.end_date,
            },
        )

        upload_to_gcs_task = PythonOperator(
            task_id="ingest_to_gcs",
            python_callable=upload_data_to_gcs_from_local,
            op_kwargs={
                "bucket_name": config.bucket_name,
                "source_file_path_local": config.source_file_path_local,
                "destination_blob_path": config.destination_blob_path,
            },
        )

        transform_data_task = PythonOperator(
            task_id="transform_data_task",
            python_callable=transform_stock_data,
            op_kwargs={
                "gcs_input_data_path": config.gcs_input_data_path,
                "gcs_output_data_path": config.gcs_output_data_path,
            },
        )

        ingest_data_into_bigquery = PythonOperator(
            task_id="ingest_data_into_bigquery",
            python_callable=ingest_from_gcs_to_bquery,
            op_kwargs={
                "dataset_name": config.dataset_name,
                "table_name": config.table_name,
                "csv_uri": config.csv_uri,
            },
        )

        (
            extract_data_task
            >> upload_to_gcs_task
            >> transform_data_task
            >> ingest_data_into_bigquery
        )

    return dag


DAG = define_dag()
