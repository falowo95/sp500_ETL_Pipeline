import pandas as pd

from pathlib import Path
from typing import List
from datetime import datetime
from dataclasses import dataclass

from datetime import timedelta

# Importing specific Airflow libraries
from airflow.decorators import dag, task


# Importing custom-defined functions
from etl_operation_functions import (
    extract_sp500_data,
    transform_stock_data,
    upload_to_gcs,
    ingest_from_gcs_to_bquery,
)


GOOGLE_APPLICATION_CREDENTIALS = (
    "creds/dataengineering-378316-2bcbcf067f34.json"  # enviromental variable
)


bucket_name = "dtc_data_lake_dataengineering-378316"
dataset_name = "sp_500_data"
file_name = f"{dataset_name}"
table_name = f"{dataset_name}_table"
csv_uri = f"gs://dtc_data_lake_dataengineering-378316/{dataset_name}"
tingo_api_key = "b8048079af04b7e50218c15f24286df5b4c51164"


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
@dag(
    dag_id="SP_500_DATA_PIPELINE_v2",  # Unique identifier for the DAG
    schedule_interval="@daily",  # How often to run the DAG
    default_args=default_config.__dict__,  # Default arguments for the DAG
    catchup=False,  # Whether or not to catch up on missed runs
)
def taskflow():
    # Define the first task
    @task
    def extract_stock_data() -> pd.DataFrame:
        """
        Task to extract data from the S&P 500.
        """
        return extract_sp500_data()

    # Define the second task
    @task
    def transformation(df: pd.DataFrame) -> pd.DataFrame:
        """
        Task to transform data from the S&P 500.
        """
        return transform_stock_data(df)

    # Define the third task
    @task
    def load_to_gcs(df:pd.DataFrame,bucket_name) -> None:
        """
        Task to load transformed data into S3.
        """
        upload_to_gcs(df,file_name, bucket_name)

    # Define the fourth task
    @task
    def bigquery_ingestion(dataset_name, table_name, csv_uri):
        return ingest_from_gcs_to_bquery(dataset_name, table_name, csv_uri)

    # Set up the dependencies between the tasks
    extracted_data = extract_stock_data()
    transfromed_data = transformation(extracted_data)
    
    load_to_gcs(transfromed_data, bucket_name)

    


# Instantiate the DAG
dag = taskflow()
