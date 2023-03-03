from datetime import datetime, timedelta
from typing import List
import pandas as pd
from dataclasses import dataclass
import os

# Importing specific Airflow libraries
from airflow.decorators import dag, task
from airflow.operators.python_operator import PythonOperator

import google.auth
from google.oauth2 import service_account

def get_gcp_authentication():
    SCOPES = ['https://www.googleapis.com/auth/sqlservice.admin']
    #switch out  your service account file
    SERVICE_ACCOUNT_FILE = '/creds/dataengineering-378316-2bcbcf067f34.json'
    credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    return credentials

# Importing custom-defined functions
from etl_operation_functions import (
    extract_sp500_data,
    transform_stock_data,
    load_data_to_s3,
)


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
    start_date=datetime(2023, 2, 22),
    email=["rushboy2000@yahoo.com"],
    email_on_failure=False,
    email_on_retry=True,
    depends_on_past=False,
    retries=2,
    retry_delay=timedelta(minutes=1),
)


# Defining the DAG using the `dag` decorator
@dag(
    dag_id="sp500_etl_pipeline",  # Unique identifier for the DAG
    schedule_interval="@daily",  # How often to run the DAG
    default_args=default_config.__dict__,  # Default arguments for the DAG
    catchup=False,  # Whether or not to catch up on missed runs
)
def taskflow():
    # Define the first task
    @task
    def get_stock_data() -> pd.DataFrame:
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
    def load(df: pd.DataFrame) -> None:
        """
        Task to load transformed data into S3.
        """
        load_data_to_s3(
            df,
            bucket="sp500-bucket-xcoms",
            key="trans_sp500_data",
            access_key=os.getenv("AWS_ACCESS_KEY_ID"),
            secret_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        )

    # Set up the dependencies between the tasks
    load(transformation(get_stock_data()))


# Instantiate the DAG
dag = taskflow()
