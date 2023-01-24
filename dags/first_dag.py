from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from etl_operation_functions import (
    transform_stock_data,
    load_data_into_db,
    extract_sp500_data,
    retrieve_data_from_db,
)


default_args = {
    "owner": "me",
    "start_date": datetime.now(),
    "email": ["rushboy2000@yahoo.com"],
    "email_on_failure": False,
    "email_on_retry": True,
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    "sp500_etl_pipeline",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
)
def taskflow():

    @task
    def get_stock_data():
        return extract_sp500_data()

    @task
    def transformation(df):
        return transform_stock_data(df)
    
    def load():
        return load_data_into_db

    def retrieve():
        return retrieve_data_from_db

    load(transformation(get_stock_data()))


dag = taskflow()