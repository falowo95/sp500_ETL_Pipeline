
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from etl_operation_functions import transform_stock_data,load_data_into_db,extract_sp500_data,retrieve_data_from_db


default_args = {
    'owner':'me',
    'start_date':datetime.now(),
    # 'end_date':datetime(2023,1,19),
    'email':['rushboy2000@yahoo.com'],
    'email_on_failure':False,
    'email_on_retry':True,
    'depends_on_past':False,
    'retries':2,
    'retry_delay':timedelta(minutes=1),
}

# create DAG
with DAG(
    'sp500_etl_pipeline_v0',
    default_args=default_args,
    description='',
    start_date=datetime.now(),
    # continue to run dag once per day
    schedule_interval = '@daily') as dag:
    
    extract_task = PythonOperator(
        task_id='extract_sp500_data',
        python_callable=extract_sp500_data,
        )

    transform_task = PythonOperator(
        task_id='transform_sp500_data',
        python_callable=transform_stock_data,
        # op_kwargs={"df":extract_sp500_data}
        )

    load_task = PythonOperator(
        task_id='load_data_into_db',
        python_callable=load_data_into_db,
    
        )
    query_database = PythonOperator(
        task_id='query_database',
        python_callable=retrieve_data_from_db,
        )

# setting up dependencies
extract_task >> transform_task >> load_task>>query_database
# transform_task>>extract_sp500_data