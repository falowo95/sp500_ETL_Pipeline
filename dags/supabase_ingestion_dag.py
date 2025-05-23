from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.models import Variable
from airflow.exceptions import AirflowException
import pandas as pd
import tempfile
import logging
from supabase_helpers import (
    get_supabase_client,
    process_stock_data,
    upload_to_supabase,
    calculate_financial_indicators
)

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=60)
}

def fetch_from_gcs(**context):
    """Fetch processed data from GCS."""
    try:
        # Get GCS hook
        gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
        bucket_name = Variable.get('GCP_GCS_BUCKET')
        
        # Get the latest processed data file
        processed_prefix = 'processed/'
        blobs = gcs_hook.list(bucket_name=bucket_name, prefix=processed_prefix)
        if not blobs:
            raise AirflowException(f"No files found in {bucket_name}/{processed_prefix}")
        
        latest_blob = max(blobs, key=lambda x: x)
        logger.info(f"Processing file: {latest_blob}")
        
        # Download the file
        with tempfile.NamedTemporaryFile() as temp_file:
            gcs_hook.download(
                bucket_name=bucket_name,
                object_name=latest_blob,
                filename=temp_file.name
            )
            # Read the CSV file
            df = pd.read_csv(temp_file.name)
            logger.info(f"Successfully loaded {len(df)} records from {latest_blob}")
            
            # Push the dataframe to XCom
            context['task_instance'].xcom_push(key='stock_data', value=df.to_dict('records'))
            return True
    except Exception as e:
        logger.error(f"Failed to fetch data from GCS: {str(e)}")
        raise AirflowException(f"GCS data fetch failed: {str(e)}")

def process_and_upload_stocks(**context):
    """Process stock data and upload to Supabase."""
    try:
        # Get data from XCom
        ti = context['task_instance']
        raw_data = ti.xcom_pull(task_ids='fetch_gcs_data', key='stock_data')
        if not raw_data:
            raise AirflowException("No data received from previous task")
        
        df = pd.DataFrame(raw_data)
        
        # Process data for Supabase
        stock_records = process_stock_data(df)
        logger.info(f"Processed {len(stock_records)} stock records")
        
        # Get Supabase client
        client = get_supabase_client()
        
        # Upload stock data in batches
        upload_to_supabase(client, 'sp500_stocks', stock_records)
        logger.info("Successfully uploaded stock data to Supabase")
        
        # Push processed records to XCom for next task
        context['task_instance'].xcom_push(key='processed_stocks', value=stock_records)
        return True
    except Exception as e:
        logger.error(f"Failed to process and upload stocks: {str(e)}")
        raise AirflowException(f"Stock processing failed: {str(e)}")

def calculate_and_upload_indicators(**context):
    """Calculate and upload financial indicators."""
    try:
        # Get processed stocks from XCom
        ti = context['task_instance']
        stock_records = ti.xcom_pull(task_ids='process_upload_stocks', key='processed_stocks')
        if not stock_records:
            raise AirflowException("No processed stock data received from previous task")
        
        # Calculate indicators
        indicators = calculate_financial_indicators(stock_records)
        logger.info(f"Calculated {len(indicators)} financial indicators")
        
        # Get Supabase client
        client = get_supabase_client()
        
        # Upload indicators in batches
        upload_to_supabase(client, 'financial_indicators', indicators)
        logger.info("Successfully uploaded financial indicators to Supabase")
        return True
    except Exception as e:
        logger.error(f"Failed to calculate and upload indicators: {str(e)}")
        raise AirflowException(f"Indicator calculation failed: {str(e)}")

with DAG(
    'supabase_ingestion_dag',
    default_args=default_args,
    description='Load processed SP500 data from GCS to Supabase',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['sp500', 'supabase'],
) as dag:
    
    fetch_gcs_data = PythonOperator(
        task_id='fetch_gcs_data',
        python_callable=fetch_from_gcs,
    )
    
    process_upload_stocks = PythonOperator(
        task_id='process_upload_stocks',
        python_callable=process_and_upload_stocks,
    )
    
    calculate_upload_indicators = PythonOperator(
        task_id='calculate_upload_indicators',
        python_callable=calculate_and_upload_indicators,
    )
    
    # Set task dependencies
    fetch_gcs_data >> process_upload_stocks >> calculate_upload_indicators 