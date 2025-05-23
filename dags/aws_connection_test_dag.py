"""
AWS Connection Test DAG

This DAG tests connectivity to various AWS services:
- S3
- Secrets Manager
- Redshift
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from config.etl_config import ETLConfig
from botocore.exceptions import ClientError

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email': ['your-email@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}



def test_secrets_manager(**context):
    """Test Secrets Manager connectivity by retrieving a test secret."""
    config = ETLConfig()
    try:
        # Get AWS config
        aws = config.aws_config()
        
        # Try to get a test secret
        secret_name = "api/tiingo"  # Replace with your actual secret name
        secret_value = aws.get_secret(secret_name)
        print(f"Successfully retrieved secret: {secret_name}  : {secret_value}")
        print("Successfully retrieved secret from Secrets Manager")
        return True
    except ValueError as ve:
        print(f"Configuration error: {str(ve)}")
        raise
    except ClientError as ce:
        print(f"AWS Client error: {str(ce)}")
        raise
    except Exception as e:
        print(f"Secrets Manager connection test failed: {str(e)}")
        raise



with DAG(
    'aws_connection_test',
    default_args=default_args,
    description='Test AWS Connectivity',
    schedule_interval=None,
    catchup=False
) as dag:

    test_secrets = PythonOperator(
        task_id='test_secrets_manager',
        python_callable=test_secrets_manager,
    )



    # Set task dependencies
    test_secrets