from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import os
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Set DBT project directory
DBT_PROJECT_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    'dbt',
    'dbt_sp500'
)

def set_dbt_env_vars(**context):
    """Set environment variables for DBT."""
    try:
        # Get Supabase connection details from Airflow variables
        os.environ['SUPABASE_DB_HOST'] = Variable.get('SUPABASE_DB_HOST')
        os.environ['SUPABASE_DB_USER'] = Variable.get('SUPABASE_DB_USER')
        os.environ['SUPABASE_DB_PASSWORD'] = Variable.get('SUPABASE_DB_PASSWORD')
        
        logger.info("Successfully set DBT environment variables")
        return True
    except Exception as e:
        logger.error(f"Failed to set DBT environment variables: {str(e)}")
        raise

with DAG(
    'dbt_transform_dag',
    default_args=default_args,
    description='Run DBT transformations on SP500 data',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['sp500', 'dbt'],
) as dag:
    
    # Set environment variables
    set_env_vars = PythonOperator(
        task_id='set_dbt_env_vars',
        python_callable=set_dbt_env_vars,
    )
    
    # Install DBT dependencies
    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt deps',
    )
    
    # Run DBT tests
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt test',
    )
    
    # Run DBT models
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt run',
    )
    
    # Set task dependencies
    set_env_vars >> dbt_deps >> dbt_test >> dbt_run 