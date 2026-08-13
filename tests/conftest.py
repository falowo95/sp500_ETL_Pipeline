"""
Pytest configuration for the airflow/tests suite.

Sets AIRFLOW_HOME to an isolated, writable directory before Airflow is imported
anywhere in the test session, so tests don't depend on (or pollute) a real
~/airflow installation.
"""
import os
import tempfile

os.environ.setdefault(
    "AIRFLOW_HOME", os.path.join(tempfile.gettempdir(), "sp500_airflow_test_home")
)
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
os.environ.setdefault("AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION", "True")
os.environ.setdefault("AIRFLOW__LOGGING__LOGGING_LEVEL", "WARNING")
