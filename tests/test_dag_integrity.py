"""
test_dag_integrity.py

Sanity-checks that DAG files under airflow/dags parse without import errors.
Catches the class of bug that only surfaces at Airflow scheduler parse time
(bad default_args, missing imports, eager network calls at module load, etc.).
"""
import os

import pytest


@pytest.fixture(autouse=True)
def gcp_env(monkeypatch):
    monkeypatch.setenv("GCP_PROJECT_ID", "test-project")
    monkeypatch.setenv("GCP_GCS_BUCKET", "test-bucket")
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "/tmp/fake-creds.json")


def _dag_bag():
    from airflow.models import DagBag

    dags_folder = os.path.join(os.path.dirname(os.path.dirname(__file__)), "dags")
    return DagBag(dag_folder=dags_folder, include_examples=False)


def test_no_import_errors():
    dag_bag = _dag_bag()
    assert dag_bag.import_errors == {}, dag_bag.import_errors


def test_main_pipeline_dag_is_present():
    dag_bag = _dag_bag()
    assert "SP_500_DATA_PIPELINE_v1" in dag_bag.dags


def test_main_pipeline_dag_has_no_cycles_and_expected_task_count():
    dag_bag = _dag_bag()
    dag = dag_bag.dags["SP_500_DATA_PIPELINE_v1"]
    assert len(dag.tasks) == 5
