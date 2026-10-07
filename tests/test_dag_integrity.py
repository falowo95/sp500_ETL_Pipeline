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
    # 5 tasks: extract -> upload to GCS -> load to BigQuery -> dbt build ->
    # record_pipeline_run. The PySpark transform task was removed; dbt's
    # stg_stocks model does all cleaning in SQL instead (see CLAUDE.md's
    # "ELT split"). record_pipeline_run (trigger_rule=ALL_DONE) is the
    # pipeline_runs audit row — the substitute for an Airflow UI that no
    # longer exists once this runs as a one-shot Cloud Run Job.
    assert len(dag.tasks) == 5


def _main_dag():
    return _dag_bag().dags["SP_500_DATA_PIPELINE_v1"]


def test_record_pipeline_run_is_the_all_done_leaf_downstream_of_every_task():
    from airflow.utils.trigger_rule import TriggerRule

    dag = _main_dag()
    record_task = dag.get_task("record_pipeline_run")

    assert record_task.trigger_rule == TriggerRule.ALL_DONE
    assert dag.leaves == [record_task]
    assert [task.task_id for task in dag.topological_sort()] == [
        "extract_data_task",
        "ingest_to_gcs",
        "ingest_data_into_bigquery",
        "dbt_build",
        "record_pipeline_run",
    ]
    assert record_task.get_flat_relative_ids(upstream=True) == {
        "extract_data_task",
        "ingest_to_gcs",
        "ingest_data_into_bigquery",
        "dbt_build",
    }


def test_record_pipeline_run_uses_the_status_propagating_callable():
    from pipeline_audit import finalize_pipeline_run

    record_task = _main_dag().get_task("record_pipeline_run")

    assert record_task.python_callable is finalize_pipeline_run
    assert set(record_task.op_kwargs) == {"dataset_name"}
    # Pipeline failure raises AirflowFailException, which Airflow does not
    # retry. A successful run whose audit insert fails raises AirflowException
    # and must still be able to retry, so this task keeps default_args retries.
    assert record_task.retries >= 1


def test_dag_start_date_is_static_and_utc_aware():
    # A dynamic start_date (datetime.now()) changes on every parse, so a
    # logical date passed to `airflow dags test` can precede it.
    first = _main_dag().default_args["start_date"]
    second = _main_dag().default_args["start_date"]

    assert first == second
    assert first.utcoffset().total_seconds() == 0
