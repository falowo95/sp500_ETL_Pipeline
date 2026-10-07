"""
test_record_run.py

Behavioural tests for finalize_pipeline_run, the record_pipeline_run task's
callable. It must ALWAYS write the audit row, then fail the task (and so the
DAG run, the `airflow dags test` exit code and the Cloud Run Job execution)
whenever any upstream task did not succeed. No Airflow DB or GCP access:
the Airflow context and the BigQuery writer are mocked.
"""
import logging
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowException, AirflowFailException

import pipeline_audit
from pipeline_audit import (
    STATUS_FAILED,
    STATUS_SUCCESS,
    AuditWriteError,
    finalize_pipeline_run,
)

OWN_TASK_ID = "record_pipeline_run"
RUN_START = datetime(2026, 10, 7, 6, 0, 0, tzinfo=timezone.utc)
RUN_END = datetime(2026, 10, 7, 6, 5, 30, tzinfo=timezone.utc)
# `airflow dags test` creates the DagRun with start_date=logical date, so
# dag_run.start_date is midnight, not when the run actually began.
LOGICAL_MIDNIGHT = datetime(2026, 10, 7, tzinfo=timezone.utc)
_UNSET = object()
ALL_SUCCESS = {
    "extract_data_task": "success",
    "ingest_to_gcs": "success",
    "ingest_data_into_bigquery": "success",
    "dbt_build": "success",
}
XCOMS = {
    "extract_data_task": {"attempted": 3, "succeeded": 3, "failed": 0},
    "ingest_data_into_bigquery": {"rows_loaded": 42},
}


def _context(task_states, dag_run_start=LOGICAL_MIDNIGHT, ti_start=_UNSET, xcoms=None):
    xcoms = XCOMS if xcoms is None else xcoms
    first_start = RUN_START if ti_start is _UNSET else ti_start
    task_instances = [
        SimpleNamespace(
            task_id=task_id,
            state=state,
            start_date=None if first_start is None else first_start + timedelta(seconds=i),
        )
        for i, (task_id, state) in enumerate(
            {**task_states, OWN_TASK_ID: "running"}.items()
        )
    ]
    dag_run = MagicMock()
    dag_run.start_date = dag_run_start
    dag_run.get_task_instances.return_value = task_instances
    ti = MagicMock()
    ti.task_id = OWN_TASK_ID
    ti.xcom_pull.side_effect = lambda task_ids: xcoms.get(task_ids)
    return {
        "ti": ti,
        "dag_run": dag_run,
        "run_id": "scheduled__2026-10-07",
        "ds": "2026-10-07",
    }


@pytest.fixture
def mock_record():
    with patch.object(pipeline_audit, "record_pipeline_run") as record, patch.object(
        pipeline_audit.timezone, "utcnow", return_value=RUN_END
    ):
        yield record


def test_successful_run_writes_success_row_and_does_not_raise(mock_record):
    finalize_pipeline_run(dataset_name="sp500", **_context(ALL_SUCCESS))

    kwargs = mock_record.call_args.kwargs
    assert kwargs["status"] == STATUS_SUCCESS
    assert kwargs["error_message"] is None
    assert kwargs["dbt_build_status"] == "success"
    assert kwargs["rows_loaded"] == 42
    assert kwargs["extract_summary"] == XCOMS["extract_data_task"]
    assert kwargs["dataset_name"] == "sp500"
    assert kwargs["run_id"] == "scheduled__2026-10-07"
    assert kwargs["logical_date"] == "2026-10-07"


def test_timing_uses_earliest_task_start_not_logical_midnight(mock_record):
    finalize_pipeline_run(dataset_name="sp500", **_context(ALL_SUCCESS))

    kwargs = mock_record.call_args.kwargs
    assert kwargs["started_at"] == RUN_START.isoformat()
    assert kwargs["finished_at"] == RUN_END.isoformat()
    assert kwargs["finished_at"].endswith("+00:00")
    assert kwargs["duration_seconds"] == pytest.approx(330.0)


def test_timing_falls_back_to_dag_run_start_when_no_task_starts(mock_record):
    # started_at is the earliest upstream task start. dag_run.start_date is
    # only the fallback, including when `airflow dags test` left it at the
    # logical date and no task start was recorded.
    dag_start = datetime(2026, 10, 7, 6, 1, 0, tzinfo=timezone.utc)

    finalize_pipeline_run(
        dataset_name="sp500",
        **_context(ALL_SUCCESS, dag_run_start=dag_start, ti_start=None),
    )

    kwargs = mock_record.call_args.kwargs
    assert kwargs["started_at"] == dag_start.isoformat()
    assert kwargs["finished_at"] == RUN_END.isoformat()
    assert kwargs["duration_seconds"] == pytest.approx(270.0)


def test_timing_ignores_unset_starts_and_does_not_use_logical_midnight(mock_record):
    ctx = _context(ALL_SUCCESS)
    upstream = [
        ti
        for ti in ctx["dag_run"].get_task_instances()
        if ti.task_id != OWN_TASK_ID
    ]
    upstream[0].start_date = None
    upstream[-1].start_date = RUN_START

    finalize_pipeline_run(dataset_name="sp500", **ctx)

    kwargs = mock_record.call_args.kwargs
    assert kwargs["started_at"] == RUN_START.isoformat()
    assert kwargs["duration_seconds"] == pytest.approx(330.0)


def test_timing_ignores_the_audit_task_own_start_date(mock_record):
    ctx = _context(ALL_SUCCESS)
    for ti in ctx["dag_run"].get_task_instances():
        if ti.task_id == OWN_TASK_ID:
            ti.start_date = LOGICAL_MIDNIGHT

    finalize_pipeline_run(dataset_name="sp500", **ctx)

    assert mock_record.call_args.kwargs["started_at"] == RUN_START.isoformat()


def test_timing_with_no_start_information_records_zero_duration(
    mock_record, caplog
):
    with caplog.at_level(logging.WARNING, logger="pipeline_audit"):
        finalize_pipeline_run(
            dataset_name="sp500",
            **_context(ALL_SUCCESS, dag_run_start=None, ti_start=None),
        )

    kwargs = mock_record.call_args.kwargs
    assert kwargs["started_at"] == RUN_END.isoformat()
    assert kwargs["finished_at"] == RUN_END.isoformat()
    assert kwargs["duration_seconds"] == 0.0
    assert any("start time" in r.getMessage() for r in caplog.records)


@pytest.mark.parametrize(
    "failing_task", ["extract_data_task", "ingest_to_gcs", "ingest_data_into_bigquery", "dbt_build"]
)
def test_any_failed_upstream_task_writes_failed_row_then_fails_task(
    mock_record, failing_task
):
    states = {**ALL_SUCCESS, failing_task: "failed"}

    with pytest.raises(AirflowFailException, match=failing_task):
        finalize_pipeline_run(dataset_name="sp500", **_context(states, xcoms={}))

    mock_record.assert_called_once()
    kwargs = mock_record.call_args.kwargs
    assert kwargs["status"] == STATUS_FAILED
    assert f"{failing_task}=failed" in kwargs["error_message"]
    assert kwargs["rows_loaded"] == 0
    assert kwargs["extract_summary"] == {}


def test_upstream_failure_is_raised_even_when_audit_write_also_fails(
    mock_record, caplog
):
    mock_record.side_effect = AuditWriteError("insert failed")
    states = {**ALL_SUCCESS, "dbt_build": "failed"}

    with caplog.at_level(logging.ERROR, logger="pipeline_audit"):
        with pytest.raises(AirflowFailException, match="dbt_build"):
            finalize_pipeline_run(dataset_name="sp500", **_context(states))

    assert any("audit" in r.getMessage().lower() for r in caplog.records)


def test_audit_write_failure_on_successful_run_fails_task_retryably(
    mock_record, caplog
):
    mock_record.side_effect = RuntimeError("bigquery unavailable")

    with caplog.at_level(logging.ERROR, logger="pipeline_audit"):
        with pytest.raises(AirflowException) as excinfo:
            finalize_pipeline_run(dataset_name="sp500", **_context(ALL_SUCCESS))

    assert not isinstance(excinfo.value, AirflowFailException)
    assert any("audit" in r.getMessage().lower() for r in caplog.records)


def test_own_task_instance_is_excluded_from_status_computation(mock_record):
    # Own TI is "running" while the callable executes; it must not turn a
    # fully successful run into FAILED.
    finalize_pipeline_run(dataset_name="sp500", **_context(ALL_SUCCESS))

    assert mock_record.call_args.kwargs["status"] == STATUS_SUCCESS


def test_non_mapping_xcom_values_are_ignored_safely(mock_record):
    xcoms = {"extract_data_task": "garbage", "ingest_data_into_bigquery": ["x"]}

    finalize_pipeline_run(dataset_name="sp500", **_context(ALL_SUCCESS, xcoms=xcoms))

    kwargs = mock_record.call_args.kwargs
    assert kwargs["extract_summary"] == {}
    assert kwargs["rows_loaded"] == 0
