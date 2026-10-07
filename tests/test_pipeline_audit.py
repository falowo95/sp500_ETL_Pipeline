"""
test_pipeline_audit.py

Unit tests for pipeline_audit's pure status computation and its BigQuery
writer. All GCP clients are mocked; nothing touches a real project.
"""
import json
import logging
from unittest.mock import MagicMock, patch

import pytest
from google.api_core.exceptions import Forbidden, NotFound

import pipeline_audit
from pipeline_audit import (
    PIPELINE_RUNS_TABLE,
    STATUS_FAILED,
    STATUS_SUCCESS,
    AuditWriteError,
    _ensure_pipeline_runs_table,
    compute_run_status,
    record_pipeline_run,
)

ALL_SUCCESS = {
    "extract_data_task": "success",
    "ingest_to_gcs": "success",
    "ingest_data_into_bigquery": "success",
    "dbt_build": "success",
}


# --- compute_run_status ------------------------------------------------------


def test_compute_run_status_is_success_when_every_upstream_task_succeeded():
    assert compute_run_status(ALL_SUCCESS) == (STATUS_SUCCESS, None)


def test_compute_run_status_is_failed_when_dbt_build_failed():
    states = {**ALL_SUCCESS, "dbt_build": "failed"}

    status, error_message = compute_run_status(states)

    assert status == STATUS_FAILED
    assert "dbt_build=failed" in error_message


def test_compute_run_status_names_every_non_successful_task_failed_first():
    states = {
        "extract_data_task": "success",
        "ingest_to_gcs": "failed",
        "ingest_data_into_bigquery": "upstream_failed",
        "dbt_build": "upstream_failed",
    }

    status, error_message = compute_run_status(states)

    assert status == STATUS_FAILED
    assert error_message.index("ingest_to_gcs=failed") < error_message.index(
        "dbt_build=upstream_failed"
    )
    assert "ingest_data_into_bigquery=upstream_failed" in error_message
    assert "extract_data_task" not in error_message


@pytest.mark.parametrize("bad_state", ["skipped", "upstream_failed", None, "running"])
def test_compute_run_status_treats_any_non_success_state_as_failed(bad_state):
    states = {**ALL_SUCCESS, "ingest_to_gcs": bad_state}

    status, error_message = compute_run_status(states)

    assert status == STATUS_FAILED
    assert f"ingest_to_gcs={bad_state}" in error_message


def test_compute_run_status_with_no_upstream_states_is_failed_not_success():
    status, error_message = compute_run_status({})

    assert status == STATUS_FAILED
    assert error_message


def test_compute_run_status_does_not_mutate_its_input():
    states = {**ALL_SUCCESS, "dbt_build": "failed"}
    snapshot = dict(states)

    compute_run_status(states)

    assert states == snapshot


# --- _ensure_pipeline_runs_table --------------------------------------------


def test_ensure_table_creates_table_only_when_it_does_not_exist():
    bq_client = MagicMock()
    bq_client.get_table.side_effect = NotFound("missing")

    _ensure_pipeline_runs_table(bq_client, "p.d.pipeline_runs")

    bq_client.create_table.assert_called_once()


def test_ensure_table_does_not_create_when_table_exists():
    bq_client = MagicMock()

    _ensure_pipeline_runs_table(bq_client, "p.d.pipeline_runs")

    bq_client.create_table.assert_not_called()


def test_ensure_table_propagates_non_notfound_errors_instead_of_creating():
    bq_client = MagicMock()
    bq_client.get_table.side_effect = Forbidden("no permission")

    with pytest.raises(Forbidden):
        _ensure_pipeline_runs_table(bq_client, "p.d.pipeline_runs")

    bq_client.create_table.assert_not_called()


# --- record_pipeline_run ----------------------------------------------------


@pytest.fixture
def mock_gcp():
    gcp = MagicMock()
    gcp.project_id = "test-project"
    gcp.bq_client.insert_rows_json.return_value = []
    with patch.object(pipeline_audit.GCPService, "get_instance", return_value=gcp):
        yield gcp


def _record(**overrides):
    kwargs = {
        "dataset_name": "sp500",
        "run_id": "manual__2026-10-07",
        "logical_date": "2026-10-07",
        "started_at": "2026-10-07T06:00:00+00:00",
        "finished_at": "2026-10-07T06:05:30+00:00",
        "status": STATUS_SUCCESS,
        "extract_summary": {
            "attempted": 3,
            "succeeded": 2,
            "failed": 1,
            "failed_tickers": ["XYZ"],
        },
        "rows_loaded": 42,
        "dbt_build_status": "success",
        "duration_seconds": 330.0,
        "error_message": None,
    }
    record_pipeline_run(**{**kwargs, **overrides})


def test_record_pipeline_run_writes_full_row_including_duration(mock_gcp):
    _record()

    table_id, rows = mock_gcp.bq_client.insert_rows_json.call_args.args
    assert table_id == f"test-project.sp500.{PIPELINE_RUNS_TABLE}"
    assert rows == [
        {
            "run_id": "manual__2026-10-07",
            "logical_date": "2026-10-07",
            "started_at": "2026-10-07T06:00:00+00:00",
            "finished_at": "2026-10-07T06:05:30+00:00",
            "status": STATUS_SUCCESS,
            "tickers_attempted": 3,
            "tickers_succeeded": 2,
            "tickers_failed": 1,
            "failed_ticker_list": json.dumps(["XYZ"]),
            "rows_loaded": 42,
            "dbt_build_status": "success",
            "duration_seconds": 330.0,
            "error_message": None,
        }
    ]


def test_record_pipeline_run_creates_missing_table_before_insert(mock_gcp):
    mock_gcp.bq_client.get_table.side_effect = NotFound("missing")

    _record()

    mock_gcp.bq_client.create_table.assert_called_once()
    mock_gcp.bq_client.insert_rows_json.assert_called_once()


def test_record_pipeline_run_tolerates_missing_extract_summary(mock_gcp):
    _record(extract_summary=None)

    row = mock_gcp.bq_client.insert_rows_json.call_args.args[1][0]
    assert row["tickers_attempted"] == 0
    assert row["failed_ticker_list"] == "[]"


def test_record_pipeline_run_raises_and_logs_when_insert_reports_errors(
    mock_gcp, caplog
):
    mock_gcp.bq_client.insert_rows_json.return_value = [{"errors": ["bad row"]}]

    with caplog.at_level(logging.ERROR, logger="pipeline_audit"):
        with pytest.raises(AuditWriteError, match="bad row"):
            _record()

    assert any("pipeline_runs" in r.getMessage() for r in caplog.records)
