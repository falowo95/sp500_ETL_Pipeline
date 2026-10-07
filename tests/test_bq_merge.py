"""
test_bq_merge.py

Tests for the load side of helper_functions.py: identifier validation for
SQL that can't be parameterized, the GCS upload, and the staging-load +
MERGE in ingest_from_gcs_to_bquery. BigQuery/GCS are fully mocked.
"""
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from google.api_core import exceptions as google_exceptions

from helper_functions import (
    TIINGO_COLUMNS,
    _get_symbol_watermarks,
    ingest_from_gcs_to_bquery,
    qualified_table_id,
    to_local,
    upload_data_to_gcs_from_local,
)


@pytest.fixture
def mock_gcp():
    gcp = MagicMock()
    gcp.project_id = "test-project"
    with patch("helper_functions.GCPService") as service:
        service.get_instance.return_value = gcp
        yield gcp


# ------------------------------------------------------ identifier checks

def test_qualified_table_id_accepts_valid_identifiers():
    assert qualified_table_id("my-proj-1", "SP_500_DATA", "SP_500_DATA_table") == (
        "my-proj-1.SP_500_DATA.SP_500_DATA_table"
    )


@pytest.mark.parametrize(
    "project,dataset,table",
    [
        ("p` ; DROP TABLE x --", "ds", "t"),
        ("test-project", "ds`.evil", "t"),
        ("test-project", "ds", "t` WHERE 1=1 --"),
        ("test-project", "", "t"),
        ("test-project", "ds", "has space"),
    ],
)
def test_qualified_table_id_rejects_injection_attempts(project, dataset, table):
    with pytest.raises(ValueError):
        qualified_table_id(project, dataset, table)


def test_watermark_query_rejects_malicious_table_name_before_querying(mock_gcp):
    with pytest.raises(ValueError):
        _get_symbol_watermarks("SP_500_DATA", "t` UNION ALL SELECT 1 --")
    mock_gcp.bq_client.query.assert_not_called()


# ------------------------------------------------------------ local + GCS

def test_to_local_and_upload_share_the_same_data_dir(monkeypatch, tmp_path, mock_gcp):
    """to_local used to write /tmp/airflow_data while the upload read
    /opt/airflow/data, so the upload task could never find the CSV."""
    monkeypatch.setenv("SP500_LOCAL_DATA_DIR", str(tmp_path))
    written = to_local(pd.DataFrame({"a": [1]}), "OUT")

    upload_data_to_gcs_from_local("bucket", "OUT.csv", "input-data/OUT.csv")

    mock_gcp.upload_blob.assert_called_once_with(
        bucket_name="bucket", source_file=str(written), destination_blob="input-data/OUT.csv"
    )


def test_upload_raises_when_source_file_missing(monkeypatch, tmp_path, mock_gcp):
    monkeypatch.setenv("SP500_LOCAL_DATA_DIR", str(tmp_path))
    with pytest.raises(FileNotFoundError):
        upload_data_to_gcs_from_local("bucket", "missing.csv", "input-data/missing.csv")
    mock_gcp.upload_blob.assert_not_called()


def test_upload_forbidden_is_raised_not_swallowed(monkeypatch, tmp_path, mock_gcp):
    """Swallowing Forbidden let the run continue and MERGE whatever stale
    CSV was already in GCS — the task must fail instead."""
    monkeypatch.setenv("SP500_LOCAL_DATA_DIR", str(tmp_path))
    to_local(pd.DataFrame({"a": [1]}), "OUT")
    mock_gcp.upload_blob.side_effect = google_exceptions.Forbidden("billing disabled")

    with pytest.raises(google_exceptions.Forbidden):
        upload_data_to_gcs_from_local("bucket", "OUT.csv", "input-data/OUT.csv")


# ------------------------------------------------------- staging + MERGE

def _ingest(mock_gcp, output_rows=5):
    mock_gcp.bq_client.load_table_from_uri.return_value.output_rows = output_rows
    return ingest_from_gcs_to_bquery("SP_500_DATA", "SP_500_DATA_table", "gs://b/x.csv")


def test_ingest_loads_staging_with_positional_schema_then_merges(mock_gcp):
    result = _ingest(mock_gcp)

    load_args, load_kwargs = mock_gcp.bq_client.load_table_from_uri.call_args
    assert load_args[1] == "test-project.SP_500_DATA.SP_500_DATA_table_staging"
    schema_names = [f.name for f in load_kwargs["job_config"].schema]
    assert schema_names == TIINGO_COLUMNS

    merge_sql = mock_gcp.bq_client.query.call_args[0][0]
    assert "MERGE `test-project.SP_500_DATA.SP_500_DATA_table` T" in merge_sql
    assert "ON T.symbol = S.symbol AND T.date = S.date" in merge_sql
    assert "symbol = S.symbol" not in merge_sql.split("UPDATE SET")[1].split("WHEN NOT")[0]
    assert result == {"rows_loaded": 5}


def test_ingest_skips_merge_when_staging_is_empty(mock_gcp):
    result = _ingest(mock_gcp, output_rows=0)

    mock_gcp.bq_client.query.assert_not_called()
    assert result == {"rows_loaded": 0}


def test_ingest_creates_dataset_and_partitioned_table_only_when_not_found(mock_gcp):
    mock_gcp.bq_client.get_dataset.side_effect = google_exceptions.NotFound("ds")
    mock_gcp.bq_client.get_table.side_effect = google_exceptions.NotFound("t")

    _ingest(mock_gcp)

    mock_gcp.bq_client.create_dataset.assert_called_once()
    table = mock_gcp.bq_client.create_table.call_args[0][0]
    assert table.time_partitioning.field == "date"
    assert table.clustering_fields == ["symbol"]


def test_ingest_propagates_permission_errors_instead_of_trying_to_create(mock_gcp):
    """A Forbidden on get_dataset used to be caught by a bare `except
    Exception` and turned into a confusing create_dataset failure."""
    mock_gcp.bq_client.get_dataset.side_effect = google_exceptions.Forbidden("no access")

    with pytest.raises(google_exceptions.Forbidden):
        _ingest(mock_gcp)
    mock_gcp.bq_client.create_dataset.assert_not_called()


def test_ingest_reraises_merge_failures(mock_gcp):
    mock_gcp.bq_client.query.return_value.result.side_effect = google_exceptions.BadRequest(
        "merge failed"
    )
    with pytest.raises(google_exceptions.BadRequest):
        _ingest(mock_gcp)


def test_ingest_rejects_invalid_dataset_name_before_any_bigquery_call(mock_gcp):
    with pytest.raises(ValueError):
        ingest_from_gcs_to_bquery("bad`name", "t", "gs://b/x.csv")
    mock_gcp.bq_client.get_dataset.assert_not_called()
