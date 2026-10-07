"""
test_gcp_config.py

Tests for GCPUtils (dags/config/gcp_config.py). Storage/BigQuery clients
and service-account loading are mocked — no credentials, no network.
"""
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from config.gcp_config import GCPUtils


@pytest.fixture
def utils():
    with patch("config.gcp_config.storage.Client") as storage_client, patch(
        "config.gcp_config.bigquery.Client"
    ) as bq_client:
        instance = GCPUtils(project_id="test-project")
        yield instance, storage_client.return_value, bq_client.return_value


@patch("config.gcp_config.bigquery.Client")
@patch("config.gcp_config.storage.Client")
@patch("config.gcp_config.service_account.Credentials.from_service_account_file")
def test_key_file_is_used_only_when_explicitly_given(mock_creds, mock_storage, mock_bq):
    instance = GCPUtils(project_id="test-project", credentials_path="/tmp/key.json")

    mock_creds.assert_called_once_with("/tmp/key.json")
    assert instance.credentials is mock_creds.return_value
    mock_storage.assert_called_once_with(credentials=mock_creds.return_value, project="test-project")


def test_upload_blob(utils):
    instance, storage_client, _ = utils
    instance.upload_blob("bucket", "/tmp/a.csv", "input-data/a.csv")

    storage_client.bucket.assert_called_once_with("bucket")
    blob = storage_client.bucket.return_value.blob
    blob.assert_called_once_with("input-data/a.csv")
    blob.return_value.upload_from_filename.assert_called_once_with("/tmp/a.csv")


def test_download_blob(utils):
    instance, storage_client, _ = utils
    instance.download_blob("bucket", "input-data/a.csv", "/tmp/a.csv")

    blob = storage_client.bucket.return_value.blob.return_value
    blob.download_to_filename.assert_called_once_with("/tmp/a.csv")


def test_list_blobs_returns_names(utils):
    instance, storage_client, _ = utils
    storage_client.list_blobs.return_value = [MagicMock(), MagicMock()]
    storage_client.list_blobs.return_value[0].name = "a.csv"
    storage_client.list_blobs.return_value[1].name = "b.csv"

    assert instance.list_blobs("bucket", prefix="input-data/") == ["a.csv", "b.csv"]
    storage_client.list_blobs.assert_called_once_with("bucket", prefix="input-data/")


def test_query_bigquery_returns_dataframe(utils):
    instance, _, bq_client = utils
    expected = pd.DataFrame({"x": [1]})
    bq_client.query.return_value.to_dataframe.return_value = expected

    assert instance.query_bigquery("SELECT 1").equals(expected)


def test_upload_to_bigquery_passes_write_disposition(utils):
    instance, _, bq_client = utils
    frame = pd.DataFrame({"x": [1]})

    instance.upload_to_bigquery(frame, "p.d.t", write_disposition="WRITE_APPEND")

    args, kwargs = bq_client.load_table_from_dataframe.call_args
    assert args == (frame, "p.d.t")
    assert kwargs["job_config"].write_disposition == "WRITE_APPEND"


def test_delete_blob(utils):
    instance, storage_client, _ = utils
    instance.delete_blob("bucket", "a.csv")

    storage_client.bucket.return_value.blob.return_value.delete.assert_called_once()
