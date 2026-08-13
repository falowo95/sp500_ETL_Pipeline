"""
test_gcp_service.py

Tests for the GCPService singleton, using mocked GCP clients so no real
credentials or network access are required.
"""
from unittest.mock import patch

import pytest

from config.gcp_service import GCPService


@pytest.fixture(autouse=True)
def clear_singleton_cache():
    GCPService.get_instance.cache_clear()
    yield
    GCPService.get_instance.cache_clear()


@patch("config.gcp_config.bigquery.Client")
@patch("config.gcp_config.storage.Client")
@patch("config.gcp_config.service_account.Credentials.from_service_account_file")
def test_get_instance_returns_the_same_singleton(mock_creds, mock_storage, mock_bq):
    first = GCPService.get_instance(
        credentials_path="/tmp/fake.json", project_id="test-project"
    )
    second = GCPService.get_instance(
        credentials_path="/tmp/fake.json", project_id="test-project"
    )

    assert first is second
    mock_creds.assert_called_once()


@patch("config.gcp_config.bigquery.Client")
@patch("config.gcp_config.storage.Client")
@patch("config.gcp_config.service_account.Credentials.from_service_account_file")
def test_get_instance_raises_without_credentials_or_project(
    mock_creds, mock_storage, mock_bq, monkeypatch
):
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
    monkeypatch.delenv("GCP_PROJECT_ID", raising=False)

    with pytest.raises(ValueError):
        GCPService.get_instance()
