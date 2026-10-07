"""
test_secret_manager.py

Tests for SecretManagerService (dags/config/gcp_secret_manager.py). The
Secret Manager client is mocked — no credentials, no network.
"""
from unittest.mock import patch

import pytest
from google.api_core import exceptions as google_exceptions

from config.gcp_secret_manager import SecretManagerService

SECRET_VALUE = "s3cr3t-value-never-logged"


@pytest.fixture(autouse=True)
def clear_singleton_cache():
    SecretManagerService.get_instance.cache_clear()
    yield
    SecretManagerService.get_instance.cache_clear()


@pytest.fixture
def mock_client():
    with patch("config.gcp_secret_manager.secretmanager.SecretManagerServiceClient") as client:
        yield client.return_value


def test_get_instance_requires_project_id(mock_client):
    with pytest.raises(ValueError):
        SecretManagerService.get_instance()


def test_get_instance_is_memoized_per_project(mock_client):
    first = SecretManagerService.get_instance(project_id="test-project")
    second = SecretManagerService.get_instance(project_id="test-project")

    assert first is second
    assert first.project_id == "test-project"


def test_get_instance_does_not_reuse_a_different_project(mock_client):
    first = SecretManagerService.get_instance(project_id="project-one")
    second = SecretManagerService.get_instance(project_id="project-two")

    assert second is not first
    assert second.project_id == "project-two"


def test_get_instance_rejects_project_path_injection(mock_client):
    with pytest.raises(ValueError):
        SecretManagerService.get_instance(project_id="test-project/secrets/other")
    mock_client.access_secret_version.assert_not_called()


def test_get_secret_reads_latest_version_and_decodes(mock_client):
    mock_client.access_secret_version.return_value.payload.data = SECRET_VALUE.encode()

    value = SecretManagerService(project_id="test-project").get_secret("api-tiingo")

    assert value == SECRET_VALUE
    mock_client.access_secret_version.assert_called_once_with(
        name="projects/test-project/secrets/api-tiingo/versions/latest"
    )


def test_get_secret_not_found_becomes_value_error(mock_client):
    mock_client.access_secret_version.side_effect = google_exceptions.NotFound("missing")

    with pytest.raises(ValueError, match="api-tiingo"):
        SecretManagerService(project_id="test-project").get_secret("api-tiingo")


@pytest.mark.parametrize(
    "error", [google_exceptions.PermissionDenied("denied"), RuntimeError("boom")]
)
def test_get_secret_reraises_other_errors(mock_client, error):
    mock_client.access_secret_version.side_effect = error

    with pytest.raises(type(error)):
        SecretManagerService(project_id="test-project").get_secret("api-tiingo")


@pytest.mark.parametrize("secret_id", ["", "../other", "a/versions/1", "x" * 256])
def test_get_secret_rejects_malformed_secret_ids(mock_client, secret_id):
    with pytest.raises(ValueError):
        SecretManagerService(project_id="test-project").get_secret(secret_id)
    mock_client.access_secret_version.assert_not_called()


@pytest.mark.parametrize("version", ["", "../latest", "latest/1", "0", "-1"])
def test_get_secret_rejects_malformed_versions(mock_client, version):
    with pytest.raises(ValueError):
        SecretManagerService(project_id="test-project").get_secret("api-tiingo", version=version)
    mock_client.access_secret_version.assert_not_called()


def test_get_secret_strips_surrounding_whitespace(mock_client):
    mock_client.access_secret_version.return_value.payload.data = b"\napi-key\n"

    assert SecretManagerService(project_id="test-project").get_secret("api-tiingo") == "api-key"


def test_get_secret_rejects_empty_payload(mock_client):
    mock_client.access_secret_version.return_value.payload.data = b"\n"

    with pytest.raises(ValueError, match="empty"):
        SecretManagerService(project_id="test-project").get_secret("api-tiingo")


def test_get_secret_never_logs_the_secret_value(mock_client, caplog):
    mock_client.access_secret_version.return_value.payload.data = SECRET_VALUE.encode()

    SecretManagerService(project_id="test-project").get_secret("api-tiingo")

    assert SECRET_VALUE not in caplog.text
