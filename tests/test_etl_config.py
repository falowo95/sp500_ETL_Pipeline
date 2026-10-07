"""
test_etl_config.py

Regression tests for ETLConfig. Guards specifically against the bug class where a
config value (e.g. tiingo_api_key) was defined as a plain method but read as a bare
attribute at the call site, silently passing a bound method object instead of the
resolved value.
"""
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pytest

from config.etl_config import ETLConfig


@pytest.fixture(autouse=True)
def gcp_env(monkeypatch):
    monkeypatch.setenv("GCP_PROJECT_ID", "test-project")
    monkeypatch.setenv("GCP_GCS_BUCKET", "test-bucket")
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "/tmp/fake-creds.json")


def test_tiingo_api_key_resolves_to_a_string_not_a_bound_method():
    config = ETLConfig()
    with patch("config.etl_config.SecretManagerService") as mock_service:
        mock_service.get_instance.return_value.get_secret.return_value = "fake-tiingo-key"
        value = config.tiingo_api_key

    assert isinstance(value, str)
    assert value == "fake-tiingo-key"


def test_tiingo_api_key_is_cached_after_first_access():
    config = ETLConfig()
    with patch("config.etl_config.SecretManagerService") as mock_service:
        mock_service.get_instance.return_value.get_secret.return_value = "fake-tiingo-key"
        _ = config.tiingo_api_key
        _ = config.tiingo_api_key

    mock_service.get_instance.return_value.get_secret.assert_called_once()


def test_data_end_date_defaults_to_today_not_a_stale_hardcoded_date():
    config = ETLConfig()
    assert config.data_end_date == datetime.now().strftime("%Y-%m-%d")


def test_dataset_name_uses_gcp_bq_dataset_when_set(monkeypatch):
    monkeypatch.setenv("GCP_BQ_DATASET", "sp_500_data")
    config = ETLConfig()
    assert config.dataset_name == "sp_500_data"
    assert config.table_name == "sp_500_data_table"


def test_dataset_name_falls_back_to_file_name(monkeypatch):
    monkeypatch.delenv("GCP_BQ_DATASET", raising=False)
    config = ETLConfig()
    assert config.dataset_name == "SP_500_DATA"
    assert config.table_name == "SP_500_DATA_table"


def test_dataset_name_rejects_sql_injection(monkeypatch):
    monkeypatch.setenv("GCP_BQ_DATASET", "sp_500`; drop")
    with pytest.raises(ValueError):
        _ = ETLConfig().dataset_name


def test_dbt_models_stay_in_the_target_dataset():
    """Custom +schema would build a second dataset IAM does not grant."""
    root = Path(__file__).resolve().parents[1] / "dags" / "dbt" / "dbt_sp500"
    project = "\n".join(
        line.split("#", 1)[0] for line in (root / "dbt_project.yml").read_text().splitlines()
    )
    assert "+schema:" not in project
    schema = (root / "models" / "staging" / "schema.yml").read_text()
    assert "{{ env_var('GCP_BQ_DATASET', 'SP_500_DATA') }}_table" in schema


def test_missing_required_env_var_raises_value_error(monkeypatch):
    monkeypatch.delenv("GCP_GCS_BUCKET", raising=False)
    config = ETLConfig()
    with pytest.raises(ValueError):
        _ = config.bucket_name
