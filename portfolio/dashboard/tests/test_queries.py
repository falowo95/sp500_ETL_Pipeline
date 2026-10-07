"""
Tests for app/queries.py — mocked BigQuery client, no real credentials or
network access required.
"""
import os
from unittest.mock import MagicMock, patch

import pytest

os.environ.setdefault("GCP_PROJECT_ID", "test-project")
os.environ.setdefault("GCP_BQ_DATASET", "test_dataset")

from app import queries  # noqa: E402  (env vars must be set before import)
from app.bq_client import dataset_ref  # noqa: E402


def test_dataset_ref_rejects_sql_injection(monkeypatch):
    monkeypatch.setenv("GCP_PROJECT_ID", "test-project")
    monkeypatch.setenv("GCP_BQ_DATASET", "ds`; DROP TABLE x --")
    with pytest.raises(ValueError):
        dataset_ref()


@patch("app.queries.get_client")
def test_top_movers_returns_rows_as_dicts(mock_get_client):
    mock_client = MagicMock()
    mock_client.query.return_value.result.return_value = [
        {"symbol": "AAPL", "trade_date": "2026-01-05", "close": 150.0, "daily_change_pct": 2.5}
    ]
    mock_get_client.return_value = mock_client

    result = queries.top_movers(limit=5)

    assert result == [{"symbol": "AAPL", "trade_date": "2026-01-05", "close": 150.0, "daily_change_pct": 2.5}]


@patch("app.queries.get_client")
def test_latest_pipeline_run_returns_empty_dict_when_table_missing(mock_get_client):
    mock_client = MagicMock()
    mock_client.query.return_value.result.side_effect = Exception("table not found")
    mock_get_client.return_value = mock_client

    assert queries.latest_pipeline_run() == {}


@patch("app.queries.get_client")
def test_stock_summary_returns_empty_dict_when_symbol_not_found(mock_get_client):
    mock_client = MagicMock()
    mock_client.query.return_value.result.return_value = []
    mock_get_client.return_value = mock_client

    assert queries.stock_summary("NOTASYMBOL") == {}
