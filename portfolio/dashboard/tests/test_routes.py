"""
Tests for app/main.py's FastAPI routes — BigQuery calls mocked at the
queries module level, no real credentials or network access required.
"""
import os
from unittest.mock import patch

os.environ.setdefault("GCP_PROJECT_ID", "test-project")
os.environ.setdefault("GCP_BQ_DATASET", "test_dataset")

from fastapi.testclient import TestClient  # noqa: E402

from app.main import app  # noqa: E402

client = TestClient(app)


def test_healthz_returns_ok_without_touching_bigquery():
    response = client.get("/healthz")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


@patch("app.main.queries.latest_pipeline_run", return_value={})
@patch("app.main.queries.signal_distribution", return_value={})
@patch("app.main.queries.top_movers", return_value=[])
@patch("app.main.queries.latest_trade_date", return_value="")
def test_index_renders_with_no_data(mock_date, mock_movers, mock_signals, mock_run):
    response = client.get("/")
    assert response.status_code == 200
    assert "no data yet" in response.text


@patch("app.main.queries.ticker_detail", return_value=[])
def test_ticker_detail_404s_for_unknown_symbol(mock_detail):
    response = client.get("/tickers/NOTASYMBOL")
    assert response.status_code == 404


def test_about_page_renders():
    response = client.get("/about")
    assert response.status_code == 200
