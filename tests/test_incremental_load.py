"""
test_incremental_load.py

Tests for the watermark-based incremental extraction logic in
helper_functions.py: _resolve_ticker_start_date (the per-symbol cold-start
exception) and _get_symbol_watermarks (reading existing watermarks from
BigQuery, mocked — no real credentials or network access required).
"""
from datetime import date, datetime
from unittest.mock import MagicMock, patch

from google.api_core import exceptions as google_exceptions

from helper_functions import _get_symbol_watermarks, _resolve_ticker_start_date


def test_resolve_ticker_start_date_uses_day_after_watermark():
    result = _resolve_ticker_start_date(
        watermark=date(2026, 1, 5), fallback_start_date="2015-01-01"
    )
    assert result == "2026-01-06"


def test_resolve_ticker_start_date_falls_back_to_full_backfill_when_no_watermark():
    """A ticker with no existing rows — brand new to the index, or the very
    first run ever — must get the full configured backfill, not be silently
    skipped or limited to just today."""
    result = _resolve_ticker_start_date(watermark=None, fallback_start_date="2015-01-01")
    assert result == "2015-01-01"


@patch("helper_functions.GCPService")
def test_get_symbol_watermarks_returns_empty_dict_when_table_not_found(mock_service):
    """Very first run ever: the target table doesn't exist yet. Every
    symbol must be treated as a cold start, not raise."""
    mock_gcp = MagicMock()
    mock_gcp.project_id = "test-project"
    mock_gcp.bq_client.query.side_effect = google_exceptions.NotFound("no table")
    mock_service.get_instance.return_value = mock_gcp

    result = _get_symbol_watermarks(dataset_name="SP_500_DATA", table_name="SP_500_DATA_table")

    assert result == {}


@patch("helper_functions.GCPService")
def test_get_symbol_watermarks_normalizes_datetime_rows_to_date(mock_service):
    """BigQuery's DATETIME column returns naive datetime.datetime values via
    the Python client; watermarks must be normalized to plain dates so
    arithmetic against them (+ timedelta) stays unambiguous."""
    mock_row = MagicMock(symbol="AAPL", last_date=datetime(2026, 1, 5, 0, 0, 0))
    mock_gcp = MagicMock()
    mock_gcp.project_id = "test-project"
    mock_gcp.bq_client.query.return_value.result.return_value = [mock_row]
    mock_service.get_instance.return_value = mock_gcp

    result = _get_symbol_watermarks(dataset_name="SP_500_DATA", table_name="SP_500_DATA_table")

    assert result == {"AAPL": date(2026, 1, 5)}
