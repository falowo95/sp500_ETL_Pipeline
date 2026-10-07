"""
test_extract.py

Tests for the extraction side of helper_functions.py: input validation
(dates, tickers), the Wikipedia constituent scrape, the per-ticker Tiingo
call, and extract_sp500_data_to_csv's orchestration/summary. All HTTP and
GCP access is mocked — no network, no credentials.
"""
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import requests

import helper_functions
from helper_functions import (
    TIINGO_COLUMNS,
    ExtractionError,
    extract_sp500_data_to_csv,
    fetch_sp500_tickers,
    is_valid_ticker,
    validate_date_range,
)

FAKE_KEY = "fake-tiingo-key-123"


def _tiingo_row(date="2026-01-05T00:00:00.000Z"):
    return {
        "date": date, "close": 1.0, "high": 2.0, "low": 0.5, "open": 1.1,
        "volume": 100, "adjClose": 1.0, "adjHigh": 2.0, "adjLow": 0.5,
        "adjOpen": 1.1, "adjVolume": 100, "divCash": 0.0, "splitFactor": 1.0,
    }


def _response(json_body=None, status_error=None):
    resp = MagicMock()
    resp.json.return_value = json_body if json_body is not None else []
    resp.raise_for_status.side_effect = status_error
    return resp


# ---------------------------------------------------------------- validation

def test_validate_date_range_accepts_ordered_iso_dates():
    assert validate_date_range("2015-01-01", "2026-01-05") == ("2015-01-01", "2026-01-05")


@pytest.mark.parametrize(
    "start,end",
    [("2015/01/01", "2026-01-05"), ("2015-01-01", "yesterday"), ("", "2026-01-05")],
)
def test_validate_date_range_rejects_non_iso_dates(start, end):
    with pytest.raises(ValueError):
        validate_date_range(start, end)


def test_validate_date_range_rejects_start_after_end():
    with pytest.raises(ValueError, match="after"):
        validate_date_range("2026-02-01", "2026-01-01")


@pytest.mark.parametrize("ticker", ["AAPL", "A", "BRK.B", "BF-B", "GOOGL"])
def test_is_valid_ticker_accepts_real_symbols(ticker):
    assert is_valid_ticker(ticker)


@pytest.mark.parametrize(
    "ticker", ["", "aapl", "../etc", "AAPL/prices?x=1", "A A", None, 123, "TOOLONGSYMBOLX"]
)
def test_is_valid_ticker_rejects_path_or_query_injection(ticker):
    assert not is_valid_ticker(ticker)


# ------------------------------------------------------- Wikipedia scrape

@patch("helper_functions.requests.get")
def test_fetch_sp500_tickers_parses_symbol_column_with_timeout_and_user_agent(mock_get):
    mock_get.return_value = MagicMock(
        text="<table><tr><th>Symbol</th></tr><tr><td>AAPL</td></tr>"
        "<tr><td>MSFT</td></tr><tr><td>AAPL</td></tr></table>"
    )

    tickers = fetch_sp500_tickers()

    assert tickers == ["AAPL", "MSFT"]  # de-duplicated, order preserved
    _, kwargs = mock_get.call_args
    assert kwargs["timeout"] == helper_functions.HTTP_TIMEOUT_SECONDS
    assert "User-Agent" in kwargs["headers"]


@patch("helper_functions.requests.get")
def test_fetch_sp500_tickers_skips_leading_tables_without_a_symbol_column(mock_get):
    mock_get.return_value = MagicMock(
        text="<table><tr><th>Note</th></tr><tr><td>x</td></tr></table>"
        "<table><tr><th>Symbol</th></tr><tr><td>AAPL</td></tr></table>"
    )
    assert fetch_sp500_tickers() == ["AAPL"]


@patch("helper_functions.requests.get")
def test_fetch_sp500_tickers_raises_when_symbol_column_missing(mock_get):
    mock_get.return_value = MagicMock(
        text="<table><tr><th>Ticker</th></tr><tr><td>AAPL</td></tr></table>"
    )
    with pytest.raises(ExtractionError, match="Symbol"):
        fetch_sp500_tickers()


@patch("helper_functions.requests.get")
def test_fetch_sp500_tickers_propagates_http_errors(mock_get):
    mock_get.return_value.raise_for_status.side_effect = requests.HTTPError("403")
    with pytest.raises(requests.HTTPError):
        fetch_sp500_tickers()


# ------------------------------------------------------------ orchestration

@pytest.fixture
def extract_env(monkeypatch, tmp_path):
    """ETLConfig, watermarks and ticker list mocked; CSV written to tmp_path."""
    monkeypatch.setenv("SP500_LOCAL_DATA_DIR", str(tmp_path))
    config = MagicMock(tiingo_api_key=FAKE_KEY, dataset_name="DS", table_name="DS_table")
    with patch("helper_functions.ETLConfig", return_value=config), patch(
        "helper_functions._get_symbol_watermarks", return_value={}
    ) as mock_wm, patch("helper_functions.fetch_sp500_tickers") as mock_tickers, patch(
        "helper_functions.requests.get"
    ) as mock_get:
        yield {"tmp": tmp_path, "tickers": mock_tickers, "get": mock_get, "wm": mock_wm}


def test_extract_sends_tiingo_token_in_header_not_url(extract_env):
    """A token in the query string ends up inside requests.HTTPError messages
    ('... for url: ...&token=...'), which the per-ticker handler logs to
    Cloud Logging. The token must travel in the Authorization header."""
    extract_env["tickers"].return_value = ["AAPL"]
    extract_env["get"].return_value = _response([_tiingo_row()])

    extract_sp500_data_to_csv("OUT", "2015-01-01", "2026-01-05")

    args, kwargs = extract_env["get"].call_args
    assert FAKE_KEY not in str(args) + str(kwargs.get("params"))
    assert kwargs["headers"]["Authorization"] == f"Token {FAKE_KEY}"
    assert kwargs["timeout"] == helper_functions.HTTP_TIMEOUT_SECONDS


def test_extract_writes_csv_in_positional_schema_order(extract_env):
    extract_env["tickers"].return_value = ["AAPL"]
    extract_env["get"].return_value = _response([_tiingo_row(), _tiingo_row()])

    summary = extract_sp500_data_to_csv("OUT", "2015-01-01", "2026-01-05")

    written = pd.read_csv(extract_env["tmp"] / "OUT.csv")
    assert list(written.columns) == TIINGO_COLUMNS
    assert len(written) == 1  # duplicate (symbol, date) rows dropped before MERGE
    assert summary == {
        "attempted": 1, "succeeded": 1, "failed": 0, "failed_tickers": [], "up_to_date": 0,
    }


def test_extract_maps_share_class_dot_to_tiingo_dash_but_keeps_symbol(extract_env):
    extract_env["tickers"].return_value = ["BRK.B"]
    extract_env["get"].return_value = _response([_tiingo_row()])

    extract_sp500_data_to_csv("OUT", "2015-01-01", "2026-01-05")

    url = extract_env["get"].call_args[0][0]
    assert url.endswith("/daily/BRK-B/prices")
    assert pd.read_csv(extract_env["tmp"] / "OUT.csv")["symbol"].tolist() == ["BRK.B"]


def test_extract_records_invalid_and_http_failed_tickers_without_calling_api(extract_env):
    extract_env["tickers"].return_value = ["AAPL", "../evil", "MSFT"]
    extract_env["get"].side_effect = [
        _response([_tiingo_row()]),
        _response(status_error=requests.HTTPError("500")),
    ]

    summary = extract_sp500_data_to_csv("OUT", "2015-01-01", "2026-01-05")

    assert extract_env["get"].call_count == 2  # invalid ticker never hits the API
    assert summary["failed_tickers"] == ["../evil", "MSFT"]
    assert summary["succeeded"] == 1


def test_extract_skips_up_to_date_tickers_and_writes_empty_placeholder(extract_env):
    from datetime import date

    extract_env["wm"].return_value = {"AAPL": date(2026, 1, 5)}
    extract_env["tickers"].return_value = ["AAPL"]

    summary = extract_sp500_data_to_csv("OUT", "2015-01-01", "2026-01-05")

    extract_env["get"].assert_not_called()
    assert summary["up_to_date"] == 1
    placeholder = pd.read_csv(extract_env["tmp"] / "OUT.csv")
    assert list(placeholder.columns) == TIINGO_COLUMNS and placeholder.empty


def test_extract_treats_empty_tiingo_response_as_up_to_date(extract_env):
    extract_env["tickers"].return_value = ["AAPL"]
    extract_env["get"].return_value = _response([])

    summary = extract_sp500_data_to_csv("OUT", "2015-01-01", "2026-01-05")

    assert summary["up_to_date"] == 1 and summary["failed"] == 0


def test_extract_raises_when_every_ticker_fails(extract_env):
    extract_env["tickers"].return_value = ["AAPL", "MSFT"]
    extract_env["get"].side_effect = requests.ConnectionError("down")

    with pytest.raises(ExtractionError):
        extract_sp500_data_to_csv("OUT", "2015-01-01", "2026-01-05")


def test_extract_treats_tiingo_schema_drift_as_ticker_failure(extract_env):
    extract_env["tickers"].return_value = ["AAPL"]
    row = {k: v for k, v in _tiingo_row().items() if k != "adjClose"}
    extract_env["get"].return_value = _response([row])

    with pytest.raises(ExtractionError):
        extract_sp500_data_to_csv("OUT", "2015-01-01", "2026-01-05")


def test_extract_rejects_bad_date_range_before_any_io(extract_env):
    with pytest.raises(ValueError):
        extract_sp500_data_to_csv("OUT", "2026-02-01", "2026-01-01")
    extract_env["tickers"].assert_not_called()
    extract_env["get"].assert_not_called()


def test_extract_failure_logs_never_contain_the_api_key(extract_env, caplog):
    extract_env["tickers"].return_value = ["AAPL"]
    extract_env["get"].side_effect = requests.ConnectionError("boom")

    with pytest.raises(ExtractionError):
        extract_sp500_data_to_csv("OUT", "2015-01-01", "2026-01-05")

    assert FAKE_KEY not in caplog.text
