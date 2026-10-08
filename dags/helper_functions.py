"""
Task callables for the SP 500 pipeline: extract S&P 500 prices from Tiingo
to a local CSV, upload it to GCS, and MERGE it into BigQuery.

Functions:
- to_local(data_frame, file_name) -> Path: save a DataFrame as CSV in the local data dir.
- extract_sp500_data_to_csv(file_name, start_date, end_date) -> dict:
  incremental per-ticker extract from Tiingo; returns a run summary (XCom).
- upload_data_to_gcs_from_local(bucket_name, source_file_path_local, destination_blob_path)
- ingest_from_gcs_to_bquery(dataset_name, table_name, csv_uri) -> dict:
  load to a staging table, then MERGE into the partitioned target table.
"""

import logging
import os
from datetime import date, datetime, timedelta
from io import StringIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
from google.api_core import exceptions as google_exceptions
from google.cloud import bigquery

from config.etl_config import ETLConfig
from config.gcp_service import GCPService
from config.validation import (
    is_valid_ticker,
    qualified_dataset_id,
    qualified_table_id,
    validate_date_range,
)

logger = logging.getLogger(__name__)

# Column order BigQuery's CSV loader maps positionally — see
# extract_sp500_data_to_csv and BQ_SCHEMA below, which must both stay in
# lockstep with this list (documented fragility, see CLAUDE.md's "Column
# order matters" note).
TIINGO_COLUMNS = [
    "symbol",
    "date",
    "close",
    "high",
    "low",
    "open",
    "volume",
    "adjClose",
    "adjHigh",
    "adjLow",
    "adjOpen",
    "adjVolume",
    "divCash",
    "splitFactor",
]

# Only raw/cleaned OHLCV + adjusted-price fields are landed here — derived
# indicators are computed downstream by dbt.
BQ_SCHEMA = [
    bigquery.SchemaField("symbol", "STRING"),
    bigquery.SchemaField("date", "DATETIME"),
    bigquery.SchemaField("close", "FLOAT"),
    bigquery.SchemaField("high", "FLOAT"),
    bigquery.SchemaField("low", "FLOAT"),
    bigquery.SchemaField("open", "FLOAT"),
    bigquery.SchemaField("volume", "INTEGER"),
    bigquery.SchemaField("adjClose", "FLOAT"),
    bigquery.SchemaField("adjHigh", "FLOAT"),
    bigquery.SchemaField("adjLow", "FLOAT"),
    bigquery.SchemaField("adjOpen", "FLOAT"),
    bigquery.SchemaField("adjVolume", "INTEGER"),
    bigquery.SchemaField("divCash", "FLOAT"),
    bigquery.SchemaField("splitFactor", "FLOAT"),
]
MERGE_KEY_COLUMNS = ("symbol", "date")

SP500_CONSTITUENTS_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
TIINGO_PRICES_URL = "https://api.tiingo.com/tiingo/daily/{symbol}/prices"
HTTP_TIMEOUT_SECONDS = 30
HTTP_USER_AGENT = "sp500-etl-pipeline/1.0 (+https://github.com/falowo95/sp500_ETL_Pipeline)"

LOCAL_DATA_DIR_ENV = "SP500_LOCAL_DATA_DIR"
DEFAULT_LOCAL_DATA_DIR = "/tmp/airflow_data"

# Per-ticker failures that mean "this ticker's data is unavailable right
# now" (HTTP/network errors, non-JSON body, Tiingo schema drift). Anything
# else is a programming error and propagates.
TICKER_FETCH_ERRORS = (requests.RequestException, ValueError, KeyError)

STATUS_OK = "ok"
STATUS_UP_TO_DATE = "up_to_date"
STATUS_FAILED = "failed"


class ExtractionError(RuntimeError):
    """Raised when extraction cannot produce a trustworthy result."""


def _local_data_dir() -> Path:
    """Directory shared by to_local (writer) and the GCS upload (reader)."""
    return Path(os.getenv(LOCAL_DATA_DIR_ENV, DEFAULT_LOCAL_DATA_DIR))


def to_local(data_frame: pd.DataFrame, file_name: str) -> Path:
    """Save a DataFrame to `<local data dir>/<file_name>.csv`."""
    data_dir = _local_data_dir()
    data_dir.mkdir(parents=True, exist_ok=True)

    # file_name becomes one path segment. A slash or ".." would write
    # outside the data directory.
    if not file_name or Path(file_name).name != file_name or file_name in {".", ".."}:
        raise ValueError(f"Invalid file name: {file_name!r}")

    path = data_dir / f"{file_name}.csv"
    data_frame.to_csv(path, index=False)
    logger.info("File has been saved at: %s", path)
    return path


# --------------------------------------------------------------- extract


def _get_symbol_watermarks(dataset_name: str, table_name: str) -> Dict[str, date]:
    """Return the latest already-loaded trade date per symbol.

    Returns an empty dict if the target table doesn't exist yet (the very
    first run ever) — every symbol is then treated as a cold start.
    """
    gcp = GCPService.get_instance()
    table_id = qualified_table_id(gcp.project_id, dataset_name, table_name)
    query = f"SELECT symbol, MAX(date) AS last_date FROM `{table_id}` GROUP BY symbol"
    try:
        rows = gcp.bq_client.query(query).result()
    except google_exceptions.NotFound:
        logger.info("%s does not exist yet — treating all symbols as cold start", table_id)
        return {}

    return {row.symbol: _coerce_watermark(row.last_date) for row in rows}


def _coerce_watermark(value: Any) -> Optional[date]:
    """Normalize one BigQuery MAX(date) value to a date.

    A DATETIME column arrives as datetime, a DATE column as date, and the
    original table stored ISO timestamps in a STRING column. Leaving a
    string in place makes the next-day calculation raise TypeError.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        return date.fromisoformat(value[:10])
    raise TypeError(f"Unsupported watermark type: {type(value).__name__}")


def _resolve_ticker_start_date(watermark: Optional[date], fallback_start_date: str) -> str:
    """Return the ISO start date to request from Tiingo for one ticker.

    A ticker with a known watermark resumes the day after it. A ticker with
    no watermark — brand new to the index, or the very first run ever —
    gets the full configured backfill instead: a single global watermark
    applied uniformly would silently under-backfill any ticker added to the
    index after the pipeline's first run.
    """
    if watermark is None:
        return fallback_start_date
    return (watermark + timedelta(days=1)).isoformat()


def fetch_sp500_tickers() -> List[str]:
    """Scrape the current S&P 500 constituent symbols from Wikipedia.

    Uses requests (explicit timeout + descriptive User-Agent, which
    Wikipedia's robot policy requires) rather than letting pandas fetch the
    URL itself. Returns de-duplicated symbols in page order.
    """
    response = requests.get(
        SP500_CONSTITUENTS_URL,
        headers={"User-Agent": HTTP_USER_AGENT},
        timeout=HTTP_TIMEOUT_SECONDS,
    )
    response.raise_for_status()

    tables = pd.read_html(StringIO(response.text))
    table = next((candidate for candidate in tables if "Symbol" in getattr(candidate, "columns", [])), None)
    if table is None:
        raise ExtractionError(
            "S&P 500 constituents table has no 'Symbol' column — page layout changed"
        )
    symbols = [str(s).strip() for s in table["Symbol"].dropna().tolist()]
    if not symbols:
        raise ExtractionError("S&P 500 constituents table is empty")
    return list(dict.fromkeys(symbols))


def _tiingo_symbol(ticker: str) -> str:
    """Tiingo spells share classes with a dash (BRK-B); Wikipedia uses a dot."""
    return ticker.replace(".", "-")


def _fetch_ticker_prices(ticker: str, start_date: str, end_date: str, api_key: str) -> pd.DataFrame:
    """Fetch one ticker's daily prices, reordered to TIINGO_COLUMNS.

    The API token travels in the Authorization header, never the query
    string, so it can't leak via exception messages (requests.HTTPError
    embeds the full URL) into logs.
    """
    response = requests.get(
        TIINGO_PRICES_URL.format(symbol=_tiingo_symbol(ticker)),
        headers={"Content-Type": "application/json", "Authorization": f"Token {api_key}"},
        params={"startDate": start_date, "endDate": end_date},
        timeout=HTTP_TIMEOUT_SECONDS,
    )
    response.raise_for_status()

    prices = pd.DataFrame(response.json())
    if prices.empty:
        return prices
    # Selecting explicit columns also fails loudly (KeyError) if Tiingo
    # ever drops/renames a field — BigQuery maps CSV columns positionally.
    return prices.assign(symbol=ticker)[TIINGO_COLUMNS]


def _extract_ticker(
    ticker: str,
    watermark: Optional[date],
    start_date: str,
    end_date: str,
    api_key: str,
) -> Tuple[str, Optional[pd.DataFrame]]:
    """Classify one ticker as (status, frame) — never raises for expected
    per-ticker failures, which are reported in the run summary instead."""
    if not is_valid_ticker(ticker):
        logger.warning("Skipping invalid ticker symbol %r from constituents list", ticker)
        return STATUS_FAILED, None

    ticker_start_date = _resolve_ticker_start_date(watermark, fallback_start_date=start_date)
    if ticker_start_date > end_date:
        # Already loaded through end_date (e.g. a same-day re-run).
        return STATUS_UP_TO_DATE, None

    try:
        prices = _fetch_ticker_prices(ticker, ticker_start_date, end_date, api_key)
    except TICKER_FETCH_ERRORS as exc:
        logger.error("Error while extracting data for %s: %s", ticker, exc)
        return STATUS_FAILED, None

    if prices.empty:
        return STATUS_UP_TO_DATE, None
    logger.info("Retrieved %d rows for %s from %s", len(prices), ticker, ticker_start_date)
    return STATUS_OK, prices


def _tickers_with_status(results: List[Tuple[str, str, Any]], status: str) -> List[str]:
    return [ticker for ticker, result_status, _ in results if result_status == status]


def _write_extract_output(frames: List[pd.DataFrame], file_name: str) -> Path:
    """Write the combined extract, or an empty correctly-shaped placeholder
    when nothing is new (downstream tasks expect the CSV to exist)."""
    if not frames:
        logger.info("No new rows for any ticker — writing empty placeholder CSV")
        return to_local(pd.DataFrame(columns=TIINGO_COLUMNS), file_name)

    combined = pd.concat(frames, axis=0, ignore_index=True)
    combined = combined.assign(date=pd.to_datetime(combined["date"]).dt.date)
    # MERGE fails if a target row matches more than one source row.
    deduplicated = combined.drop_duplicates(subset=list(MERGE_KEY_COLUMNS), keep="last")
    return to_local(deduplicated, file_name)


def extract_sp500_data_to_csv(file_name: str, start_date: str, end_date: str) -> Dict[str, Any]:
    """Extract all S&P 500 tickers from Tiingo, incrementally, to a local CSV.

    Each ticker is pulled starting the day after its own watermark (the
    latest trade date already loaded into BigQuery) — see
    _resolve_ticker_start_date for the cold-start exception.

    The Tiingo API key is resolved from GCP Secret Manager at task-execution
    time (not DAG-parse time) so DAG parsing never depends on live GCP
    credentials.

    Returns a summary dict (pushed to XCom, consumed by the pipeline_runs
    audit row). Raises ExtractionError if every attempted ticker failed.
    """
    validate_date_range(start_date, end_date)

    config = ETLConfig()
    api_key = config.tiingo_api_key
    watermarks = _get_symbol_watermarks(dataset_name=config.dataset_name, table_name=config.table_name)
    sp500_tickers = fetch_sp500_tickers()

    results = [
        (ticker, *_extract_ticker(ticker, watermarks.get(ticker), start_date, end_date, api_key))
        for ticker in sp500_tickers
    ]
    frames = [frame for _, status, frame in results if status == STATUS_OK]
    failed_tickers = _tickers_with_status(results, STATUS_FAILED)
    up_to_date_tickers = _tickers_with_status(results, STATUS_UP_TO_DATE)

    if failed_tickers:
        logger.warning("Failed to retrieve data for %d tickers: %s", len(failed_tickers), failed_tickers)
    if up_to_date_tickers:
        logger.info("%d tickers already up to date, skipped", len(up_to_date_tickers))
    if not frames and failed_tickers and not up_to_date_tickers:
        # Every ticker failed outright (e.g. Tiingo/network down) — a real
        # failure, not the steady-state "nothing new today" case.
        raise ExtractionError("No data retrieved: every ticker failed")

    save_to = _write_extract_output(frames, file_name)
    logger.info("Extract written to: %s", save_to)

    return {
        "attempted": len(sp500_tickers),
        "succeeded": len(frames),
        "failed": len(failed_tickers),
        "failed_tickers": failed_tickers,
        "up_to_date": len(up_to_date_tickers),
    }


# ------------------------------------------------------------------ load


def upload_data_to_gcs_from_local(
    bucket_name: str, source_file_path_local: str, destination_blob_path: str
) -> None:
    """Upload the extract CSV from the local data dir to GCS.

    Errors (including Forbidden) propagate: an earlier version swallowed
    Forbidden and "backed up" to an ephemeral local dir, letting the BigQuery
    load proceed against whatever stale file was already in GCS.
    """
    source_path = _local_data_dir() / source_file_path_local
    if not source_path.exists():
        raise FileNotFoundError(f"Source file not found: {source_path}")

    gcp = GCPService.get_instance()
    gcp.upload_blob(
        bucket_name=bucket_name,
        source_file=str(source_path),
        destination_blob=destination_blob_path,
    )
    logger.info("File %s uploaded to gs://%s/%s", source_path, bucket_name, destination_blob_path)


def _ensure_dataset(bq_client, dataset_ref: str) -> None:
    """Create the dataset only if it is genuinely missing (NotFound)."""
    try:
        bq_client.get_dataset(dataset_ref)
        logger.info("Using existing dataset: %s", dataset_ref)
    except google_exceptions.NotFound:
        bq_client.create_dataset(bigquery.Dataset(dataset_ref))
        logger.info("Created dataset: %s", dataset_ref)


def _ensure_partitioned_target_table(bq_client, table_id: str, schema) -> None:
    """Create the target table, partitioned by date and clustered by symbol,
    if it doesn't already exist. BigQuery can't retrofit partitioning onto
    an existing table, so this only takes the create path once."""
    try:
        bq_client.get_table(table_id)
    except google_exceptions.NotFound:
        table = bigquery.Table(table_id, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, field="date"
        )
        table.clustering_fields = ["symbol"]
        bq_client.create_table(table)
        logger.info("Created partitioned/clustered target table: %s", table_id)


def _build_merge_sql(target_table_id: str, staging_table_id: str) -> str:
    """MERGE keyed on (symbol, date). Table ids must already be validated
    by qualified_table_id; column names come from the static BQ_SCHEMA."""
    columns = [field.name for field in BQ_SCHEMA]
    update_clause = ", ".join(f"{c} = S.{c}" for c in columns if c not in MERGE_KEY_COLUMNS)
    insert_columns = ", ".join(columns)
    insert_values = ", ".join(f"S.{c}" for c in columns)
    return f"""
        MERGE `{target_table_id}` T
        USING `{staging_table_id}` S
        ON T.symbol = S.symbol AND T.date = S.date
        WHEN MATCHED THEN UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN
            INSERT ({insert_columns}) VALUES ({insert_values})
    """


def ingest_from_gcs_to_bquery(dataset_name: str, table_name: str, csv_uri: str) -> Dict[str, int]:
    """Load the day's CSV from GCS into a staging table, then MERGE it into
    the partitioned/clustered target table keyed on (symbol, date).

    MERGE makes re-running the same day's load (e.g. after a Cloud Run Job
    retry) a no-op rather than a duplicate. Returns {"rows_loaded": n}.
    """
    gcp = GCPService.get_instance()
    dataset_ref = qualified_dataset_id(gcp.project_id, dataset_name)
    target_table_id = qualified_table_id(gcp.project_id, dataset_name, table_name)
    staging_table_id = qualified_table_id(gcp.project_id, dataset_name, f"{table_name}_staging")

    _ensure_dataset(gcp.bq_client, dataset_ref)
    _ensure_partitioned_target_table(gcp.bq_client, target_table_id, BQ_SCHEMA)

    staging_job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.CSV,
        schema=BQ_SCHEMA,
    )
    try:
        load_job = gcp.bq_client.load_table_from_uri(
            csv_uri, staging_table_id, job_config=staging_job_config
        )
        load_job.result()
        logger.info("Loaded %s rows into staging table %s", load_job.output_rows, staging_table_id)

        if not load_job.output_rows:
            logger.info("Staging table empty — nothing to merge this run")
            return {"rows_loaded": 0}

        gcp.bq_client.query(_build_merge_sql(target_table_id, staging_table_id)).result()
        logger.info("Merged staging rows into %s", target_table_id)
        return {"rows_loaded": load_job.output_rows}
    except google_exceptions.GoogleAPIError as exc:
        logger.error("Error loading data into BigQuery (%s): %s", target_table_id, exc)
        raise
