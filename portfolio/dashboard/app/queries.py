"""
Named, parameterized read-only queries against the pipeline's BigQuery
marts (int_stock_daily_metrics, fct_stock_analysis, dim_stock_summary) and
its pipeline_runs audit table.
"""
from typing import Any, Dict, List

from google.cloud import bigquery

from .bq_client import dataset_ref, get_client


def latest_trade_date() -> str:
    client = get_client()
    query = f"SELECT MAX(trade_date) AS latest FROM `{dataset_ref()}.int_stock_daily_metrics`"
    row = next(iter(client.query(query).result()), None)
    return row.latest.isoformat() if row and row.latest else ""


def top_movers(limit: int = 10) -> List[Dict[str, Any]]:
    """Largest absolute daily % change on the most recent trade date."""
    client = get_client()
    query = f"""
        SELECT symbol, trade_date, close, daily_change_pct
        FROM `{dataset_ref()}.int_stock_daily_metrics`
        WHERE trade_date = (SELECT MAX(trade_date) FROM `{dataset_ref()}.int_stock_daily_metrics`)
        ORDER BY ABS(daily_change_pct) DESC
        LIMIT @limit
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("limit", "INT64", limit)]
    )
    return [dict(row) for row in client.query(query, job_config=job_config).result()]


def signal_distribution() -> Dict[str, int]:
    """Count of tickers in each RSI signal bucket on the most recent date."""
    client = get_client()
    query = f"""
        SELECT rsi_signal, COUNT(*) AS n
        FROM `{dataset_ref()}.fct_stock_analysis`
        WHERE trade_date = (SELECT MAX(trade_date) FROM `{dataset_ref()}.fct_stock_analysis`)
        GROUP BY rsi_signal
    """
    return {row.rsi_signal: row.n for row in client.query(query).result()}


def latest_pipeline_run() -> Dict[str, Any]:
    """Most recent pipeline_runs row — the dashboard's substitute for an
    Airflow UI, which doesn't exist in the Cloud Run Jobs architecture."""
    client = get_client()
    query = f"""
        SELECT *
        FROM `{dataset_ref()}.pipeline_runs`
        ORDER BY finished_at DESC
        LIMIT 1
    """
    try:
        rows = list(client.query(query).result())
    except Exception:
        # Table may not exist yet on a brand-new deployment before the
        # first pipeline run has ever completed.
        return {}
    return dict(rows[0]) if rows else {}


def ticker_detail(symbol: str, days: int = 180) -> List[Dict[str, Any]]:
    client = get_client()
    query = f"""
        SELECT
            trade_date, close, ma_20, ma_50, ma_200,
            bollinger_upper, bollinger_lower, rsi_14,
            macd_approx, macd_signal_approx,
            trend_signal, rsi_signal, macd_trend_signal, bollinger_signal
        FROM `{dataset_ref()}.fct_stock_analysis`
        WHERE symbol = @symbol
        ORDER BY trade_date DESC
        LIMIT @days
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("symbol", "STRING", symbol),
            bigquery.ScalarQueryParameter("days", "INT64", days),
        ]
    )
    return [dict(row) for row in client.query(query, job_config=job_config).result()]


def stock_summary(symbol: str) -> Dict[str, Any]:
    client = get_client()
    query = f"SELECT * FROM `{dataset_ref()}.dim_stock_summary` WHERE symbol = @symbol"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("symbol", "STRING", symbol)]
    )
    rows = list(client.query(query, job_config=job_config).result())
    return dict(rows[0]) if rows else {}
