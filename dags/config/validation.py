"""
Input validation for values that cross a system boundary in the pipeline:
dates handed to Tiingo, ticker symbols scraped from Wikipedia (they end up
in a URL path), and BigQuery identifiers interpolated into SQL (identifiers
can't be bound as query parameters, so they're allow-listed by regex).
"""
import re
from datetime import date
from typing import Any, Tuple

# Upper-case root of 1-10 chars plus an optional share-class suffix, e.g.
# "AAPL", "BRK.B", "BF-B". Anything else (slashes, query chars, spaces) is
# rejected before it can be placed into the Tiingo URL path.
TICKER_PATTERN = re.compile(r"^[A-Z][A-Z0-9]{0,9}(?:[.\-][A-Z0-9]{1,4})?$")
MAX_TICKER_LENGTH = 12

# GCP project ids: 6-30 chars, lowercase letters/digits/hyphens, start with
# a letter, not ending in a hyphen.
GCP_PROJECT_PATTERN = re.compile(r"^[a-z][a-z0-9-]{4,28}[a-z0-9]$")
# BigQuery dataset/table names: letters, digits, underscores.
BQ_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,1023}$")


def is_valid_ticker(ticker: Any) -> bool:
    """True if `ticker` is a plausible exchange symbol safe to put in a URL."""
    return (
        isinstance(ticker, str)
        and len(ticker) <= MAX_TICKER_LENGTH
        and TICKER_PATTERN.fullmatch(ticker) is not None
    )


def validate_date_range(start_date: str, end_date: str) -> Tuple[str, str]:
    """Validate two ISO (YYYY-MM-DD) dates with start <= end.

    Returns the inputs unchanged; raises ValueError with a clear message
    otherwise.
    """
    start = _parse_iso_date(start_date, "start_date")
    end = _parse_iso_date(end_date, "end_date")
    if start > end:
        raise ValueError(f"start_date {start_date} is after end_date {end_date}")
    return start_date, end_date


def _parse_iso_date(value: Any, name: str) -> date:
    if not isinstance(value, str):
        raise ValueError(f"{name} must be an ISO date string, got {type(value).__name__}")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be YYYY-MM-DD, got {value!r}") from exc


def qualified_table_id(project_id: str, dataset_name: str, table_name: str) -> str:
    """Build `project.dataset.table` after allow-listing each component.

    Raises ValueError for anything that could break out of a backtick-quoted
    identifier in SQL.
    """
    _require_match(GCP_PROJECT_PATTERN, project_id, "GCP project id")
    _require_match(BQ_NAME_PATTERN, dataset_name, "BigQuery dataset name")
    _require_match(BQ_NAME_PATTERN, table_name, "BigQuery table name")
    return f"{project_id}.{dataset_name}.{table_name}"


def qualified_dataset_id(project_id: str, dataset_name: str) -> str:
    """Build `project.dataset` after allow-listing each component."""
    _require_match(GCP_PROJECT_PATTERN, project_id, "GCP project id")
    _require_match(BQ_NAME_PATTERN, dataset_name, "BigQuery dataset name")
    return f"{project_id}.{dataset_name}"


def _require_match(pattern: "re.Pattern[str]", value: Any, kind: str) -> None:
    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        raise ValueError(f"Invalid {kind}: {value!r}")
