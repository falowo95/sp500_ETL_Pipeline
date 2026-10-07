"""
test_validation.py

Direct tests for dags/config/validation.py's allow-list helpers.
"""
import pytest

from config.validation import is_valid_ticker, qualified_dataset_id, validate_date_range


def test_qualified_dataset_id_accepts_valid_pair():
    assert qualified_dataset_id("test-project", "SP_500_DATA") == "test-project.SP_500_DATA"


@pytest.mark.parametrize("project", ["UPPER-case", "ab", "ends-with-", "has_underscore", None])
def test_qualified_dataset_id_rejects_bad_project_ids(project):
    with pytest.raises(ValueError):
        qualified_dataset_id(project, "SP_500_DATA")


def test_validate_date_range_rejects_non_string():
    with pytest.raises(ValueError, match="ISO date string"):
        validate_date_range(20150101, "2026-01-01")


def test_validate_date_range_accepts_same_day():
    assert validate_date_range("2026-01-05", "2026-01-05") == ("2026-01-05", "2026-01-05")


def test_is_valid_ticker_rejects_trailing_newline():
    assert not is_valid_ticker("AAPL\n")
