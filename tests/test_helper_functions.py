"""
test_helper_functions.py

Tests for the 'to_local' helper in airflow/dags/helper_functions.py.
"""
import pandas as pd
import pytest

from helper_functions import to_local


def test_to_local_round_trips_dataframe(monkeypatch, tmp_path) -> None:
    # Arrange
    monkeypatch.setenv("SP500_LOCAL_DATA_DIR", str(tmp_path / "data"))
    data_frame = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6], "C": [7, 8, 9]})

    # Act
    path = to_local(data_frame, "test_file")

    # Assert
    assert path == tmp_path / "data" / "test_file.csv"
    assert path.is_file()
    assert data_frame.equals(pd.read_csv(path))


def test_to_local_rejects_path_traversal(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("SP500_LOCAL_DATA_DIR", str(tmp_path))
    with pytest.raises(ValueError):
        to_local(pd.DataFrame({"A": [1]}), "../outside")
    assert not (tmp_path.parent / "outside.csv").exists()


def test_to_local_defaults_to_tmp_airflow_data(monkeypatch, tmp_path) -> None:
    monkeypatch.delenv("SP500_LOCAL_DATA_DIR", raising=False)
    path = to_local(pd.DataFrame({"A": [1]}), "w5_default_dir_probe")
    try:
        assert str(path.parent) == "/tmp/airflow_data"
    finally:
        path.unlink()
