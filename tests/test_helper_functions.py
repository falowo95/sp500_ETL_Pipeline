"""
test_helper_functions.py

Tests for the 'to_local' helper in airflow/dags/helper_functions.py.
"""
import pandas as pd

from helper_functions import to_local


def test_to_local() -> None:
    """
    Test function for to_local.

    Raises:
        AssertionError: If any of the test assertions fail.
    """
    # Create a sample DataFrame
    data_frame = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6], "C": [7, 8, 9]})

    # Call the to_local function with a file name
    file_name = "test_file"
    path = to_local(data_frame, file_name)

    # Check that the file exists
    assert path.is_file()

    # Read the file and check that the data matches the original DataFrame
    df2 = pd.read_csv(path)
    assert data_frame.equals(df2)

    # Delete the file
    path.unlink()
