"""
airflow_dag_tests.py

This module contains test functions for the helper functions in the Airflow DAGs package.
The helper functions being tested are 'to_local' and 'extract_sp500_data_to_csv'.

Functions:
- test_to_local() -> None: Test function for the 'to_local' function.
- test_extract_sp500_data_to_csv()-> None: Test function for the 'extract_sp500_data_to_csv' 
    function.
"""

import pandas as pd

from sp500_ETL_Pipeline.airflow.dags.helper_functions import (
    to_local,
)


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


# def test_extract_sp500_data_to_csv() -> None:
#     """
#     Test function for extract_sp500_data_to_csv.

#     Raises:
#         AssertionError: If any of the test assertions fail.
#     """
#     # Call the extract_sp500_data_to_csv function with a file name
#     # Set up API key for Tiingo
#     tiingo_api_key = os.getenv("TIINGO_API_KEY")
#     file_name = "test_sp500_data"
#     data_frame = extract_sp500_data_to_csv(file_name, tiingo_api_key)

#     # Check that the returned object is a pandas DataFrame
#     assert isinstance(data_frame, pd.DataFrame)

#     # Check that the DataFrame has at least one row and one column
#     assert data_frame.shape[0] > 0 and data_frame.shape[1] > 0
