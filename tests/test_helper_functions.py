from airflow.dags.helper_functions import (
    get_gcp_authentication,
    to_local,
    extract_sp500_data_to_csv,
    upload_data_to_gcs_from_local,
    ingest_from_gcs_to_bquery,
)


import pandas as pd
from pathlib import Path


def test_to_local():
    # Create a sample DataFrame
    df = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6], "C": [7, 8, 9]})

    # Call the to_local function with a file name
    file_name = "test_file"
    path = to_local(df, file_name)

    # Check that the file exists
    assert path.is_file()

    # Read the file and check that the data matches the original DataFrame
    df2 = pd.read_csv(path)
    assert df.equals(df2)

    # Delete the file
    path.unlink()


def test_extract_sp500_data_to_csv():
    # Call the extract_sp500_data_to_csv function with a file name
    file_name = "test_sp500_data"
    df = extract_sp500_data_to_csv(file_name)

    # Check that the returned object is a pandas DataFrame
    assert isinstance(df, pd.DataFrame)

    # Check that the DataFrame has at least one row and one column
    assert df.shape[0] > 0 and df.shape[1] > 0

    # Delete the file
    path.unlink()
