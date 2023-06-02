from pathlib import Path
import os
from datetime import datetime

import pandas as pd
import pandas_datareader as pdr


from google.oauth2 import service_account
from google.cloud import storage, bigquery


def get_gcp_authentication():

    key_path = os.getenv(
        "GOOGLE_APPLICATION_CREDENTIALS"
    )  # set enviromental variable to the file
    credentials = service_account.Credentials.from_service_account_file(key_path)
    return credentials


def to_local(df: pd.DataFrame, file_name: str) -> Path:
    """
    Saves a DataFrame to a local CSV file.

    Args:
        df (pd.DataFrame): The DataFrame to be saved.
        file_name (str): The name of the output file.

    Returns:
        Path: The path object representing the saved file.
    """
    # Set the path to the file
    path = Path(f"{file_name}.csv", index=True)

    # Save the DataFrame to the CSV file
    df.to_csv(path, index=False)

    # Print the path to the saved file
    print(f"File has been saved at: {path}")

    return path


def extract_sp500_data_to_csv(file_name) -> pd.DataFrame:

    """
    Extracts data for all S&P 500 stocks from Tiingo using pandas-datareader
    """
    # Get the list of S&P 500 stock tickers from Wikipedia

    end_date = datetime.today().strftime("%Y-%m-%d")

    sp500_tickers = pd.read_html(
        "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    )[0]["Symbol"].tolist()

    # Set up API key for Tiingo

    tingo_api_key = os.getenv("TIINGO_API_KEY")
    # tingo_api_key = "b8048079af04b7e50218c15f24286df5b4c51164"

    # Create empty lists for successful and failed tickers
    successful_tickers = []
    failed_tickers = []

    # Loop over all S&P 500 tickers and attempt to retrieve data for each one

    for ticker in sp500_tickers:
        try:
            # Retrieve data for the current ticker using Tiingo
            df = pdr.DataReader(ticker, "tiingo", api_key=tingo_api_key, end=end_date)
            df.reset_index(drop=False, inplace=True)
            successful_tickers.append(df)
        except Exception as e:
            # If there is an error, print a message and add the ticker to the failed list
            print(f"Error while extracting data for {ticker}: {e}")
            failed_tickers.append(ticker)

    # If any tickers failed, print a message listing them
    if failed_tickers:
        print(f"Failed to retrieve data for the following tickers: {failed_tickers}")

    # Concatenate the data for all successful tickers into a single DataFrame
    df = pd.concat(successful_tickers)

    # Convert the timestamp column to datetime objects
    df.date = pd.to_datetime(df.date)
    # Extract the date component of the datetime objects
    df.date = df.date.dt.date

    print("ingestion from api completed")

    save_to = to_local(df, file_name)
    print(save_to)


def upload_data_to_gcs_from_local(
    bucket_name, source_file_path_local, destination_blob_path
) -> None:
    """
    Uploads a file to Google Cloud Storage.

    Parameters
    ----------
    bucket_name : str
        The name of the bucket where the file will be uploaded.
    source_file_path_local : str
        The local path of the file to be uploaded.
    destination_blob_path : str
        The destination path of the file within the bucket, including the file name.
    """

    credentials = get_gcp_authentication()
    storage_client = storage.Client(credentials=credentials)
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_path)

    blob.upload_from_filename(source_file_path_local)

    print(
        f"File {source_file_path_local} locally and  uploaded to {destination_blob_path} in bucket {bucket_name}."
    )


def ingest_from_gcs_to_bquery(dataset_name, table_name, csv_uri) -> None:
    """Ingest the data from Google Cloud Storage into BigQuery.

    This function will load the data into BigQuery.

    Args:
        dataset_name (str): The name of the BigQuery dataset.
        table_name (str): The name of the BigQuery table.
        csv_uri (str): The URI of the CSV file in Google Cloud Storage.
    """
    credentials = get_gcp_authentication()
    # Initialize the BigQuery client
    client = bigquery.Client(credentials=credentials)

    # Create the BigQuery dataset if it doesn't exist
    dataset_ref = client.dataset(dataset_name)
    dataset = bigquery.Dataset(dataset_ref)
    try:
        dataset = client.create_dataset(dataset)  # Make an API request.
        print("Created dataset {}.{}".format(client.project, dataset.dataset_id))
    except Exception as e:
        print("Error creating dataset: {}".format(e))

    # Create the BigQuery table if it doesn't exist
    table_ref = dataset_ref.table(table_name)
    try:
        client.get_table(table_ref)
        print("Table {}.{} already exists.".format(dataset_name, table_name))
    except Exception as e:
        print("Creating table {}.{}".format(dataset_name, table_name))
        schema = [
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
            bigquery.SchemaField("daily_pct_change", "FLOAT"),
            bigquery.SchemaField("twenty_day_moving", "FLOAT"),
            bigquery.SchemaField("two_hundred_day_moving", "FLOAT"),
            bigquery.SchemaField("std", "FLOAT"),
            bigquery.SchemaField("bollinger_up", "FLOAT"),
            bigquery.SchemaField("bollinger_down", "FLOAT"),
            bigquery.SchemaField("cum_daily_returns", "FLOAT"),
            bigquery.SchemaField("cum_monthly_returns", "FLOAT"),
            bigquery.SchemaField("daily_log_returns", "FLOAT"),
            bigquery.SchemaField("volatility", "FLOAT"),
            bigquery.SchemaField("returns", "FLOAT"),
            bigquery.SchemaField("sharpe_ratio", "FLOAT"),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        table = client.create_table(table)  # Make an API request.
        print("Created table {}.{}".format(dataset_name, table_name))

    # Load the data into BigQuery
    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.CSV,
    )
    load_job = client.load_table_from_uri(
        csv_uri, table_ref, job_config=job_config
    )  # Make an API request.
    load_job.result()  # Wait for the job to complete.

    # Print the number of rows loaded
    destination_table = client.get_table(table_ref)  # Make an API request.
    print(
        "Loaded {} rows into {}.{}".format(
            destination_table.num_rows, dataset_name, table_name
        )
    )
