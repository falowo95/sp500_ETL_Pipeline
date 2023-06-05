"""
This module provides functions for extracting stock data for all S&P 500 stocks from Tiingo using pandas-datareader,
saving the data to a local CSV file, uploading the file to Google Cloud Storage (GCS), and ingesting the data from GCS into BigQuery.

Functions:
- get_gcp_authentication(): Retrieves Google Cloud Platform (GCP) authentication credentials from a service account key file.
- to_local(data_frame: pd.DataFrame, file_name: str) -> Path: Saves a DataFrame to a local CSV file.
- extract_sp500_data_to_csv(file_name: str) -> None: Extracts data for all S&P 500 stocks from Tiingo and saves it to a local CSV file.
- upload_data_to_gcs_from_local(bucket_name: str, source_file_path_local: str, destination_blob_path: str) -> None:
  Uploads a file to Google Cloud Storage.
- ingest_from_gcs_to_bquery(dataset_name: str, table_name: str, csv_uri: str) -> None:
  Ingests the data from Google Cloud Storage into BigQuery.
"""

from pathlib import Path
import os

import pandas as pd
import pandas_datareader as pdr
import logging

from google.oauth2 import service_account
from google.cloud import storage, bigquery


def get_gcp_authentication():
    """
    Retrieves Google Cloud Platform (GCP) authentication credentials
    from a service account key file.

    Returns:
        credentials (google.auth.credentials.Credentials): GCP authentication credentials.
    """
    key_path = os.getenv(
        "GOOGLE_APPLICATION_CREDENTIALS"
    )  # set environmental variable to the file
    credentials = service_account.Credentials.from_service_account_file(key_path)
    return credentials


def to_local(data_frame: pd.DataFrame, file_name: str) -> Path:
    """
    Saves a DataFrame to a local CSV file.

    Args:
        data_frame (pd.DataFrame): The DataFrame to be saved.
        file_name (str): The name of the output file.

    Returns:
        Path: The path object representing the saved file.
    """
    # Set the path to the file
    path = Path(f"{file_name}.csv", index=True)

    # Save the DataFrame to the CSV file
    data_frame.to_csv(path, index=False)

    # Print the path to the saved file
    print(f"File has been saved at: {path}")

    return path


def extract_sp500_data_to_csv(
    file_name: str, tiingo_api_key: str, start_date: str, end_date: str
) -> None:
    """
    Extracts data for all S&P 500 stocks from Tiingo using pandas-datareader.

    Args:
        file_name (str): The name of the output file.
        tiingo_api_key (str): The API key for Tiingo.
        start_date (str): The start date for data extraction (YYYY-MM-DD).
        end_date (str): The end date for data extraction (YYYY-MM-DD).
    """
    # Get the list of S&P 500 stock tickers from Wikipedia
    sp500_tickers = pd.read_html(
        "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    )[0]["Symbol"].tolist()

    # Create empty lists for successful and failed tickers
    successful_tickers = []
    failed_tickers = []

    logger = logging.getLogger(__name__)  # Create a logger instance

    # Loop over all S&P 500 tickers and attempt to retrieve data for each one
    for ticker in sp500_tickers:
        try:
            # Retrieve data for the current ticker using Tiingo
            data_frame = pdr.get_data_tiingo(
                ticker, start=start_date, end=end_date, api_key=tiingo_api_key
            )
            data_frame.reset_index(drop=False, inplace=True)
            successful_tickers.append(data_frame)
        except Exception as specific_exception:
            # If there is an error, log the message and add the ticker to the failed list
            logger.error(
                f"Error while extracting data for {ticker}: {specific_exception}"
            )
            failed_tickers.append(ticker)
    # If any tickers failed, print a message listing them
    if failed_tickers:
        logger.info(
            f"Failed to retrieve data for the following tickers: {failed_tickers}"
        )

    # Concatenate the data for all successful tickers into a single DataFrame
    data_frame = pd.concat(successful_tickers)

    # Convert the timestamp column to datetime objects
    data_frame["date"] = pd.to_datetime(data_frame["date"]).dt.date

    logger.info("Ingestion from API completed")

    save_to = to_local(data_frame, file_name)
    logger.info(save_to)


def upload_data_to_gcs_from_local(
    bucket_name: str, source_file_path_local: str, destination_blob_path: str
) -> None:
    """
    Uploads a file to Google Cloud Storage.

    Args:
        bucket_name (str): The name of the bucket where the file will be uploaded.
        source_file_path_local (str): The local path of the file to be uploaded.
        destination_blob_path (str): The destination path of the file within the bucket
        , including the file name.
    """
    credentials = get_gcp_authentication()
    storage_client = storage.Client(credentials=credentials)
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_path)

    blob.upload_from_filename(source_file_path_local)

    print(
        f"File {source_file_path_local} locally uploaded to {destination_blob_path} in bucket {bucket_name}."
    )


def ingest_from_gcs_to_bquery(dataset_name: str, table_name: str, csv_uri: str) -> None:
    """
    Ingest the data from Google Cloud Storage into BigQuery.

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
    try:
        dataset = client.get_dataset(dataset_ref)
        print(f"Using existing dataset: {client.project}.{dataset.dataset_id}")
    except Exception as specific_exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset = client.create_dataset(dataset)
        logging.info(f"Created dataset {client.project}.{dataset.dataset_id}")

    # Create the BigQuery table if it doesn't exist
    table_ref = dataset_ref.table(table_name)
    try:
        table = client.get_table(table_ref)
        logging.info(f"Using existing table: {dataset_name}.{table_name}")
    except Exception as specific_exception:
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
        try:
            table = client.create_table(table)  # Make an API request.
            logging.error(f"Error while creating table: {specific_exception}")
            logging.info(f"Created table {dataset_name}.{table_name}")
        except Exception as specific_exception:
            logging.error(f"Error while creating table: {specific_exception}")
            logging.info(f"Using existing table: {dataset_name}.{table_name}")

    # Load the data into BigQuery
    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.CSV,
    )
    load_job = client.load_table_from_uri(
        csv_uri, table_ref, job_config=job_config
    )  # Make an API request.
    try:
        load_job.result()  # Wait for the job to complete.
        logging.info(
            f"Loaded {load_job.output_rows} rows into {dataset_name}.{table_name}"
        )
    except Exception as specific_exception:
        logging.error(f"Error while loading data into BigQuery: {specific_exception}")
