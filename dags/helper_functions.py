"""
This module provides functions for extracting stock data for all S&P 500 stocks from Tiingo using pandas-datareader,
saving the data to a local CSV file, uploading the file to Google Cloud Storage (GCS), and ingesting the data from GCS into BigQuery.

Functions:
- to_local(data_frame: pd.DataFrame, file_name: str) -> Path: Saves a DataFrame to a local CSV file.
- extract_sp500_data_to_csv(file_name: str) -> None: Extracts data for all S&P 500 stocks from Tiingo and saves it to a local CSV file.
- upload_data_to_gcs_from_local(bucket_name: str, source_file_path_local: str, destination_blob_path: str) -> None:
  Uploads a file to Google Cloud Storage.
- ingest_from_gcs_to_bquery(dataset_name: str, table_name: str, csv_uri: str) -> None:
  Ingests the data from Google Cloud Storage into BigQuery.
"""

from pathlib import Path

import logging
import requests

import pandas as pd
from google.cloud import bigquery
from config.gcp_service import GCPService
from google.api_core import exceptions as google_exceptions


def to_local(data_frame: pd.DataFrame, file_name: str) -> Path:
    """
    Saves a DataFrame to a local CSV file.
    """
    # Set the path to the file in the /opt/airflow/data directory
    data_dir = Path("/opt/airflow/data")
    data_dir.mkdir(parents=True, exist_ok=True)

    path = data_dir / f"{file_name}.csv"

    # Save the DataFrame to the CSV file
    data_frame.to_csv(path, index=False)
    print(f"File has been saved at: {path}")

    return path


def extract_sp500_data_to_csv(
    file_name: str, tiingo_api_key: str, start_date: str, end_date: str
) -> None:
    """
    Extracts data for all S&P 500 stocks from Tiingo API.
    """
    # Add proper error handling using the logging configuration
    logger = logging.getLogger(__name__)

    try:
        # Get the list of S&P 500 stock tickers from Wikipedia
        sp500_tickers = pd.read_html(
            "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        )[0]["Symbol"].tolist()

        # Create empty lists for successful and failed tickers
        successful_tickers = []
        failed_tickers = []

        # Loop over all S&P 500 tickers and attempt to retrieve data for each one
        for ticker in sp500_tickers:
            try:
                # Make direct API call to Tiingo
                headers = {"Content-Type": "application/json"}
                url = f"https://api.tiingo.com/tiingo/daily/{ticker}/prices"
                params = {
                    "startDate": start_date,
                    "endDate": end_date,
                    "token": tiingo_api_key,
                }

                response = requests.get(url, headers=headers, params=params)
                response.raise_for_status()  # Raise exception for bad status codes

                # Convert JSON response to DataFrame
                df = pd.DataFrame(response.json())

                # Add symbol column
                df["symbol"] = ticker

                successful_tickers.append(df)
                logging.info(f"Successfully retrieved data for {ticker}")

            except Exception as specific_exception:
                logging.error(
                    f"Error while extracting data for {ticker}: {specific_exception}"
                )
                failed_tickers.append(ticker)

        # If any tickers failed, print a message listing them
        if failed_tickers:
            logging.info(
                f"Failed to retrieve data for the following tickers: {failed_tickers}"
            )

        # Concatenate all DataFrames only if we have successful extractions
        if successful_tickers:
            data_frame = pd.concat(successful_tickers, axis=0, ignore_index=True)

            # Convert the timestamp column to datetime objects
            data_frame["date"] = pd.to_datetime(data_frame["date"]).dt.date

            # Save the concatenated data to a file
            save_to = to_local(data_frame, file_name)
            logger.info(f"Data saved to: {save_to}")
        else:
            logger.error("No data was successfully retrieved for any tickers")
            raise Exception("No data retrieved")

    except Exception as e:
        logger.error(f"Failed to extract SP500 data: {str(e)}")
        raise


def upload_data_to_gcs_from_local(
    bucket_name: str, source_file_path_local: str, destination_blob_path: str
) -> None:
    """
    Uploads a file to Google Cloud Storage.
    Falls back to local storage if GCP is unavailable.
    """
    try:
        # Ensure the source file path is absolute
        source_path = Path("/opt/airflow/data") / source_file_path_local

        if not source_path.exists():
            raise FileNotFoundError(f"Source file not found: {source_path}")

        gcp = GCPService.get_instance()
        gcp.upload_blob(
            bucket_name=bucket_name,
            source_file=str(source_path),
            destination_blob=destination_blob_path,
        )

        print(
            f"File {source_path} uploaded to {destination_blob_path} "
            f"in bucket {bucket_name}."
        )
    except google_exceptions.Forbidden as e:
        print("GCP Authentication/Billing Error. Falling back to local storage.")
        print(f"Original error: {str(e)}")

        # Create a local backup directory
        backup_dir = Path("/opt/airflow/data/backup")
        backup_dir.mkdir(parents=True, exist_ok=True)

        # Copy file to backup location
        from shutil import copy2

        backup_path = backup_dir / source_file_path_local
        copy2(source_path, backup_path)

        print(f"File backed up locally to: {backup_path}")
        # Don't raise the error since we handled it
    except Exception as e:
        print(f"Error uploading to GCS: {str(e)}")
        raise


def ingest_from_gcs_to_bquery(dataset_name: str, table_name: str, csv_uri: str) -> None:
    """
    Ingest the data from Google Cloud Storage into BigQuery.

    Args:
        dataset_name (str): The name of the BigQuery dataset.
        table_name (str): The name of the BigQuery table.
        csv_uri (str): The URI of the CSV file in Google Cloud Storage.
    """
    gcp = GCPService.get_Instance()

    # Create the dataset if it doesn't exist
    dataset_ref = f"{gcp.project_id}.{dataset_name}"
    try:
        gcp.bq_client.get_dataset(dataset_ref)
        print(f"Using existing dataset: {dataset_ref}")
    except Exception as e:
        dataset = bigquery.Dataset(dataset_ref)
        gcp.bq_client.create_dataset(dataset)
        print(f"Created dataset: {dataset_ref}")

    # Define table schema
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

    # Configure the load job
    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.CSV,
        schema=schema,
    )

    table_id = f"{dataset_ref}.{table_name}"
    try:
        load_job = gcp.bq_client.load_table_from_uri(
            csv_uri, table_id, job_config=job_config
        )
        load_job.result()  # Wait for completion
        print(f"Loaded {load_job.output_rows} rows into {table_id}")
    except Exception as e:
        print(f"Error loading data into BigQuery: {str(e)}")
        raise
