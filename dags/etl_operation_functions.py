
import numpy as np
import pandas as pd
import pandas_datareader as pdr
import logging
import io
import os
import csv


from pathlib import Path
from typing import Dict,Union
from google.oauth2 import service_account
from google.cloud import storage, bigquery
from datetime import datetime



def to_local(df: pd.DataFrame, file_name: str) -> Path:
    """
    Saves a pandas DataFrame to a local CSV file with the given filename, and returns the path to the saved file.
    
    Args:
        df: A pandas DataFrame to be saved to a CSV file.
        file_name: A string representing the desired name of the output CSV file.
        
    Returns:
        A Path object representing the path to the saved CSV file.
    """
    # Create a Path object representing the desired file path.
    path = Path(f"{file_name}.csv")

    # Use pandas DataFrame.to_csv() method to save the DataFrame to a CSV file at the given path.
    df.to_csv(path)

    # Return the Path object representing the saved file path.
    return path

def extract_sp500_data_to_csv(file_name: str) -> Path:
    """
    Extracts data for all S&P 500 stocks from Tiingo using pandas-datareader,
    and saves the data to a local CSV file with the given filename.
    
    Args:
        file_name: A string representing the desired name of the output CSV file.
        
    Returns:
        A Path object representing the path to the saved CSV file.
    """
    # Get the list of S&P 500 stock tickers from Wikipedia
    sp500_tickers = pd.read_html(
        "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    )[0]["Symbol"].tolist()

    # Set up API key for Tiingo
    api_key = "b8048079af04b7e50218c15f24286df5b4c51164"

    # Create empty lists for successful and failed tickers
    successful_tickers = []
    failed_tickers = []

    # Loop over all S&P 500 tickers and attempt to retrieve data for each one
    for ticker in sp500_tickers:
        try:
            # Retrieve data for the current ticker using Tiingo
            df = pdr.DataReader(ticker, "tiingo", api_key=api_key, end=datetime.today().strftime("%Y-%m-%d"))
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

    # Save the DataFrame to a local CSV file using the to_local function
    return to_local(df, file_name)









def transform_stock_data(df: pd.DataFrame) -> Union[None, pd.DataFrame]:
    """
    Applies a series of transformations on the input DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame containing stock data.

    Returns
    -------
    pd.DataFrame
        The transformed DataFrame.
    """
    
    # Constants
    MIN_PERIODS = 75
    TRADING_DAYS = 252
    
    try:
        # Check if the input DataFrame is empty
        if df.empty:
            print("Dataframe is empty, nothing to transform")
            return None
        
        # Convert date column to datetime format and set it as index
        df.date = pd.to_datetime(df.date)
        df.set_index("date", inplace=True)

        # Calculate daily percentage change, rolling averages, standard deviation and Bollinger bands
        df["daily_pct_change"] = df["adjClose"].pct_change()
        df["20_day"] = df["adjClose"].rolling(20).mean()
        df["200_day"] = df["adjClose"].rolling(200).mean()
        df["std"] = df["adjClose"].rolling(20).std()
        df["bollinger_up"] = df["20_day"] + df["std"] * 2
        df["bollinger_down"] = df["20_day"] - df["std"] * 2
        
        # Calculate cumulative daily and monthly returns, daily log returns, volatility, and Sharpe ratio
        df["cum_daily_returns"] = (1 + df["daily_pct_change"]).cumprod()
        df["cum_monthly_returns"] = df["cum_daily_returns"].resample("M").mean()
        df["daily_log_returns"] = np.log(df["daily_pct_change"] + 1)
        df["volatility"] = df["adjClose"].rolling(MIN_PERIODS).std() * np.sqrt(MIN_PERIODS)
        df["returns"] = np.log(df["adjClose"] / df["adjClose"].shift(1))
        df["sharpe_ratio"] = (
            df["returns"].rolling(window=TRADING_DAYS).std() * np.sqrt(TRADING_DAYS).mean()
            / df["returns"].rolling(window=TRADING_DAYS).std() * np.sqrt(TRADING_DAYS)
        )
        
        # Reset index
        df.reset_index(drop=False, inplace=True)

        # Return the transformed DataFrame
        print("Transformation function complete")
        return df
    except Exception as e:
        # Log the error message
        logging.error("An error occurred during transformation: " + str(e))
        return None


def load_data_to_s3(
    data: pd.DataFrame,
    bucket_name: str,
    file_name: str,
    access_key: str,
    secret_key: str,
) -> None:
    try:
        s3 = boto3.resource(
            "s3", aws_access_key_id=access_key, aws_secret_access_key=secret_key
        )
        if not bucket_exists(s3, bucket_name):
            create_bucket(s3, bucket_name)
        data_json = data.to_json()
        encoded_data = data_json.encode("utf-8")
        file_stream = io.BytesIO(encoded_data)
        s3.Bucket(bucket_name).upload_fileobj(file_stream, file_name)
        logging.info(f"Data stored successfully in S3 bucket: {bucket_name}")
    except Exception as e:
        logging.error(f"Error while storing data in S3 bucket: {e}")
        raise


def bucket_exists(s3, bucket_name: str) -> bool:
    response = s3.list_buckets()
    for bucket in response["Buckets"]:
        if bucket["Name"] == bucket_name:
            return True
    return False


def create_bucket(s3, bucket_name: str):
    s3.create_bucket(Bucket=bucket_name)
