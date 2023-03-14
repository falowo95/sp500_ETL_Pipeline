from pathlib import Path
import os
import pandas as pd
from google.oauth2 import service_account
from google.cloud import storage, bigquery
from datetime import datetime
from typing import Union

from pyspark.sql.window import Window
import pandas_datareader as pdr
import logging

from audioop import avg
from pyspark.sql.functions import *
from pyspark.sql import SparkSession


def get_gcp_authentication():

    key_path = os.getenv(
        "GCP_SERVICE_ACCOUNT_FILE"
    )  # set enviromental variable to the file
    credentials = service_account.Credentials.from_service_account_file(key_path)
    print(f"credentials found at ::: {credentials}")
    return credentials


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
    path = Path(f"{file_name}", index=True)  # can update this with a file directory

    # Use pandas DataFrame.to_csv() method to save the DataFrame to a CSV file at the given path.
    df.to_csv(path, index=False)

    # Return the Path object representing the saved file path.
    print(f"file has been saved at :{path}")
    return path


def extract_sp500_data() -> pd.DataFrame:

    """
    Extracts data for all S&P 500 stocks from Tiingo using pandas-datareader
    """
    # Get the list of S&P 500 stock tickers from Wikipedia

    end_date = datetime.today().strftime("%Y-%m-%d")

    sp500_tickers = pd.read_html(
        "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    )[0]["Symbol"].tolist()

    # Set up API key for Tiingo

    api_key = os.getenv("TIINGO_API_KEY")
    


    # Create empty lists for successful and failed tickers
    successful_tickers = []
    failed_tickers = []

    # Loop over all S&P 500 tickers and attempt to retrieve data for each one
    tickers = ['AAPL']
    # for ticker in sp500_tickers:
    for ticker in tickers:
        try:
            # Retrieve data for the current ticker using Tiingo
            df = pdr.DataReader(ticker, "tiingo", api_key=api_key, end=end_date)
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
    return df


def transform_stock_data(df: pd.DataFrame)-> pd.DataFrame:
    """
    Applies a series of transformations on the input DataFrame.

    Parameters
    ----------
    df : DataFrame
        The DataFrame containing stock data.

    Returns
    -------
    DataFrame
        The transformed DataFrame.
    """
    df = df.reset_index(drop=True, inplace=True)
    spark = (
        SparkSession.builder.appName("PandasToRDD")
        .getOrCreate()
    )
    df_spark = spark.createDataFrame(df)

    # Constants
    MIN_PERIODS = 75
    TRADING_DAYS = 252
    try:
        # Check if the input DataFrame is empty
        if df_spark.count() == 0:
            print("Dataframe is empty, nothing to transform")
            return None

        # Convert date column to timestamp format and set it as index
        df_spark = df_spark.withColumn("date", col("date"))
        df_spark = df_spark.orderBy("date").repartition(10)
        df_spark.createOrReplaceTempView("stock_data")

        df_spark = spark.sql("SELECT * FROM stock_data SORT BY date")
        df_spark.createOrReplaceTempView("stock_data")

        # Calculate daily percentage change, rolling averages, standard deviation and Bollinger bands
        window1 = Window.partitionBy("symbol").orderBy("date")
        window2 = Window.partitionBy("symbol").orderBy("date").rowsBetween(-19, 0)
        window3 = Window.partitionBy("symbol").orderBy("date").rowsBetween(-199, 0)
        window4 = (
            Window.partitionBy("symbol")
            .orderBy("date")
            .rowsBetween(-(MIN_PERIODS - 1), 0)
        )
        window5 = (
            Window.partitionBy("symbol")
            .orderBy("date")
            .rowsBetween(-(TRADING_DAYS - 1), 0)
        )
        df_spark = df_spark.withColumn(
            "daily_pct_change", col("adjClose") / lag("adjClose", 1).over(window1) - 1
        )
        df_spark = df_spark.withColumn(
            "twenty_day_moving", avg(col("adjClose")).over(window2)
        )
        df_spark = df_spark.withColumn(
            "two_hundred_day_moving", avg(col("adjClose")).over(window3)
        )
        df_spark = df_spark.withColumn("std", stddev_pop(col("adjClose")).over(window2))
        df_spark = df_spark.withColumn(
            "bollinger_up", col("twenty_day_moving") + col("std") * 2
        )
        df_spark = df_spark.withColumn(
            "bollinger_down", col("twenty_day_moving") - col("std") * 2
        )

        # Calculate cumulative daily and monthly returns, daily log returns, volatility, and Sharpe ratio
        df_spark = df_spark.withColumn(
            "cum_daily_returns",
            exp(sum(log(1 + col("daily_pct_change"))).over(window1)),
        )
        df_spark = df_spark.withColumn(
            "cum_monthly_returns",
            avg(col("cum_daily_returns")).over(
                Window.partitionBy(year(col("date")), month(col("date")))
            ),
        )
        df_spark = df_spark.withColumn(
            "daily_log_returns", log(col("daily_pct_change") + 1)
        )
        df_spark = df_spark.withColumn(
            "volatility",
            stddev_pop(col("adjClose")).over(window4) * sqrt(lit(MIN_PERIODS)),
        )
        df_spark = df_spark.withColumn(
            "returns", log(col("adjClose") / lag("adjClose", 1).over(window1))
        )
        df_spark = df_spark.withColumn(
            "sharpe_ratio",
            stddev_pop(col("returns")).over(window5)
            * sqrt(lit(TRADING_DAYS))
            / avg(col("returns")).over(window5),
        )
        # Return the transformed DataFrame
        df_pandas = df_spark.toPandas()
        
        # return to_local(df_pandas, file_name)
        print(df_pandas.columns)
        return df_pandas

    except Exception as e:
        # Log the error message
        logging.error("An error occurred during transformation: " + str(e))
        return None


def upload_to_gcs(path:Path,file_name,bucket_name):
    
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    
    
    # Initialize a client object with the service account credentials
    credentials = get_gcp_authentication()
    client = storage.Client(credentials=credentials)

    # Get the bucket object
    bucket = client.bucket(bucket_name)

    # Create a blob object and upload the file to the bucket
    blob = bucket.blob(file_name)
    with open(path) as f:
        blob.upload_from_file(f)

    print('File was successfully uploaded to the bucket.')


def ingest_from_gcs_to_bquery(dataset_name, table_name, csv_uri):
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
