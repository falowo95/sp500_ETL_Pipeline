"""
This module provides a function to clean and type-cast raw stock data using PySpark,
writing the result to Google Cloud Storage. Derived financial indicators (moving
averages, Bollinger bands, RSI, MACD, volatility, Sharpe ratio) are computed downstream
by the dbt project running against BigQuery, not here — keeping a single SQL-based
source of truth for indicator math instead of duplicating it in PySpark.

Functions:
- transform_stock_data(gcs_input_data_path: str, gcs_output_data_path: str) -> None:
  Cleans raw stock data using PySpark and writes it to Google Cloud Storage.
"""

import logging
import os
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, to_timestamp

logger = logging.getLogger(__name__)


def transform_stock_data(gcs_input_data_path: str, gcs_output_data_path: str) -> None:
    """
    Cleans and type-casts raw stock data, writing the result to GCS as CSV.

    Args:
        gcs_input_data_path (str): The Google Cloud Storage path of the input data file.
        gcs_output_data_path (str): The Google Cloud Storage path where the cleaned data
            will be written.

    Returns:
        None
    """
    conf = (
        SparkConf()
        .setMaster("local[*]")
        .setAppName("sp500-clean")
        .set("spark.jars", "/opt/spark/jars/gcs-connector-hadoop2-2.1.1.jar")
        .set("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .set(
            "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
            os.getenv("GOOGLE_APPLICATION_CREDENTIALS"),
        )
    )

    spark_context = SparkContext(conf=conf)

    spark_context._jsc.hadoopConfiguration().set(
        "fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
    )
    spark_context._jsc.hadoopConfiguration().set(
        "fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
    )
    spark_context._jsc.hadoopConfiguration().set(
        "fs.gs.auth.service.account.json.keyfile",
        os.getenv("GOOGLE_APPLICATION_CREDENTIALS"),
    )
    spark_context._jsc.hadoopConfiguration().set(
        "fs.gs.auth.service.account.enable", "true"
    )

    spark = (
        SparkSession.builder.master("local[*]")
        .appName("StockDataCleaning")
        .config(conf=spark_context.getConf())
        .getOrCreate()
    )
    spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

    df_spark = spark.read.csv(gcs_input_data_path, header=True, inferSchema=True)

    try:
        if df_spark.count() == 0:
            logger.warning("Dataframe is empty, nothing to transform")
            return

        df_spark = df_spark.withColumn(
            "date",
            to_timestamp(date_format(col("date"), "yyyy-MM-dd HH:mm:ss")).cast(
                "timestamp"
            ),
        )
        df_spark = df_spark.orderBy("symbol", "date").repartition(10)

        df_spark.coalesce(1).write.mode("overwrite").option("header", "true").csv(
            gcs_output_data_path
        )

        logger.info(f"Writing cleaned data to GCS at: {gcs_output_data_path}")
    except Exception as specific_exception:
        logger.error(f"An error occurred during transformation: {specific_exception}")
        raise
    finally:
        spark.stop()
