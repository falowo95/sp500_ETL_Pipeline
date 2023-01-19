from typing import Dict

import pandas as pd

import numpy as np
import sqlite3
from datetime import datetime


import pandas_datareader as pdr
# import yfinance as yfin
# yfin.pdr_override()


# import pandas_datareader as web



def extract_sp500_data() -> pd.DataFrame:
    sp500_tickers = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")[0]["Symbol"].tolist()
    api_key = "b8048079af04b7e50218c15f24286df5b4c51164"
    data = []
    failed_tickers = []
    for ticker in sp500_tickers:
        try:
            df = pdr.DataReader(ticker, 'tiingo',api_key=api_key)
            data.append(df)
        except Exception as e:
            print(f"Error while extracting data for {ticker}: {e}")
            failed_tickers.append(ticker)
    if failed_tickers:
        print(f"Failed to retrieve data for the following tickers: {failed_tickers}")
    df =  pd.concat(data)
    return df

def transform_stock_data(df:pd.DataFrame) -> pd.DataFrame:
    min_periods = 75
    TRADING_DAYS = 252
    try:
        if df.empty:
            print("Dataframe is empty, nothing to transform")
            return
        df = df.reset_index()
        df.date = pd.to_datetime(df.date)
        df.set_index('date',inplace = True)
        df['daily_pct_change'] = df['adjClose'].pct_change()
        df['20_day'] = df['adjClose'].rolling(20).mean()
        df['200_day'] = df['adjClose'].rolling(200).mean()
        df['std'] = df['adjClose'].rolling(20).std()
        df['bollinger_up'] = df["20_day"] + df["std"] * 2  # Calculate top band
        df['bollinger_down'] = df['20_day'] - df["std"] * 2  # Calculate bottom band
        df['cum_daily_returns'] = (1 + df['daily_pct_change']).cumprod()
        df['cum_monthly_returns'] = df['cum_daily_returns'].resample('M').mean()
        df['daily_log_returns'] = np.log(df['daily_pct_change'] + 1)
        df["volaltility"] = df['adjClose'].rolling(min_periods).std() * np.sqrt(min_periods)
        df['returns']= np.log(df['adjClose']/df['adjClose'].shift(1))
        df['sharpe_ratio'] = df['returns'].rolling(window=TRADING_DAYS).std()*np.sqrt(TRADING_DAYS).mean() / df['returns'].rolling(window=TRADING_DAYS).std()*np.sqrt(TRADING_DAYS)
        return df
    except:
        print('error within transformation step')

def load_data_into_db(df:pd.DataFrame, db_file):
    try:
        conn = sqlite3.connect(db_file)
        df.to_sql("sp500_data", conn, if_exists="replace")
        print("Data stored successfully in the database.")
    except Exception as e:
        print(f"Error while storing data in the database: {e}")
        raise
    finally:
        conn.close()

def retrieve_data_from_db(db_file):
    try:
        conn = sqlite3.connect(db_file)
        df = pd.read_sql_query("SELECT * from sp500_data", conn)
        return df
    except Exception as e:
        print(f"Error while retrieving data from the database: {e}")
        raise
    finally:
        conn.close()