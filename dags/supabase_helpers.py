from typing import Dict, List
import pandas as pd
import numpy as np
from datetime import datetime
from supabase import create_client, Client
from airflow.models import Variable
from airflow.exceptions import AirflowException
import logging

logger = logging.getLogger(__name__)

def get_supabase_client() -> Client:
    """Create and return a Supabase client."""
    try:
        url = Variable.get("SUPABASE_URL")
        key = Variable.get("SUPABASE_KEY")
        client = create_client(url, key)
        return client
    except Exception as e:
        logger.error(f"Failed to create Supabase client: {str(e)}")
        raise AirflowException(f"Supabase client creation failed: {str(e)}")

def process_stock_data(df: pd.DataFrame) -> List[Dict]:
    """Process stock data DataFrame into a format suitable for Supabase."""
    try:
        records = []
        for _, row in df.iterrows():
            record = {
                'symbol': row['symbol'],
                'date': row['date'].strftime('%Y-%m-%d'),
                'open': float(row['open']),
                'high': float(row['high']),
                'low': float(row['low']),
                'close': float(row['close']),
                'volume': int(row['volume']),
                'data_type': 'processed',
                'created_at': datetime.now().isoformat(),
            }
            records.append(record)
        return records
    except Exception as e:
        logger.error(f"Failed to process stock data: {str(e)}")
        raise AirflowException(f"Stock data processing failed: {str(e)}")

def upload_to_supabase(client: Client, table: str, records: List[Dict], batch_size: int = 100) -> None:
    """Upload records to Supabase table in batches."""
    try:
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            response = client.table(table).insert(batch).execute()
            logger.info(f"Uploaded batch {i//batch_size + 1} to {table}")
            if hasattr(response, 'error') and response.error:
                raise Exception(f"Supabase upload error: {response.error}")
    except Exception as e:
        logger.error(f"Failed to upload to Supabase table {table}: {str(e)}")
        raise AirflowException(f"Supabase upload failed: {str(e)}")

def calculate_financial_indicators(stock_data: List[Dict]) -> List[Dict]:
    """Calculate financial indicators from stock data."""
    try:
        indicators = []
        symbol_data = {}
        
        # Group data by symbol
        for record in stock_data:
            symbol = record['symbol']
            if symbol not in symbol_data:
                symbol_data[symbol] = []
            symbol_data[symbol].append(record)
        
        for symbol, data in symbol_data.items():
            # Sort data by date
            sorted_data = sorted(data, key=lambda x: x['date'])
            df = pd.DataFrame(sorted_data)
            
            if len(df) >= 20:  # Minimum data points needed
                # Convert price columns to float
                df['close'] = df['close'].astype(float)
                df['high'] = df['high'].astype(float)
                df['low'] = df['low'].astype(float)
                df['volume'] = df['volume'].astype(float)
                
                # Calculate Moving Averages (MA)
                ma_periods = [20, 50, 200]
                for period in ma_periods:
                    if len(df) >= period:
                        ma = df['close'].rolling(window=period).mean()
                        for i in range(period-1, len(df)):
                            indicators.append({
                                'stock_id': df.iloc[i]['id'],
                                'indicator_type': f'MA_{period}',
                                'value': float(ma.iloc[i]),
                                'calculation_date': df.iloc[i]['date'],
                                'created_at': datetime.now().isoformat()
                            })
                
                # Calculate Relative Strength Index (RSI)
                if len(df) >= 14:
                    delta = df['close'].diff()
                    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
                    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
                    rs = gain / loss
                    rsi = 100 - (100 / (1 + rs))
                    for i in range(13, len(df)):
                        indicators.append({
                            'stock_id': df.iloc[i]['id'],
                            'indicator_type': 'RSI',
                            'value': float(rsi.iloc[i]),
                            'calculation_date': df.iloc[i]['date'],
                            'created_at': datetime.now().isoformat()
                        })
                
                # Calculate MACD
                exp1 = df['close'].ewm(span=12, adjust=False).mean()
                exp2 = df['close'].ewm(span=26, adjust=False).mean()
                macd = exp1 - exp2
                signal = macd.ewm(span=9, adjust=False).mean()
                for i in range(25, len(df)):
                    indicators.append({
                        'stock_id': df.iloc[i]['id'],
                        'indicator_type': 'MACD',
                        'value': float(macd.iloc[i]),
                        'calculation_date': df.iloc[i]['date'],
                        'created_at': datetime.now().isoformat()
                    })
                    indicators.append({
                        'stock_id': df.iloc[i]['id'],
                        'indicator_type': 'MACD_SIGNAL',
                        'value': float(signal.iloc[i]),
                        'calculation_date': df.iloc[i]['date'],
                        'created_at': datetime.now().isoformat()
                    })
                
                # Calculate Bollinger Bands
                if len(df) >= 20:
                    ma20 = df['close'].rolling(window=20).mean()
                    std20 = df['close'].rolling(window=20).std()
                    upper_band = ma20 + (std20 * 2)
                    lower_band = ma20 - (std20 * 2)
                    for i in range(19, len(df)):
                        indicators.append({
                            'stock_id': df.iloc[i]['id'],
                            'indicator_type': 'BB_UPPER',
                            'value': float(upper_band.iloc[i]),
                            'calculation_date': df.iloc[i]['date'],
                            'created_at': datetime.now().isoformat()
                        })
                        indicators.append({
                            'stock_id': df.iloc[i]['id'],
                            'indicator_type': 'BB_LOWER',
                            'value': float(lower_band.iloc[i]),
                            'calculation_date': df.iloc[i]['date'],
                            'created_at': datetime.now().isoformat()
                        })
        
        return indicators
    except Exception as e:
        logger.error(f"Failed to calculate financial indicators: {str(e)}")
        raise AirflowException(f"Financial indicator calculation failed: {str(e)}")

def get_stock_analysis(client: Client, symbol: str, days: int = 30) -> Dict:
    """Get analysis for a specific stock."""
    try:
        # Get recent stock data
        stock_data = client.table('sp500_stocks').select('*').eq('symbol', symbol).order('date', desc=True).limit(days).execute()
        
        # Get indicators
        indicators = client.table('financial_indicators').select('*').eq('symbol', symbol).order('date', desc=True).limit(days).execute()
        
        return {
            'stock_data': stock_data.data,
            'indicators': indicators.data
        }
    except Exception as e:
        logger.error(f"Failed to get stock analysis for {symbol}: {str(e)}")
        raise AirflowException(f"Stock analysis failed: {str(e)}") 