with daily_metrics as (
    select * from {{ ref('int_stock_daily_metrics') }}
)

select
    symbol,
    trade_date,
    close,
    ma_20,
    ma_50,
    ma_200,
    rsi_14,
    macd_approx,
    macd_signal_approx,
    bollinger_upper,
    bollinger_lower,
    volatility_annualized,
    case
        when close > ma_200 then 'ABOVE_200MA'
        else 'BELOW_200MA'
    end as trend_signal,
    case
        when rsi_14 >= 70 then 'OVERBOUGHT'
        when rsi_14 <= 30 then 'OVERSOLD'
        else 'NEUTRAL'
    end as rsi_signal,
    case
        when macd_approx > macd_signal_approx then 'BULLISH'
        else 'BEARISH'
    end as macd_trend_signal,
    case
        when close > bollinger_upper then 'ABOVE_UPPER_BAND'
        when close < bollinger_lower then 'BELOW_LOWER_BAND'
        else 'WITHIN_BANDS'
    end as bollinger_signal
from daily_metrics
