with stocks as (
    select * from {{ ref('stg_stocks') }}
),

with_returns as (
    select
        *,
        safe_divide(adj_close, lag(adj_close) over (partition by symbol order by trade_date)) - 1
            as daily_pct_change,
        ln(safe_divide(adj_close, lag(adj_close) over (partition by symbol order by trade_date)))
            as daily_log_return
    from stocks
),

with_moving_averages as (
    select
        *,
        avg(adj_close) over w_20 as ma_20,
        avg(adj_close) over w_50 as ma_50,
        avg(adj_close) over w_200 as ma_200,
        stddev_pop(adj_close) over w_20 as stddev_20,
        stddev_pop(daily_log_return) over w_252 * sqrt(252) as volatility_annualized
    from with_returns
    window
        w_20 as (partition by symbol order by trade_date rows between 19 preceding and current row),
        w_50 as (partition by symbol order by trade_date rows between 49 preceding and current row),
        w_200 as (partition by symbol order by trade_date rows between 199 preceding and current row),
        w_252 as (partition by symbol order by trade_date rows between 251 preceding and current row)
),

with_bollinger as (
    select
        *,
        ma_20 + (stddev_20 * 2) as bollinger_upper,
        ma_20 - (stddev_20 * 2) as bollinger_lower
    from with_moving_averages
),

with_rsi_inputs as (
    select
        *,
        greatest(close - lag(close) over (partition by symbol order by trade_date), 0) as gain,
        greatest(lag(close) over (partition by symbol order by trade_date) - close, 0) as loss
    from with_bollinger
),

with_rsi as (
    select
        *,
        100 - (100 / (1 + safe_divide(
            avg(gain) over (partition by symbol order by trade_date rows between 13 preceding and current row),
            avg(loss) over (partition by symbol order by trade_date rows between 13 preceding and current row)
        ))) as rsi_14
    from with_rsi_inputs
),

-- MACD here is approximated using simple moving averages (SMA) of `close` rather than
-- the textbook exponential moving average (EMA): BigQuery Standard SQL has no recursive
-- CTEs, and a true EWMA needs one (or a persistent JS/remote UDF). This SMA-based
-- approximation trades a small amount of responsiveness for a fully declarative,
-- easily-tested window-function expression. Revisit with a BigQuery UDF if exact
-- EWMA parity with the textbook MACD definition is required.
with_macd as (
    select
        *,
        avg(close) over (partition by symbol order by trade_date rows between 11 preceding and current row)
            - avg(close) over (partition by symbol order by trade_date rows between 25 preceding and current row)
            as macd_approx
    from with_rsi
),

with_macd_signal as (
    select
        *,
        avg(macd_approx) over (partition by symbol order by trade_date rows between 8 preceding and current row)
            as macd_signal_approx
    from with_macd
)

select
    symbol,
    trade_date,
    open,
    high,
    low,
    close,
    volume,
    adj_close,
    (close - open) as daily_change,
    safe_divide(close - open, open) * 100 as daily_change_pct,
    (high - low) as daily_range,
    safe_divide(high - low, low) * 100 as daily_volatility_pct,
    volume * close as daily_value_traded,
    daily_pct_change,
    daily_log_return,
    ma_20,
    ma_50,
    ma_200,
    bollinger_upper,
    bollinger_lower,
    volatility_annualized,
    rsi_14,
    macd_approx,
    macd_signal_approx
from with_macd_signal
