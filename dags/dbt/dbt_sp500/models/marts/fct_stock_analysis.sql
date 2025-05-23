with daily_metrics as (
    select * from {{ ref('int_stock_daily_metrics') }}
),

indicators as (
    select * from {{ ref('stg_indicators') }}
),

stock_indicators as (
    select
        i.stock_id,
        i.calculation_date,
        max(case when i.indicator_type = 'MA_20' then i.value end) as ma_20,
        max(case when i.indicator_type = 'MA_50' then i.value end) as ma_50,
        max(case when i.indicator_type = 'MA_200' then i.value end) as ma_200,
        max(case when i.indicator_type = 'RSI' then i.value end) as rsi,
        max(case when i.indicator_type = 'MACD' then i.value end) as macd,
        max(case when i.indicator_type = 'MACD_SIGNAL' then i.value end) as macd_signal,
        max(case when i.indicator_type = 'BB_UPPER' then i.value end) as bollinger_upper,
        max(case when i.indicator_type = 'BB_LOWER' then i.value end) as bollinger_lower
    from indicators i
    group by i.stock_id, i.calculation_date
)

select
    m.*,
    i.ma_20,
    i.ma_50,
    i.ma_200,
    i.rsi,
    i.macd,
    i.macd_signal,
    i.bollinger_upper,
    i.bollinger_lower,
    case
        when m.close > i.ma_200 then 'ABOVE_200MA'
        else 'BELOW_200MA'
    end as trend_signal,
    case
        when i.rsi >= 70 then 'OVERBOUGHT'
        when i.rsi <= 30 then 'OVERSOLD'
        else 'NEUTRAL'
    end as rsi_signal,
    case
        when i.macd > i.macd_signal then 'BULLISH'
        else 'BEARISH'
    end as macd_signal,
    case
        when m.close > i.bollinger_upper then 'ABOVE_UPPER_BAND'
        when m.close < i.bollinger_lower then 'BELOW_LOWER_BAND'
        else 'WITHIN_BANDS'
    end as bollinger_signal
from daily_metrics m
left join stock_indicators i
    on m.id = i.stock_id
    and m.date = i.calculation_date 