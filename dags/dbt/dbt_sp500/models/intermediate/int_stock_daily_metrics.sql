with stocks as (
    select * from {{ ref('stg_stocks') }}
),

daily_metrics as (
    select
        id,
        symbol,
        date,
        open,
        high,
        low,
        close,
        volume,
        (close - open) as daily_change,
        ((close - open) / open) * 100 as daily_change_pct,
        (high - low) as daily_range,
        ((high - low) / low) * 100 as daily_volatility_pct,
        volume * close as daily_value_traded
    from stocks
    where data_type = 'processed'
)

select * from daily_metrics 