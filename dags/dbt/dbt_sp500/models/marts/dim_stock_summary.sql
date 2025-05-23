with daily_metrics as (
    select * from {{ ref('int_stock_daily_metrics') }}
),

summary_stats as (
    select
        symbol,
        min(date) as first_trading_date,
        max(date) as last_trading_date,
        count(distinct date) as trading_days,
        avg(close) as avg_price,
        min(close) as min_price,
        max(close) as max_price,
        avg(volume) as avg_volume,
        avg(daily_value_traded) as avg_daily_value,
        avg(daily_volatility_pct) as avg_volatility,
        sum(case when daily_change >= 0 then 1 else 0 end)::float / 
            count(*) * 100 as up_day_percentage
    from daily_metrics
    group by symbol
)

select * from summary_stats 