with source as (
    select * from {{ source('sp500', 'SP_500_DATA_table') }}
),

renamed as (
    select
        symbol,
        cast(date as date) as trade_date,
        open,
        high,
        low,
        close,
        volume,
        adjOpen as adj_open,
        adjHigh as adj_high,
        adjLow as adj_low,
        adjClose as adj_close,
        adjVolume as adj_volume,
        divCash as dividend_cash,
        splitFactor as split_factor
    from source
)

select * from renamed
