with source as (
    select * from {{ source('sp500', 'sp500_stocks') }}
),

renamed as (
    select
        id,
        symbol,
        date,
        open,
        high,
        low,
        close,
        volume,
        data_type,
        created_at,
        updated_at
    from source
)

select * from renamed 