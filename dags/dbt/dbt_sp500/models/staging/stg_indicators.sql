with source as (
    select * from {{ source('sp500', 'financial_indicators') }}
),

renamed as (
    select
        id,
        stock_id,
        indicator_type,
        value,
        calculation_date,
        created_at
    from source
)

select * from renamed 