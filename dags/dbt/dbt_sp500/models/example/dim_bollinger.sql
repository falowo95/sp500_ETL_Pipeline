{{ config(materialized='table') }}

SELECT
    symbol AS ticker,
    bollinger_up AS bolinger_up,
    bollinger_down AS bolinger_down
FROM
    {{ source('sp500_dbt_source', 'sp_500_data_table') }}
