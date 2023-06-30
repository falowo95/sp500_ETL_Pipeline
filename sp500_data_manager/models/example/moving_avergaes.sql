{{ config(materialized='table') }}



  SELECT
    date,
    symbol AS ticker,
    twenty_day_moving AS twenty_ma,
    two_hundred_day_moving AS two_hundred_ma
  FROM
    {{source('sp500_dbt_source', 'sp_500_data_table')}}
