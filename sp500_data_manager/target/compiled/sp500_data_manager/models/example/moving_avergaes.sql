



  SELECT
<<<<<<< HEAD
    sp500_dbt_source.date,
    sp500_dbt_source.symbol AS ticker,
    sp500_dbt_source.twenty_day_moving AS twenty_ma,
    sp500_dbt_source.two_hundred_day_moving AS two_hundred_ma
  FROM
    `dataengineering-378316`.`sp_500_data`.`sp_500_data_table`


create or replace table sp500_dbt_source.dim_bollinger as (

  SELECT
    sp500_dbt_source.symbol AS ticker,
    sp500_dbt_source.bollinger_up AS bolinger_up,
    sp500_dbt_source.bollinger_down AS bolinger_down,
  FROM
    `dataengineering-378316`.`sp_500_data`.`sp_500_data_table`
)
=======
    date,
    symbol AS ticker,
    twenty_day_moving AS twenty_ma,
    two_hundred_day_moving AS two_hundred_ma
  FROM
    `dataengineering-378316`.`sp_500_data`.`sp_500_data_table`
>>>>>>> main
