



  SELECT
    date,
    symbol AS ticker,
    twenty_day_moving AS twenty_ma,
    two_hundred_day_moving AS two_hundred_ma
  FROM
    `dataengineering-378316`.`sp_500_data`.`sp_500_data_table`