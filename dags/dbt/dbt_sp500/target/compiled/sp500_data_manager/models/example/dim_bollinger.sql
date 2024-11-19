

SELECT
    symbol AS ticker,
    bollinger_up AS bolinger_up,
    bollinger_down AS bolinger_down
FROM
    `dataengineering-378316`.`sp_500_data`.`sp_500_data_table`