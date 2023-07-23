-- create table `dataengineering-378316.sp_500_data.moving_averages` as (
-- SELECT
--   date,
--   symbol as ticker,
<<<<<<< HEAD



=======
--   twenty_day_moving as twenty_ma,
--   two_hundred_day_moving as two_hundred_ma
-- -- FROM `dataengineering-378316.sp_500_data.sp_500_data_table`
-- )
-- FROM
-- `dataengineering-378316`.`sp_500_data`.`sp_500_data_table`



-- SELECT
--   date,
--   symbol as ticker,
--   twenty_day_moving as twenty_ma,
--   two_hundred_day_moving as two_hundred_ma
-- -- FROM `dataengineering-378316.sp_500_data.sp_500_data_table`
-- -- )
-- FROM
-- `dataengineering-378316`.`sp_500_data`.`sp_500_data_table`


>>>>>>> main


  SELECT
    date,
    symbol AS ticker,
    twenty_day_moving AS twenty_ma,
    two_hundred_day_moving AS two_hundred_ma
  FROM
    `dataengineering-378316`.`sp_500_data`.`sp_500_data_table`