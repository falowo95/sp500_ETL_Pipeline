
  
    

    create or replace table `dataengineering-378316`.`sp_500_data`.`my_first_dbt_model`
    
    
    OPTIONS()
    as (
      /*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

CREATE or replace table `dataengineering-378316.sp_500_data.my_first_dbt_model` as
SELECT
  date,
  symbol as ticker,
  twenty_day_moving as twenty_ma,
  two_hundred_day_moving as two_hundred_ma
FROM `dataengineering-378316.sp_500_data.sp_500_data_table`
WHERE symbol = 'AAPL'
LIMIT 10;

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
    );
  