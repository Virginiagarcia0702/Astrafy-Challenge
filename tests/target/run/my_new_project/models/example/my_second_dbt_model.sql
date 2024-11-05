
  
    

    create or replace table `sql-for-bigquery-440715`.`dbt_virginiagarcia0702`.`my_second_dbt_model`
      
    
    

    OPTIONS()
    as (
      -- Use the `ref` function to select from other models

select *
from `sql-for-bigquery-440715`.`dbt_virginiagarcia0702`.`my_first_dbt_model`
where id = 1
    );
  