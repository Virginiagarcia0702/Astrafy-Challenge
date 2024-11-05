{{ config(materialized='table') }}

SELECT COUNT(orders_id) AS orders
FROM `sql-for-bigquery-440715.dbt_virginiagarcia0702.astrf_orders` 
WHERE FORMAT_DATE('%Y', date_date) = '2023'