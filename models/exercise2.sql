
{{ config(materialized='table') }}

SELECT 
    COUNT(orders_id) AS orders
    ,FORMAT_DATE('%b-%Y', date_date) AS year_month
FROM `sql-for-bigquery-440715.dbt_virginiagarcia0702.astrf_orders`
WHERE FORMAT_DATE('%Y', date_date) = '2023'
GROUP BY 
    year_month
ORDER BY 
    PARSE_DATE('%b-%Y', year_month) ASC