
{{ config(materialized='table') }}

WITH AUX AS(
SELECT 
     FORMAT_DATE('%b-%Y', o.date_date) AS year_month
    ,orders_id 
    ,SUM(s.qty) AS total_products_per_order
FROM `sql-for-bigquery-440715.dbt_virginiagarcia0702.astrf_orders` o
JOIN `sql-for-bigquery-440715.dbt_virginiagarcia0702.astrf_sales` s
    ON o.orders_id = s.transaction_id
WHERE FORMAT_DATE('%Y', o.date_date) = '2023'
GROUP BY 
    orders_id,year_month
)
SELECT
    AUX.year_month
   ,ROUND(AVG(AUX.total_products_per_order)) AS avg_products_per_order
FROM AUX
GROUP BY 
    AUX.year_month
ORDER BY 
    PARSE_DATE('%b-%Y', year_month) ASC
