

{{ config(materialized='table') }}


WITH astrf_orders_with_qty_product AS (
    SELECT 
        o.date_date
        ,o.customers_id
        ,o.orders_id
        ,o.ca_ht
        ,IFNULL(SUM(s.qty), 0) AS qty_product
    FROM 
        `sql-for-bigquery-440715.dbt_virginiagarcia0702.astrf_orders` o
    LEFT JOIN 
        `sql-for-bigquery-440715.dbt_virginiagarcia0702.astrf_sales` s 
    ON 
        o.orders_id = s.transaction_id
    GROUP BY
        o.date_date
        ,o.customers_id
        ,o.orders_id
        ,o.ca_ht
)
SELECT * FROM astrf_orders_with_qty_product