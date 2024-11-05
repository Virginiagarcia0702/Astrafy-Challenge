
{{ config(materialized='table') }}


WITH segmented_orders AS (
    SELECT
        o.date_date,
        o.customers_id,
        o.orders_id,
        o.ca_ht,
        (
            SELECT COUNT(*)
            FROM `sql-for-bigquery-440715.dbt_virginiagarcia0702.astrf_orders` prior_orders
            WHERE prior_orders.customers_id = o.customers_id
              AND prior_orders.date_date >= DATE_SUB(o.date_date, INTERVAL 12 MONTH)
              AND prior_orders.date_date < o.date_date
        ) AS prior_12_months_orders -- Calculates the number of orders in the last 12 months
    FROM
        `sql-for-bigquery-440715.dbt_virginiagarcia0702.astrf_orders` o
    WHERE
        FORMAT_DATE('%Y', o.date_date) = '2023' -- Filters only orders from 2023
)

SELECT
    date_date,
    customers_id,
    orders_id,
    ca_ht,
    CASE
        WHEN prior_12_months_orders = 0 THEN 'New'
        WHEN prior_12_months_orders BETWEEN 1 AND 3 THEN 'Returning'
        ELSE 'VIP'
    END AS segment
FROM
    segmented_orders
ORDER BY 
    customers_id
