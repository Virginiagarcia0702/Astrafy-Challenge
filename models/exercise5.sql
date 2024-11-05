
{{ config(materialized='table') }}


WITH ordered_data AS (
    SELECT
        o.date_date,
        o.customers_id,
        o.orders_id,
        o.ca_ht,
        ROW_NUMBER() OVER (
            PARTITION BY o.customers_id
            ORDER BY o.date_date
        ) AS order_rank -- Jerarquiza todos los pedidos de cada cliente por fecha
    FROM
        `sql-for-bigquery-440715.dbt_virginiagarcia0702.astrf_orders` o
    WHERE
        FORMAT_DATE('%Y', o.date_date) = '2023' -- Filtramos solo los pedidos de 2023
),

segmented_orders AS (
    SELECT
        current_order.date_date,
        current_order.customers_id,
        current_order.orders_id,
        current_order.ca_ht,
        current_order.order_rank,
        (
            SELECT COUNT(*)
            FROM ordered_data prior_orders
            WHERE prior_orders.customers_id = current_order.customers_id
              AND prior_orders.date_date >= DATE_SUB(current_order.date_date, INTERVAL 12 MONTH)
              AND prior_orders.date_date < current_order.date_date
        ) AS prior_12_months_orders -- Calcula los pedidos en los últimos 12 meses
    FROM
        ordered_data current_order
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