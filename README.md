# Astrafy-Challenge

## dbt and Google Cloud introduction

In order to start with the coding-challenge, I've used the dbt transformation workflow using dbt Cloud in order to transform the data using version control from github and Google Cloud as a data warehouse.
I attach the following videos that have helped me to familiarize with the tools previously mentioned:

Video: https://youtu.be/6zDTbM6OUcs?si=wVvPbcAr3hHcxqRw
[![](https://img.youtube.com/vi/wVvPbcAr3hHcxqRw/0.jpg)](https://youtu.be/6zDTbM6OUcs?si=wVvPbcAr3hHcxqRw)

Video: https://youtu.be/ucA27rM043o?si=pBuWUOYmgtoI2Lhj
[![](https://img.youtube.com/vi/pBuWUOYmgtoI2Lhj/0.jpg)](https://youtu.be/ucA27rM043o?si=pBuWUOYmgtoI2Lhj)

Video: https://youtu.be/_C_pYeuF6_s?si=bdDSAQIU62k_nS5m
[![](https://img.youtube.com/vi/bdDSAQIU62k_nS5m/0.jpg)](https://youtu.be/_C_pYeuF6_s?si=bdDSAQIU62k_nS5m)

The datasets can be accessed from:

https://console.cloud.google.com/bigquery?ws=!1m4!1m3!3m2!1ssql-for-bigquery-440715!2sdbt_virginiagarcia0702

## Exercises development
Once the dataset model is set, I did some basic analysis on the tables 
`astrf_orders` and `astrf_sales` in order to better understand their 
content and their possible connections. 
On the `astrf_orders` table we find information about the orders generated
by the different customers, the dates and the amount spent on each transaction (`ca_ht`).
On the `astrf_sales`, on the other hand, we have disaggregated information aobut the specific
products sold for each transaction, their price and also the quantity sold.
Both tables can be join from the fields `transaction_id` and `orders_id` or either `clients_id` and `customer_id` as they refer to the same information.

It is relevant to highlight that I am used to work on Oracle SQL and therefore all of the correspondent function equivalences with BigQuery are being consulted either via ChatGPT or StackOverflow.

```sql
SELECT DISTINCT * FROM astrf_orders
-- we find 3661 distinct orders
SELECT DISTINCT *  FROM astrf_sales;
-- 28361 total unique entries
```

## Exercise 1
### What is the number of orders in the year 2023?
We are using the command `count()` to count the number of orders in the table `astrf_orders`.
(Note that we are not using ```distinct count``` as `orders_id` in `astrf_orders` have already unique values).

```sql
SELECT COUNT(orders_id) AS orders
FROM `sql-for-bigquery-440715.dbt_virginiagarcia0702.astrf_orders` 
WHERE FORMAT_DATE('%Y', date_date) = '2023'; --Used to specify the year 2023
```

## Exercise 2
### What is the number of orders per month in the year 2023?
We are using the command `count(orders_id)` as before in order to count the amount of orders per month. 
For the date we are using a formatting function `FORMAT_DATE('%b-%Y', date_date)` to show the date as 'year-month' (which now is a `STRING`).
Finally, we order the results by `year_month` using the function `PARSE_DATE('%b-%Y', year_month)`, which converts the year_month again in a `DATE` in order to chronologically order the results using the command `ASC`. 

```sql
SELECT 
    COUNT(orders_id) AS orders
    ,FORMAT_DATE('%b-%Y', date_date) AS year_month --%b -> short month, %Y -> complete year
FROM `sql-for-bigquery-440715.dbt_virginiagarcia0702.astrf_orders`
WHERE FORMAT_DATE('%Y', date_date) = '2023'
GROUP BY 
    year_month
ORDER BY 
    PARSE_DATE('%b-%Y', year_month) ASC; -- chronologically ordered
```

## Exercise 3
### What is the average number of products per order for each month of the year 2023?
Within the created CTE we calculate the total quantity of products for each order by summing the `qty` field in _astrf_sales_
grouping by each unique `orders_id` and `year_month` combination to get the total quantity for each order per month.
To do that, we need to join the `astrf_orders` table with the `astrf_sales` table using the co-relation between `orders_id` and `transaction_id`. 
Finally we calculate the average number of products per order for each month, rounding the result: `ROUND(AVG(AUX.total_products_per_order))`.

```sql
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
    PARSE_DATE('%b-%Y', year_month) ASC;
```

## Exercise 4
### Create a table (1 line per order) for all orders in the year 2022 and 2023; this table is similar to orders with an additional column: the qty_product column that gives the quantity of products in the order, for all orders in 2022 and 2023
```sql
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
SELECT * FROM astrf_orders_with_qty_product;
```

## Exercise 5 & Exercise 6: Order segmentation
### Orders are segmented into 3 groups:
- **New: it's the 1st order of the customer (client_id) in the past 12 months. In
the 12 months prior to this order, the customer did not place any orders**
- **Returning: it's between the 2nd and the 4th order of the customer in the
past 12 months. In the 12 months prior to this order, the customer had
already placed between 1 and 3 orders**
- **VIP: it's the 5th or more order of the customer in the past 12 months. In the 12
months prior to this order, the customer had already placed at least 4
orders or more**

### Calculate for each order placed in 2023, the segment of this order and create a table (1 line per order) for all orders of the year 2023 only; with an additional column: the order_segmentation column which gives the segment of this order

```sql
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
```