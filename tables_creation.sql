-- Tabla para astrf_orders
CREATE TABLE astrf_orders (
    date_date DATE,
    customer_id VARCHAR2(50),
    orders_id VARCHAR2(50) PRIMARY KEY,
    ca_ht NUMBER(10, 2)
);

-- Tabla para astrf_sales
CREATE TABLE astrf_sales (
    date_date DATE,
    client_id VARCHAR2(50),
    transaction_id VARCHAR2(50) PRIMARY KEY,
    products_id VARCHAR2(50),
    turnover NUMBER(10, 2),
    qty NUMBER
);
