CREATE OR REPLACE TEMPORARY VIEW orders
USING parquet
OPTIONS (path '/m3.6/orders');

SELECT COUNT(*) AS order_count, SUM(amount) AS total_amount
FROM orders
WHERE amount > 50.00;
