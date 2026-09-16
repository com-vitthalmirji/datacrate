CREATE OR REPLACE TEMPORARY VIEW orders
USING parquet
OPTIONS (path '/spark-comet/orders.parquet');

SELECT COUNT(*) AS order_count, SUM(amount) AS total_amount
FROM orders
WHERE amount > 50.00;
