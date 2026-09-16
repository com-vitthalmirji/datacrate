CREATE OR REPLACE TEMPORARY VIEW orders
USING parquet
OPTIONS (path '/join-shuffle/orders_join.parquet');

CREATE OR REPLACE TEMPORARY VIEW shipments
USING parquet
OPTIONS (path '/join-shuffle/shipments_join.parquet');

SELECT COUNT(*) AS shipped_order_count, SUM(orders.amount) AS shipped_total_amount
FROM orders JOIN shipments ON orders.id = shipments.order_id
WHERE orders.amount > 50.00;
