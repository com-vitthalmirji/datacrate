CREATE OR REPLACE TEMPORARY VIEW orders
USING parquet
OPTIONS (path '/parity/orders.parquet');

SELECT
  id,
  amount,
  note,
  EXTRACT(DAY FROM placed_at) AS placed_day,
  SUM(amount) OVER (ORDER BY placed_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total,
  RANK() OVER (ORDER BY amount DESC) AS amount_rank
FROM orders
ORDER BY note NULLS FIRST, id;
