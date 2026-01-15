CREATE OR REFRESH MATERIALIZED VIEW book_store_catalog.gold.gold_customer_order_summary
AS
SELECT c.customer_id,
c.customer_name,
c.date_of_birth,
c.telephone,
c.email,
c.address_line_1,
c.city,
c.state,
c.postcode,
COUNT(DISTINCT o.order_id) AS total_orders,
SUM(o.item_quantity) AS total_items_ordered,
SUM(o.item_quantity * o.item_price) AS total_order_amount
FROM book_store_catalog.silver.silver_customer c
JOIN book_store_catalog.silver.silver_addresses a ON c.customer_id = a.customer_id
JOIN book_store_catalog.silver.silver_orders o ON c.customer_id = o.customer_id
WHERE a._END_AT IS NULL   -- quiero los ultimos cambios ya que es una tabla dimension SC2
GROUP BY ALL;