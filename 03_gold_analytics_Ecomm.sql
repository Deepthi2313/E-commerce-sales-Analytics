-- Databricks notebook source
-- MAGIC %md
-- MAGIC **Create GOLD views (KPIs)**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Daily sales performance

-- COMMAND ----------

CREATE OR REPLACE VIEW workspace.gold.vw_daily_sales AS
SELECT
  o.order_date,
  COUNT(DISTINCT o.order_id)              AS total_orders,
  SUM(o.total_amount)                     AS total_revenue,
  AVG(o.total_amount)                     AS avg_order_value
FROM workspace.silver.fact_orders o
GROUP BY o.order_date
ORDER BY o.order_date;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Product performance

-- COMMAND ----------

CREATE OR REPLACE VIEW workspace.gold.vw_product_performance AS
SELECT
  p.product_id,
  p.product_name,
  p.category,
  p.brand,
  SUM(oi.quantity)        AS total_quantity_sold,
  SUM(oi.line_amount)     AS total_revenue
FROM workspace.silver.fact_order_items oi
JOIN workspace.silver.dim_product p
  ON oi.product_id = p.product_id
GROUP BY p.product_id, p.product_name, p.category, p.brand
ORDER BY total_revenue DESC;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Funnel conversion metrics

-- COMMAND ----------

CREATE OR REPLACE VIEW workspace.gold.vw_funnel_conversion AS
WITH session_events AS (
    SELECT
      session_id,
      MAX(CASE WHEN event_type = 'PageView'   THEN 1 ELSE 0 END) AS viewed,
      MAX(CASE WHEN event_type = 'AddToCart'  THEN 1 ELSE 0 END) AS added_cart,
      MAX(CASE WHEN event_type = 'Checkout'   THEN 1 ELSE 0 END) AS checked_out,
      MAX(CASE WHEN event_type = 'Purchase'   THEN 1 ELSE 0 END) AS purchased
    FROM workspace.silver.fact_funnel_events
    GROUP BY session_id
)
SELECT
  COUNT(*)                                             AS total_sessions,
  SUM(viewed)                                          AS sessions_with_view,
  SUM(added_cart)                                      AS sessions_with_add_to_cart,
  SUM(checked_out)                                     AS sessions_with_checkout,
  SUM(purchased)                                       AS sessions_with_purchase,
  ROUND(SUM(added_cart) * 1.0 / NULLIF(SUM(viewed),0), 4)        AS view_to_cart_rate,
  ROUND(SUM(checked_out) * 1.0 / NULLIF(SUM(added_cart),0), 4)   AS cart_to_checkout_rate,
  ROUND(SUM(purchased) * 1.0 / NULLIF(SUM(checked_out),0), 4)    AS checkout_to_purchase_rate,
  ROUND(SUM(purchased) * 1.0 / NULLIF(SUM(viewed),0), 4)         AS overall_conversion_rate
FROM session_events;


-- COMMAND ----------

SELECT * FROM workspace.gold.vw_daily_sales LIMIT 20;


-- COMMAND ----------

SELECT * FROM workspace.gold.vw_product_performance LIMIT 20;


-- COMMAND ----------

SELECT * FROM workspace.gold.vw_funnel_conversion;