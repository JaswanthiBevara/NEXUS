-- ============================================================
-- PROJECT NEXUS: BUSINESS ANALYTICS
-- Source: nexus_catalog.streaming.curated_order_events
-- Read-only analytical queries
-- ============================================================

-- Prepare the latest order state.
-- Updates for an order must not be counted as separate orders.

CREATE OR REPLACE TEMP VIEW current_orders AS
WITH ranked_orders AS (
    SELECT
        order_id,
        customer_id,
        product_id,
        quantity,
        unit_price,
        discount_amount,
        event_type,
        event_time,
        ingest_time,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY ingest_time DESC, event_time DESC
        ) AS order_rank
    FROM nexus_catalog.streaming.curated_order_events
    WHERE order_id IS NOT NULL
)
SELECT
    order_id,
    customer_id,
    product_id,
    quantity,
    unit_price,
    discount_amount,
    event_type,
    event_time,
    ingest_time,
    CAST(
        quantity * unit_price - COALESCE(discount_amount, 0.0)
        AS DOUBLE
    ) AS net_revenue
FROM ranked_orders
WHERE order_rank = 1
  AND event_type <> 'ORDER_CANCELLED';


-- 1. TOTAL SALES REVENUE

SELECT
    ROUND(SUM(net_revenue), 2) AS total_sales_revenue
FROM current_orders;


-- 2. TOTAL ORDERS

SELECT
    COUNT(DISTINCT order_id) AS total_orders
FROM current_orders;


-- 3. TOP 10 BEST-SELLING PRODUCTS

SELECT
    product_id,
    SUM(quantity) AS total_quantity_sold,
    ROUND(SUM(net_revenue), 2) AS revenue_contribution
FROM current_orders
GROUP BY product_id
ORDER BY
    total_quantity_sold DESC,
    revenue_contribution DESC
LIMIT 10;


-- 4. MONTHLY SALES TREND

SELECT
    DATE_FORMAT(event_time, 'yyyy-MM') AS sales_month,
    ROUND(SUM(net_revenue), 2) AS monthly_revenue
FROM current_orders
GROUP BY DATE_FORMAT(event_time, 'yyyy-MM')
ORDER BY sales_month;


-- 5. TOP 10 CUSTOMERS

SELECT
    customer_id,
    COUNT(DISTINCT order_id) AS total_orders,
    ROUND(SUM(net_revenue), 2) AS total_revenue
FROM current_orders
GROUP BY customer_id
ORDER BY total_revenue DESC
LIMIT 10;


-- ============================================================
-- NOTES
-- ============================================================
-- 1. Latest event per order is selected using ingest_time
--    followed by event_time.
-- 2. ORDER_CANCELLED orders are excluded.
-- 3. Revenue = (quantity * unit_price) - discount_amount.
-- 4. discount_amount is treated as the total order discount.
-- 5. These queries do not modify production Iceberg tables.
-- ============================================================