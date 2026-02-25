-- Drop view if it exists (safe)
DROP MATERIALIZED VIEW IF EXISTS analytics.daily_revenue;

-- Then create the materialized view
CREATE MATERIALIZED VIEW analytics.daily_revenue AS
SELECT
    fs.date_id,
    dd.full_date,
    SUM(fs.revenue) AS total_revenue,
    SUM(fs.profit) AS total_profit
FROM warehouse.fact_sales fs
JOIN warehouse.dim_date dd ON fs.date_id = dd.date_id
GROUP BY fs.date_id, dd.full_date
ORDER BY fs.date_id;



-- Product-region sales
DROP MATERIALIZED VIEW IF EXISTS analytics.product_region_sales;
CREATE MATERIALIZED VIEW analytics.product_region_sales AS
SELECT
    fs.product_id,
    dp.product_name,
    dp.category,
    dc.region AS customer_region,
    SUM(fs.quantity) AS total_quantity_sold,
    SUM(fs.revenue) AS total_revenue,
    SUM(fs.profit) AS total_profit
FROM warehouse.fact_sales fs
JOIN warehouse.dim_product dp ON fs.product_id = dp.product_id
JOIN warehouse.dim_customer dc ON fs.customer_id = dc.customer_id
GROUP BY fs.product_id, dp.product_name, dp.category, dc.region
ORDER BY total_revenue DESC;

-- Payments summary
DROP MATERIALIZED VIEW IF EXISTS analytics.payments_summary;
CREATE MATERIALIZED VIEW analytics.payments_summary AS
SELECT
    fp.date_id,
    dd.full_date,
    fp.payment_status,
    SUM(fp.amount_paid) AS total_amount
FROM warehouse.fact_payments fp
JOIN warehouse.dim_date dd ON fp.date_id = dd.date_id
GROUP BY fp.date_id, dd.full_date, fp.payment_status
ORDER BY fp.date_id;

-- Delivery performance
DROP MATERIALIZED VIEW IF EXISTS analytics.delivery_performance;
CREATE MATERIALIZED VIEW analytics.delivery_performance AS
SELECT
    fs.warehouse_id,
    dw.warehouse_name,
    AVG(fs.days_to_deliver) AS avg_days_to_deliver,
    COUNT(fs.order_id) AS total_shipments
FROM warehouse.fact_shipments fs
JOIN warehouse.dim_warehouse dw ON fs.warehouse_id = dw.warehouse_id
GROUP BY fs.warehouse_id, dw.warehouse_name
ORDER BY avg_days_to_deliver;

-- Inventory status
DROP MATERIALIZED VIEW IF EXISTS analytics.inventory_status;
CREATE MATERIALIZED VIEW analytics.inventory_status AS
SELECT
    fi.warehouse_id,
    dw.warehouse_name,
    fi.product_id,
    dp.product_name,
    SUM(fi.stock_qty) AS total_stock
FROM warehouse.fact_inventory fi
JOIN warehouse.dim_warehouse dw ON fi.warehouse_id = dw.warehouse_id
JOIN warehouse.dim_product dp ON fi.product_id = dp.product_id
GROUP BY fi.warehouse_id, dw.warehouse_name, fi.product_id, dp.product_name
ORDER BY total_stock DESC;




-- ========================
-- 4️⃣ Inventory KPIs
-- ========================
DROP MATERIALIZED VIEW IF EXISTS analytics.inventory_kpis;

CREATE MATERIALIZED VIEW analytics.inventory_kpis AS
SELECT
    fi.product_id,
    dp.product_name,
    fi.warehouse_id,
    dw.warehouse_name,
    SUM(fi.stock_qty) AS total_stock,
    COALESCE(SUM(fs.quantity),0) AS total_sold,
    CASE WHEN SUM(fi.stock_qty) = 0 THEN 1 ELSE 0 END AS stockout_flag,
    COALESCE(SUM(fs.quantity)::float / NULLIF(SUM(fi.stock_qty)::float,0),0) AS turnover_ratio
FROM warehouse.fact_inventory fi
LEFT JOIN warehouse.fact_sales fs 
    ON fi.product_id = fs.product_id
LEFT JOIN warehouse.dim_product dp ON fi.product_id = dp.product_id
LEFT JOIN warehouse.dim_warehouse dw ON fi.warehouse_id = dw.warehouse_id
GROUP BY fi.product_id, dp.product_name, fi.warehouse_id, dw.warehouse_name;


-- ========================
-- 5️⃣ Delivery Performance
-- ========================
DROP MATERIALIZED VIEW IF EXISTS analytics.delivery_kpis;

CREATE MATERIALIZED VIEW analytics.delivery_kpis AS
SELECT
    s.warehouse_id,
    dw.warehouse_name,
    AVG(EXTRACT(EPOCH FROM (s.delivery_date - s.ship_date))/86400) AS avg_delivery_days,
    COUNT(s.order_id) AS total_shipments
FROM warehouse.fact_shipments s
JOIN warehouse.dim_warehouse dw ON s.warehouse_id = dw.warehouse_id
GROUP BY s.warehouse_id, dw.warehouse_name
ORDER BY avg_delivery_days;


-- ========================
-- 6️⃣ Customer KPIs
-- ========================
DROP MATERIALIZED VIEW IF EXISTS analytics.customer_kpis;

CREATE MATERIALIZED VIEW analytics.customer_kpis AS
SELECT
    c.customer_id,
    c.name,
    c.region,
    COUNT(DISTINCT fs.order_id) AS total_orders,
    SUM(fs.revenue) AS lifetime_value,
    CASE WHEN COUNT(DISTINCT fs.order_id) > 1 THEN 1 ELSE 0 END AS repeat_customer
FROM warehouse.fact_sales fs
JOIN warehouse.dim_customer c ON fs.customer_id = c.customer_id
GROUP BY c.customer_id, c.name, c.region;


-- ========================
-- 7️⃣ Refresh Script (Optional)
-- ========================
-- You can refresh all views after loading new data
REFRESH MATERIALIZED VIEW analytics.daily_revenue;
REFRESH MATERIALIZED VIEW analytics.product_region_sales;
REFRESH MATERIALIZED VIEW analytics.payments_summary;
REFRESH MATERIALIZED VIEW analytics.inventory_kpis;
REFRESH MATERIALIZED VIEW analytics.delivery_kpis;
REFRESH MATERIALIZED VIEW analytics.customer_kpis;
