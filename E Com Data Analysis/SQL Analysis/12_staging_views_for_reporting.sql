-- 12 Staging Views for Reporting


-- Sales Views
CREATE OR REPLACE VIEW staging.vw_monthly_sales_analysis AS
WITH monthly_sales AS (
    SELECT
        DATE_TRUNC('month', order_date)::DATE AS month,
        COUNT(DISTINCT order_id) AS total_orders,
        ROUND(SUM(net_revenue)::numeric, 2) AS revenue, -- Fixed cast inside CTE
        ROUND(SUM(net_profit)::numeric, 2) AS profit    -- Fixed cast inside CTE
    FROM public.orders
    WHERE order_status <> 'Cancelled'
    GROUP BY DATE_TRUNC('month', order_date)::DATE
)
SELECT
    month,
    total_orders,
    revenue,
    profit,
    -- Fixed: Cast the entire MoM growth division math to numeric before rounding
    ROUND(
        ((revenue - LAG(revenue) OVER (ORDER BY month)) 
        / NULLIF(LAG(revenue) OVER (ORDER BY month), 0))::numeric,
        4
    ) AS mom_growth,
    -- Fixed: Cast the running total aggregation to numeric before rounding
    ROUND(
        (SUM(revenue) OVER (ORDER BY month))::numeric, 
        2
    ) AS running_revenue
FROM monthly_sales;


-- Products Views
CREATE OR REPLACE VIEW staging.vw_product_analysis AS
SELECT
    p.product_id,
    p.product_name,
    p.category,
    p.sub_category,
    p.brand,
    -- Best Practice: Cast aggregate quantities to bigint for consistent data typing
    SUM(oi.quantity)::bigint AS units_sold,
    ROUND(SUM(oi.line_total)::numeric, 2) AS revenue,   -- Fixed cast
    ROUND(SUM(oi.line_profit)::numeric, 2) AS profit,   -- Fixed cast
    -- Fixed: Cast the aggregate profit margin division math to numeric before rounding
    ROUND(
        (SUM(oi.line_profit) / NULLIF(SUM(oi.line_total), 0))::numeric,
        4
    ) AS profit_margin
FROM public.order_items oi
JOIN public.products p
ON oi.product_id = p.product_id
JOIN public.orders o
ON oi.order_id = o.order_id
WHERE o.order_status <> 'Cancelled'
GROUP BY
    p.product_id,
    p.product_name,
    p.category,
    p.sub_category,
    p.brand;


-- Customer RFM View
CREATE OR REPLACE VIEW staging.vw_rfm_analysis AS
WITH rfm_base AS (
    SELECT
        c.customer_id,
        c.customer_name,
        c.city,
        c.acquisition_channel,
        MAX(o.order_date) AS last_purchase_date,
        -- Fixed: Cast to DATE first so the subtraction yields an integer natively
        (DATE '2026-01-01' - MAX(o.order_date)::DATE) AS recency_days,
        COUNT(DISTINCT o.order_id) AS frequency,
        ROUND(SUM(o.net_revenue)::numeric, 2) AS monetary_value -- Fixed cast
    FROM public.customers c
    JOIN public.orders o
    ON c.customer_id = o.customer_id
    WHERE o.order_status <> 'Cancelled'
    GROUP BY
        c.customer_id,
        c.customer_name,
        c.city,
        c.acquisition_channel
),
rfm_score AS (
    SELECT
        *,
        -- Fixed: Flipped NTILE sorting so 5 is always the best score
        NTILE(5) OVER (ORDER BY recency_days ASC) AS recency_score,
        NTILE(5) OVER (ORDER BY frequency DESC) AS frequency_score,
        NTILE(5) OVER (ORDER BY monetary_value DESC) AS monetary_score
    FROM rfm_base
)
SELECT
    customer_id,
    customer_name,
    city,
    acquisition_channel,
    last_purchase_date,
    recency_days,
    frequency,
    monetary_value,
    recency_score,
    frequency_score,
    monetary_score,
    (recency_score + frequency_score + monetary_score) AS rfm_total_score,
    CASE
        WHEN recency_score >= 4 AND frequency_score >= 4 AND monetary_score >= 4 THEN 'Champions'
        WHEN frequency_score >= 4 AND monetary_score >= 4 THEN 'Loyal Customers'
        WHEN recency_score >= 4 AND frequency_score <= 2 THEN 'New Customers'
        WHEN recency_score <= 2 AND frequency_score >= 3 THEN 'At Risk'
        WHEN recency_score <= 2 AND frequency_score <= 2 THEN 'Lost Customers'
        ELSE 'Regular Customers'
    END AS rfm_segment
FROM rfm_score;




