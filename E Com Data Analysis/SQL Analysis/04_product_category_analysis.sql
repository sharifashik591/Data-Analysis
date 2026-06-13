-- 04 Product Category Analysis
SELECT
    p.category,
    p.sub_category,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(oi.quantity) AS units_sold,
    ROUND(SUM(oi.line_total)::numeric, 2) AS revenue,
    ROUND(SUM(oi.line_profit)::numeric, 2) AS profit,
    ROUND((SUM(oi.line_profit) / NULLIF(SUM(oi.line_total), 0))::numeric, 4) AS profit_margin
FROM public.order_items oi
JOIN public.products p
ON oi.product_id = p.product_id
JOIN public.orders o
ON oi.order_id = o.order_id
WHERE o.order_status <> 'Cancelled'
GROUP BY p.category, p.sub_category
ORDER BY revenue DESC;



-- Product Ranking Analysis

WITH product_sales AS (
    SELECT
        p.product_id,
        p.product_name,
        p.category,
        p.brand,
        SUM(oi.quantity) AS units_sold,
        ROUND(SUM(oi.line_total), 2) AS revenue,
        ROUND(SUM(oi.line_profit), 2) AS profit
    FROM public.order_items oi
    JOIN public.products p
    ON oi.product_id = p.product_id
    JOIN public.orders o
    ON oi.order_id = o.order_id
    WHERE o.order_status <> 'Cancelled'
    GROUP BY p.product_id, p.product_name, p.category, p.brand
)
SELECT
    *,
    RANK() OVER (ORDER BY revenue DESC) AS revenue_rank,
    RANK() OVER (ORDER BY profit DESC) AS profit_rank
FROM product_sales
ORDER BY revenue_rank
LIMIT 20;

-- ABC Product Analysis

WITH product_revenue AS (
    SELECT
        p.product_id,
        p.product_name,
        p.category,
        ROUND(SUM(oi.line_total)::numeric, 2) AS revenue -- Fixed cast inside first CTE
    FROM public.order_items oi
    JOIN public.products p
    ON oi.product_id = p.product_id
    JOIN public.orders o
    ON oi.order_id = o.order_id
    WHERE o.order_status <> 'Cancelled'
    GROUP BY p.product_id, p.product_name, p.category
),
revenue_share AS (
    SELECT
        product_id,
        product_name,
        category,
        revenue,
        -- Fixed: Cast the total revenue share division to numeric
        ROUND(
            (revenue / NULLIF(SUM(revenue) OVER (), 0))::numeric,
            4
        ) AS revenue_share,
        -- Fixed: Cast the running total/cumulative share division to numeric
        ROUND(
            (SUM(revenue) OVER (ORDER BY revenue DESC) / NULLIF(SUM(revenue) OVER (), 0))::numeric,
            4
        ) AS cumulative_revenue_share
    FROM product_revenue
)
SELECT
    product_id,
    product_name,
    category,
    revenue,
    revenue_share,
    cumulative_revenue_share,
    CASE
        WHEN cumulative_revenue_share <= 0.80 THEN 'A - High Value'
        WHEN cumulative_revenue_share <= 0.95 THEN 'B - Medium Value'
        ELSE 'C - Low Value'
    END AS abc_category
FROM revenue_share
ORDER BY revenue DESC;