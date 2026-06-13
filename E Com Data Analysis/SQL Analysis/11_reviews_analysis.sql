-- 11 Reviews Analysis
-- Review and Rating Analysis

SELECT
    p.category,
    p.sub_category,
    COUNT(r.review_id) AS total_reviews,
    ROUND(AVG(r.rating), 2) AS avg_rating,
    COUNT(*) FILTER (WHERE r.rating <= 2) AS negative_reviews,
    COUNT(*) FILTER (WHERE r.rating >= 4) AS positive_reviews
FROM public.reviews r
JOIN public.products p
ON r.product_id = p.product_id
GROUP BY p.category, p.sub_category
ORDER BY avg_rating DESC;


-- Product Quality Risk Analysis

WITH review_summary AS (
    SELECT
        product_id,
        COUNT(*) AS total_reviews,
        ROUND(AVG(rating)::numeric, 2) AS avg_rating, -- Fixed cast
        COUNT(*) FILTER (WHERE rating <= 2) AS negative_reviews
    FROM public.reviews
    GROUP BY product_id
),
return_summary AS (
    SELECT
        product_id,
        COUNT(*) AS total_returns,
        ROUND(SUM(refund_amount)::numeric, 2) AS refund_amount -- Fixed cast
    FROM public.returns
    GROUP BY product_id
)
SELECT
    p.product_id,
    p.product_name,
    p.category,
    COALESCE(rs.total_reviews, 0) AS total_reviews,
    -- Fixed: Swapped fallback literal to 0.00 for consistent decimal output
    COALESCE(rs.avg_rating, 0.00) AS avg_rating,
    COALESCE(rs.negative_reviews, 0) AS negative_reviews,
    COALESCE(rt.total_returns, 0) AS total_returns,
    -- Fixed: Swapped fallback literal to 0.00 for consistent decimal output
    COALESCE(rt.refund_amount, 0.00) AS refund_amount,
    CASE
        WHEN COALESCE(rs.avg_rating, 5.00) < 3 -- Matched type to decimal
             AND COALESCE(rt.total_returns, 0) > 0
        THEN 'High Quality Risk'
        WHEN COALESCE(rs.avg_rating, 5.00) < 4 -- Matched type to decimal
        THEN 'Medium Quality Risk'
        ELSE 'Low Quality Risk'
    END AS quality_risk_level
FROM public.products p
LEFT JOIN review_summary rs
ON p.product_id = rs.product_id
LEFT JOIN return_summary rt
ON p.product_id = rt.product_id
ORDER BY refund_amount DESC, avg_rating ASC;


-- Discount Impact on Profit
SELECT
    p.category,
    ROUND(SUM(oi.discount_amount)::numeric, 2) AS total_discount,
    ROUND(SUM(oi.line_total)::numeric, 2) AS revenue_after_discount,
    ROUND(SUM(oi.line_profit)::numeric, 2) AS profit,
    -- Fixed: Added numeric cast to the aggregate discount rate division math
    ROUND(
        (SUM(oi.discount_amount) / NULLIF(SUM(oi.gross_amount), 0))::numeric,
        4
    ) AS discount_rate,
    -- Fixed: Added numeric cast to the aggregate profit margin division math
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
GROUP BY p.category
ORDER BY discount_rate DESC;

-- City-Wise Business Performance

SELECT
    shipping_city,
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(DISTINCT customer_id) AS total_customers,
    -- Fixed: Cast sums and averages to numeric before rounding, then coalesce
    COALESCE(ROUND(SUM(net_revenue)::numeric, 2), 0.00) AS revenue,
    COALESCE(ROUND(SUM(net_profit)::numeric, 2), 0.00) AS profit,
    COALESCE(ROUND(AVG(net_revenue)::numeric, 2), 0.00) AS average_order_value,
    COUNT(*) FILTER (WHERE delivery_status = 'Delayed') AS delayed_orders,
    ROUND(
        COUNT(*) FILTER (WHERE delivery_status = 'Delayed')::numeric
        / NULLIF(COUNT(*), 0),
        4
    ) AS delay_rate
FROM public.orders
WHERE order_status <> 'Cancelled'
GROUP BY shipping_city
ORDER BY revenue DESC;

-- Market Basket Analysis
