-- 03 Sales Trend Analysis
WITH monthly_sales AS (
    SELECT
        DATE_TRUNC('month', order_date)::DATE AS month,
        COUNT(DISTINCT order_id) AS total_orders,
        ROUND(SUM(net_revenue)::numeric, 2) AS revenue,
        ROUND(SUM(net_profit)::numeric, 2) AS profit,
        -- Grab the previous month's revenue right here:
        LAG(ROUND(SUM(net_revenue)::numeric, 2)) OVER (ORDER BY DATE_TRUNC('month', order_date)::DATE) AS prev_revenue
    FROM public.orders
    WHERE order_status <> 'Cancelled'
    GROUP BY DATE_TRUNC('month', order_date)::DATE
)
SELECT
    month,
    total_orders,
    revenue,
    profit,
    -- Much cleaner formula to read and maintain:
    ROUND(((revenue - prev_revenue) / NULLIF(prev_revenue, 0))::numeric, 4) AS month_over_month_growth,
    ROUND((SUM(revenue) OVER (ORDER BY month))::numeric, 2) AS running_total_revenue
FROM monthly_sales
ORDER BY month;