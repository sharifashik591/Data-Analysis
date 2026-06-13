-- 05 Customer Analysis
SELECT
    c.customer_id,
    c.customer_name,
    c.city,
    c.acquisition_channel,
    c.customer_segment,
    COUNT(DISTINCT o.order_id) AS total_orders,
    ROUND(SUM(o.net_revenue)::numeric, 2) AS total_spent,
    ROUND(AVG(o.net_revenue)::numeric, 2) AS average_order_value,
    ROUND(SUM(o.net_profit)::numeric, 2) AS total_profit,
    MIN(o.order_date) AS first_order_date,
    MAX(o.order_date) AS last_order_date
FROM public.customers c
JOIN public.orders o
ON c.customer_id = o.customer_id
WHERE o.order_status <> 'Cancelled'
GROUP BY
    c.customer_id,
    c.customer_name,
    c.city,
    c.acquisition_channel,
    c.customer_segment
ORDER BY total_spent DESC
LIMIT 20;