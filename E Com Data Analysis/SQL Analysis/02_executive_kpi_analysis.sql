-- 02 Executive KPI Analysis
SELECT
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(DISTINCT customer_id) AS total_customers,
    ROUND(SUM(net_revenue)::numeric, 2) AS total_revenue,
    ROUND(SUM(net_profit)::numeric, 2) AS total_profit,
    
    -- Option A: Row-based average (Fixed with casting)
    ROUND(AVG(net_revenue)::numeric, 2) AS average_row_value,
    
    -- Option B: True Average Order Value (Total Revenue / Total Orders)
    ROUND((SUM(net_revenue) / NULLIF(COUNT(DISTINCT order_id), 0))::numeric, 2) AS average_order_value,
    
    ROUND((SUM(net_profit) / NULLIF(SUM(net_revenue), 0))::numeric, 4) AS profit_margin,
    COUNT(DISTINCT CASE WHEN order_status = 'Returned' THEN order_id END) AS returned_orders,
    ROUND(
        COUNT(DISTINCT CASE WHEN order_status = 'Returned' THEN order_id END)::numeric
        / NULLIF(COUNT(DISTINCT order_id), 0),
        4
    ) AS return_rate
FROM public.orders
WHERE order_status <> 'Cancelled';

