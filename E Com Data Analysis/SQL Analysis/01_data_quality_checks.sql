-- 01 Data Quality Checks
-- 1. Row Count of All Tables

SELECT 'customers' AS table_name, COUNT(*) AS row_count FROM public.customers
UNION ALL
SELECT 'products', COUNT(*) FROM public.products
UNION ALL
SELECT 'orders', COUNT(*) FROM public.orders
UNION ALL
SELECT 'order_items', COUNT(*) FROM public.order_items
UNION ALL
SELECT 'payments', COUNT(*) FROM public.payments
UNION ALL
SELECT 'returns', COUNT(*) FROM public.returns
UNION ALL
SELECT 'marketing_campaigns', COUNT(*) FROM public.marketing_campaigns
UNION ALL
SELECT 'sessions', COUNT(*) FROM public.sessions
UNION ALL
SELECT 'inventory', COUNT(*) FROM public.inventory
UNION ALL
SELECT 'reviews', COUNT(*) FROM public.reviews;

-- 2. Duplicate Primary Key Check
SELECT customer_id, COUNT(*)
FROM public.customers
GROUP BY customer_id
HAVING COUNT(*) > 1;

SELECT order_id, COUNT(*)
FROM public.orders
GROUP BY order_id
HAVING COUNT(*) > 1;

SELECT product_id, COUNT(*)
FROM public.products
GROUP BY product_id
HAVING COUNT(*) > 1;

-- 3. Orders Without Valid Customers
SELECT o.*
FROM public.orders o
LEFT JOIN public.customers c
ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- 4. Order Items Without Valid Products
SELECT oi.*
FROM public.order_items oi
LEFT JOIN public.products p
ON oi.product_id = p.product_id
WHERE p.product_id IS NULL;

-- 5. Order Total Accuracy Check
SELECT 
    o.order_id,
    o.items_total AS stored_items_total,
    ROUND(SUM(oi.line_total), 2) AS calculated_items_total,
    ROUND(o.items_total - SUM(oi.line_total), 2) AS difference
FROM public.orders o
JOIN public.order_items oi
ON o.order_id = oi.order_id
GROUP BY o.order_id, o.items_total
HAVING ROUND(o.items_total, 2) <> ROUND(SUM(oi.line_total), 2);

-- 6. Negative Revenue or Profit Check
SELECT *
FROM public.orders
WHERE order_total < 0
   OR net_revenue < 0
   OR items_total < 0;