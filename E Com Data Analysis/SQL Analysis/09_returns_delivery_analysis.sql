-- 09 Returns & Delivery Analysis
SELECT
    r.return_reason,
    COUNT(*) AS total_returns,
    SUM(r.returned_quantity) AS total_units_returned,
    ROUND(SUM(r.refund_amount)::numeric, 2) AS total_refund_amount, -- Fixed cast
    ROUND(AVG(r.refund_amount)::numeric, 2) AS avg_refund_amount,   -- Fixed cast
    ROUND(SUM(r.return_processing_cost)::numeric, 2) AS total_processing_cost -- Fixed cast
FROM public.returns r
GROUP BY r.return_reason
ORDER BY total_returns DESC;


-- Return Rate by Product Category
WITH sold AS (
    SELECT
        p.category,
        COUNT(DISTINCT o.order_id) AS total_orders,
        SUM(oi.quantity) AS units_sold
    FROM public.orders o
    JOIN public.order_items oi
    ON o.order_id = oi.order_id
    JOIN public.products p
    ON oi.product_id = p.product_id
    WHERE o.order_status <> 'Cancelled'
    GROUP BY p.category
),
returned AS (
    SELECT
        p.category,
        COUNT(DISTINCT r.return_id) AS total_returns,
        SUM(r.returned_quantity) AS units_returned,
        ROUND(SUM(r.refund_amount)::numeric, 2) AS refund_amount -- Fixed cast here
    FROM public.returns r
    JOIN public.products p
    ON r.product_id = p.product_id
    GROUP BY p.category
)
SELECT
    s.category,
    s.total_orders,
    s.units_sold,
    COALESCE(r.total_returns, 0) AS total_returns,
    COALESCE(r.units_returned, 0) AS units_returned,
    -- Fixed: Force COALESCE fallback to use numeric decimal formatting
    COALESCE(r.refund_amount, 0.00) AS refund_amount,
    ROUND(
        COALESCE(r.units_returned, 0)::numeric
        / NULLIF(s.units_sold, 0),
        4
    ) AS unit_return_rate
FROM sold s
LEFT JOIN returned r
ON s.category = r.category
ORDER BY unit_return_rate DESC;

-- Late Delivery Impact on Returns

SELECT
    o.delivery_status,
    COUNT(DISTINCT o.order_id) AS total_orders,
    COUNT(DISTINCT r.return_id) AS total_returns,
    ROUND(
        COUNT(DISTINCT r.return_id)::numeric
        / NULLIF(COUNT(DISTINCT o.order_id), 0),
        4
    ) AS return_rate,
    -- Fixed: Cast the sum of the floats to numeric before rounding, then coalesce the final result
    COALESCE(ROUND(SUM(r.refund_amount)::numeric, 2), 0.00) AS total_refund_amount
FROM public.orders o
LEFT JOIN public.returns r
ON o.order_id = r.order_id
WHERE o.order_status <> 'Cancelled'
GROUP BY o.delivery_status
ORDER BY return_rate DESC;


-- Payment Method Analysis
SELECT
    payment_method,
    payment_status,
    COUNT(*) AS total_transactions,
    -- Fixed: Cast sums and averages to numeric before rounding, then coalesce
    COALESCE(ROUND(SUM(amount_paid)::numeric, 2), 0.00) AS total_amount_paid,
    COALESCE(ROUND(SUM(transaction_fee)::numeric, 2), 0.00) AS total_transaction_fee,
    COALESCE(ROUND(AVG(transaction_fee)::numeric, 2), 0.00) AS avg_transaction_fee,
    COALESCE(ROUND(SUM(refund_amount)::numeric, 2), 0.00) AS total_refund_amount
FROM public.payments
GROUP BY payment_method, payment_status
ORDER BY total_amount_paid DESC;