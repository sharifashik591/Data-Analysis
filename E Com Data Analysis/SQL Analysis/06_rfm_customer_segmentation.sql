-- 06 RFM Customer Segmentation
WITH rfm_base AS (
    SELECT
        c.customer_id,
        c.customer_name,
        c.city,
        c.acquisition_channel,
        MAX(o.order_date) AS last_purchase_date,
        DATE '2026-01-01' - MAX(o.order_date) AS recency_days,
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
        -- Fixed: Low recency days = Recent purchase = High score (5)
        NTILE(5) OVER (ORDER BY recency_days ASC) AS recency_score,
        -- Fixed: High frequency = Loyal buyer = High score (5)
        NTILE(5) OVER (ORDER BY frequency DESC) AS frequency_score,
        -- Fixed: High monetary value = Big spender = High score (5)
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
FROM rfm_score
ORDER BY rfm_total_score DESC, monetary_value DESC;

-- RFM Segment Summary

WITH rfm_segments AS (
    WITH rfm_base AS (
        SELECT
            c.customer_id,
            MAX(o.order_date) AS last_purchase_date,
            -- Fixed: Cast the max order date to a DATE first so the subtraction results in an integer natively
            (DATE '2026-01-01' - MAX(o.order_date)::DATE) AS recency_days,
            COUNT(DISTINCT o.order_id) AS frequency,
            ROUND(SUM(o.net_revenue)::numeric, 2) AS monetary_value
        FROM public.customers c
        JOIN public.orders o
        ON c.customer_id = o.customer_id
        WHERE o.order_status <> 'Cancelled'
        GROUP BY c.customer_id
    ),
    rfm_score AS (
        SELECT
            *,
            NTILE(5) OVER (ORDER BY recency_days ASC) AS recency_score,
            NTILE(5) OVER (ORDER BY frequency DESC) AS frequency_score,
            NTILE(5) OVER (ORDER BY monetary_value DESC) AS monetary_score
        FROM rfm_base
    )
    SELECT
        *,
        CASE
            WHEN recency_score >= 4 AND frequency_score >= 4 AND monetary_score >= 4 THEN 'Champions'
            WHEN frequency_score >= 4 AND monetary_score >= 4 THEN 'Loyal Customers'
            WHEN recency_score >= 4 AND frequency_score <= 2 THEN 'New Customers'
            WHEN recency_score <= 2 AND frequency_score >= 3 THEN 'At Risk'
            WHEN recency_score <= 2 AND frequency_score <= 2 THEN 'Lost Customers'
            ELSE 'Regular Customers'
        END AS rfm_segment
    FROM rfm_score
)
SELECT
    rfm_segment,
    COUNT(*) AS total_customers,
    ROUND(AVG(recency_days)::numeric, 2) AS avg_recency_days,       
    ROUND(AVG(frequency)::numeric, 2) AS avg_frequency,             
    ROUND(AVG(monetary_value)::numeric, 2) AS avg_customer_value,   
    ROUND(SUM(monetary_value)::numeric, 2) AS total_revenue         
FROM rfm_segments
GROUP BY rfm_segment
ORDER BY total_revenue DESC;



-- Cohort Retention Analysis


WITH first_purchase AS (
    SELECT
        customer_id,
        DATE_TRUNC('month', MIN(order_date))::DATE AS cohort_month
    FROM public.orders
    WHERE order_status <> 'Cancelled'
    GROUP BY customer_id
),
customer_orders AS (
    SELECT
        o.customer_id,
        fp.cohort_month,
        DATE_TRUNC('month', o.order_date)::DATE AS order_month,
        -- Fixed: Safer calendar month difference math bypassing the AGE() function interval trap
        (
            (EXTRACT(YEAR FROM DATE_TRUNC('month', o.order_date)) - EXTRACT(YEAR FROM fp.cohort_month)) * 12
            + (EXTRACT(MONTH FROM DATE_TRUNC('month', o.order_date)) - EXTRACT(MONTH FROM fp.cohort_month))
        )::INT AS month_number
    FROM public.orders o
    JOIN first_purchase fp
    ON o.customer_id = fp.customer_id
    WHERE o.order_status <> 'Cancelled'
),
cohort_size AS (
    SELECT
        cohort_month,
        COUNT(DISTINCT customer_id) AS total_customers
    FROM first_purchase
    GROUP BY cohort_month
)
SELECT
    co.cohort_month,
    co.month_number,
    COUNT(DISTINCT co.customer_id) AS active_customers,
    cs.total_customers AS cohort_size,
    ROUND(
        COUNT(DISTINCT co.customer_id)::NUMERIC
        / NULLIF(cs.total_customers, 0),
        4
    ) AS retention_rate
FROM customer_orders co
JOIN cohort_size cs
ON co.cohort_month = cs.cohort_month
GROUP BY co.cohort_month, co.month_number, cs.total_customers
ORDER BY co.cohort_month, co.month_number;