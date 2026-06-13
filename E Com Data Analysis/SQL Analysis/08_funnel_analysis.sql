-- 08 Funnel Analysis
SELECT
    traffic_source,
    device_type,
    COUNT(DISTINCT session_id) AS total_sessions,
    COUNT(DISTINCT CASE WHEN added_to_cart = 'Yes' THEN session_id END) AS cart_sessions,
    COUNT(DISTINCT CASE WHEN checkout_started = 'Yes' THEN session_id END) AS checkout_sessions,
    COUNT(DISTINCT CASE WHEN order_placed = 'Yes' THEN session_id END) AS order_sessions,
    ROUND(
        COUNT(DISTINCT CASE WHEN added_to_cart = 'Yes' THEN session_id END)::NUMERIC
        / NULLIF(COUNT(DISTINCT session_id), 0),
        4
    ) AS cart_rate,
    ROUND(
        COUNT(DISTINCT CASE WHEN checkout_started = 'Yes' THEN session_id END)::NUMERIC
        / NULLIF(COUNT(DISTINCT CASE WHEN added_to_cart = 'Yes' THEN session_id END), 0),
        4
    ) AS checkout_rate,
    ROUND(
        COUNT(DISTINCT CASE WHEN order_placed = 'Yes' THEN session_id END)::NUMERIC
        / NULLIF(COUNT(DISTINCT session_id), 0),
        4
    ) AS conversion_rate
FROM public.sessions
GROUP BY traffic_source, device_type
ORDER BY conversion_rate DESC;


-- Cart Abandonment Analysis
SELECT
    traffic_source,
    device_type,
    COUNT(*) AS total_sessions,
    COUNT(*) FILTER (WHERE added_to_cart = 'Yes') AS added_to_cart_sessions,
    COUNT(*) FILTER (
        WHERE added_to_cart = 'Yes'
        AND order_placed = 'No'
    ) AS cart_abandoned_sessions,
    ROUND(
        COUNT(*) FILTER (
            WHERE added_to_cart = 'Yes'
            AND order_placed = 'No'
        )::NUMERIC
        / NULLIF(COUNT(*) FILTER (WHERE added_to_cart = 'Yes'), 0),
        4
    ) AS cart_abandonment_rate
FROM public.sessions
GROUP BY traffic_source, device_type
ORDER BY cart_abandonment_rate DESC;