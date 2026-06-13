-- 10 Inventory Analysis
SELECT
    p.product_id,
    p.product_name,
    p.category,
    i.warehouse_location,
    i.beginning_stock,
    i.units_sold,
    i.units_returned,
    i.units_restocked,
    i.ending_stock,
    i.stockout_risk,
    ROUND(
        i.units_sold::NUMERIC
        / NULLIF(i.beginning_stock + i.units_restocked, 0),
        4
    ) AS inventory_turnover_rate
FROM public.inventory i
JOIN public.products p
ON i.product_id = p.product_id
ORDER BY inventory_turnover_rate DESC;




-- High Demand but Low Stock Products

SELECT
    p.product_id,
    p.product_name,
    p.category,
    i.units_sold,
    i.ending_stock,
    i.stockout_risk,
    ROUND(
        i.units_sold::NUMERIC
        / NULLIF(i.ending_stock, 0),
        2
    ) AS demand_to_stock_ratio
FROM public.inventory i
JOIN public.products p
ON i.product_id = p.product_id
WHERE i.ending_stock > 0
ORDER BY demand_to_stock_ratio DESC
LIMIT 20;

