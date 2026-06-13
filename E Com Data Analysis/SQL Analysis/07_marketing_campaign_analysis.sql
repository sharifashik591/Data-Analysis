-- 07 Marketing Campaign Analysis
SELECT
    campaign_id,
    campaign_name,
    channel,
    campaign_cost,
    impressions,
    clicks,
    conversions,
    revenue_generated,
    -- Fixed: Added numeric cast for ROAS calculation
    ROUND((revenue_generated / NULLIF(campaign_cost, 0))::numeric, 2) AS roas,
    -- Kept your excellent fix here:
    ROUND(conversions::numeric / NULLIF(clicks, 0), 4) AS conversion_rate,
    -- Fixed: Added numeric cast for CPA calculation
    ROUND((campaign_cost / NULLIF(conversions, 0))::numeric, 2) AS cost_per_acquisition
FROM public.marketing_campaigns
ORDER BY roas DESC;


-- Campaign Actual Revenue from Orders 
SELECT
    mc.campaign_id,
    mc.campaign_name,
    mc.channel,
    mc.campaign_cost,
    COUNT(DISTINCT o.order_id) AS actual_orders,
    ROUND(SUM(o.net_revenue)::numeric, 2) AS actual_revenue, -- Fixed cast
    ROUND(SUM(o.net_profit)::numeric, 2) AS actual_profit,   -- Fixed cast
    -- Fixed: Aggregated revenue divided by cost, cast to numeric for rounding
    ROUND((SUM(o.net_revenue) / NULLIF(mc.campaign_cost, 0))::numeric, 2) AS actual_roas
FROM public.marketing_campaigns mc
LEFT JOIN public.orders o
ON mc.campaign_id = o.campaign_id
AND o.order_status <> 'Cancelled'
GROUP BY
    mc.campaign_id,
    mc.campaign_name,
    mc.channel,
    mc.campaign_cost
ORDER BY actual_roas DESC;
