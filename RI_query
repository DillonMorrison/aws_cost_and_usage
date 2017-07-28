SELECT 
	DATE_FORMAT(from_iso8601_timestamp(cost_and_usage.lineitem_usagestartdate),'%Y-%m') AS "cost_and_usage.usage_start_month",
	COALESCE(SUM(cost_and_usage.lineitem_blendedcost ), 0) AS "cost_and_usage.total_blended_cost",
	COALESCE(SUM(CASE WHEN (CASE
         WHEN cost_and_usage.lineitem_lineitemtype = 'DiscountedUsage' THEN 'RI Line Item'
         WHEN cost_and_usage.lineitem_lineitemtype = 'RIFee' THEN 'RI Line Item'
         WHEN cost_and_usage.lineitem_lineitemtype = 'Fee' THEN 'RI Line Item'
         ELSE 'Non RI Line Item'
        END = 'RI Line Item') THEN cost_and_usage.lineitem_blendedcost  ELSE NULL END), 0) AS "cost_and_usage.total_reserved_blended_cost",
	1.0 * (COALESCE(SUM(CASE WHEN (CASE
         WHEN cost_and_usage.lineitem_lineitemtype = 'DiscountedUsage' THEN 'RI Line Item'
         WHEN cost_and_usage.lineitem_lineitemtype = 'RIFee' THEN 'RI Line Item'
         WHEN cost_and_usage.lineitem_lineitemtype = 'Fee' THEN 'RI Line Item'
         ELSE 'Non RI Line Item'
        END = 'RI Line Item') THEN cost_and_usage.lineitem_blendedcost  ELSE NULL END), 0)) / NULLIF((COALESCE(SUM(cost_and_usage.lineitem_blendedcost ), 0)),0)  AS "cost_and_usage.percent_spend_on_ris",
	COALESCE(SUM(CASE WHEN (CASE
         WHEN cost_and_usage.lineitem_lineitemtype = 'DiscountedUsage' THEN 'RI Line Item'
         WHEN cost_and_usage.lineitem_lineitemtype = 'RIFee' THEN 'RI Line Item'
         WHEN cost_and_usage.lineitem_lineitemtype = 'Fee' THEN 'RI Line Item'
         ELSE 'Non RI Line Item'
        END = 'Non RI Line Item') THEN cost_and_usage.lineitem_blendedcost  ELSE NULL END), 0) AS "cost_and_usage.total_non_reserved_blended_cost",
	1.0 * (COALESCE(SUM(CASE WHEN (CASE
         WHEN cost_and_usage.lineitem_lineitemtype = 'DiscountedUsage' THEN 'RI Line Item'
         WHEN cost_and_usage.lineitem_lineitemtype = 'RIFee' THEN 'RI Line Item'
         WHEN cost_and_usage.lineitem_lineitemtype = 'Fee' THEN 'RI Line Item'
         ELSE 'Non RI Line Item'
        END = 'Non RI Line Item') THEN cost_and_usage.lineitem_blendedcost  ELSE NULL END), 0)) / NULLIF((COALESCE(SUM(cost_and_usage.lineitem_blendedcost ), 0)),0)  AS "cost_and_usage.percent_spend_on_non_ris"
FROM aws_optimizer.cost_and_usage_raw  AS cost_and_usage

WHERE 
	(((from_iso8601_timestamp(cost_and_usage.lineitem_usagestartdate)) >= ((DATE_ADD('month', -5, DATE_TRUNC('MONTH', CAST(NOW() AS DATE))))) AND (from_iso8601_timestamp(cost_and_usage.lineitem_usagestartdate)) < ((DATE_ADD('month', 6, DATE_ADD('month', -5, DATE_TRUNC('MONTH', CAST(NOW() AS DATE))))))))
GROUP BY 1
ORDER BY 2 DESC
LIMIT 500
