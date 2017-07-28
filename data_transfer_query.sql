SELECT 
	cost_and_usage.lineitem_productcode  AS "cost_and_usage.product_code",
	COALESCE(SUM(cost_and_usage.lineitem_usageamount ), 0) AS "cost_and_usage.total_usage_amount",
	COALESCE(SUM(CASE WHEN REGEXP_LIKE(cost_and_usage.product_usagetype, 'DataTransfer')    THEN cost_and_usage.lineitem_blendedcost  ELSE NULL END), 0) AS "cost_and_usage.total_data_transfer_cost",
	COALESCE(SUM(CASE WHEN REGEXP_LIKE(cost_and_usage.product_usagetype, 'DataTransfer-In')    THEN cost_and_usage.lineitem_blendedcost  ELSE NULL END), 0) AS "cost_and_usage.total_inbound_data_transfer_cost",
	COALESCE(SUM(CASE WHEN REGEXP_LIKE(cost_and_usage.product_usagetype, 'DataTransfer-Out')    THEN cost_and_usage.lineitem_blendedcost  ELSE NULL END), 0) AS "cost_and_usage.total_outbound_data_transfer_cost"
FROM aws_optimizer.cost_and_usage_raw  AS cost_and_usage

WHERE 
	(((from_iso8601_timestamp(cost_and_usage.lineitem_usagestartdate)) >= ((DATE_ADD('month', -5, DATE_TRUNC('MONTH', CAST(NOW() AS DATE))))) AND (from_iso8601_timestamp(cost_and_usage.lineitem_usagestartdate)) < ((DATE_ADD('month', 6, DATE_ADD('month', -5, DATE_TRUNC('MONTH', CAST(NOW() AS DATE))))))))
GROUP BY 1
ORDER BY 3 DESC
LIMIT 500
