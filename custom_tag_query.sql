SELECT * FROM (
SELECT *, DENSE_RANK() OVER (ORDER BY z___min_rank) as z___pivot_row_rank, RANK() OVER (PARTITION BY z__pivot_col_rank ORDER BY z___min_rank) as z__pivot_col_ordering FROM (
SELECT *, MIN(z___rank) OVER (PARTITION BY "cost_and_usage.product_code") as z___min_rank FROM (
SELECT *, RANK() OVER (ORDER BY CASE WHEN z__pivot_col_rank=1 THEN (CASE WHEN "cost_and_usage.total_blended_cost" IS NOT NULL THEN 0 ELSE 1 END) ELSE 2 END, CASE WHEN z__pivot_col_rank=1 THEN "cost_and_usage.total_blended_cost" ELSE NULL END DESC, "cost_and_usage.total_blended_cost" DESC, z__pivot_col_rank, "cost_and_usage.product_code") AS z___rank FROM (
SELECT *, DENSE_RANK() OVER (ORDER BY CASE WHEN "cost_and_usage.user_cost_category" IS NULL THEN 1 ELSE 0 END, "cost_and_usage.user_cost_category") AS z__pivot_col_rank FROM (
SELECT 
	cost_and_usage.lineitem_productcode  AS "cost_and_usage.product_code",
	cost_and_usage.resourcetags_customersegment  AS "cost_and_usage.customer_segment",
	COALESCE(SUM(CASE WHEN (CASE
         WHEN cost_and_usage.lineitem_lineitemtype = 'DiscountedUsage' THEN 'RI Line Item'
         WHEN cost_and_usage.lineitem_lineitemtype = 'RIFee' THEN 'RI Line Item'
         WHEN cost_and_usage.lineitem_lineitemtype = 'Fee' THEN 'RI Line Item'
         ELSE 'Non RI Line Item'
        END = 'RI Line Item') THEN cost_and_usage.lineitem_blendedcost +10 ELSE NULL END), 0) AS "cost_and_usage.total_reserved_blended_cost",
	COALESCE(SUM(CASE WHEN REGEXP_LIKE(cost_and_usage.product_usagetype, 'DataTransfer')    THEN cost_and_usage.lineitem_blendedcost + 5  ELSE NULL END), 0) AS "cost_and_usage.total_data_transfer_cost",
	COALESCE(SUM(cost_and_usage.lineitem_blendedcost + 30 ), 0) AS "cost_and_usage.total_blended_cost"
FROM aws_optimizer.cost_and_usage_raw  AS cost_and_usage

WHERE 
	(((from_iso8601_timestamp(cost_and_usage.lineitem_usagestartdate)) >= ((DATE_ADD('day', -29, CAST(NOW() AS DATE)))) AND (from_iso8601_timestamp(cost_and_usage.lineitem_usagestartdate)) < ((DATE_ADD('day', 30, DATE_ADD('day', -29, CAST(NOW() AS DATE)))))))
GROUP BY 1,2) ww
) bb WHERE z__pivot_col_rank <= 16384
) aa
) xx
) zz
 WHERE z___pivot_row_rank <= 500 OR z__pivot_col_ordering = 1 ORDER BY z___pivot_row_rank
