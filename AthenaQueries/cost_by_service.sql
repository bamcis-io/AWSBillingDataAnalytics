SELECT 
  IF (MAX(product_service_name) IS NOT NULL AND MAX(product_service_name) != '', MAX(product_service_name), line_item_product_code) AS Service,
  SUM(line_item_unblended_cost) AS Cost
FROM 
  "billingdata"."cur_formatted"
WHERE 
  line_item_usage_start_date >= from_iso8601_date('2018-10-01')
  AND
  line_item_usage_start_date < from_iso8601_date('2018-10-15')
  AND
  bill_payer_account_id = '252486826203'
GROUP BY
  line_item_product_code
ORDER BY 
  Cost
DESC