SELECT 
  line_item_product_code AS Service,
  product_from_location AS "From",
  product_to_location AS "To",
  SUM(line_item_unblended_cost) AS InterRegionDataTransferCosts,
  COUNT(*) AS LineItemCount
FROM 
  "billingdata"."cur_formatted"
WHERE 
  product_product_family = 'Data Transfer'
  AND
  product_to_location != product_from_location 
  AND 
  product_to_location != 'External'
  AND
  product_from_location != 'External'
  AND
  line_item_usage_start_date >= from_iso8601_date('2018-10-01')
  AND
  line_item_usage_start_date < from_iso8601_date('2018-10-12')
  AND
  bill_payer_account_id = '252486826203'
GROUP BY
  line_item_product_code,
  product_to_location,
  product_from_location
ORDER BY 
  InterRegionDataTransferCosts,
  LineItemCount
DESC