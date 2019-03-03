SELECT 
  line_item_product_code AS Service,
  product_from_location AS Region,
  SUM(line_item_unblended_cost) AS ExternalDataTransferCosts,
  COUNT(*) AS LineItemCount
FROM 
  "billingdata"."cur_formatted"
WHERE 
  product_product_family = 'Data Transfer'
  AND
  product_to_location = 'External' 
  AND 
  bill_payer_account_id = '252486826203'
  AND
  billing_period = from_iso8601_date('2018-10-01')
GROUP BY
  line_item_product_code,
  product_from_location
ORDER BY 
  ExternalDataTransferCosts,
  LineItemCount
DESC