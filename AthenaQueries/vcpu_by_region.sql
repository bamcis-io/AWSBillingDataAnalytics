WITH
  resource_vcpu
AS
(
SELECT 
  line_item_resource_id,
  line_item_product_code,
  product_region,
  MAX(product_vcpu) AS vCPU
FROM 
  "billingdata"."cur_formatted"
WHERE 
  product_vcpu IS NOT NULL 
  AND
  product_vcpu != 0
  AND
  bill_payer_account_id = '252486826203'
  AND
  billing_period = from_iso8601_date('2018-10-01')
GROUP BY
  line_item_resource_id,
  line_item_product_code,
  product_region
)

SELECT 
  line_item_product_code As Service,
  product_region AS Region,
  SUM(vCPU) as vCPUs
FROM 
  resource_vcpu
GROUP BY
  line_item_product_code,
  product_region