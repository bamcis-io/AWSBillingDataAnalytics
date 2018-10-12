WITH
  resource_vcpu
AS
(
SELECT 
  lineitem_resourceid,
  lineitem_productcode,
  product_region,
  MAX(product_vcpu) AS vCPU
FROM 
  "billingdata"."cur_formatted"
WHERE 
  product_vcpu IS NOT NULL 
  AND
  product_vcpu != 0
  AND
  bill_payeraccountid = '252486826203'
  AND
  billingperiod = from_iso8601_date('2018-10-01')
GROUP BY
  lineitem_resourceid,
  lineitem_productcode,
  product_region
)

SELECT 
  lineitem_productcode As Service,
  product_region AS Region,
  SUM(vCPU) as vCPUs
FROM 
  resource_vcpu
GROUP BY
  lineitem_productcode,
  product_region