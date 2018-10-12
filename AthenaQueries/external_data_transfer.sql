SELECT 
  lineitem_productcode AS Service,
  product_fromlocation AS Region,
  SUM(lineitem_unblendedcost) AS ExternalDataTransferCosts,
  COUNT(*) AS LineItemCount
FROM 
  "billingdata"."cur_formatted"
WHERE 
  product_productfamily = 'Data Transfer'
  AND
  product_tolocation = 'External' 
  AND 
  bill_payeraccountid = '252486826203'
  AND
  billingperiod = from_iso8601_date('2018-10-01')
GROUP BY
  lineitem_productcode,
  product_fromlocation
ORDER BY 
  ExternalDataTransferCosts,
  LineItemCount
DESC