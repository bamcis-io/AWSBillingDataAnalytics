SELECT 
  IF (MAX(product_servicename) IS NOT NULL AND MAX(product_servicename) != '', MAX(product_servicename), lineitem_productcode) AS Service,
  SUM(lineitem_unblendedcost) AS Cost
FROM 
  "billingdata"."cur_formatted"
WHERE 
  lineitem_usagestartdate >= from_iso8601_date('2018-10-01')
  AND
  lineitem_usagestartdate < from_iso8601_date('2018-10-15')
  AND
  bill_payeraccountid = '252486826203'
GROUP BY
  lineitem_productcode
ORDER BY 
  Cost
DESC