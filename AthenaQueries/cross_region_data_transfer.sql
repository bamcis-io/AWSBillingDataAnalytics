SELECT 
  lineitem_productcode AS Service,
  product_fromlocation AS "From",
  product_tolocation AS "To",
  SUM(lineitem_unblendedcost) AS InterRegionDataTransferCosts,
  COUNT(*) AS LineItemCount
FROM 
  "billingdata"."cur_formatted"
WHERE 
  product_productfamily = 'Data Transfer'
  AND
  product_tolocation != product_fromlocation 
  AND 
  product_tolocation != 'External'
  AND
  product_fromlocation != 'External'
  AND
  lineitem_usagestartdate >= from_iso8601_date('2018-10-01')
  AND
  lineitem_usagestartdate < from_iso8601_date('2018-10-12')
  AND
  bill_payeraccountid = '252486826203'
GROUP BY
  lineitem_productcode,
  product_tolocation,
  product_fromlocation
ORDER BY 
  InterRegionDataTransferCosts,
  LineItemCount
DESC