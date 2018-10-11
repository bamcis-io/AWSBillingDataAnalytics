SELECT 
    instancetype,
    region,
    upfrontfee,
    adjustedpriceperunit AS recurringhourlyfee,
    ondemandhourlycost,
    reservedinstancecost,
    leaseterm,
    purchaseoption,
    offeringclass,
    percentsavings
FROM 
    "billingdata"."reservedinstancepricelistdata" AS p 
WHERE 
    p.instancetype = 'p2.8xlarge' AND 
    p.service = 'AmazonEC2' AND 
    p.platform = 'Windows with SQL Ent' AND
    p.tenancy = 'Shared' AND
    p.offeringclass = 'STANDARD' AND
    p.leaseterm = 1
    
ORDER BY 
    p.reservedinstancecost ASC,
    p.percentsavings DESC