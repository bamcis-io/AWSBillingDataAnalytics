SELECT 
    instancetype,
    vcpu,
    memory,
    region,
    upfrontfee,
    adjustedpriceperunit AS recurringhourlyfee,
    ondemandhourlycost,
    reservedinstancecost,
    percentsavings,
    leaseterm,
    purchaseoption,
    offeringclass
FROM 
    "billingdata"."reservedinstancepricelistdata" AS p 
WHERE 
    p.service = 'AmazonEC2' AND 
    p.operatingsystem = 'Linux' AND
    p.tenancy = 'Shared' AND
    p.offeringclass = 'STANDARD' AND
    p.leaseterm = 1 AND
    p.vcpu >= 8 AND
    p.memory >= 32 AND
    p.instancetype NOT LIKE 't2%' AND
    p.region NOT LIKE 'us-gov%'
    
ORDER BY 
    p.reservedinstancecost ASC,
    p.percentsavings DESC