import sys
import boto3
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

## @params: [JOB_NAME, file_path]
args = getResolvedOptions(sys.argv, [
		'JOB_NAME',
        'table',
        'database',
        'destination_bucket'
	]
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Data Catalog: database and table name
db_name = args['database']
tbl_name = args['table']

# S3 Output Path
output_path = "s3://" + args['destination_bucket']

# Get the source data
datasource0 = glueContext.create_dynamic_frame.from_catalog(
    database = db_name, 
    table_name = tbl_name, 
    transformation_ctx = "datasource0"
)

# Map the desired fields to new field names
applymapping1 = ApplyMapping.apply(
    frame = datasource0, 
    mappings = [
        ("bill/invoiceid", "string", "bill_invoiceid", "string"), 
        ("bill/billtype", "string", "bill_billtype", "string"), 
        ("bill/payeraccountid", "string", "bill_payeraccountid", "string"), 
        ("bill/billingperiodstartdate", "string", "bill_billingperiodstartdate", "timestamp"), 
        ("bill/billingperiodenddate", "string", "bill_billingperiodenddate", "timestamp"), 
        ("lineitem/usageaccountid", "string", "lineitem_usageaccountid", "string"), 
        ("lineitem/lineitemtype", "string", "lineitem_lineitemtype", "string"), 
        ("lineitem/usagestartdate", "string", "lineitem_usagestartdate", "timestamp"), 
        ("lineitem/usageenddate", "string", "lineitem_usageenddate", "timestamp"), 
        ("lineitem/productcode", "string", "lineitem_productcode", "string"), 
        ("lineitem/usagetype", "string", "lineitem_usagetype", "string"), 
        ("lineitem/operation", "string", "lineitem_operation", "string"), 
        ("lineitem/availabilityzone", "string", "lineitem_availabilityzone", "string"), 
        ("lineitem/resourceid", "string", "lineitem_resourceid", "string"), 
        ("lineitem/usageamount", "string", "lineitem_usageamount", "double"), 
        ("lineitem/normalizationfactor", "string", "lineitem_normalizationfactor", "bigint"), 
        ("lineitem/normalizedusageamount", "string", "lineitem_normalizedusageamount", "double"),
        ("lineitem/unblendedrate", "string", "lineitem_unblendedrate", "double"), 
        ("lineitem/unblendedcost", "string", "lineitem_unblendedcost", "double"),
        ("lineitem/blendedrate", "string", "lineitem_blendedrate", "double"), 
        ("lineitem/blendedcost", "string", "lineitem_blendedcost", "double"), 
        ("lineitem/lineitemdescription", "string", "lineitem_lineitemdescription", "string"), 
        ("product/productname", "string", "product_productname", "string"), 
        ("product/description", "string", "product_description", "string"), 
        ("product/instancetype", "string", "product_instancetype", "string"), 
        ("product/operatingsystem", "string", "product_operatingsystem", "string"), 
        ("product/location", "string", "product_location", "string"), 
        ("product/locationtype", "string", "product_locationtype", "string"), 
        ("product/operation", "string", "product_operation", "string"), 
        ("product/productfamily", "string", "product_productfamily", "string"), 
        ("product/region", "string", "product_region", "string"), 
        ("product/servicecode", "string", "product_servicecode", "string"), 
        ("product/servicename", "string", "product_servicename", "string"), 
        ("product/sku", "string", "product_sku", "string"), 
        ("product/tenancy", "string", "product_tenancy", "string"),
        ("product/transfertype", "string", "product_transfertype", "string"), 
        ("product/usagetype", "string", "product_usagetype", "string"), 
        ("product/version", "string", "product_version", "string"), 
        ("pricing/publicondemandcost", "string", "pricing_publicondemandcost", "double"), 
        ("pricing/publicondemandrate", "string", "pricing_publicondemandrate", "double"), 
        ("pricing/term", "string", "pricing_term", "string"),
        ("pricing/unit", "string", "pricing_unit", "string"), 
        ("reservation/amortizedupfrontcostforusage", "string", "reservation_amortizedupfrontcostforusage", "double"),
        ("reservation/amortizedupfrontfeeforbillingperiod", "string", "reservation_amortizedupfrontfeeforbillingperiod", "double"),
        ("reservation/effectivecost", "string", "reservation_effectivecost", "double"), 
        ("reservation/endtime", "string", "reservation_endtime", "timestamp"), 
        ("reservation/modificationstatus", "string", "reservation_modificationstatus", "string"), 
        ("reservation_normalizedunitsperreservation", "string", "reservation_normalizedunitsperreservation", "bigint"), 
        ("reservation_recurringfeeforusage", "string", "reservation_recurringfeeforusage", "double"), 
        ("reservation/starttime", "string", "reservation_starttime", "timestamp"), 
        ("reservation/totalreservednormalizedunits", "string", "reservation_totalreservednormalizedunits", "bigint"), 
        ("reservation/totalreservedunits", "string", "reservation_totalreservedunits", "bigint"), 
        ("reservation/unitsperreservation", "string", "reservation_unitsperreservation", "bigint"), 
        ("reservation/unusedamortizedupfrontfeeforbillingperiod", "string", "reservation_unusedamortizedupfrontfeeforbillingperiod", "double"), 
        ("reservation/unusednormalizedunitquantity", "string", "reservation_unusednormalizedunitquantity", "bigint"), 
        ("reservation/unusedquantity", "string", "reservation_unusedquantity", "double"), 
        ("reservation/unusedrecurringfee", "string", "reservation_unusedrecurringfee", "double"), 
        ("reservation/upfrontvalue", "string", "reservation_upfrontvalue", "double")
    ],
    transformation_ctx = "applymapping1"
)

mapped_df = applymapping1.toDF()
mapped_df = mapped_df.withColumn("billingperiod", mapped_df.bill_billingperiodstartdate.cast("date"))

# Delete all the matching s3 objects
s3 = boto3.resource("s3")

s3_args = {
    'Bucket' : args['destination_bucket'], 
    'Prefix' : 'bill_payeraccountid=%s/billingperiod=%s/' % (mapped_df.first()['bill_payeraccountid'], mapped_df.first()['bill_billingperiodstartdate'].date())    
}

while True:
    try:
        response = s3.meta.client.list_objects_v2(**s3_args)
        if 'Contents' in response and response['Contents'] is not None:
            delete_keys = { 'Objects': [] }
            delete_keys['Objects'] = [ {'Key' : key } for key in [s3_object['Key'] for s3_object in response.get('Contents', [])]]
            
            print("[INFO] DELETING THE FOLLOWING KEYS:")
            print('\n'.join(obj['Key'] for obj in delete_keys['Objects']))
            
            s3.meta.client.delete_objects(Bucket = args['destination_bucket'], Delete = delete_keys)
            if 'NextContinuationToken' in response and response['NextContinuationToken'] is not None and response['NextContinuationToken'] != '':
                s3_args['ContinuationToken'] = response['NextContinuationToken']
            else:
                break
        else:
            print("[WARNING] NO CONTENTS RETURNED FROM S3 LIST_OBJECTS_V2 REQUEST")
            break
    except Exception as e:
        print("[ERROR] An EXCEPTION occured: ", sys.exc_info()[0], " : ", str(e))  
        break

# Convert data frame back to dynamic frame
applymapping1 = DynamicFrame.fromDF(mapped_df, glueContext, "applymapping1")

# Save data to S3
datasink2 = glueContext.write_dynamic_frame.from_options(
    frame = applymapping1, 
    connection_type = "s3", 
    connection_options = {
        "path": output_path,
        "partitionKeys": [
            "bill_payeraccountid",
            "billingperiod",
            "lineitem_usageaccountid",
            "product_region"
        ]
    }, 
    format = "parquet", 
    transformation_ctx = "datasink2"
)

job.commit()