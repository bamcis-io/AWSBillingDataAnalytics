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
        ("bill/invoiceid", "string", "bill_invoice_id", "string"), 
        ("bill/billtype", "string", "bill_bill_type", "string"), 
        ("bill/payeraccountid", "string", "bill_payer_account_id", "string"), 
        ("bill/billingperiodstartdate", "string", "bill_billing_period_start_date", "timestamp"), 
        ("bill/billingperiodenddate", "string", "bill_billing_period_end_date", "timestamp"), 
        ("lineitem/usageaccountid", "string", "line_item_usage_account_id", "string"), 
        ("lineitem/lineitemtype", "string", "line_item_line_item_type", "string"), 
        ("lineitem/usagestartdate", "string", "line_item_usage_start_date", "timestamp"), 
        ("lineitem/usageenddate", "string", "line_item_usage_end_date", "timestamp"), 
        ("lineitem/productcode", "string", "line_item_product_code", "string"), 
        ("lineitem/usagetype", "string", "line_item_usage_type", "string"), 
        ("lineitem/operation", "string", "line_item_operation", "string"), 
        ("lineitem/availabilityzone", "string", "line_item_availability_zone", "string"), 
        ("lineitem/resourceid", "string", "line_item_resource_id", "string"), 
        ("lineitem/usageamount", "string", "line_item_usage_amount", "double"), 
        ("lineitem/normalizationfactor", "string", "line_item_normalization_factor", "double"), 
        ("lineitem/normalizedusageamount", "string", "line_item_normalized_usage_amount", "double"),
        ("lineitem/unblendedrate", "string", "line_item_unblended_rate", "double"), 
        ("lineitem/unblendedcost", "string", "line_item_unblended_cost", "double"),
        ("lineitem/blendedrate", "string", "line_item_blended_rate", "double"), 
        ("lineitem/blendedcost", "string", "line_item_blended_cost", "double"), 
        ("lineitem/lineitemdescription", "string", "line_item_line_item_description", "string"), 
        ("product/productname", "string", "product_product_name", "string"), 
        ("product/description", "string", "product_description", "string"), 
        ("product/fromlocation", "string", "product_from_location", "string"), 
        ("product/fromlocationtype", "string", "product_from_location_type", "string"), 
        ("product/instancetype", "string", "product_instance_type", "string"), 
        ("product/operatingsystem", "string", "product_operating_system", "string"), 
        ("product/location", "string", "product_location", "string"), 
        ("product/locationtype", "string", "product_location_type", "string"), 
        ("product/memory", "string", "product_memory", "string"), 
        ("product/operation", "string", "product_operation", "string"), 
        ("product/preinstalledsw", "string", "product_pre_installed_sw", "string"), 
        ("product/productfamily", "string", "product_product_family", "string"), 
        ("product/region", "string", "product_region", "string"), 
        ("product/servicecode", "string", "product_service_code", "string"), 
        ("product/servicename", "string", "product_service_name", "string"), 
        ("product/sku", "string", "product_sku", "string"), 
        ("product/tenancy", "string", "product_tenancy", "string"),
        ("product/tolocation", "string", "product_to_location", "string"), 
        ("product/tolocationtype", "string", "product_to_location_type", "string"), 
        ("product/transfertype", "string", "product_transfer_type", "string"), 
        ("product/usagetype", "string", "product_usage_type", "string"),
        ("product/vcpu", "string", "product_vcpu", "bigint"),
        ("product/version", "string", "product_version", "string"), 
        ("pricing/publicondemandcost", "string", "pricing_public_on_demand_cost", "double"), 
        ("pricing/publicondemandrate", "string", "pricing_public_on_demand_rate", "double"), 
        ("pricing/term", "string", "pricing_term", "string"),
        ("pricing/unit", "string", "pricing_unit", "string"), 
        ("reservation/amortizedupfrontcostforusage", "string", "reservation_amortized_upfront_cost_for_usage", "double"),
        ("reservation/amortizedupfrontfeeforbillingperiod", "string", "reservation_amortized_upfront_fee_for_billing_period", "double"),
        ("reservation/effectivecost", "string", "reservation_effective_cost", "double"), 
        ("reservation/endtime", "string", "reservation_end_time", "timestamp"), 
        ("reservation/modificationstatus", "string", "reservation_modification_status", "string"), 
        ("reservation_normalizedunitsperreservation", "string", "reservation_normalized_units_per_reservation", "bigint"), 
        ("reservation_recurringfeeforusage", "string", "reservation_recurring_fee_for_usage", "double"), 
        ("reservation/starttime", "string", "reservation_start_time", "timestamp"), 
        ("reservation/totalreservednormalizedunits", "string", "reservation_total_reserved_normalized_units", "bigint"), 
        ("reservation/totalreservedunits", "string", "reservation_total_reserved_units", "bigint"), 
        ("reservation/unitsperreservation", "string", "reservation_units_per_reservation", "bigint"), 
        ("reservation/unusedamortizedupfrontfeeforbillingperiod", "string", "reservation_unused_amortized_upfront_fee_for_billing_period", "double"), 
        ("reservation/unusednormalizedunitquantity", "string", "reservation_unused_normalized_unit_quantity", "bigint"), 
        ("reservation/unusedquantity", "string", "reservation_unused_quantity", "double"), 
        ("reservation/unusedrecurringfee", "string", "reservation_unused_recurring_fee", "double"), 
        ("reservation/upfrontvalue", "string", "reservation_upfront_value", "double")
    ],
    transformation_ctx = "applymapping1"
)

mapped_df = applymapping1.toDF()
mapped_df = mapped_df.withColumn("billing_period", mapped_df.bill_billing_period_start_date.cast("date"))

# Delete all the matching s3 objects
s3 = boto3.resource("s3")

s3_args = {
    'Bucket' : args['destination_bucket'], 
    'Prefix' : 'bill_payer_account_id=%s/billing_period=%s/' % (mapped_df.first()['bill_payer_account_id'], mapped_df.first()['bill_billing_period_start_date'].date())    
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
            "bill_payer_account_id",
            "billing_period",
            "line_item_usage_account_id",
            "product_region"
        ]
    }, 
    format = "parquet", 
    transformation_ctx = "datasink2"
)

job.commit()