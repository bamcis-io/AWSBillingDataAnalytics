import sys
import boto3
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job


## Use this ETL job when the source files from the CUR are already in parquet format. AWS changed the
## column names for CUR files delivered in Parquet

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
        ("bill_invoice_id", "string", "bill_invoice_id", "string"), 
        ("bill_bill_type", "string", "bill_bill_type", "string"), 
        ("bill_payer_account_id", "string", "bill_payer_account_id", "string"), 
        ("bill_billing_period_start_date", "timestamp", "bill_billing_period_start_date", "timestamp"), 
        ("bill_billing_period_end_date", "timestamp", "bill_billing_period_end_date", "timestamp"), 
        ("line_item_usage_account_id", "string", "line_item_usage_account_id", "string"), 
        ("line_item_line_item_type", "string", "line_item_line_item_type", "string"), 
        ("line_item_usage_start_date", "timestamp", "line_item_usage_start_date", "timestamp"), 
        ("line_item_usage_end_date", "timestamp", "line_item_usage_end_date", "timestamp"), 
        ("line_item_product_code", "string", "line_item_product_code", "string"), 
        ("line_item_usage_type", "string", "line_item_usage_type", "string"), 
        ("line_item_operation", "string", "line_item_operation", "string"), 
        ("line_item_availability_zone", "string", "line_item_availability_zone", "string"), 
        ("line_item_resource_id", "string", "line_item_resource_id", "string"), 
        ("line_item_usage_amount", "double", "line_item_usage_amount", "double"), 
        ("line_item_normalization_factor", "double", "line_item_normalization_factor", "double"), 
        ("line_item_normalized_usage_amount", "double", "line_item_normalized_usage_amount", "double"),
        ("line_item_unblended_rate", "string", "line_item_unblended_rate", "double"), 
        ("line_item_unblended_cost", "double", "line_item_unblended_cost", "double"),
        ("line_item_blended_rate", "string", "line_item_blended_rate", "double"), 
        ("line_item_blended_cost", "double", "line_item_blended_cost", "double"), 
        ("line_item_line_item_description", "string", "line_item_line_item_description", "string"), 
        ("product_product_name", "string", "product_product_name", "string"), 
        ("product_description", "string", "product_description", "string"), 
        ("product_from_location", "string", "product_from_location", "string"), 
        ("product_from_location_type", "string", "product_from_location_type", "string"), 
        ("product_instance_type", "string", "product_instance_type", "string"), 
        ("product_operating_system", "string", "product_operating_system", "string"), 
        ("product_location", "string", "product_location", "string"), 
        ("product_location_type", "string", "product_location_type", "string"), 
        ("product_memory", "string", "product_memory", "string"), 
        ("product_operation", "string", "product_operation", "string"), 
        ("product_pre_installed_sw", "string", "product_pre_installed_sw", "string"), 
        ("product_product_family", "string", "product_product_family", "string"), 
        ("product_region", "string", "product_region", "string"), 
        ("product_servicecode", "string", "product_service_code", "string"), 
        ("product_servicename", "string", "product_service_name", "string"), 
        ("product_sku", "string", "product_sku", "string"), 
        ("product_tenancy", "string", "product_tenancy", "string"),
        ("product_to_location", "string", "product_to_location", "string"), 
        ("product_to_location_type", "string", "product_to_location_type", "string"), 
        ("product_transfer_type", "string", "product_transfer_type", "string"), 
        ("product_usage_type", "string", "product_usage_type", "string"),
        ("product_vcpu", "string", "product_vcpu", "bigint"),
        ("product_version", "string", "product_version", "string"), 
        ("pricing_public_on_demand_cost", "double", "pricing_public_on_demand_cost", "double"), 
        ("pricing_public_on_demand_rate", "string", "pricing_public_on_demand_rate", "double"), 
        ("pricing_term", "string", "pricing_term", "string"),
        ("pricing_unit", "string", "pricing_unit", "string"), 
        ("reservation_amortized_upfront_cost_for_usage", "double", "reservation_amortized_upfront_cost_for_usage", "double"),
        ("reservation_amortized_upfront_fee_for_billing_period", "double", "reservation_amortized_upfront_fee_for_billing_period", "double"),
        ("reservation_effective_cost", "double", "reservation_effective_cost", "double"), 
        ("reservation_end_time", "string", "reservation_endtime", "timestamp"), 
        ("reservation_modification_status", "string", "reservation_modification_status", "string"), 
        ("reservation_normalized_units_per_reservation", "string", "reservation_normalized_units_per_reservation", "bigint"), 
        ("reservation_recurring_fee_for_usage", "double", "reservation_recurring_fee_for_usage", "double"), 
        ("reservation_start_time", "string", "reservation_start_time", "timestamp"), 
        ("reservation_total_reserved_normalized_units", "string", "reservation_total_reserved_normalized_units", "bigint"), 
        ("reservation_total_reserved_units", "string", "reservation_total_reserved_units", "bigint"), 
        ("reservation_units_per_reservation", "string", "reservation_units_per_reservation", "bigint"), 
        ("reservation_unused_amortized_upfront_fee_for_billing_period", "double", "reservation_unused_amortized_upfront_fee_for_billing_period", "double"), 
        ("reservation_unused_normalized_unit_quantity", "double", "reservation_unused_normalized_unit_quantity", "bigint"), 
        ("reservation_unused_quantity", "double", "reservation_unused_quantity", "double"), 
        ("reservation_unused_recurring_fee", "double", "reservation_unused_recurring_fee", "double"), 
        ("reservation_upfront_value", "double", "reservation_upfront_value", "double")
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