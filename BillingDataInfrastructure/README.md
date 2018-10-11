# Billing Data Infrastructure

This deploys the infrastructure to house price list API data and Cost and Usage Report data in AWS Glue.

+ 1 S3 Bucket to contain the ETL'd CUR data plus Bucket Policy
+ 1 Glue Database
+ 1 Glue Table for the price list reserved instance data (and Athena query that creates the same)
+ 1 Glue Table for the formatted CUR data delivered from the Glue ETL Job
+ 1 Glue ETL Job to convert data in a raw CUR table to formatted data 