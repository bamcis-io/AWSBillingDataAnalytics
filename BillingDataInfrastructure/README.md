# Billing Data Infrastructure

This deploys the infrastructure to house price list API data and Cost and Usage Report data in AWS Glue.

+ 1 S3 Bucket to contain the ETL'd CUR data plus Bucket Policy
+ 1 Glue Database
+ 1 Glue Table for the price list reserved instance data (and Athena query that creates the same)
+ 1 Glue Table for the formatted CUR data delivered from the Glue ETL Job
+ 1 Glue ETL Job to convert data in a raw CUR table to formatted data that uses a common schema

## Dependencies
This setup assumes that you have a table per month in another database with the current CUR data. The ETL job will take the data from the specified table and add or update the partition in the new database for that month. Whenever the source CUR data is updated in the month, a good strategy is to launch the CUR ETL job after the new data has arrived. 

Make sure the table you are pulling data from contains deduplicated CUR data. In the standard CUR delivery model, new versions of the CUR are added to the S3 bucket, and each file is differential from the beginning of the month, so they all contain duplicate data. Either pull from a table that is receiving CUR reports that are using the overwrite option, or perform another method of deduplication. You can use the serverless application [here](#https://github.com/bamcis-io/AWSCURManager) to manage the delivery of CUR files and ensure only the most current data for each month is in the Glue table the ETL job pulls from. The specified application can be configured to automatically kick-off the ETL job included in this solution.