# BAMCIS AWS Billing Data Analytics

This provides AWS Glue and AWS Athena infrastructure to run analytics on Cost and Usage Report data as well as Reserved Instace Price List API data. This project relies on the data delivered from these two separate projects:

+ [AWS CUR Manager](https://github.com/bamcis-io/AWSCURManager)
+ [AWS Price List Reserved Instance Helper](https://github.com/bamcis-io/AWSPriceListReservedInstanceHelper)

The queries are works in progress. I will add new queries when I find interesting insights into the CUR and price list data.

The entire pipeline is serverless. The two required projects use Lambda to initiate workflows, and the CUR Manager also uses Glue to perform a transform of the CUR data through a job defined in this template. So, make sure you use the same AWS Glue ETL Job name in each CloudFormation script.

## Table of Contents
- [Usage](#usage)
  * [Reserved Instance Recommendation](#reserved-instance-recommendation)
  * [Best RI Deals](#best-ri-deals)
  * [External Data Transfer Costs](#external-data-transfer)
  * [Inter Region Data Transfer](#inter-region-data-transfer)
  * [vCPU By Region](#vcpu-by-region)
- [Revision History](#revision-history)

## Usage

Once you have deployed the two required projects, the AWS CUR Manager and AWS Price List Reserved Instance Helper, you can deploy the infrastructure in the BillingDataInfrastructure.template CloudFormation script. This will deploy the S3, Glue, and Athena resources you need. Then, upload the `cur_etl.py` script to the bucket you specified for the ETL Job, this defaults to

    s3://aws-glue-scripts-${AWS::AccountId}-${AWS::Region}/admin/cur_etl.py

Once that is uploaded, wait for the CUR Manager and your Price List data to start populating in S3, this may take 1 or more days. Once you have data, you can start running the Athena queries.

**Make sure you use the same name for the Glue ETL job in the CUR Manager application manager as you do in this application. The defaults are the same, but if you modified, make sure you change it in both places.**

### Reserved Instance Recommendation

+ RI_recommendation.sql

You provide input at the top of the script, the start date, end date, and whether you want to compute either *Regional* or *Zonal* reserved instances for EC2. Make sure you update the database and table names where appropriate. The query will provide recommendations for how many RIs to buy for the different purchasing options.

### Best RI Deals

+ RI_one_year_best_deals.sql

This will give you an idea of where the most significant cost savings for reserved instances are. This is useful if you have net new workloads moving to AWS that you know will run 24/7 or will benefit from RIs. You might find regions that you didn't think about that offer better deals on the instance type you're looking for.

Obviously, tailor the `WHERE` clause in the script to find the types of instances you're looking for. The price list data contains OS, memory, vcpu, region, and tenancy all as options you can filter against.

### External Data Transfer

+ external_data_transfer.sql

Provides insight to the services and regions that are generating your greatest external data transfer costs.

### Inter Region Data Transfer

+ cross_region_data_transfer.sql

Provides insight to the services and regions that are generating your greatest cross region data transfer costs.

### vCPU By Region

+ vcpu_by_region.sql

Identifies how many vCPUs on unique resources you ran per service in each region to give an idea of overall utilization in regions. For example, if you ran 2 EC2 instances for 1 week each with 2 vCPUs and 1 EC2 instance for 3 weeks with 4 vCPUs all in us-east-1 during a specified billing period, you would see that you have 8 vCPUs in us-east-1 for EC2. If the instances changed type during the billing period, the greatest vCPU value is used.

## Revision History

### 1.0.0
Initial release of the application.