WITH 
/*
	================================================== VARIABLES ==================================================
	
	Set up the pseudo-variables we will use with the query, the
	start, end, and ec2 RI type
*/
    variables 
AS
(
    SELECT
        CAST(start_date as DATE) AS start_date,
        CAST(end_date as DATE) AS end_date,
        ec2_ritype,
		payer_accountid
    FROM
        (
          VALUES
          ('2018-10-01', '2018-10-11', 'Regional', '123456789012')
        )
        AS t1(start_date, end_date, ec2_ritype, payer_accountid)
),



/* 
	================================================== USAGE BY RESOURCE ID ==================================================

	First, aggregate all of the usage data for the products we care about
	and indicate actual instance usage based on the usage type 
*/
    usage_by_resource_id
AS 
(
    SELECT 
        line_item_resource_id, -- The resource id
        MAX(line_item_usage_start_date) AS latest, -- The most recent usage time
        MAX(product_sku) AS sku, -- The unique sku for the charge
        MAP_AGG(line_item_usage_start_date, line_item_usage_amount) AS usagemap, -- ALL the usage for that resource, use a map here so we can index directly into it later
        MAX(ec2_ritype) AS ec2_ritype, -- The RI type, zonal or regional specifically for EC2, changes the way we group rows
        MAX(start_date) AS start_date, -- The start date for the report, inclusive
        MAX(end_date) AS end_date -- The end date for the report, non-inclusive
    FROM 
        "billingdata"."cur_formatted", variables AS var
    WHERE 
		pricing_term = 'OnDemand'
		AND
		(
			(product_service_code = 'AmazonEC2'
				AND 
			 regexp_like(product_usage_type, '\bBoxUsage\b|HeavyUsage|DedicatedUsage')
			) -- EC2 instance usage
  
			OR 
  
			(product_service_code = 'AmazonRDS'
				AND 
			 regexp_like(product_usage_type, 'Multi-AZUsage|InstanceUsage')
			) -- RDS instance usage
  
			OR 
  
			(product_service_code = 'AmazonElastiCache'
				AND 
			 regexp_like(product_usage_type,'NodeUsage')
			) -- Elasticache instance usage 

			OR 
  
			(product_service_code = 'AmazonRedshift'
				AND 
			 regexp_like(product_usage_type,'\bNode\b')
			 ) -- Redshift instance usage

			OR 
  
			(product_service_code = 'AmazonDynamoDB'
				AND 
			 regexp_like(product_usage_type,'\bWriteCapacityUnit-Hrs|\bReadCapacityUnit-Hrs')
			) -- DynamoDB read/write capacity units 
		)
        AND
            line_item_usage_start_date >= var.start_date
        AND
            line_item_usage_start_date < var.end_date 
		AND 
			bill_payer_account_id = var.payer_accountid
     GROUP BY 
         line_item_resource_id 
),



/* 
	================================================== UNIQUE RESOURCE DETAILS WITH USAGE ==================================================

	Next we're going to join that data with the most recent usage row's 
	content for the other fields, that way we get the aggregate usage in order
	and also get the instance details from *when it most recently ran* - this is the key
	since the above statement doesn't allow us to get the details about the most recent
	usage, this is important because an instance may have changed instance type over time,
	but the instance id remains the same, we want to know what it most recently was
*/
	unique_resource_details_with_usage
AS
(
	
	SELECT 
		b.line_item_resource_id,
		b.line_item_product_code,
		b.line_item_usage_amount,
		b.line_item_availability_zone,
		b.product_region,
		b.product_service_code,
		b.product_sku,
		b.product_usage_type,
		b.product_operation,
		b.pricing_term,
		b.product_tenancy,
        usage_by_resource_id.usagemap,
        usage_by_resource_id.ec2_ritype,
        usage_by_resource_id.start_date,
        usage_by_resource_id.end_date
	FROM 
		"billingdata"."cur_formatted" AS b
	INNER JOIN -- Only select the elements where both sides match
		usage_by_resource_id
	ON -- This combination should uniquely identify the row that has
	   -- the most recent usage data for the instance
		b.line_item_resource_id = usage_by_resource_id.line_item_resource_id
		AND 
		b.line_item_usage_start_date = usage_by_resource_id.latest 
		AND 
		b.product_sku = usage_by_resource_id.sku
),



/*
	================================================== GROUPED RESOURCES ==================================================

	Now, group by usage type, operation, and tenancy, which will provide a unique mapping of the item, the sku will
	also represent this uniquely. Also we'll calculate the total number of running instances and the total usage 
	amount for each hour during the report for these unique groupings. These numbers will enable us to find the greatest
	number of running instances that is over the breakeven threshold.
*/
	grouped_resource_usage_data
AS (

	SELECT 
		product_sku as SKU,
		IF (ec2_ritype = 'Regional', product_region, COALESCE(line_item_availability_zone, product_region)) as Location,
		product_usage_type as UsageType,
		product_operation as Operation,
		CASE product_tenancy
			WHEN NULL THEN 'Shared'		-- Only EC2 will have product_tenancy
			WHEN '' THEN 'Shared'		-- So everything else is default shared
			ELSE product_tenancy
		END as Tenancy,
		MAX(line_item_availability_zone) as AvailabilityZone,
        ARRAY_AGG(line_item_resource_id) as ResourceIds,
        MAX(ec2_ritype) as EC2_ReservedInstanceType,
		map_agg(line_item_resource_id, usagemap) as ResourceUsage, -- A map of resource id to each hour it was running, useful for proof purposes
		reduce(
			map_values(map_agg(line_item_resource_id, usagemap)), -- Hacky way to get all of the usagemaps into an array
			CAST(												-- The reduce function won't take a map as the initial state, so cast it to a ROW, which it will take
				  ROW(
					MAP(										-- Create a map of all of the possible start times as keys and assign an initial 0 as the value
					  SEQUENCE(start_date, end_date, INTERVAL '1' HOUR), 
					  transform(
                        SEQUENCE(start_date, end_date, INTERVAL '1' HOUR), x -> 
                        CAST(ROW(0.0, 0) AS ROW(usageamount DOUBLE, totalinstances INTEGER)) 
					  )
                    )    
				  ) 
                  AS ROW(map MAP(
                    timestamp, 
                    ROW(usageamount double, totalinstances integer)
                    )
                  ) 
			), 
			(initial_state, resource_usage_map) -> CAST(
                ROW(	
					transform_values(initial_state.map, (key, value) ->										-- Modify the initial state by adding the actual usage amount for that hour, for Windows
						IF (resource_usage_map[key] IS NULL, value,											-- and RHEL, this will always be 1, since they don't get per second billing, but for
							IF (resource_usage_map[key] > 1 AND product_service_code != 'AmazonDynamoDB',    -- Linux OS, this could be less than 1, if it is more than 1, the instance got stopped/started
																											-- during the hour, so we ignore that and only count it as 1 running resource for that hour since
																											-- multiple RIs wouldn't help reduce costs beyond that 1 instance, unless it's DynamoDB, then we
																											-- want to count the number of capacity units in that hour. We also add to the total instance count
																											-- so we know both the total usage and the total number of instances, these numbers will not necessarily
																											-- be equal due to per-second billing on Linux
								CAST(ROW(value.usageamount + 1, value.totalinstances + 1) AS ROW(usageamount DOUBLE, totalinstances INTEGER)),		-- If it's over 1 and not dynamo, just add 1 to each
								CAST(ROW(value.usageamount + resource_usage_map[key], value.totalinstances + 1) AS ROW(usageamount DOUBLE, totalinstances INTEGER)) -- If it's not over 1 or it is Dynamo, add the amount
                            )
						)																																						
					)													
				) AS ROW(map MAP(TIMESTAMP, ROW(usageamount DOUBLE, totalinstances INTEGER)))
            ), 
			s -> s.map	
		) AS UsageAmountPerHour		
	FROM
		unique_resource_details_with_usage
	GROUP BY
		IF (ec2_ritype = 'Regional', product_region, COALESCE(line_item_availability_zone, product_region)), -- Select the grouping based on EC2 RI type, zonal or regional
		product_sku,  -- The sku is the same for all AZs in a region
        product_usage_type,
		product_operation,
		product_tenancy,
        product_service_code, -- Include this so we can use it in the sequence generation, not needed after
        start_date, -- Include this so we can use it in the sequence generation, not needed after
        end_date	-- Include this so we can use it in the sequence generation, not needed after
)



/*
	================================================== FINAL SELECT STATEMENT ==================================================

	Now that we have all of the usage grouped to unique resource types and also the usage per hour calculated, we can compute the
	optimal number of reserved instances
*/
SELECT 
	grouped_resource_usage_data.SKU as SKU,
	grouped_resource_usage_data.Location as LocationToBuy,
    grouped_resource_usage_data.EC2_ReservedInstanceType as EC2_ReservedInstanceType,
    grouped_resource_usage_data.AvailabilityZone as AvailabilityZone,
	grouped_resource_usage_data.UsageType as UsageType,
	grouped_resource_usage_data.Operation as Operation,
	grouped_resource_usage_data.Tenancy as Tenancy,
	COALESCE(
		reduce(
			transform(
				map_values(grouped_resource_usage_data.UsageAmountPerHour), 
				x -> x.totalinstances
			),
			CAST(
				ROW(
					MAP(														-- Create a map of the total instances running and the number of that many instances were running each hour
						array_distinct(
							transform(
								map_values(grouped_resource_usage_data.UsageAmountPerHour),
								row -> row.totalinstances
							)
						),
						transform(
							array_distinct(
								transform(
									map_values(grouped_resource_usage_data.UsageAmountPerHour),
									row -> row.totalinstances
								)
							),
							x -> 0
						)
					)
				) AS ROW(map MAP(INTEGER, INTEGER))
			), 
			(initial_state, totalinstances) -> CAST(
				ROW(
					transform_values(initial_state.map, (key, value) -> 
						IF (totalinstances <= key, value + 1, value)	-- Update all values if this usage quantity is less than or equal
																		-- So if 6 were running, we also count that towards 5, 4, 3, 2, 1
					)															
				) AS ROW (map MAP(INTEGER, INTEGER))
			), 
			updated_state -> array_max( 
				map_keys(
					map_filter(											-- Only take usage quantities that are greater than the breakeven
						updated_state.map,
						(key, total_running_instances) -> total_running_instances >= breakevenpercentage * cardinality(UsageAmountPerHour)
					)
				)
			)
		), 
		0
	) AS RecommendedOptimalQuantityBasedOnInstanceCount,
	COALESCE(
		reduce( 
			transform(
				map_values(grouped_resource_usage_data.UsageAmountPerHour), 
				x -> CAST(floor(x.usageamount) AS INTEGER)						-- Convert the double to int since we only care about whole numbers of instances to purchase RIs for
			),
			CAST(
				ROW(
					MAP(														-- Create a map of the hourly usage amount and the number of that much usage occured each hour
						array_distinct(
							transform(
								map_values(grouped_resource_usage_data.UsageAmountPerHour),
								row -> CAST(floor(row.usageamount) AS INTEGER)
							)
						),
						transform(
							array_distinct(
								transform(
									map_values(grouped_resource_usage_data.UsageAmountPerHour),
									row -> CAST(floor(row.usageamount) AS INTEGER)
								)
							),
							x -> 0
						)
					)
				) AS ROW(map MAP(INTEGER, INTEGER))
			),
			(initial_state, usage_amount) -> CAST(
				ROW(
					transform_values(initial_state.map, (key, value) ->	
						IF(usage_amount <= key, value + 1, value)		-- Update all values if this usage quantity is less than or equal
																		-- So if 6 were running, we also count that towards 5, 4, 3, 2, 1
					)													
				) AS ROW (map map(INTEGER, INTEGER))
			), 
			updated_state -> array_max( 
				map_keys(
					map_filter(											-- Only take usage quantities that are greater than the breakeven
						updated_state.map,
						(key, total_usage_amount) -> total_usage_amount >= breakevenpercentage * cardinality(UsageAmountPerHour) -- cardinality(UsageAmountPerHour) represents the number of hours in our evaluation timespan
					)
				)
			)
		), 
		0
	) AS RecommendedOptimalQuantityBasedOnUsage,
	rip.service as Service,
	rip.platform as Platform,
	rip.operatingsystem as OperatingSystem,
	rip.instancetype as InstanceType,
	rip.ondemandhourlycost AS OnDemand,
	rip.upfrontfee as UpfrontFee,
	rip.leaseterm as LeaseTerm,
	rip.purchaseoption as PurchaseOption,
	rip.offeringclass as OfferingClass,
	rip.termtype as TermType,
	rip.adjustedpriceperunit as AdjustedPricePerUnit,
	rip.reservedinstancecost as AmortizedRICost,
	rip.ondemandcostforterm as OnDemandCostForTerm,
	rip.costsavings as TotalCostSavingsForTerm,
	rip.percentsavings as PercentSavings,
	rip.breakevenpercentage as BreakevenPercentage,
    grouped_resource_usage_data.ResourceIds as ResourceIds,
	grouped_resource_usage_data.UsageAmountPerHour as UsageAmountPerHour
FROM
	grouped_resource_usage_data
LEFT OUTER JOIN
	 reservedinstancepricelistdata as rip
ON
  grouped_resource_usage_data.SKU = rip.sku