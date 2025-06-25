
**********************************************************
-- insert into silver layer 
-- table name: [silver].[crm_cust_info]
**********************************************************

create or alter procedure silver.load_silver as
begin
	declare @start_date datetime, @end_date datetime
	print '>> Truncating table: [silver].[crm_cust_info]'
	truncate table [silver].[crm_cust_info]

	print '>> Inserting data into: [silver].[crm_cust_info]'
	set @start_date = getdate();
	INSERT INTO [silver].[crm_cust_info] (
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date
	)
	SELECT
		cst_id,
		cst_key,
		TRIM(cst_firstname),
		TRIM(cst_lastname),
		CASE
			WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
			WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
			ELSE 'n/a'
		END as cst_marital_status,
		CASE
			WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			ELSE 'n/a'
		END as cst_gndr,
		cst_create_date
	FROM (
		SELECT *,
			ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rnk
		FROM [bronze].[crm_cust_info]
	) AS sub
	WHERE rnk = 1;
	set @end_date = getdate();
	print '>> Total load duration: ' + cast(datediff(second,@start_date,@end_date) as nvarchar) + 'seconds'
	print '--------------------------------'

--	************************************************************************************************************

--	**********************************************************
	-- insert into silver layer 
	-- table name: [silver].[crm_prd_info]
--	**********************************************************

	print '>> Truncating table: [silver].[crm_prd_info]'
	truncate table [silver].[crm_prd_info]

	print '>> Inserting data into: [silver].[crm_prd_info]'
	set @start_date = getdate();
	insert into [silver].[crm_prd_info] (prd_id,cat_id,prd_key,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt)
	SELECT 
		prd_id,
		-- prd_key,
		replace(left(prd_key,5),'-','_') as cat_id,
		substring(prd_key,7, len(prd_key)) as prd_key,
		prd_nm,
		isnull(prd_cost,0) as prd_cost,
		case
			when upper(Trim(prd_line)) = 'M' then 'Mountain'
			when upper(Trim(prd_line)) = 'R' then 'Road'
			when upper(Trim(prd_line)) = 'S' then 'Other sales'
			when upper(Trim(prd_line)) = 'T' then 'Touring'
			else 'n/a'
		end as prd_line,
		prd_start_dt,
		dateadd(day,-1,lead(prd_start_dt,1) over(partition by prd_key order by prd_start_dt )) as prd_end_dt
	from [bronze].[crm_prd_info]
	set @end_date = getdate();
	print '>> Total load duration: ' + cast(datediff(second,@start_date,@end_date) as nvarchar) + 'seconds'
	print '--------------------------------'

--	************************************************************************************************************


--	**********************************************************
	-- insert into silver layer 
	-- table name: [silver].[crm_sales_details]
--	**********************************************************
	print '>> Truncating table: [silver].[crm_sales_details]'
	truncate table [silver].[crm_sales_details]

	print '>> Inserting data into: [silver].[crm_sales_details]'
	set @start_date = getdate();
	insert into [silver].[crm_sales_details] (sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_ship_dt,sls_due_dt,sls_sales,sls_quantity,sls_price)

	select
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		case
			when sls_order_dt = 0 or len(sls_order_dt) != 8 then null
			else cast(cast(sls_order_dt as varchar) as date)
		end as sls_order_dt,
		case
			when sls_ship_dt = 0 or len(sls_ship_dt) !=8 then null
			else cast(cast(sls_ship_dt as varchar) as date)
		end as sls_ship_dt,
		case
			when sls_due_dt = 0 or len(sls_due_dt) !=8 then null
			else cast(cast(sls_due_dt as varchar) as date)
		end as sls_due_dt,
		case
			when sls_sales is null or sls_sales < 0 or sls_sales != sls_quantity * abs(sls_price) then (abs(sls_price) * sls_quantity)
			else sls_sales
		end as sls_sales,
		sls_quantity,
		case
			when sls_price is null or sls_price <= 0  then (sls_sales/nullif(sls_quantity,0))
			else sls_price
		end as sls_price
	
	from [bronze].[crm_sales_details]
	set @end_date = getdate();
	print '>> Total load duration: ' + cast(datediff(second,@start_date,@end_date) as nvarchar) + 'seconds'
	print '--------------------------------'

--	************************************************************************************************************

--	**********************************************************
	-- insert into silver layer 
	-- table name: [silver].[erp_CUST_AZ12]
--	**********************************************************

	print '>> Truncating table: [silver].[erp_CUST_AZ12]'
	truncate table [silver].[erp_CUST_AZ12]

	print '>> Inserting data into: [silver].[erp_CUST_AZ12]'
	set @start_date = getdate();
	insert into [silver].[erp_CUST_AZ12] (CID,BDATE,GEN)
	select
		case
			when cid like 'NAS%' then substring(cid,4,len(cid))
			else cid
		end as CID,
		case
			when bdate > getdate() then null
			else bdate
		end as BDATE,
		case
			when upper(trim(GEN)) in ('M', 'Male') then 'Male'
			when upper(trim(GEN)) in ('F','Female') then 'Female'
			when GEN is null then 'n/a'
			else 'n/a'
		end as GEN
	from [bronze].[erp_CUST_AZ12]
	set @end_date = getdate();
	print '>> Total load duration: ' + cast(datediff(second,@start_date,@end_date) as nvarchar) + 'seconds'
	print '--------------------------------'


--	************************************************************************************************************

--	**********************************************************
	-- insert into silver layer 
	-- table name: [silver].[erp_LOC_A101]
--	**********************************************************

	print '>> Truncating table: [silver].[erp_LOC_A101]'
	truncate table [silver].[erp_LOC_A101]

	print '>> Inserting data into: [silver].[erp_LOC_A101]'
	set @start_date = getdate();
	insert into [silver].[erp_LOC_A101] (cid, cntry) 
	select
		replace(cid,'-','') as cid,
		case
			when upper(trim(cntry)) in ('US','USA') then 'United States'
			when upper(trim(cntry)) = 'DE' then 'Germany'
			when trim(cntry) is null or trim(cntry) = '' then 'n/a'
			else trim(cntry)

		end as cntry
	from [bronze].[erp_LOC_A101]
	set @end_date = getdate();
	print '>> Total load duration: ' + cast(datediff(second,@start_date,@end_date) as nvarchar) + 'seconds'
	print '--------------------------------'


--	************************************************************************************************************

--	**********************************************************
	-- insert into silver layer 
	-- table name: [silver].[erp_LOC_A101]
--	**********************************************************

	print '>> Truncating table: [silver].[erp_PX_CAT_G1V2]'
	truncate table [silver].[erp_PX_CAT_G1V2]

	print '>> Inserting data into: [silver].[erp_PX_CAT_G1V2]'
	set @start_date = getdate();
	insert into [silver].[erp_PX_CAT_G1V2] (id,cat, subcat, maintenance)
	select * from [bronze].[erp_PX_CAT_G1V2]
	set @end_date = getdate();
	print '>> Total load duration: ' + cast(datediff(second,@start_date,@end_date) as nvarchar) + 'seconds'
	print '--------------------------------'
end 

exec [silver].[load_silver]
