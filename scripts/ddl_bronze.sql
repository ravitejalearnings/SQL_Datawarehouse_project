create or alter procedure bronze.load_bronze as 
begin
	
	print '===============================================';
	print 'Loading Bronze layer';
	print '===============================================';


	print '------------------------------------------------';
	print 'loading CRM tables';
	print '------------------------------------------------';

	
	print '>> Truncating table : [bronze].[crm_cust_info]';
	truncate table [bronze].[crm_cust_info];

	print '>> Inserting data into : [bronze].[crm_cust_info]';


	bulk insert [bronze].[crm_cust_info]
	from "C:\Users\Ravi\Downloads\sql-data-warehouse-project\datasets\source_crm\cust_info.csv"
	with (
		firstrow = 2,
		fieldterminator = ',',
		tablock
	);


	-- prod table

	print '>> Truncating table : [bronze].[crm_prd_info]';
	truncate table [bronze].[crm_prd_info];

	print '>> Inserting data into : [bronze].[crm_prd_info]';

	bulk insert [bronze].[crm_prd_info]
	from "C:\Users\Ravi\Downloads\sql-data-warehouse-project\datasets\source_crm\prd_info.csv"
	with (
		firstrow = 2,
		fieldterminator = ',',
		tablock
	);


	-- sales table

	print '>> Truncating table : [bronze].[crm_sales_details]';
	truncate table [bronze].[crm_sales_details];

	print '>> Inserting data into : [bronze].[crm_sales_details]';

	bulk insert [bronze].[crm_sales_details]
	from "C:\Users\Ravi\Downloads\sql-data-warehouse-project\datasets\source_crm\sales_details.csv"
	with (
		firstrow = 2,
		fieldterminator = ',',
		ROWTERMINATOR = '\n',
		tablock
	);


	print '------------------------------------------------';
	print 'loading ERP tables';
	print '------------------------------------------------';

	print '>> Truncating table : [bronze].[erp_CUST_AZ12]';
	truncate table [bronze].[erp_CUST_AZ12];

	print '>> Inserting data into : [bronze].[erp_CUST_AZ12]';

	bulk insert [bronze].[erp_CUST_AZ12]
	from "C:\Users\Ravi\Downloads\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv"
	with (
		firstrow = 2,
		fieldterminator = ',',
		tablock
	);


	print '>> Truncating table : [bronze].[erp_LOC_A101]';
	truncate table [bronze].[erp_LOC_A101];

	print '>> Inserting data into : [bronze].[erp_LOC_A101]';

	bulk insert [bronze].[erp_LOC_A101]
	from "C:\Users\Ravi\Downloads\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv"
	with (
		firstrow = 2,
		fieldterminator = ',',
		tablock
	);


	print '>> Truncating table : [bronze].[erp_PX_CAT_G1V2]';
	truncate table [bronze].[erp_PX_CAT_G1V2];

	print '>> Inserting data into : [bronze].[erp_PX_CAT_G1V2]';
	bulk insert [bronze].[erp_PX_CAT_G1V2]
	from "C:\Users\Ravi\Downloads\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv"
	with (
		firstrow = 2,
		fieldterminator = ',',
		tablock
	);

end 

exec [bronze].[load_bronze]
