
================================================================
-- Gold Layer:- 
-- We create only views in Gold layer and data is pulled from silver layer.
-- No load processing steps like store_proc's etc
-- Add Business logics & aggregations & rules etc etc
-- Rename column name to business friendly names
================================================================

-- *************** creating dim_customer view *************** 


if object_id('gold.dim_customer', 'V') is not null
	drop view gold.dim_customer;
go
create view gold.dim_customer as (
select
	row_number() over(order by crm_cst.cst_id ) as customer_key,
	crm_cst.cst_id as customer_id,
	crm_cst.cst_key as customer_number,
	crm_cst.cst_firstname as first_name,
	crm_cst.cst_lastname as last_name,
	erp_loc.cntry as country,
	crm_cst.cst_marital_status as marital_status,
	case
		when crm_cst.cst_gndr != 'n/a' then crm_cst.cst_gndr
		else coalesce(erp_cst.gen,'n/a')
	end as gender,
	erp_cst.bdate as birth_date,
	crm_cst.cst_create_date as create_date
	
from [silver].[crm_cust_info] as crm_cst
left join [silver].[erp_CUST_AZ12] as erp_cst
on crm_cst.cst_key = erp_cst.cid
left join [silver].[erp_LOC_A101] as erp_loc
on crm_cst.cst_key = erp_loc.cid
)

--	************************************************************************************************************

-- *************** creating dim_products view *************** 

if object_id('gold.dim_products','V') is not null
	drop view gold.dim_products;
go
create view gold.dim_products as (
	select
		row_number() over(order by crm_prd.prd_start_dt, crm_prd.prd_key) as product_key,
		crm_prd.prd_id as product_id,
		crm_prd.prd_key as product_number,
		crm_prd.prd_nm as product_name,
		crm_prd.cat_id as category_id,
		erp_prd.cat as category,
		erp_prd.subcat as subcategory,
		erp_prd.maintenance as maintenance,
		crm_prd.prd_cost as cost,
		crm_prd.prd_line as product_line,
		crm_prd.prd_start_dt as start_date
	
	from [silver].[crm_prd_info] as crm_prd
	left join [silver].[erp_PX_CAT_G1V2] as erp_prd
	on crm_prd.cat_id = erp_prd.id
	where crm_prd.prd_end_dt is null -- filtering out all historical data, which means we do not have historical data
)

--	************************************************************************************************************

-- *************** creating fact_sales view *************** 


if object_id('gold.fact_sales','V') is not null
	drop view gold.fact_sales;
go
create view gold.fact_sales as (
	select 
		crm_s.sls_ord_num as order_number,
		g_prd.product_key,
		g_cst.customer_key,
		crm_s.sls_order_dt as order_date,
		crm_s.sls_ship_dt as shipment_date,
		crm_s.sls_due_dt as due_date,
		crm_s.sls_sales as sales,
		crm_s.sls_quantity as quantity,
		crm_s.sls_price as price

	from [silver].[crm_sales_details] as crm_s
	left join gold.dim_products as g_prd
		on crm_s.sls_prd_key = g_prd.product_number
	left join gold.dim_customer as g_cst
		on sls_cust_id = g_cst.customer_id

)
