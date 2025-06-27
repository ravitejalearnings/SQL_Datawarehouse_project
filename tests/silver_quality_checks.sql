use [datawarehouse]

 ==================================================================================================
-- Data profile of silver layer tables, before implementation of any kind of data transformations
===================================================================================================

-- check for nulls or duplicates in primary key
-- expecation: No Result
-- table name: [bronze].[crm_cust_info]

select cst_id, count(*) as records 
from [bronze].[crm_cust_info]
group by cst_id
having count(*) > 1 or cst_id is null

-- validation check for duplication 

select * from [bronze].[crm_cust_info]
where cst_id = '29473'

-- transformation logic to remove duplication data

with cte1 as (
select
	*,
	row_number() over(partition by cst_id order by [cst_create_date] desc) as rnk
from [bronze].[crm_cust_info]
)
select * from cte1
where rnk = 1 and cst_id = '29473'

-- validation check for string values

-- method 1:-
select
	cst_firstname,
	len(cst_firstname),
	len(trim(cst_firstname))
from [bronze].[crm_cust_info]
where len(cst_firstname) != len(trim(cst_firstname))


-- method 2:-
select
	cst_firstname
from [bronze].[crm_cust_info]
where cst_firstname != trim(cst_firstname)


-- data consistency for low cardinality cols

select 
	distinct cst_marital_status
from [bronze].[crm_cust_info]


select 
	distinct cst_gndr
from [bronze].[crm_cust_info]


-- transformation logic:- 
-- remove/trim spaces for string values
-- duplication
-- value changes using case statement etc, etc

with cte1 as (
select
	cst_id,
	cst_key,
	trim(cst_firstname) as cst_firstname,
	trim(cst_lastname) as cst_lastname,
	case
		when upper(trim(cst_marital_status)) = 'M' then 'Married'
		when upper(trim(cst_marital_status)) = 'S' then 'Single'
		else 'n/a'
	end as cst_marital_status,
	case
		when upper(trim(cst_gndr)) = 'M' then 'Male'
		when upper(trim(cst_gndr)) = 'F' then 'Female'
		else 'n/a'
	end as cst_gndr,
	cst_create_date,
	row_number() over(partition by cst_id order by [cst_create_date] desc) as rnk
from [bronze].[crm_cust_info]
)
select * from cte1
where rnk = 1 


-- check for nulls or duplicates in primary key
-- expecation: No Result
-- table name: [bronze].[crm_cust_info]

select prd_id, count(*) as records 
from [silver].[crm_prd_info]
group by prd_id
having count(*) > 1 or prd_id is null

-- validation check for duplication 

select * from [bronze].[crm_cust_info]
where cst_id = '29473'

-- transformation logic to remove duplication data

with cte1 as (
select
	*,
	row_number() over(partition by cst_id order by [cst_create_date] desc) as rnk
from [bronze].[crm_cust_info]
)
select * from cte1
where rnk = 1 and cst_id = '29473'


-- validation check for string values

-- method 1:-
select
	cst_firstname,
	len(cst_firstname),
	len(trim(cst_firstname))
from [silver].[crm_cust_info]
where len(cst_firstname) != len(trim(cst_firstname))


-- method 2:-
select
	prd_nm
from [bronze].[crm_prd_info]
where prd_nm != trim(prd_nm)


-- data consistency for low cardinality cols

select 
	distinct [prd_line]
from [bronze].[crm_prd_info]


select 
	distinct cst_gndr
from [silver].[crm_cust_info]

-- validation check for numeric cols
-- this depends on business rules

select 
	prd_cost
from [silver].[crm_prd_info]
where prd_cost < 0 or prd_cost is null


-- validation check for date cols
-- this depends on business rules

select 
	[prd_start_dt],
	[prd_end_dt]
from [silver].[crm_prd_info]
where  [prd_start_dt]  > [prd_end_dt] 

select 
	[prd_start_dt],
	[prd_end_dt]
from [silver].[crm_prd_info]
where [prd_start_dt] is null or [prd_end_dt] is null

-- validation checks

select distinct id from [bronze].[erp_PX_CAT_G1V2]
where id not in (select distinct replace(left(prd_key,5),'-','_') from [bronze].[crm_prd_info])

select distinct replace(left(prd_key,5),'-','_') from [bronze].[crm_prd_info]
where replace(left(prd_key,5),'-','_') not in (select distinct id from [bronze].[erp_PX_CAT_G1V2])

select top 10 * from [bronze].[crm_sales_details]


************************************************************************************************************
-- validation for sales table

select
	sls_ord_num
from [bronze].[crm_sales_details]
where sls_ord_num != trim(sls_ord_num)


select * from [bronze].[crm_sales_details]
where sls_prd_key not in (select prd_key from [silver].[crm_prd_info])

select * from [bronze].[crm_sales_details]
where sls_cust_id not in (select cst_id from [silver].[crm_cust_info])

select sls_order_dt from [bronze].[crm_sales_details]
where 
	sls_order_dt = 0 
or len(sls_order_dt) != 8
or sls_order_dt >= 20500101
or sls_order_dt <= 19000101

select sls_due_dt from [bronze].[crm_sales_details]
where 
	sls_due_dt = 0 
or len(sls_due_dt) != 8
or sls_due_dt >= 20500101
or sls_due_dt <= 19000101


select * from [bronze].[crm_sales_details]
where (sls_order_dt > sls_ship_dt) or (sls_order_dt > sls_due_dt )

select sls_sales,sls_quantity,sls_price from [bronze].[crm_sales_details]
where 
	sls_sales is null 
or sls_quantity is null 
or sls_price is null
or sls_sales < 0 
or sls_sales != (abs(sls_price) * sls_quantity)
or sla_price < 0 

select sls_price from [bronze].[crm_sales_details]
where sls_price is null


************************************************************************************************************
-- validation for erp_customer table

select * from [bronze].[erp_CUST_AZ12]

select cid, count(*)
from [bronze].[erp_CUST_AZ12]
group by cid
having count(*) > 1

select distinct gen from [bronze].[erp_CUST_AZ12]


select 
	bdate
from  [bronze].[erp_CUST_AZ12]
where len(bdate) != 10 or len(bdate) < 10 or len(bdate) > 10

select 
	bdate
from  [bronze].[erp_CUST_AZ12]
where bdate > getdate() or bdate < '1924-01-01'


************************************************************************************************************
-- validation for erp_location table
select * from [silver].[erp_CUST_AZ12];

select * from [bronze].[erp_LOC_A101];

select distinct cntry 
from [bronze].[erp_LOC_A101];



************************************************************************************************************
-- validation for erp_product_category table

select * from [bronze].[erp_PX_CAT_G1V2];
select * from [silver].[crm_prd_info]

select distinct maintenance
from [bronze].[erp_PX_CAT_G1V2];

select * 
from [bronze].[erp_PX_CAT_G1V2]
where cat != trim(cat) or subcat != trim(subcat) or maintenance != trim(maintenance)


************************************************************************************************************
-- integration checks

select
	crm_cst.cst_gndr,
	erp_cst.gen,
	case
		when crm_cst.cst_gndr != 'n/a' then crm_cst.cst_gndr
		else coalesce(erp_cst.gen,'n/a')
	end as new_gen

from [silver].[crm_cust_info] as crm_cst
left join [silver].[erp_CUST_AZ12] as erp_cst
on crm_cst.cst_key = erp_cst.cid
left join [silver].[erp_LOC_A101] as erp_loc
on crm_cst.cst_key = erp_loc.cid


************************************************************************************************************


select count(*) from gold.fact_sales


select count(*) from gold.fact_sales as s
left join gold.dim_customer as c
	on s.customer_key = c.customer_key
left join gold.dim_products as p
	on s.product_key = p.product_key


select * from gold.fact_sales as s
left join gold.dim_customer as c
	on s.customer_key = c.customer_key
left join gold.dim_products as p
	on s.product_key = p.product_key
where c.customer_key is null or p.product_key is null
