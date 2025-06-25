
================================================================
-- Silver Layer 
================================================================

================================================================
-- create ddl statements in Silver layer as per bronze layer
================================================================
-- Create tables 

IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info (
    cst_id INT ,
    cst_key VARCHAR(20),
    cst_firstname VARCHAR(50),
    cst_lastname VARCHAR(50),
    cst_marital_status VARCHAR(50),     
    cst_gndr VARCHAR(50),
    cst_create_date DATE,
	dwh_create_date datetime2 default getdate() -- this col comes from bronze layer
);

if object_id('silver.crm_prd_info','U') is not null
	drop table silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info (
    prd_id INT,
	cat_id VARCHAR(50),
    prd_key VARCHAR(50),
    prd_nm VARCHAR(100),
    prd_cost DECIMAL(10, 2),
    prd_line VARCHAR(100),
    prd_start_dt DATE,
    prd_end_dt DATE,
	dwh_create_date datetime2 default getdate() -- this col comes from bronze layer
);



if object_id('silver.crm_sales_details','U') is not null
	drop table silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details (
    sls_ord_num    VARCHAR(20),       
    sls_prd_key    VARCHAR(50),       
    sls_cust_id    INT,               
    sls_order_dt   date,              
    sls_ship_dt    date,              
    sls_due_dt     date,              
    sls_sales      INT,     
    sls_quantity   INT,               
    sls_price      INT,
	dwh_create_date datetime2 default getdate() -- this col comes from bronze layer
);


if object_id('silver.erp_CUST_AZ12','U') is not null
	drop table silver.erp_CUST_AZ12;
CREATE TABLE silver.erp_CUST_AZ12 (
    CID VARCHAR(20) ,
    BDATE DATE,
    GEN VARCHAR(10),
	dwh_create_date datetime2 default getdate() -- this col comes from bronze layer
);


if object_id('silver.erp_LOC_A101','U') is not null
	drop table silver.erp_LOC_A101;
CREATE TABLE silver.erp_LOC_A101 (
    CID VARCHAR(20),
    CNTRY VARCHAR(50),
	dwh_create_date datetime2 default getdate() -- this col comes from bronze layer
);


if object_id('silver.erp_PX_CAT_G1V2','U') is not null
	drop table silver.erp_PX_CAT_G1V2;
CREATE TABLE silver.erp_PX_CAT_G1V2 (
    id VARCHAR(10),
    cat VARCHAR(50),
    subcat VARCHAR(100),
    maintenance VARCHAR(3),
	dwh_create_date datetime2 default getdate() -- this col comes from bronze layer
);
