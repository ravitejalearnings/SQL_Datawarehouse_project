use master
================================================================
-- Creating of database, schema like bronze, silver, gold
================================================================
-- create database
create database datawarehouse

use datawarehouse;


-- create different schema within database
create schema bronze;

create schema silver;

create schema gold;


================================================================
-- create ddl statements as per source data
================================================================


-- Create tables 
IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info (
    cst_id INT ,
    cst_key VARCHAR(20),
    cst_firstname VARCHAR(50),
    cst_lastname VARCHAR(50),
    cst_marital_status CHAR(1),     
    cst_gndr CHAR(1),
    cst_create_date DATE
);

if object_id('bronze.crm_prd_info','U') is not null
	drop table bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info (
    prd_id INT,
    prd_key VARCHAR(50),
    prd_nm VARCHAR(100),
    prd_cost DECIMAL(10, 2),
    prd_line VARCHAR(100),
    prd_start_dt DATE,
    prd_end_dt DATE
);



if object_id('bronze.crm_sales_details','U') is not null
	drop table bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details (
    sls_ord_num    VARCHAR(20),       
    sls_prd_key    VARCHAR(50),       
    sls_cust_id    INT,               
    sls_order_dt   INT,              
    sls_ship_dt    INT,              
    sls_due_dt     INT,              
    sls_sales      INT,     
    sls_quantity   INT,               
    sls_price      INT      
);




if object_id('bronze.erp_CUST_AZ12','U') is not null
	drop table bronze.erp_CUST_AZ12;
CREATE TABLE bronze.erp_CUST_AZ12 (
    CID VARCHAR(20) ,
    BDATE DATE,
    GEN VARCHAR(10)
);


if object_id('bronze.erp_LOC_A101','U') is not null
	drop table bronze.erp_LOC_A101;
CREATE TABLE bronze.erp_LOC_A101 (
    CID VARCHAR(20),
    CNTRY VARCHAR(50)
);


if object_id('bronze.erp_PX_CAT_G1V2','U') is not null
	drop table bronze.erp_PX_CAT_G1V2;
CREATE TABLE bronze.erp_PX_CAT_G1V2 (
    id VARCHAR(10),
    cat VARCHAR(50),
    subcat VARCHAR(100),
    maintenance VARCHAR(3)
);
