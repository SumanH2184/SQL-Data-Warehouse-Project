/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO
CREATE VIEW gold.dim_customers as
SELECT
ROW_NUMBER() OVER(ORDER BY cst_id) as customer_key,
a.cst_id as customer_id,
a.cst_key as customer_number,
a.cst_firstname as first_name,
a.cst_lastname as last_name,
c.cntry as country,
a.cst_material_status as marital_status,
CASE WHEN a.cst_gndr != 'n/a' THEN a.cst_gndr
	ELSE COALESCE(b.gen,'n/a')
END as gender,
b.bdate as birthdate,
a.cst_create_date as create_date
FROM silver.crm_cust_info as a
LEFT JOIN silver.erp_cust_az12 as b
ON a.cst_key=b.cid
LEFT JOIN silver.erp_loc_a101 as c
ON a.cst_key=c.cid

GO

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO
CREATE VIEW gold.dim_product as
SELECT
ROW_NUMBER() OVER(ORDER BY d.prd_start_dt,d.prd_key) as product_key, 
d.prd_id as product_id,
d.prd_key as product_number,
d.prd_nm as product_name,
d.cat_id as category_id,
e.cat as category,
e.subcat as subcategory,
e.maintenance,
d.prd_cost as cost,
d.prd_line as product_line,
d.prd_start_dt as start_date
FROM silver.crm_prd_info as d
LEFT JOIN silver.erp_px_cat_g1v2 as e
on d.cat_id=e.id
where d.prd_end_dt is null

GO

-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
select
x.sls_ord_num as order_number,
y.product_key,
z.customer_key,
x.sls_order_dt as order_date,
x.sls_ship_dt as shipping_date,
x.sls_due_dt as due_date,
x.sls_sales as sales_amount,
x.sls_quantity as quantity,
x.sls_price as price
from silver.crm_sales_details as x
LEFT JOIN gold.dim_product as y
ON x.sls_prd_key=y.product_number
LEFT JOIN gold.dim_customers as z
ON x.sls_cust_id=z.customer_id   
