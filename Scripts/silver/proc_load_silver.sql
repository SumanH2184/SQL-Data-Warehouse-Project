/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @total_start_time DATETIME,@total_end_time DATETIME;
	SET @total_start_time = GETDATE()
	BEGIN TRY
		print '===========================================================';
		print 'Loading Silver Layer';
		print '===========================================================';

		print '-----------------------------------------------------------';
		print 'Loading CRM Tables';
		print '-----------------------------------------------------------';

		SET @start_time = GETDATE();
		print '>> Truncating Table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		print '>> Inserting Data Into: silver.crm_cust_info';
		insert into silver.crm_cust_info (
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_material_status,
			cst_gndr,
			cst_create_date
		)
		select
			cst_id,
			cst_key,
			trim(cst_firstname) as cst_firstname,
			trim(cst_lastname) as cst_lastname,
			case 
				when upper(trim(cst_material_status))='M' then 'Married'
				when upper(trim(cst_material_status))='S' then 'Single'
				else 'n/a'
			end as cst_material_status, -- Normalize marital status values readable format
			case  
				when upper(trim(cst_gndr))='M' then 'Male'
				when upper(trim(cst_gndr))='F' then 'Female'
				else 'n/a'
			end as cst_gndr, -- Normalize gender values readable format
			cst_create_date
		from(
			select
				*,
				row_number() over (partition by cst_id order by cst_create_date desc) as flag_last
			from bronze.crm_cust_info
			where cst_id is not null
		) as a
		where flag_last=1 -- Select the most recent record per customer
		SET @end_time = GETDATE();
		print '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time,@end_time) AS NVARCHAR) + ' Seconds';
		print '----------------------------';

		SET @start_time = GETDATE();
		print '>> Truncating Table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		print '>> Inserting Data Into: silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info (
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
		SELECT 
			   [prd_id]
			  ,replace(substring([prd_key],1,5),'-','_') as cat_id
			  ,SUBSTRING(prd_key,7, len(prd_key)) as prd_key
			  ,[prd_nm]
			  ,isnull([prd_cost],0) as prd_cost
			  ,CASE upper(trim(prd_line))
					WHEN 'M' THEN 'Mountain'
					WHEN 'R' THEN 'Road'
					WHEN 'S' THEN 'Other Sales'
					WHEN 'T' THEN 'Touring'
					ELSE 'n/a' 
				END as prd_line
			  ,cast([prd_start_dt] as date) as prd_start_dt
			  ,cast(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 as date) as prd_end_dt
		  FROM [bronze].[crm_prd_info]
		  SET @end_time = GETDATE();
		print '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time,@end_time) AS NVARCHAR) + ' Seconds';
		print '----------------------------';

		SET @start_time = GETDATE();
		print '>> Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		print '>> Inserting Data Into: silver.crm_sales_details';
		  insert into silver.crm_sales_details (
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)
		SELECT
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		case when sls_order_dt=0 or len(sls_order_dt)!=8 then NULL
			else cast(cast(sls_order_dt as varchar) as date)
		end as sls_order_dt,
		case when sls_ship_dt=0 or len(sls_ship_dt)!=8 then NULL
			else cast(cast(sls_ship_dt as varchar) as date)
		end as sls_ship_dt,
		case when sls_due_dt=0 or len(sls_due_dt)!=8 then NULL
			else cast(cast(sls_due_dt as varchar) as date)
		end as sls_due_dt,
		CASE WHEN sls_sales is null or sls_sales<=0 or sls_sales != sls_quantity * abs(sls_price)
			THEN sls_quantity * abs(sls_price)
			ELSE sls_sales
		END AS sls_sales,
		sls_quantity,
		CASE WHEN sls_price is null or sls_price<=0 
			THEN sls_sales/nullif(sls_quantity,0)
			ELSE sls_price
		END as sls_price
		 FROM [bronze].[crm_sales_details]
		 SET @end_time = GETDATE();
		print '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time,@end_time) AS NVARCHAR) + ' Seconds';
		print '----------------------------';
		
		print '-----------------------------------------------------------';
		print 'Loading ERP Tables';
		print '-----------------------------------------------------------';

		SET @start_time = GETDATE();
		print '>> Truncating Table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		print '>> Inserting Data Into: silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12 (cid,bdate,gen)
		SELECT
		CASE WHEN cid like 'NAS%' THEN SUBSTRING(cid,4,len(cid))
			ELSE cid
		END as cid,
		CASE WHEN bdate > GETDATE()
			THEN NULL
			ELSE bdate
		END as bdate,
		CASE WHEN upper(trim(gen)) in ('F','FEMALE') THEN 'Female'
			WHEN upper(trim(gen)) in ('M','MALE') THEN 'Male'
			ELSE 'n/a'
		END as gen
		FROM [bronze].[erp_cust_az12]
		SET @end_time = GETDATE();
		print '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time,@end_time) AS NVARCHAR) + ' Seconds';
		print '----------------------------';

		SET @start_time = GETDATE();
		print '>> Truncating Table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		print '>> Inserting Data Into: silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101 (cid,cntry)
		SELECT
		replace(cid,'-','') as cid,
		CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			WHEN TRIM(cntry) in ('USA','US') THEN 'United States'
			WHEN TRIM(cntry) is null or trim(cntry) = '' THEN 'n/a'
			ELSE TRIM(cntry)
		END as cntry
		FROM [bronze].[erp_loc_a101]
		SET @end_time = GETDATE();
		print '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time,@end_time) AS NVARCHAR) + ' Seconds';
		print '----------------------------';

		SET @start_time = GETDATE();
		print '>> Truncating Table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		print '>> Inserting Data Into: silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2 ( id,cat,subcat,maintenance)
		SELECT 
		id,
		cat,
		subcat,
		maintenance
		FROM [bronze].[erp_px_cat_g1v2]
		SET @end_time = GETDATE();
		print '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time,@end_time) AS NVARCHAR) + ' Seconds';
		print '----------------------------';
	END TRY
	BEGIN CATCH
	PRINT '===================================================';
	PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
	PRINT 'Error Message' + ERROR_MESSAGE();
	PRINT 'Error Number' + CAST(ERROR_NUMBER() AS NVARCHAR);
	PRINT 'Error State' + CAST(ERROR_STATE() AS NVARCHAR);
	PRINT '================================= ==================';
	END CATCH
	SET @total_end_time = GETDATE();
	print '----------------------------'
	print '>> Total Load Duration: ' + CAST(DATEDIFF(SECOND, @total_start_time,@total_end_time) AS NVARCHAR) + ' Seconds';
	print '----------------------------';
END
