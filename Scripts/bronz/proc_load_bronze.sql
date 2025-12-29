/*
====================================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
====================================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files.
    It performes the following actions:
    - Truncate the bronze tables before loading data.
    - Uses the 'BULK INSERT' command to load data from csv Files to bronze tables.

Parameters:
    None.
  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
=====================================================================================
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @total_start_time DATETIME,@total_end_time DATETIME;
	SET @total_start_time = GETDATE()
	BEGIN TRY
		print '===========================================================';
		print 'Loading Bronze Layer';
		print '===========================================================';

		print '-----------------------------------------------------------';
		print 'Loading CRM Tables';
		print '-----------------------------------------------------------';

		SET @start_time = GETDATE();
		print '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;

		print '>> Inserting Data Into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		from 'D:\Data Engineer\SQL\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		print '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time,@end_time) AS NVARCHAR) + ' Seconds';
		print '----------------------------';

		SET @start_time = GETDATE();
		print '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		print '>> Inserting Data Into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		from 'D:\Data Engineer\SQL\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		print '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time,@end_time) AS NVARCHAR) + ' Seconds';
		print '----------------------------';

		SET @start_time = GETDATE();
		print '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		print '>> Inserting Data Into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		from 'D:\Data Engineer\SQL\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		print '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time,@end_time) AS NVARCHAR) + ' Seconds';
		print '----------------------------';
	
		print '-----------------------------------------------------------';
		print 'Loading ERP Tables';
		print '-----------------------------------------------------------';
		SET @start_time = GETDATE();
		print '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE [bronze].[erp_cust_az12];

		print '>> Inserting Data Into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		from 'D:\Data Engineer\SQL\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		with (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		print '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time,@end_time) AS NVARCHAR) + ' Seconds';
		print '----------------------------';

		SET @start_time = GETDATE();
		print '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE [bronze].[erp_loc_a101];

		print '>> Inserting Data Into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		from 'D:\Data Engineer\SQL\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		with (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		print '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time,@end_time) AS NVARCHAR) + ' Seconds';
		print '----------------------------';

		SET @start_time = GETDATE();
		print '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE [bronze].[erp_px_cat_g1v2];

		print '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		from 'D:\Data Engineer\SQL\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		with (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

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
