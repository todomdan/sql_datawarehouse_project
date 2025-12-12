/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/


Create or Alter procedure bronze.load_bronze as begin

begin try 



print '===========================================================================';
print'Loading the bromze layer';
print '===========================================================================';

print'--------------------------------------------------------------------------';
print'loading CRM tables';
print'--------------------------------------------------------------------------';

print ' <<  Truncating table : Bronze.crm_cust_info >>';
print ' << Inserting data into : Bronze.crm_cust_info >>';


truncate table Bronze.crm_cust_info; 
bulk insert  Bronze.crm_cust_info
from 'C:\Users\todom\Downloads\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
with (   firstrow = 2,
       fieldterminator = ',' ,
	   tablock
	   );
	    
--select *from  Bronze.crm_cust_info
--select count(*)  from  Bronze.crm_cust_info


print ' <<  Truncating table : Bronze.crm_prd_info  >>';
print ' << Inserting data into : Bronze.crm_prd_info >>';


truncate table Bronze.crm_prd_info 
bulk insert Bronze.crm_prd_info 
from 'C:\Users\todom\Downloads\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
with (   firstrow = 2,
       fieldterminator = ',' ,
	   tablock 
	   );


print ' <<  Truncating table :Bronze.crm_sales_details  >>';
print ' << Inserting data into :Bronze.crm_sales_details >>';


truncate table Bronze.crm_sales_details 
bulk insert Bronze.crm_sales_details
from 'C:\Users\todom\Downloads\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
with (   firstrow = 2,
       fieldterminator = ',' ,
	   tablock 
	   ); 


print'--------------------------------------------------------------------------';
print'loading ERP layer';
print'--------------------------------------------------------------------------';

print ' <<  Truncating table :Bronze.erp_cust_az12  >>';
print ' << Inserting data into :Bronze.erp_cust_az12  >>';


truncate table Bronze.erp_cust_az12 
bulk insert Bronze.erp_cust_az12
from 'C:\Users\todom\Downloads\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
with (   firstrow = 2,
       fieldterminator = ',' ,
	   tablock 
	   ); 
	   

print ' <<  Truncating table :[Bronze].[Bronze.erp_loc_a101] >>';
print ' << Inserting data into :[Bronze].[Bronze.erp_loc_a101]>>';


truncate table[Bronze].[Bronze.erp_loc_a101]
bulk insert [Bronze].[Bronze.erp_loc_a101]
from 'C:\Users\todom\Downloads\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
with (   firstrow = 2,
       fieldterminator = ',' ,
	   tablock
	   ); 
	   
	   --Exec sp_rename '[Bronze].[Bronze.erp_looc_a101]', 'Bronze.erp_loc_a101'
 --select *from  [Bronze].[Bronze.erp_loc_a101];


 print ' <<  Truncating table :Bronze.erp_px_cat_g1v2 >>';
print ' << Inserting data into :Bronze.erp_px_cat_g1v2 >>';



 truncate table Bronze.erp_px_cat_g1v2
bulk insert Bronze.erp_px_cat_g1v2
from 'C:\Users\todom\Downloads\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
with (   firstrow = 2,
       fieldterminator = ',' ,
	   tablock      
	   ); 

end try 
begin catch 
end catch 

  end
