/*
            Script Name : LI_03D_process_ar_bill.pig
 jUnit Test Script Name : PigUnit03DProcessArBill.java
                Purpose : pig script for processing the AR data from OBI.
                          The script processes Billed transactions.
             Dependency : LI_01_ingest_dimensions.pig                          
*/

SET pig.exec.mapPartAgg '$pig_exec_mappart_agg_enabled';
SET pig.user.cache.enabled '$pig_user_cache_mode_enabled';
SET pig.auto.local.enabled '$pig_auto_local_mode_enabled';
SET pig.local.io.sort.mb '$pig_auto_local_io_sort_mb';
SET pig.auto.local.input.maxbytes '$pig_auto_local_input_maxbytes';

DEFINE COALESCE datafu.pig.util.Coalesce();

bill_transactions = LOAD '$brrLineItemSqoopDir/bill_transactions/part*' using PigStorage('$delimiter') AS
                      (
                       bid:chararray,
                       global_acct_id:chararray,
                       acct_id:chararray,
                       payment_method_id:chararray,
                       created_dt:chararray,
                       first_bill_flag:chararray,
                       line_item_balance_id:chararray,
                       gross_amount:double,
                       cycle:chararray,
                       sid:chararray,
                       bill_dt:chararray,
                       line_item_bill_id:chararray,
                       lineitem:chararray,
                       bill_amount:double,
                       last_transaction_amount:double,
                       credited_amount:double,
                       pm_id:chararray,
                       brandi:chararray,
                       pm_acct_num:chararray
                      );
                      
bill_transactions_xform = FOREACH bill_transactions GENERATE 
						    ToString(AddDuration(ToDate(created_dt,'yyyy-MM-dd HH:mm:ss'),'PT15H'),'yyyy-MM-dd HH:mm:ss') AS bill_applied_date,
							line_item_balance_id AS line_item_balance_id,
							'-1' AS cash_credit_id,
							'0' AS payment_processor_activity,
							'BL' AS trans_type,
							'0' AS retry_count,
							'A' AS result_code,
							acct_id AS acct_id,
							created_dt AS trans_date,
							pm_id AS pm_id,
							lineitem AS lineitem,
							'' AS seq_no,
							'' AS reason_code,
							last_transaction_amount AS trans_amt,
							sid AS sid,
							bid AS bid,
							global_acct_id AS global_acct_id,
							'0' AS processing_division,
							'0' AS payment_processor,
							'0' AS campaign_id,
							line_item_bill_id AS line_item_bill_id,							
							COALESCE(cycle,'0') AS bill_cycle,
							bill_dt AS bill_dt,
							'' AS write_off_source,
							acct_id AS master_acct_num,
							0 AS bill_applied_amt,
							gross_amount AS charge_amt,
							last_transaction_amount AS bill_amt,
							((TRIM(first_bill_flag) == '1') ? '0' : '1') AS resub_flg,
							brandi AS brandi,
							payment_method_id AS payment_method_id,
							pm_acct_num AS pm_acct_num;                               						 
  
---------------------------------------------------------------------------------------------------                                    
-- Dataset Name - bill_transactions_xform  
-- The output be input to 'GATHER' and then to 'Generate BRR line item extract' subgraph
--------------------------------------------------------------------------------------------------- 

 --Datset bill_transactions_xform will be 'GATHER' and then to 'Generate BRR line item extract' subgraph
STORE bill_transactions_xform INTO '$brrLineItemInterimDir/LI_03D_process_ar_bill' USING PigStorage('$delimiter');                       