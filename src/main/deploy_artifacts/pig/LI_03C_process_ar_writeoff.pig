/*
            Script Name : LI_03C_process_ar_writeoff.pig
 jUnit Test Script Name : PigUnit03CProcessArWriteoff.java
                Purpose : pig script for processing the AR data from OBI.
                          The script processes financial writeoffs.
             Dependency : LI_01_ingest_dimensions.pig                          
*/

SET pig.exec.mapPartAgg '$pig_exec_mappart_agg_enabled';
SET pig.user.cache.enabled '$pig_user_cache_mode_enabled';
SET pig.auto.local.enabled '$pig_auto_local_mode_enabled';
SET pig.local.io.sort.mb '$pig_auto_local_io_sort_mb';
SET pig.auto.local.input.maxbytes '$pig_auto_local_input_maxbytes';

IMPORT '$brrLineItemPigDir/LI_01_ingest_dimensions.pig';

financial_writeoff = LOAD '$brrLineItemSqoopDir/financial_writeoff/part*' using PigStorage('$delimiter') AS
                      (
                       line_item_balance_id:chararray,
                       acct_id:chararray,
                       payment_method_id:chararray,
                       created_dt:chararray,
                       pm_id:chararray,
                       lineitem:chararray,
                       amount:double,
                       sid:chararray,
                       bid:chararray,
                       global_acct_id:chararray,
                       line_item_bill_id:chararray,
                       cycle:chararray,
                       bill_dt:chararray,
                       brandi:chararray,
                       prior_osb_bucket:chararray,
                       pm_acct_num:chararray
                      );
                                          
financial_writeoff_xform = FOREACH financial_writeoff GENERATE 
							created_dt AS bill_applied_date, 
							line_item_balance_id AS line_item_balance_id, 
							'-1' AS cash_credit_id, 
							'0' AS payment_processor_activity, 
							(
							  (prior_osb_bucket IS NOT NULL AND prior_osb_bucket=='3') ? 'E3' :  
							                      ((prior_osb_bucket IS NOT NULL AND (prior_osb_bucket=='2' OR prior_osb_bucket=='0')) ? 'E2' : 'E')
							
							) AS trans_type,
							'0' AS retry_ct, 
							'A'  AS result_code, 
							acct_id AS acct_id, 
							created_dt AS trans_date, 
							pm_id AS pm_id, 
							lineitem AS lineitem, 
							'' AS seq_no, 
							'' AS reason_code, 
							ABS(amount) AS trans_amt, 
							sid AS sid, 
							bid AS bid,
							global_acct_id AS global_acct_id, 
							'-1' AS processing_division, 
							'0' AS payment_processor, 
							'0' AS campaign_id, 
							line_item_bill_id AS line_item_bill_id, 
							((cycle IS NULL) ? '0' : cycle) AS bill_cycle, 
							bill_dt AS bill_dt,
							'AR' AS write_off_source, 
							acct_id AS master_acct_num, 
							ABS(amount) AS bill_applied_amt, 
							0 AS charge_amt, 
							0 AS bill_amt, 
							'0' AS resub_flg, 
							brandi AS brandi, 
							payment_method_id AS payment_method_id,
							pm_acct_num AS pm_acct_num;
					
financial_writeoff_group = GROUP financial_writeoff_xform BY (line_item_balance_id);
financial_writeoff_dedup = FOREACH 	financial_writeoff_group {
                                    sorted = ORDER financial_writeoff_xform by bill_dt DESC;
                                    top_rec = LIMIT sorted 1;
                                    GENERATE FLATTEN(top_rec);  
                                    };

---------------------------------------------------------------------------------------------------                                    
-- Dataset Name - financial_writeoff_dedup 
-- The output be input to 'GATHER' and then to 'Generate BRR line item extract' subgraph
---------------------------------------------------------------------------------------------------  

--Datset financial_writeoff_dedup will be 'GATHER' and then to 'Generate BRR line item extract' subgraph
STORE financial_writeoff_dedup INTO '$brrLineItemInterimDir/LI_03C_process_ar_writeoff' USING PigStorage('$delimiter');  
                                   