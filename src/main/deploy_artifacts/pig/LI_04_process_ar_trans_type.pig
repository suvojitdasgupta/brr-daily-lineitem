/*
            Script Name : LI_04_process_ar_trans_type.pig
 jUnit Test Script Name : PigUnit04ProcessArTransType.java
                Purpose : pig script to determine transaction_type of AR transaction. This is Script 1 or X. 
                          All dimension data needs to be imported in this script.
             Dependency : LI_03A_process_ar_credits.pig.                          
*/

SET pig.exec.mapPartAgg '$pig_exec_mappart_agg_enabled';
SET pig.user.cache.enabled '$pig_user_cache_mode_enabled';
SET pig.auto.local.enabled '$pig_auto_local_mode_enabled';
SET pig.local.io.sort.mb '$pig_auto_local_io_sort_mb';
SET pig.auto.local.input.maxbytes '$pig_auto_local_input_maxbytes';

%declare  BAT_PP_CHARGEBACK  '500';
%declare  BAT_MANUAL_CHARGEBACK '600';
%declare  BAT_ACC_CHARGEBACK '1010';

IMPORT '$brrLineItemPigDir/LI_01_ingest_dimensions.pig';

trans_type_input = LOAD '$brrLineItemInterimDir/LI_03A_process_ar_credits/part*' using PigStorage('$delimiter') AS
	                      (
							line_item_balance_id:chararray,
							approve:chararray,
							forced_trans_flag:chararray,
							ar_result:chararray,
							acct_id:chararray,
							payment_method_id:chararray,
							bill_activity_type:chararray,
							bill_dt:chararray,
							ppa_created_dt:chararray,
							pm_id:chararray,
							lineitem:chararray,
							retry_count:chararray,
							action_code:chararray,
							reason_code:chararray,
							processed_amount:double,
							sid:chararray,
							bid:chararray,
							global_acct_id:chararray,
							processing_division:chararray,
							ppv_id:chararray,
							campaign_id:chararray,
							line_item_bill_id:chararray,
							cycle:chararray,
							payment_processor_activity_id:chararray,
							libh_created_dt:chararray,
							activity_amount:double,
							net_amount:double,
							first_bill_flag:chararray,
							bill_amount:double,
							cb_created_dt:chararray,
							bsh_status:chararray,
							brandi:chararray,
							prior_osb_bucket:chararray,
							merchant_order_number:chararray,
							pm_acct_num:chararray
	                      );
	                     
bad_debt_writeoff = LOAD '$brrLineItemSqoopDir/bad_debt_writeoff_counts/part*' using PigStorage('$delimiter') AS
	                      (
	                        line_item_balance_id:chararray,
	                        count_bad_debt:double,
	                        count_uppaid:double	 
	                      );                     

 
SPLIT trans_type_input INTO trans_type_filter_Y IF (
                                                    ( approve=='1' 
                                                    OR
	                                                  (
	                                                   bill_activity_type == '${BAT_PP_CHARGEBACK}' OR
	                                                   bill_activity_type == '${BAT_MANUAL_CHARGEBACK}' OR
	                                                   bill_activity_type == '${BAT_ACC_CHARGEBACK}'
	                                                  )
	                                                )
                                                    AND
                                                    activity_amount == 0 
                                                    ),
                             trans_type_filter_N OTHERWISE;
                                                                                                  
trans_type_join = JOIN  trans_type_filter_N BY (line_item_balance_id) LEFT OUTER, bad_debt_writeoff BY (line_item_balance_id);
trans_type_xform = FOREACH trans_type_join GENERATE 
                            trans_type_filter_N::line_item_balance_id AS line_item_balance_id,
                            
                            (
                             ( bad_debt_writeoff::count_bad_debt IS NOT NULL AND 
                               bad_debt_writeoff::count_bad_debt == 1.0
                             ) ? 1 : ( 
                                        ( bad_debt_writeoff::count_bad_debt IS NOT NULL AND
                                          bad_debt_writeoff::count_bad_debt > 1.0
                                        ) ? 2 : 0
                                     )
                            
							) AS witternoff_bad_debt_cnt,
							
							trans_type_filter_N::approve AS approve,
							trans_type_filter_N::forced_trans_flag AS forced_trans_flag,
							trans_type_filter_N::ar_result AS ar_result,
							trans_type_filter_N::acct_id AS acct_id,
							trans_type_filter_N::payment_method_id AS payment_method_id,
							trans_type_filter_N::bill_activity_type AS bill_activity_type,
							trans_type_filter_N::bill_dt AS bill_dt,
							trans_type_filter_N::ppa_created_dt AS ppa_created_dt,
							trans_type_filter_N::pm_id AS pm_id,
							trans_type_filter_N::lineitem AS lineitem,
							trans_type_filter_N::retry_count AS retry_count,
							trans_type_filter_N::action_code AS action_code,
							trans_type_filter_N::reason_code AS reason_code,
							trans_type_filter_N::processed_amount AS processed_amount,
							trans_type_filter_N::sid AS sid,
							trans_type_filter_N::bid AS bid,
							trans_type_filter_N::global_acct_id AS global_acct_id,
							trans_type_filter_N::processing_division AS processing_division,
							trans_type_filter_N::ppv_id AS ppv_id,
							trans_type_filter_N::campaign_id AS campaign_id,
							trans_type_filter_N::line_item_bill_id AS line_item_bill_id,
							trans_type_filter_N::cycle AS cycle,
							trans_type_filter_N::payment_processor_activity_id AS payment_processor_activity_id,
							trans_type_filter_N::libh_created_dt AS libh_created_dt,
							trans_type_filter_N::activity_amount AS activity_amount,
							trans_type_filter_N::net_amount AS net_amount,
							trans_type_filter_N::first_bill_flag AS first_bill_flag,
							trans_type_filter_N::bill_amount AS bill_amount,
							trans_type_filter_N::cb_created_dt AS cb_created_dt,
							trans_type_filter_N::bsh_status AS bsh_status,
							trans_type_filter_N::brandi AS brandi,
							trans_type_filter_N::prior_osb_bucket AS prior_osb_bucket,
							trans_type_filter_N::merchant_order_number AS merchant_order_number,
							trans_type_filter_N::pm_acct_num AS pm_acct_num;							
--Dataset Name - trans_type_xform					

--Datset for intermediate storage 
STORE trans_type_xform INTO '$brrLineItemInterimDir/LI_04_process_ar_trans_type' USING PigStorage('$delimiter');  

--Dataset for Zero dollar rows. 
STORE trans_type_filter_Y INTO '$brrLineItemRejectsDir/LI_04_process_ar_trans_type' USING PigStorage('$delimiter');                                                             