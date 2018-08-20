/*
            Script Name : LI_07_process_ar_trans_type.pig
 jUnit Test Script Name : PigUnit07ProcessArTransType.java
                Purpose : pig script to populate trans_type , write the logs and error files for transaction types.
             Dependency : LI_06_process_ar_trans_type.pig.                          
*/

SET pig.exec.mapPartAgg '$pig_exec_mappart_agg_enabled';
SET pig.user.cache.enabled '$pig_user_cache_mode_enabled';
SET pig.auto.local.enabled '$pig_auto_local_mode_enabled';
SET pig.local.io.sort.mb '$pig_auto_local_io_sort_mb';
SET pig.auto.local.input.maxbytes '$pig_auto_local_input_maxbytes';

DEFINE Over org.apache.pig.piggybank.evaluation.Over('int');
DEFINE Stitch org.apache.pig.piggybank.evaluation.Stitch();
DEFINE COALESCE datafu.pig.util.Coalesce();

IMPORT '$brrLineItemPigDir/LI_01_ingest_dimensions.pig';

trans_log_input = LOAD '$brrLineItemInterimDir/LI_06_process_ar_trans_type/part*' using PigStorage('$delimiter') AS
	                       (
	                        bill_applied_date:chararray,
							line_item_balance_id:long,
							cash_credit_id:chararray,
							payment_processor_activity:chararray,
							trans_type1:chararray,
							retry_count:chararray,
							result_code:chararray,
							account_no:chararray,
							trans_date:chararray,
							pay_method:chararray,
							line_item:chararray,
							seq_no:chararray,
							reason_code:chararray,
							trans_amount:double,
							sid:chararray,
							bid:chararray,
							global_acct_id:chararray,
							processing_division:chararray,
							payment_processor:chararray,
							campaign_id:chararray,
							bill_id:long,
							bill_cycle:chararray,
							bill_date:chararray,
							write_off_source:chararray,
							master_acct_num:chararray,
							bill_applied_amount:double,
							charge_amount:double,
							bill_amount:double,
							resub_flg:chararray,
							brandi:chararray,
							payment_method_id:chararray,
							pm_acct_num:chararray
						   );
						   
trans_log_input_coalesced = FOREACH trans_log_input GENERATE 
                            bill_applied_date AS bill_applied_date,
							line_item_balance_id AS line_item_balance_id,
							cash_credit_id AS cash_credit_id,
							payment_processor_activity AS payment_processor_activity,
							trans_type1 AS trans_type1,
							COALESCE(retry_count,'0') AS retry_count,
							result_code AS result_code,
							account_no AS account_no,
							trans_date AS trans_date,
							pay_method AS pay_method,
							line_item AS line_item,
							seq_no AS seq_no,
							reason_code AS reason_code,
							trans_amount AS trans_amount,
							sid AS sid,
							bid AS bid,
							global_acct_id AS global_acct_id,
							processing_division AS processing_division,
							payment_processor AS payment_processor,
							campaign_id AS campaign_id,
							bill_id AS bill_id,
							bill_cycle AS bill_cycle,
							bill_date AS bill_date,
							write_off_source AS write_off_source,
							master_acct_num AS master_acct_num,
							bill_applied_amount AS bill_applied_amount,
							charge_amount AS charge_amount,
							bill_amount AS bill_amount,
							resub_flg AS resub_flg,
							brandi AS brandi,
							payment_method_id AS payment_method_id,
							pm_acct_num AS pm_acct_num;						   
						   
trans_log_group = GROUP trans_log_input_coalesced BY (line_item_balance_id,payment_processor_activity,trans_type1,retry_count);	
trans_log_dedup = FOREACH trans_log_group {
                          sorted = ORDER trans_log_input_coalesced BY line_item_balance_id DESC,
                                                                      payment_processor_activity DESC,
                                                                      trans_type1 DESC,
                                                                      retry_count DESC;
                          top_rec = LIMIT sorted 1;
                          GENERATE FLATTEN(top_rec);
                        };
-- Branch 1
trans_log_rollup1 = FOREACH trans_log_group GENERATE 
                   FLATTEN(group),
                   SUM(trans_log_input_coalesced.bill_applied_amount) AS bill_applied_amount,	
                   SUM(trans_log_input_coalesced.charge_amount) AS charge_amount;
                                   
trans_log_rollup_AI1 = JOIN trans_log_dedup BY (top_rec::line_item_balance_id,top_rec::payment_processor_activity,top_rec::trans_type1,top_rec::retry_count) LEFT OUTER,
                           trans_log_rollup1 BY (group::line_item_balance_id,group::payment_processor_activity,group::trans_type1,group::retry_count);
                           

SPLIT trans_log_rollup_AI1 INTO trans_log_null_amount1 if (trans_log_dedup::top_rec::trans_amount IS NULL AND trans_log_rollup1::charge_amount IS NULL),
                                trans_log_valid_amount1 if NOT(trans_log_dedup::top_rec::trans_amount IS NULL AND trans_log_rollup1::charge_amount IS NULL);

-- Dropping not needed fields
trans_log_null_amount1_xform = FOREACH trans_log_null_amount1 GENERATE 
                                       trans_log_dedup::top_rec::bill_applied_date AS bill_applied_date,
                                       trans_log_dedup::top_rec::line_item_balance_id AS line_item_balance_id,
                                       trans_log_dedup::top_rec::cash_credit_id AS cash_credit_id,
                                       trans_log_dedup::top_rec::payment_processor_activity AS payment_processor_activity,
                                       trans_log_dedup::top_rec::trans_type1 AS trans_type1,
                                       trans_log_dedup::top_rec::retry_count AS retry_count,
                                       trans_log_dedup::top_rec::result_code AS result_code,
                                       trans_log_dedup::top_rec::account_no AS account_no,
                                       trans_log_dedup::top_rec::trans_date AS trans_date,
                                       trans_log_dedup::top_rec::pay_method AS pay_method,
                                       trans_log_dedup::top_rec::line_item AS line_item,
                                       trans_log_dedup::top_rec::seq_no AS seq_no,
                                       trans_log_dedup::top_rec::reason_code AS reason_code,
                                       trans_log_dedup::top_rec::trans_amount AS trans_amount,
                                       trans_log_dedup::top_rec::sid AS sid,
                                       trans_log_dedup::top_rec::bid AS bid,
                                       trans_log_dedup::top_rec::global_acct_id AS global_acct_id,
                                       trans_log_dedup::top_rec::processing_division AS processing_division,
                                       trans_log_dedup::top_rec::payment_processor AS payment_processor,
                                       trans_log_dedup::top_rec::campaign_id AS campaign_id,
                                       trans_log_dedup::top_rec::bill_id AS bill_id,
                                       trans_log_dedup::top_rec::bill_cycle AS bill_cycle,
                                       trans_log_dedup::top_rec::bill_date AS bill_date,
                                       trans_log_dedup::top_rec::write_off_source AS write_off_source,
                                       trans_log_dedup::top_rec::master_acct_num AS master_acct_num,
                                       trans_log_rollup1::bill_applied_amount AS bill_applied_amount,
                                       trans_log_rollup1::charge_amount AS charge_amount,
                                       trans_log_dedup::top_rec::bill_amount AS bill_amount,
                                       trans_log_dedup::top_rec::resub_flg AS resub_flg,
                                       trans_log_dedup::top_rec::brandi AS brandi,
                                       trans_log_dedup::top_rec::payment_method_id AS payment_method_id,
                                       trans_log_dedup::top_rec::pm_acct_num AS pm_acct_num;
-- Dropping not needed fields                                       
trans_log_valid_amount1_xform = FOREACH trans_log_valid_amount1 GENERATE 
                                       trans_log_dedup::top_rec::bill_applied_date AS bill_applied_date,
                                       trans_log_dedup::top_rec::line_item_balance_id AS line_item_balance_id,
                                       trans_log_dedup::top_rec::cash_credit_id AS cash_credit_id,
                                       trans_log_dedup::top_rec::payment_processor_activity AS payment_processor_activity,
                                       trans_log_dedup::top_rec::trans_type1 AS trans_type1,
                                       trans_log_dedup::top_rec::retry_count AS retry_count,
                                       trans_log_dedup::top_rec::result_code AS result_code,
                                       trans_log_dedup::top_rec::account_no AS account_no,
                                       trans_log_dedup::top_rec::trans_date AS trans_date,
                                       trans_log_dedup::top_rec::pay_method AS pay_method,
                                       trans_log_dedup::top_rec::line_item AS line_item,
                                       trans_log_dedup::top_rec::seq_no AS seq_no,
                                       trans_log_dedup::top_rec::reason_code AS reason_code,
                                       trans_log_dedup::top_rec::trans_amount AS trans_amount,
                                       trans_log_dedup::top_rec::sid AS sid,
                                       trans_log_dedup::top_rec::bid AS bid,
                                       trans_log_dedup::top_rec::global_acct_id AS global_acct_id,
                                       trans_log_dedup::top_rec::processing_division AS processing_division,
                                       trans_log_dedup::top_rec::payment_processor AS payment_processor,
                                       trans_log_dedup::top_rec::campaign_id AS campaign_id,
                                       trans_log_dedup::top_rec::bill_id AS bill_id,
                                       trans_log_dedup::top_rec::bill_cycle AS bill_cycle,
                                       trans_log_dedup::top_rec::bill_date AS bill_date,
                                       trans_log_dedup::top_rec::write_off_source AS write_off_source,
                                       trans_log_dedup::top_rec::master_acct_num AS master_acct_num,
                                       trans_log_rollup1::bill_applied_amount AS bill_applied_amount,
                                       trans_log_rollup1::charge_amount AS charge_amount,
                                       trans_log_dedup::top_rec::bill_amount AS bill_amount,
                                       trans_log_dedup::top_rec::resub_flg AS resub_flg,
                                       trans_log_dedup::top_rec::brandi AS brandi,
                                       trans_log_dedup::top_rec::payment_method_id AS payment_method_id,
                                       trans_log_dedup::top_rec::pm_acct_num AS pm_acct_num;                                      
                                                                                                                                                                             
STORE trans_log_null_amount1_xform INTO '$brrLineItemRejectsDir/LI_07_process_ar_trans_type_null_amount' USING PigStorage('$delimiter');                               
STORE trans_log_valid_amount1_xform INTO '$brrLineItemInterimDir/LI_07_process_ar_trans_type' USING PigStorage('$delimiter');           				   

--Branch 2
trans_log_add_row_num = FOREACH trans_log_group {
                        sorted = ORDER trans_log_input_coalesced BY line_item_balance_id DESC,
                                                            payment_processor_activity DESC,
                                                            trans_type1 DESC,
                                                            retry_count DESC;
                        GENERATE FLATTEN (Stitch(sorted, Over(sorted.account_no, 'row_number')));                                     
                        }; 

SPLIT trans_log_add_row_num INTO trans_log_uniques if ($32 == 1),
                                 trans_log_dups if ($32 > 1);

--Dataset Name - trans_log_uniques;                                  
--Dataset Name - trans_log_dups;  

STORE trans_log_uniques INTO '$brrLineItemRejectsDir/LI_07_process_ar_trans_type_uniques' USING PigStorage('$delimiter');                               
STORE trans_log_dups INTO '$brrLineItemRejectsDir/LI_07_process_ar_trans_type_dups' USING PigStorage('$delimiter');  