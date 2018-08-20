/*
            Script Name : LI_03B_process_ar_refunds.pig
 jUnit Test Script Name : PigUnit03AProcessArCredits.java
                Purpose : pig script for processing the AR refunds data from OBI.
                          All dimension data needs to be imported in this script.
             Dependency : LI_01_ingest_dimensions.pig.                          
*/

SET pig.exec.mapPartAgg '$pig_exec_mappart_agg_enabled';
SET pig.user.cache.enabled '$pig_user_cache_mode_enabled';
SET pig.auto.local.enabled '$pig_auto_local_mode_enabled';
SET pig.local.io.sort.mb '$pig_auto_local_io_sort_mb';
SET pig.auto.local.input.maxbytes '$pig_auto_local_input_maxbytes';

DEFINE COALESCE datafu.pig.util.Coalesce();
IMPORT '$brrLineItemPigDir/LI_01_ingest_dimensions.pig';

%declare REFUND_ANNUAL_REASON_CODE 510
%declare REASON_CODE 450
%declare BAT_BILLING_CHGBK 900
%declare EMPTY ''

cash_credit_history = LOAD '$brrLineItemSqoopDir/cash_credit_history/part*' using PigStorage('$delimiter') AS
                      (
                       approve:chararray,
                       acct_id:chararray,
                       payment_method_id:chararray,
                       processed_dt:chararray,
                       pm_id:chararray,
                       lineitem:chararray,
                       line_item_bill_id:chararray,
                       bill_activity_type:chararray,
                       reason_code:chararray,
                       action_code:chararray,
                       credit_amount:double,
                       sid:chararray,
                       bid:chararray,
                       global_acct_id:chararray,
                       processing_division:chararray,
                       ppv_id:chararray,
                       campaign_id:chararray,
                       cash_credit_id:chararray,
                       payment_processor_activity_id:chararray,
                       created_dt:chararray,
                       credited_amount:double,
                       brandi:chararray,
                       cycle:chararray,
                       cc_reason_code:chararray,
                       txn_id:chararray,
                       taxed_flag:chararray,
                       credit_type_id:chararray,
                       pm_acct_num:chararray,
                       line_item_balance_id:chararray
                      );
                      
line_item_balance = LOAD '$brrLineItemSqoopDir/tax_line_item_balance/part*' using PigStorage('$delimiter') AS
                      (
                        line_item_balance_id:chararray,
                        acct_id:chararray,
                        reason_text:chararray,
                        gross_amount:double
                      );
                        
balance_impact = LOAD '$brrLineItemSqoopDir/tax_balance_impact/part*' using PigStorage('$delimiter') AS
                      (
                        line_item_balance_id:chararray,
                        reason_text:chararray,
                        gross_amount:double
                      );
                      
 cash_credit_history_filter = FILTER cash_credit_history BY
                             (
                               bill_activity_type == '500' OR
                               (
                                 bill_activity_type == '100' AND 
                                  (
                                    action_code == 'N' OR
                                    action_code == 'R' OR
                                    cc_reason_code == '${REFUND_ANNUAL_REASON_CODE}' OR
                                    cc_reason_code == '${REASON_CODE}'
                                  )   
                               )
                             ); 
                                                                                        
-------------------------------------------					
-- Component : refund transactions - Start
-------------------------------------------
  
refund_trans = FOREACH cash_credit_history_filter GENERATE 
                            created_dt AS bill_applied_date,
                            COALESCE(line_item_balance_id,'-1') AS line_item_balance_id,
							cash_credit_id AS cash_credit_id,
							payment_processor_activity_id AS payment_processor_activity,
							
                            (
                             (bill_activity_type == '500') ? 'U' 
                                                           : 
                                                           ( (cc_reason_code == '${REASON_CODE}') ? 'RO' 
                                                                                                  : ( (cc_reason_code == '${REFUND_ANNUAL_REASON_CODE}') ? 'RA' 
                                                                                                                                                         : 
                                                                                                                                                         action_code )
                                                                                                    )

                            ) AS trans_type,                         
							'0' AS retry_count,
							((approve == '1') ? 'A':'C' ) AS result_code,
							acct_id AS acct_id,
							processed_dt AS trans_date,
							pm_id as pm_id,
							lineitem AS lineitem,
							'' AS seq_no,
							reason_code AS reason_code,
							ABS(credit_amount) AS trans_amt,
							sid AS sid,
							bid AS bid,
							global_acct_id AS global_acct_id,
							(
							 (processing_division IS NULL OR bill_activity_type == '${BAT_BILLING_CHGBK}' OR ppv_id != '2') ?  '-1' : processing_division
							) AS processing_division,
							ppv_id AS payment_processor,							
							COALESCE(campaign_id,'0') AS campaign_id,						
							line_item_bill_id AS line_item_bill_id,							
							COALESCE(cycle,'0') AS bill_cycle,		
							(
							  (TRIM(processed_dt) IS NULL OR TRIM(processed_dt) == '$EMPTY') ? created_dt : processed_dt
							) AS bill_date,
							cc_reason_code AS write_off_source,
							acct_id AS master_acct_num,
							ABS(credited_amount) AS bill_applied_amt,
							0 AS charge_amt,
							0 AS bill_amt,
							'0' AS resub_flg,
							brandi AS brandi,
							payment_method_id AS payment_method_id,
							taxed_flag AS taxed_flag,
							pm_acct_num AS pm_acct_num;
						
refund_trans_tax = FILTER refund_trans BY (taxed_flag == '1');
refund_trans_tax1 = FILTER refund_trans_tax BY (line_item_balance_id =='-1');

refund_trans_tax1_join = JOIN refund_trans_tax1 BY (acct_id) , line_item_balance BY (acct_id);
refund_trans_tax1_split = FOREACH refund_trans_tax1_join GENERATE 
                                  refund_trans_tax1::bill_applied_date AS bill_applied_date,
                                  line_item_balance::line_item_balance_id AS line_item_balance_id, --Pick balance_id from line_item_balance dataset
                                  refund_trans_tax1::cash_credit_id AS cash_credit_id,
                                  refund_trans_tax1::payment_processor_activity AS payment_processor_activity,
                                  refund_trans_tax1::trans_type AS trans_type,
                                  refund_trans_tax1::retry_count AS retry_count,
                                  refund_trans_tax1::result_code AS result_code,
                                  refund_trans_tax1::acct_id AS acct_id,
                                  refund_trans_tax1::trans_date AS trans_date,
                                  refund_trans_tax1::pm_id AS pm_id,
                                  refund_trans_tax1::lineitem AS lineitem,
                                  refund_trans_tax1::seq_no AS seq_no,
                                  refund_trans_tax1::reason_code AS reason_code,
                                  refund_trans_tax1::trans_amt AS trans_amt,
                                  refund_trans_tax1::sid AS sid,
                                  refund_trans_tax1::bid AS bid,
                                  refund_trans_tax1::global_acct_id AS global_acct_id,
                                  refund_trans_tax1::processing_division AS processing_division,
                                  refund_trans_tax1::payment_processor AS payment_processor,
                                  refund_trans_tax1::campaign_id AS campaign_id,
                                  refund_trans_tax1::line_item_bill_id AS line_item_bill_id,
                                  refund_trans_tax1::bill_cycle AS bill_cycle,
                                  refund_trans_tax1::bill_date AS bill_date,
                                  refund_trans_tax1::write_off_source AS write_off_source,
                                  refund_trans_tax1::master_acct_num AS master_acct_num,
                                  refund_trans_tax1::bill_applied_amt AS bill_applied_amt,
                                  refund_trans_tax1::charge_amt AS charge_amt,
                                  refund_trans_tax1::bill_amt AS bill_amt,
                                  refund_trans_tax1::resub_flg AS resub_flg,
                                  refund_trans_tax1::brandi AS brandi,
                                  refund_trans_tax1::payment_method_id AS payment_method_id,
                                  refund_trans_tax1::taxed_flag AS taxed_flag,
                                  refund_trans_tax1::pm_acct_num AS pm_acct_num, 
                                  line_item_balance::line_item_balance_id AS lib_line_item_balance_id,
                                  line_item_balance::acct_id AS lib_acct_id,
                                  FLATTEN(STRSPLIT (line_item_balance::reason_text,'=',2)) AS (lib_reason_text_desc:chararray, 
                                                                                               lib_reason_text_cash_credit_id:chararray),                                                                                                                                                               
                                  line_item_balance::gross_amount AS lib_gross_amount;

refund_trans_tax1_filter = FILTER refund_trans_tax1_split BY (lib_reason_text_cash_credit_id MATCHES cash_credit_id);
refund_trans_tax1_group = GROUP refund_trans_tax1_filter BY (lib_line_item_balance_id);
refund_trans_tax1_dedup = FOREACH refund_trans_tax1_group {
                                 top_rec = LIMIT refund_trans_tax1_filter 1;
                                 GENERATE FLATTEN(top_rec);
                          };
refund_trans_tax1_sum = FOREACH refund_trans_tax1_group GENERATE 
                                FLATTEN(group) AS line_item_balance_id,
							    ABS(SUM(refund_trans_tax1_filter.lib_gross_amount)) AS trans_amt;

refund_trans_tax1_sum_join = JOIN refund_trans_tax1_dedup BY (top_rec::line_item_balance_id) LEFT OUTER, 
                                    refund_trans_tax1_sum BY (line_item_balance_id);

refund_trans_tax1_final = FOREACH refund_trans_tax1_sum_join GENERATE 
							refund_trans_tax1_dedup::top_rec::bill_applied_date AS bill_applied_date,
							refund_trans_tax1_dedup::top_rec::line_item_balance_id AS line_item_balance_id,
							refund_trans_tax1_dedup::top_rec::cash_credit_id AS cash_credit_id,
							refund_trans_tax1_dedup::top_rec::payment_processor_activity AS payment_processor_activity,
							'RT' AS trans_type,
							refund_trans_tax1_dedup::top_rec::retry_count AS retry_count,
							refund_trans_tax1_dedup::top_rec::result_code AS result_code,
							refund_trans_tax1_dedup::top_rec::acct_id AS acct_id,
							refund_trans_tax1_dedup::top_rec::trans_date AS trans_date,
							refund_trans_tax1_dedup::top_rec::pm_id AS pm_id,
							refund_trans_tax1_dedup::top_rec::lineitem AS lineitem,
							refund_trans_tax1_dedup::top_rec::seq_no AS seq_no,
							refund_trans_tax1_dedup::top_rec::reason_code AS reason_code,
							refund_trans_tax1_sum::trans_amt AS trans_amt,
							refund_trans_tax1_dedup::top_rec::sid AS sid,
							refund_trans_tax1_dedup::top_rec::bid AS bid,
							refund_trans_tax1_dedup::top_rec::global_acct_id AS global_acct_id,
							refund_trans_tax1_dedup::top_rec::processing_division AS processing_division,
							refund_trans_tax1_dedup::top_rec::payment_processor AS payment_processor,
							refund_trans_tax1_dedup::top_rec::campaign_id AS campaign_id,
							refund_trans_tax1_dedup::top_rec::line_item_bill_id AS line_item_bill_id,
							refund_trans_tax1_dedup::top_rec::bill_cycle AS bill_cycle,
							refund_trans_tax1_dedup::top_rec::bill_date AS bill_date,
							refund_trans_tax1_dedup::top_rec::write_off_source AS write_off_source,
							refund_trans_tax1_dedup::top_rec::master_acct_num AS master_acct_num,
							refund_trans_tax1_dedup::top_rec::bill_applied_amt AS bill_applied_amt,
							refund_trans_tax1_dedup::top_rec::charge_amt AS charge_amt,
							refund_trans_tax1_dedup::top_rec::bill_amt AS bill_amt,
							refund_trans_tax1_dedup::top_rec::resub_flg AS resub_flg,
							refund_trans_tax1_dedup::top_rec::brandi AS brandi,
							refund_trans_tax1_dedup::top_rec::payment_method_id AS payment_method_id,
							--refund_trans_tax1_dedup::top_rec::taxed_flag AS taxed_flag,
							refund_trans_tax1_dedup::top_rec::pm_acct_num AS pm_acct_num;				

refund_trans_tax2 = FILTER refund_trans_tax BY NOT(line_item_balance_id =='-1');
refund_trans_tax2_join = JOIN refund_trans_tax2 BY (line_item_balance_id) , balance_impact BY (line_item_balance_id);
refund_trans_tax2_split = FOREACH refund_trans_tax2_join GENERATE 
                                  refund_trans_tax2::bill_applied_date AS bill_applied_date,
                                  refund_trans_tax2::line_item_balance_id AS line_item_balance_id,
                                  refund_trans_tax2::cash_credit_id AS cash_credit_id,
                                  refund_trans_tax2::payment_processor_activity AS payment_processor_activity,
                                  refund_trans_tax2::trans_type AS trans_type,
                                  refund_trans_tax2::retry_count AS retry_count,
                                  refund_trans_tax2::result_code AS result_code,
                                  refund_trans_tax2::acct_id AS acct_id,
                                  refund_trans_tax2::trans_date AS trans_date,
                                  refund_trans_tax2::pm_id AS pm_id,
                                  refund_trans_tax2::lineitem AS lineitem,
                                  refund_trans_tax2::seq_no AS seq_no,
                                  refund_trans_tax2::reason_code AS reason_code,
                                  refund_trans_tax2::trans_amt AS trans_amt,
                                  refund_trans_tax2::sid AS sid,
                                  refund_trans_tax2::bid AS bid,
                                  refund_trans_tax2::global_acct_id AS global_acct_id,
                                  refund_trans_tax2::processing_division AS processing_division,
                                  refund_trans_tax2::payment_processor AS payment_processor,
                                  refund_trans_tax2::campaign_id AS campaign_id,
                                  refund_trans_tax2::line_item_bill_id AS line_item_bill_id,
                                  refund_trans_tax2::bill_cycle AS bill_cycle,
                                  refund_trans_tax2::bill_date AS bill_date,
                                  refund_trans_tax2::write_off_source AS write_off_source,
                                  refund_trans_tax2::master_acct_num AS master_acct_num,
                                  refund_trans_tax2::bill_applied_amt AS bill_applied_amt,
                                  refund_trans_tax2::charge_amt AS charge_amt,
                                  refund_trans_tax2::bill_amt AS bill_amt,
                                  refund_trans_tax2::resub_flg AS resub_flg,
                                  refund_trans_tax2::brandi AS brandi,
                                  refund_trans_tax2::payment_method_id AS payment_method_id,
                                  refund_trans_tax2::taxed_flag AS taxed_flag,
                                  refund_trans_tax2::pm_acct_num AS pm_acct_num,
                                  balance_impact::line_item_balance_id AS bi_line_item_balance_id,
                                  balance_impact::reason_text AS bi_reason_text,
                                  FLATTEN(STRSPLIT (balance_impact::reason_text,'=',2)) AS (bi_reason_text_desc:chararray, 
                                                                                            bi_reason_text_cash_credit_id:chararray),
                                  balance_impact::gross_amount AS bi_gross_amount;
                                  
refund_trans_tax2_filter = FILTER refund_trans_tax2_split BY (bi_reason_text_cash_credit_id MATCHES cash_credit_id);
refund_trans_tax2_group = GROUP refund_trans_tax2_filter BY (bi_line_item_balance_id);

refund_trans_tax2_dedup = FOREACH refund_trans_tax2_group {
                                 top_rec = LIMIT refund_trans_tax2_filter 1;
                                 GENERATE FLATTEN(top_rec);
                          };

refund_trans_tax2_sum = FOREACH refund_trans_tax2_group GENERATE 
                                FLATTEN(group) AS line_item_balance_id,
							    ABS(SUM(refund_trans_tax2_filter.bi_gross_amount)) AS trans_amt;


refund_trans_tax2_sum_join = JOIN refund_trans_tax2_dedup BY (top_rec::line_item_balance_id) LEFT OUTER, 
                                    refund_trans_tax2_sum BY (line_item_balance_id);

refund_trans_tax2_final = FOREACH refund_trans_tax2_sum_join GENERATE 
							      refund_trans_tax2_dedup::top_rec::bill_applied_date AS bill_applied_date,
							      refund_trans_tax2_dedup::top_rec::line_item_balance_id AS line_item_balance_id,
							      refund_trans_tax2_dedup::top_rec::cash_credit_id AS cash_credit_id,
							      refund_trans_tax2_dedup::top_rec::payment_processor_activity AS payment_processor_activity,
							      'RT' AS trans_type,
							      refund_trans_tax2_dedup::top_rec::retry_count AS retry_count,
							      refund_trans_tax2_dedup::top_rec::result_code AS result_code,
							      refund_trans_tax2_dedup::top_rec::acct_id AS acct_id,
							      refund_trans_tax2_dedup::top_rec::trans_date AS trans_date,
							      refund_trans_tax2_dedup::top_rec::pm_id AS pm_id,
							      refund_trans_tax2_dedup::top_rec::lineitem AS lineitem,
							      refund_trans_tax2_dedup::top_rec::seq_no AS seq_no,
							      refund_trans_tax2_dedup::top_rec::reason_code AS reason_code,
							      refund_trans_tax2_sum::trans_amt AS trans_amt,
							      refund_trans_tax2_dedup::top_rec::sid AS sid,
							      refund_trans_tax2_dedup::top_rec::bid AS bid,
							      refund_trans_tax2_dedup::top_rec::global_acct_id AS global_acct_id,
							      refund_trans_tax2_dedup::top_rec::processing_division AS processing_division,
							      refund_trans_tax2_dedup::top_rec::payment_processor AS payment_processor,
							      refund_trans_tax2_dedup::top_rec::campaign_id AS campaign_id,
							      refund_trans_tax2_dedup::top_rec::line_item_bill_id AS line_item_bill_id,
							      refund_trans_tax2_dedup::top_rec::bill_cycle AS bill_cycle,
							      refund_trans_tax2_dedup::top_rec::bill_date AS bill_date,
							      refund_trans_tax2_dedup::top_rec::write_off_source AS write_off_source,
							      refund_trans_tax2_dedup::top_rec::master_acct_num AS master_acct_num,
							      refund_trans_tax2_dedup::top_rec::bill_applied_amt AS bill_applied_amt,
							      refund_trans_tax2_dedup::top_rec::charge_amt AS charge_amt,
							      refund_trans_tax2_dedup::top_rec::bill_amt AS bill_amt,
							      refund_trans_tax2_dedup::top_rec::resub_flg AS resub_flg,
							      refund_trans_tax2_dedup::top_rec::brandi AS brandi,
							      refund_trans_tax2_dedup::top_rec::payment_method_id AS payment_method_id,
							      --refund_trans_tax2_dedup::top_rec::taxed_flag AS taxed_flag,
							      refund_trans_tax2_dedup::top_rec::pm_acct_num AS pm_acct_num;
														
refund_trans_with_gross = UNION refund_trans_tax1_final, refund_trans_tax2_final;
--Dataset Name - refund_trans_with_gross

refund_trans_join = JOIN refund_trans BY (cash_credit_id) FULL OUTER, refund_trans_with_gross BY (cash_credit_id);

refund_trans_join_inner = FILTER refund_trans_join BY (refund_trans::cash_credit_id IS NOT NULL AND refund_trans_with_gross::cash_credit_id IS NOT NULL);
refund_trans_join_unused0 = FILTER refund_trans_join BY (refund_trans::cash_credit_id IS NOT NULL AND refund_trans_with_gross::cash_credit_id IS NULL);

refund_trans_join_inner_xform = FOREACH refund_trans_join_inner GENERATE
								refund_trans::bill_applied_date AS bill_applied_date,
								refund_trans::line_item_balance_id AS line_item_balance_id,
								refund_trans::cash_credit_id AS cash_credit_id,
								refund_trans::payment_processor_activity AS payment_processor_activity,
								refund_trans::trans_type AS trans_type,
								refund_trans::retry_count AS retry_count,
								refund_trans::result_code AS result_code ,
								refund_trans::acct_id AS acct_id,
								refund_trans::trans_date AS trans_date,
								refund_trans::pm_id AS pm_id,
								refund_trans::lineitem AS lineitem,
								refund_trans::seq_no AS seq_no,
								refund_trans::reason_code AS reason_code,
								(refund_trans::trans_amt - refund_trans_with_gross::trans_amt) AS trans_amt,
								refund_trans::sid AS sid,
								refund_trans::bid AS bid,
								refund_trans::global_acct_id AS global_acct_id,
								refund_trans::processing_division AS processing_division,
								refund_trans::payment_processor AS payment_processor,
								refund_trans::campaign_id AS campaign_id,
								refund_trans::line_item_bill_id AS line_item_bill_id,
								refund_trans::bill_cycle AS bill_cycle,
								refund_trans::bill_date AS bill_date,
								refund_trans::write_off_source AS write_off_source,
								refund_trans::master_acct_num AS master_acct_num,
								refund_trans::bill_applied_amt AS bill_applied_amt,
								refund_trans::charge_amt AS charge_amt,
								refund_trans::bill_amt AS bill_amt,
								refund_trans::resub_flg AS resub_flg,
								refund_trans::brandi AS brandi,
								refund_trans::payment_method_id AS payment_method_id,
								--refund_trans::taxed_flag AS taxed_flag,
								refund_trans::pm_acct_num AS pm_acct_num;
								
refund_trans_join_unused0_xform = FOREACH refund_trans_join_unused0 GENERATE
									refund_trans::bill_applied_date AS bill_applied_date,
									refund_trans::line_item_balance_id AS line_item_balance_id,
									refund_trans::cash_credit_id AS cash_credit_id,
									refund_trans::payment_processor_activity AS payment_processor_activity,
									refund_trans::trans_type AS trans_type,
									refund_trans::retry_count AS retry_count,
									refund_trans::result_code AS result_code ,
									refund_trans::acct_id AS acct_id,
									refund_trans::trans_date AS trans_date,
									refund_trans::pm_id AS pm_id,
									refund_trans::lineitem AS lineitem,
									refund_trans::seq_no AS seq_no,
									refund_trans::reason_code AS reason_code,
									refund_trans::trans_amt AS trans_amt,
									refund_trans::sid AS sid,
									refund_trans::bid AS bid,
									refund_trans::global_acct_id AS global_acct_id,
									refund_trans::processing_division AS processing_division,
									refund_trans::payment_processor AS payment_processor,
									refund_trans::campaign_id AS campaign_id,
									refund_trans::line_item_bill_id AS line_item_bill_id,
									refund_trans::bill_cycle AS bill_cycle,
									refund_trans::bill_date AS bill_date,
									refund_trans::write_off_source AS write_off_source,
									refund_trans::master_acct_num AS master_acct_num,
									refund_trans::bill_applied_amt AS bill_applied_amt,
									refund_trans::charge_amt AS charge_amt,
									refund_trans::bill_amt AS bill_amt,
									refund_trans::resub_flg AS resub_flg,
									refund_trans::brandi AS brandi,
									refund_trans::payment_method_id AS payment_method_id,
									--refund_trans::taxed_flag AS taxed_flag,
									refund_trans::pm_acct_num AS pm_acct_num;								

refund_trans_ALL = UNION refund_trans_with_gross, refund_trans_join_inner_xform, refund_trans_join_unused0_xform;                         
refund_trans_ALL_group = GROUP refund_trans_ALL BY (bill_applied_date, 
                                                    line_item_balance_id, 
                                                    cash_credit_id, 
                                                    payment_processor_activity, 
                                                    trans_type, retry_count, 
                                                    result_code, 
                                                    acct_id, 
                                                    trans_date, 
                                                    pm_id, 
                                                    lineitem, 
                                                    seq_no, 
                                                    reason_code, 
                                                    trans_amt, 
                                                    sid, 
                                                    bid, 
                                                    global_acct_id, 
                                                    processing_division, 
                                                    payment_processor, 
                                                    campaign_id, 
                                                    line_item_bill_id, 
                                                    bill_cycle, 
                                                    bill_date, 
                                                    write_off_source, 
                                                    master_acct_num, 
                                                    bill_applied_amt,
                                                    charge_amt, 
                                                    bill_amt, 
                                                    resub_flg, 
                                                    brandi, 
                                                    payment_method_id);

refund_trans_final = FOREACH refund_trans_ALL_group {
                             top_rec = LIMIT refund_trans_ALL 1;
                             GENERATE FLATTEN(top_rec);
                            };

-----------------------------------------------------------------------------------------------------------------                                        
--Dataset Name - refund_trans_final
--refund_trans_no_dups feed will be the input to "Gather Multi-Files for eval_trans_extract" component
------------------------------------------------------------------------------------------------------------------ 
-------------------------------------------					
-- Component : refund transactions - End
------------------------------------------- 

cash_credit_history_not_filter = FILTER cash_credit_history BY
                             NOT(
                               bill_activity_type == '500' OR
                               (
                                 bill_activity_type == '100' AND 
                                  (
                                    action_code == 'N' OR
                                    action_code == 'R' OR
                                    cc_reason_code == '${REFUND_ANNUAL_REASON_CODE}' OR
                                    cc_reason_code == '${REASON_CODE}'
                                  )   
                               )
                             );

refund_error = FOREACH cash_credit_history_not_filter GENERATE 
                  approve AS approve,
                  acct_id AS acct_id,
                  payment_method_id AS payment_method_id,
                  processed_dt AS processed_dt,
                  pm_id AS pm_id,
                  lineitem AS lineitem,
                  bill_activity_type AS bill_activity_type,
                  reason_code AS reason_code,
                  action_code AS action_code,
                  credit_amount AS credit_amount,
                  sid AS sid,
                  bid AS bid,
                  global_acct_id AS global_acct_id,
                  processing_division AS processing_division,
                  ppv_id AS ppv_id,
                  campaign_id AS campaign_id,
                  cash_credit_id AS cash_credit_id,
                  payment_processor_activity_id AS payment_processor_activity_id,
                  created_dt AS created_dt,
                  credited_amount AS credited_amount,
                  brandi AS brandi,
                  cycle AS cycle;
                  
--Datset refund_trans_final will be input to "Gather Multi-Files for eval_trans_extract" component
STORE refund_trans_final INTO '$brrLineItemInterimDir/LI_03B_process_ar_refunds' USING PigStorage('$delimiter');    

STORE refund_error INTO '$brrLineItemRejectsDir/LI_03B_process_ar_refunds' USING PigStorage('$delimiter');                                                                         