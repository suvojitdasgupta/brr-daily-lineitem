/*
            Script Name : LI_05_process_ar_trans_type.pig
 jUnit Test Script Name : PigUnit05ProcessArTransType.java
                Purpose : pig script to to populate  Transaction Type for AR data.
             Dependency : LI_04_process_ar_trans_type.pig                          
*/

SET pig.exec.mapPartAgg '$pig_exec_mappart_agg_enabled';
SET pig.user.cache.enabled '$pig_user_cache_mode_enabled';
SET pig.auto.local.enabled '$pig_auto_local_mode_enabled';
SET pig.local.io.sort.mb '$pig_auto_local_io_sort_mb';
SET pig.auto.local.input.maxbytes '$pig_auto_local_input_maxbytes';

%declare BAT_MANUAL_PAYMENT (long)300
%declare BAT_LOCKBOX_PAYMENT (long)400
%declare BAT_PP_CHARGEBACK (long)500
%declare BAT_MANUAL_CHARGEBACK (long)600
%declare BAT_WRITEOFF (long)700
%declare BAT_ASSUMED_PAYMENT (long)800
%declare BAT_BILLING_CHGBK (long)900
%declare BAT_ACC_PAYMENT (long)1000
%declare BAT_ACC_CHARGEBACK (long)1010
%declare BAT_ACC_WRITE_OFF (long)1020
%declare BAT_ACC_REV_WRITEOFF (long)1030
%declare BAT_REV_WRITEOFF (long)1200
%declare REFUND_ANNUAL_REASON_CODE (long)510

line_item_input = LOAD '$brrLineItemInterimDir/LI_04_process_ar_trans_type' USING PigStorage('$delimiter') AS
							(
								line_item_balance_id:long,
								witternoff_bad_debt_cnt:long,
								approve:chararray,
								forced_trans_flag:chararray,
								ar_result:chararray,
								acct_id:long,
								payment_method_id:long,
								bill_activity_type:long,
								bill_dt:chararray, -- YYYY-MM-DD HH24-MI-SS
								ppa_created_dt:chararray, -- YYYY-MM-DD HH24-MI-SS
								pm_id:long,
								lineitem:long,
								retry_count:long,
								action_code:chararray,
								reason_code:chararray,
								processed_amount:long,
								sid:long,
								bid:long,
								global_acct_id:chararray,
								processing_division:chararray,
								ppv_id:long,
								campaign_id:long,
								line_item_bill_id:long,
								cycle:long,
								payment_processor_activity_id:long,
								libh_created_dt:chararray, -- YYYY-MM-DD HH24-MI-SS
								activity_amount:long,
								net_amount:long,
								first_bill_flag:chararray,
								bill_amount:long,
								cb_created_dt:chararray, -- YYYY-MM-DD HH24-MI-SS
								bsh_status:long,
								brandi:long,
								prior_osb_bucket:long,
								merchant_order_number:chararray,
								pm_acct_num:chararray
							);
						
SPLIT line_item_input INTO 
						line_item_filter IF (bill_activity_type != (long)1100),				
						line_item_filter_out OTHERWISE;

line_item_output_data = FOREACH line_item_filter GENERATE 
									(chararray)ToString(ToDate(libh_created_dt, 'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss') as bill_applied_date,
									line_item_balance_id as balance_id,
									(long) -1 as cash_credit_id,
									
									payment_processor_activity_id as payment_processor_activity,
									(
										(bill_activity_type == 100 OR bill_activity_type == $BAT_ASSUMED_PAYMENT) ?
											(
												(merchant_order_number is not null AND INDEXOF(TRIM(merchant_order_number),'J',0) == (long)1) ? 'J'
												: (
													(
														(merchant_order_number is not null AND INDEXOF(TRIM(merchant_order_number),'S',0) == (long)1)
														OR (TRIM(action_code) == 'D' AND TRIM(forced_trans_flag) == '0')
													) ? 
														(
															(bsh_status is null OR bsh_status != 105) ? 'S' :
															((
																prior_osb_bucket == 2
															) ? 'S2' : 'S1' )
														)
													:(
														(TRIM(action_code) == 'B' OR TRIM(action_code) == 'D' OR TRIM(action_code) == 'H') ?
														(
															(bsh_status is null OR bsh_status != 105) ? TRIM(action_code) :
															((
																prior_osb_bucket == 2
															) ? 'S2' : 'S1')
														):''
													)			
												)
											):
										(
											(bill_activity_type == $BAT_ACC_PAYMENT) ? 
												(
													(
													  cb_created_dt is not null  
													  AND MilliSecondsBetween(ToDate(libh_created_dt, 'yyyy-MM-dd HH:mm:ss'),ToDate(cb_created_dt, 'yyyy-MM-dd HH:mm:ss')) > (long)'0'
													) ?
													(
														(bsh_status is null OR bsh_status != 105) ? 'AO' :
														((prior_osb_bucket == 2) ? '02' : '01')
													):
													(
														(bsh_status is null OR bsh_status != 105) ? 'PA' :
														((prior_osb_bucket == 2) ? 'M2' : 'M1')
													)
													
												):
												(
													((bill_activity_type == $BAT_MANUAL_PAYMENT OR bill_activity_type == $BAT_LOCKBOX_PAYMENT)
													AND (action_code is null OR action_code != '' OR action_code == 'B') ) ?
													(
														(
														  cb_created_dt is not null  
														  AND  MilliSecondsBetween(ToDate(libh_created_dt, 'yyyy-MM-dd HH:mm:ss'), ToDate(cb_created_dt, 'yyyy-MM-dd HH:mm:ss')) > (long)'0'
														) ?
														(
															(bsh_status is null OR bsh_status != 105) ? 'AO' :
															((prior_osb_bucket == 2) ? '02' : '01')
														):
														(
															(bsh_status is null OR bsh_status != 105) ? 'P' :
															((prior_osb_bucket == 2) ? 'P2' : 'PE')
														)	
													):
													(
														(bill_activity_type == $BAT_PP_CHARGEBACK AND ((TRIM(action_code) == 'D' AND forced_trans_flag == '1') OR TRIM(action_code) == 'H') ) ? 'X'
														:(
															(bill_activity_type == $BAT_PP_CHARGEBACK AND (TRIM(action_code) == 'R' OR TRIM(action_code) == 'N')) ? 'U'
															:(
																(bill_activity_type == $BAT_PP_CHARGEBACK AND (TRIM(action_code) == 'D' OR TRIM(action_code) == 'B')) ? 
																(
																	(witternoff_bad_debt_cnt == 0 ) ? 'G' :
																	((witternoff_bad_debt_cnt >= 2) ? 'G2' : 'G1')
																)
																:(
																	(bill_activity_type == $BAT_PP_CHARGEBACK AND ppv_id == 28) ? 'X'
																	:(
																		(bill_activity_type == $BAT_MANUAL_CHARGEBACK) ? 'C'
																		:(
																			(bill_activity_type == $BAT_BILLING_CHGBK) ? 'W'
																			:(
																				(bill_activity_type == $BAT_WRITEOFF OR bill_activity_type == $BAT_ACC_WRITE_OFF) ? 
																				(
																					((bsh_status is null OR bsh_status != 105) AND (prior_osb_bucket == 0 OR prior_osb_bucket == 2)) ?
																					(
																						(reason_code == '$REFUND_ANNUAL_REASON_CODE') ? 'R2' : 'E2'
																					)
																					:(
																						(bsh_status is null OR bsh_status != 105) ? 
																						(
																							(reason_code == '$REFUND_ANNUAL_REASON_CODE') ? 'R1' : 'E'
																						)
																						:'ME'
																					)
																				)
																				:(
																					(bill_activity_type == $BAT_ACC_CHARGEBACK) ? 'WA'
																					:(
																						((bill_activity_type == $BAT_ACC_REV_WRITEOFF) OR (bill_activity_type == $BAT_REV_WRITEOFF)) ? 
																						(
																							(bsh_status is not null AND bsh_status == 105) ? 'A3'
																							:(
																								((prior_osb_bucket == 2) ? 'A2':'A1' )
																							)
																						)
																						:''
																					)
																				)
																			)
																		)
																	)
																)
															)
														)
													)
												)
										)
									) as trans_type,
									
									((retry_count is null) ? (long)0 : retry_count) as Retry_Ct,
									(
										(approve == '1') ? 
										(
											(forced_trans_flag is null OR forced_trans_flag != '1') ? 'A' : 'F'
										)
										:(
											(bill_activity_type == $BAT_MANUAL_CHARGEBACK) ? 'H' 
											:(
												(ar_result is null) ? '' : TRIM(ar_result)
											)
										)
									)  as result_code,
									acct_id as account_no,
									ppa_created_dt as trans_date, -- YYYY-MM-DD HH24-MI-SS
									pm_id as pay_method,
									lineitem as line_item,
									'0'  as seq_no,
									((reason_code is null) ? '' : reason_code) as reason_code,
									(
										(activity_amount == 0 AND retry_count != 0) ? processed_amount
										:(
											(bill_activity_type == 0 OR bill_activity_type == $BAT_ASSUMED_PAYMENT) ? ABS(bill_amount)
											:(
												(bill_activity_type == 100 OR bill_activity_type == $BAT_MANUAL_PAYMENT OR bill_activity_type == $BAT_PP_CHARGEBACK OR bill_activity_type == $BAT_MANUAL_CHARGEBACK OR bill_activity_type == $BAT_BILLING_CHGBK OR bill_activity_type == $BAT_WRITEOFF OR bill_activity_type == $BAT_ACC_WRITE_OFF OR bill_activity_type == $BAT_ACC_PAYMENT OR bill_activity_type == $BAT_ACC_CHARGEBACK OR bill_activity_type == $BAT_ACC_REV_WRITEOFF OR bill_activity_type == $BAT_LOCKBOX_PAYMENT) ? ABS(processed_amount) : (long)0
											)
										)
									) as trans_amt,
									sid as sid,
									bid as bid,
									global_acct_id as guid,

									(
										(processing_division is null OR bill_activity_type == $BAT_BILLING_CHGBK OR (ppv_id != 2 AND ppv_id != 13) ) ? '-1' : processing_division
									) as processing_division,
									ppv_id as payment_processor,
									((campaign_id is null) ? (long)0 : campaign_id) as campaign_id,
									line_item_bill_id as bill_id,
									((cycle is null) ? (long)0: cycle) as bill_cycle,

									(chararray)ToString(ToDate(bill_dt, 'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss') as bill_date,
									(
										(bill_activity_type == $BAT_WRITEOFF) ? 'MS'
										:(
											(bill_activity_type == $BAT_ACC_WRITE_OFF OR bill_activity_type == $BAT_ACC_REV_WRITEOFF) ? 'AC' : ''
										)
									) as write_off_source,
								
									
									acct_id as master_acct_num,
									processed_amount as processed_amount,
									activity_amount as activity_amount,
									(
										((bill_activity_type == 100 OR bill_activity_type == $BAT_ASSUMED_PAYMENT) AND first_bill_flag == '1') ? net_amount : 0
									) as charge_amt,
									ABS(bill_amount) as bill_amt,
									((first_bill_flag == '1') ? '0':'1') as resub_flg,
									brandi as brandi,
									payment_method_id as payment_method_id,
									pm_acct_num as pm_acct_num;
--Dataset Name - line_item_output_data;

-- Added new logic based on the findings for RRT discrepancies
-- The following transaction_types are assinged values differently than rest.
-- PA, M1, M2, P, P2, PE, S, S1, S2, D, B, H, G, G1, G2

line_item_output_data_final = FOREACH line_item_output_data GENERATE 
                                      bill_applied_date AS bill_applied_dt,
                                      balance_id AS balance_id,
                                      cash_credit_id AS cash_credit_id,
                                      payment_processor_activity AS payment_processor_activity,
                                      trans_type AS trans_type,
                                      Retry_Ct AS retry_count,
                                      result_code AS result_code,
                                      account_no AS acct_no,
                                      trans_date AS trans_date,
                                      pay_method AS pay_method,
                                      line_item AS line_item,
                                      seq_no AS seq_no,
                                      reason_code AS reason_code,
                                      trans_amt AS trans_amt,
                                      sid AS sid,
                                      bid AS bid,
                                      guid AS guid,
                                      processing_division AS processing_division,
                                      payment_processor AS payment_processor,
                                      campaign_id AS campaign_id,
                                      bill_id AS bill_id,
                                      bill_cycle AS bill_cycle,
                                      bill_date AS bill_date,
                                      write_off_source AS write_off_source,
                                      master_acct_num AS master_acct_num,
                                        (
                                          (
                                            trans_type=='PA' 
                                         OR trans_type=='M1'
                                         OR trans_type=='M2'
                                         OR trans_type=='P'
                                         OR trans_type=='P2'
                                         OR trans_type=='PE'
                                         OR trans_type=='S'
                                         OR trans_type=='S1'
                                         OR trans_type=='S2' 
                                         OR trans_type=='D'
                                         OR trans_type=='B'
                                         OR trans_type=='H'
                                         OR trans_type=='G' 
                                         OR trans_type=='G1'
                                         OR trans_type=='G2'
                                          ) ?  ABS(activity_amount) : ABS(processed_amount)
                                          ) AS bill_applied_amount,
                                         charge_amt AS charge_amount,
                                         bill_amt AS bill_amount,
                                         resub_flg AS resub_flag,
                                         brandi AS brandi,
                                         payment_method_id AS payment_method_id,
                                         pm_acct_num AS pm_acct_num;
                                                                       									
rejected_data = FOREACH line_item_filter_out GENERATE
							line_item_balance_id,
							witternoff_bad_debt_cnt,
							approve,
							forced_trans_flag,
							ar_result,
							acct_id,
							payment_method_id,
							bill_activity_type,
							bill_dt, -- YYYY-MM-DD HH24:MI:SS
							ppa_created_dt, -- YYYY-MM-DD HH24:MI:SS
							pm_id,
							lineitem,
							retry_count,
							action_code,
							reason_code,
							processed_amount,
							sid,
							bid,
							global_acct_id,
							processing_division,
							ppv_id,
							campaign_id,
							line_item_bill_id,
							cycle,
							payment_processor_activity_id,
							libh_created_dt, -- YYYY-MM-DD HH24:MI:SS
							activity_amount,
							net_amount,
							first_bill_flag,
							bill_amount,
							cb_created_dt, -- YYYY-MM-DD HH24-MI-SS
							bsh_status,
							brandi,
							prior_osb_bucket,
							merchant_order_number,
							pm_acct_num;

-- Store Dataset
STORE line_item_output_data_final INTO '$brrLineItemInterimDir/LI_05_process_ar_trans_type/' USING PigStorage('$delimiter');							
STORE rejected_data INTO '$brrLineItemRejectsDir/LI_05_process_ar_trans_type' USING PigStorage('$delimiter');						