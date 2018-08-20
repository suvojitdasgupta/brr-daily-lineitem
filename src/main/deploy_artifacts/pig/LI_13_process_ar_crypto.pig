/*
            Script Name : LI_13_process_ar_crypto.pig
 jUnit Test Script Name : PigUnit13ProcessArCrypto.java
                Purpose : pig script to read the output decripted json and process the data.
             Dependency : LI_10_process_ar_crypto.pig                                                
*/

SET pig.exec.mapPartAgg '$pig_exec_mappart_agg_enabled';
SET pig.user.cache.enabled '$pig_user_cache_mode_enabled';
SET pig.auto.local.enabled '$pig_auto_local_mode_enabled';
SET pig.local.io.sort.mb '$pig_auto_local_io_sort_mb';
SET pig.auto.local.input.maxbytes '$pig_auto_local_input_maxbytes';
SET mapreduce.reduce.memory.mb 3240;
SET pig.exec.reducers.bytes.per.reducer 100000000;

DEFINE COALESCE datafu.pig.util.Coalesce();
IMPORT '$brrLineItemPigDir/LI_01_ingest_dimensions.pig';

ar_main = LOAD '$brrLineItemInterimDir/LI_10_process_ar_crypto/part*' using PigStorage('$delimiter') AS 
                       (
                        bid:chararray,
						sid:chararray,
						global_acct_id:chararray,
						obi_acct:chararray,
						ar_payment_method_id:chararray,
						pm_id:chararray,
						pm_acct_num:chararray,
						routing_number:chararray,
						cycle:chararray,
						billed_dt:chararray,
						trans_type:chararray,
						retry_count:chararray,
						result_code:chararray,
						reason_code:chararray,
						lineitem:chararray,
						charge_amount:double,
						bill_amount:double,
						paid_amount:double,
						trans_amount:double,
						tax_amount:double,
						trans_source_id:chararray,
						ppv_id:chararray,
						nb_reason_code:chararray,
						nb_amount:double,
						processing_division:chararray,
						trans_apply_dt:chararray,
						adjust_amount:double,
						balance_id:chararray,
						bill_id:chararray,
						resub_flg:chararray,
						write_off_source:chararray,
						write_off_reason_cd:chararray,
						city:chararray,
						state:chararray,
						zipcode:chararray,
						country_code:chararray,
						iso_currency_id:chararray,
						origin_of_sales:chararray,
						paid_dt:chararray,
						first_name:chararray,
						last_name:chararray,
						email_address:chararray,
						phone_number:chararray,
						street:chararray,
						payment_ref_id:chararray,
						payment_token:chararray
                       );                    
paypal_trans =  LOAD '$brrLineItemSqoopDir/paypal_transactions/part*' using PigStorage('$delimiter') AS 
                       ( 
                        bill_id:chararray,
                        paypal_trans_id:chararray,                    
                        bill_dt:chararray
                       );

ar_main_paypal_join = JOIN ar_main BY (bill_id) LEFT OUTER, paypal_trans BY (bill_id);    
ar_main_paypal_xform = FOREACH  ar_main_paypal_join GENERATE
	                            ar_main::bid AS bid,
							    ar_main::sid AS sid,
							    ar_main::global_acct_id AS global_acct_id,
							    ar_main::obi_acct AS obi_acct,
							    ar_main::ar_payment_method_id AS ar_payment_method_id,
							    TRIM(ar_main::pm_id) AS pm_id,
	                            ar_main::pm_acct_num AS pm_acct_num,
	                            ar_main::routing_number AS routing_number,
							    ar_main::cycle AS cycle,
							    ar_main::billed_dt AS billed_dt,
							    ar_main::trans_type AS trans_type,
							    ar_main::retry_count AS retry_cnt,
								ar_main::result_code AS result_code,
							    COALESCE(TRIM(ar_main::reason_code),'100') AS reason_code,
								ar_main::lineitem AS lineitem,
								ar_main::charge_amount AS charge_amount,
								ar_main::bill_amount AS bill_amount,
								ar_main::paid_amount AS paid_amount,
								ar_main::trans_amount AS trans_amount,
								ar_main::tax_amount AS tax_amount,
								ar_main::trans_source_id AS trans_source_id,
								ar_main::ppv_id AS ppv_id,
								ar_main::nb_reason_code AS nb_reason_code,
								ar_main::nb_amount AS nb_amount,
								ar_main::processing_division AS processing_division,
								ar_main::trans_apply_dt AS trans_apply_dt,
								ar_main::adjust_amount AS adjust_amount,
								ar_main::balance_id AS balance_id,
								ar_main::bill_id AS bill_id,
								ar_main::resub_flg AS resub_flg,
								TRIM(ar_main::write_off_source) AS write_off_source,
								TRIM(ar_main::write_off_reason_cd) AS write_off_reason_cd,
								ar_main::city AS city,
								ar_main::state AS state,
								ar_main::zipcode AS zipcode,
								ar_main::country_code AS country_code,
								ar_main::iso_currency_id AS iso_currency_id,
								ar_main::origin_of_sales AS origin_of_sales,
								ar_main::paid_dt AS paid_dt,
								ar_main::first_name AS first_name,
								ar_main::last_name AS last_name,
								ar_main::email_address AS email_address,
								ar_main::phone_number AS phone_number,
								ar_main::street AS street,
								ar_main::payment_ref_id AS payment_ref_id,
							    paypal_trans::paypal_trans_id AS ext_transaction_id,
								paypal_trans::bill_dt AS ext_transaction_init_dt
								;
										 
STORE ar_main_paypal_xform INTO '$brrLineItemInterimDir/LI_13_process_ar_crypto' USING PigStorage('$delimiter');                              