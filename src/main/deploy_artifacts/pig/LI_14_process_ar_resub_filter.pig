/*
            Script Name : LI_14_process_ar_resub_filter.pig
 jUnit Test Script Name : PigUnit14ProcessArResubFilter.java
                Purpose : pig script to filter out all resubmits from AR flow.
             Dependency : LI_13_process_ar_crypto.pig                         
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

DEFINE enforce_schema(input_records) RETURNS output_records {
       $output_records = FOREACH $input_records GENERATE 
                                 (chararray)$0 AS bid,
                                 (chararray)$1 AS sid,
                                 (chararray)$2 AS global_acct_id,
                                 (chararray)$3 AS obi_acct,
                                 (chararray)$4 AS ar_payment_method_id,
                                 (chararray)$5 AS pm_id,
                                 (chararray)$6 AS pm_acct_num,
                                 (chararray)$7 AS routing_number,
                                 (chararray)$8 AS cycle,
                                 (chararray)$9 AS billed_dt,
                                 (chararray)$10 AS trans_type,
                                 (chararray)$11 AS retry_cnt,
                                 (chararray)$12 AS result_code,
                                 (chararray)$13 AS reason_code,
                                 (chararray)$14 AS lineitem,
                                 (double)$15 AS charge_amount,
                                 (double)$16 AS bill_amount,
                                 (double)$17 AS paid_amount,
                                 (double)$18 AS trans_amount,
                                 (double)$19 AS tax_amount,
                                 (chararray)$20 AS trans_source_id,
                                 (chararray)$21 AS ppv_id,
                                 (chararray)$22 AS nb_reason_code,
                                 (double)$23 AS nb_amount,
                                 (chararray)$24 AS processing_division,
                                 (chararray)$25 AS trans_apply_dt,
                                 (double)$26 AS adjust_amount,
                                 (long)$27 AS balance_id,
                                 (long)$28 AS bill_id,
                                 (chararray)$29 AS resub_flg,
                                 (chararray)$30 AS write_off_source,
                                 (chararray)$31 AS write_off_reason_cd,
                                 (chararray)$32 AS city,
                                 (chararray)$33 AS state,
                                 (chararray)$34 AS zipcode,
                                 (chararray)$35 AS country_code,
                                 (chararray)$36 AS iso_currency_id,
                                 (chararray)$37 AS origin_of_sales,
                                 (chararray)$38 AS paid_dt,
                                 (chararray)$39 AS first_name,
                                 (chararray)$40 AS last_name,
                                 (chararray)$41 AS email_address,
                                 (chararray)$42 AS phone_number,
                                 (chararray)$43 AS street,
                                 (chararray)$44 AS payment_ref_id,
                                 (chararray)$45 AS ext_transaction_id,
                                 (chararray)$46 AS ext_transaction_init_dt;
                                 };

resub_input = LOAD '$brrLineItemInterimDir/LI_13_process_ar_crypto/part*' using PigStorage('$delimiter') AS
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
						balance_id:long,
						bill_id:long,
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
						ext_transaction_id:chararray,
						ext_transaction_init_dt:chararray
					);

-- Added new logic based on the findings for RRT discrepancies
-- The following transaction_types are bypassed of RESUBMIT Filter logic.
-- PA, M1, M2, P, P2, PE, S, S1, S2, D, B, H, G, G1, G2

SPLIT resub_input INTO resub_bypass IF (
                                          TRIM(trans_type)=='PA' 
                                       OR TRIM(trans_type)=='M1'
                                       OR TRIM(trans_type)=='M2'
                                       OR TRIM(trans_type)=='P'
                                       OR TRIM(trans_type)=='P2'
                                       OR TRIM(trans_type)=='PE'
                                       OR TRIM(trans_type)=='S'
                                       OR TRIM(trans_type)=='S1'
                                       OR TRIM(trans_type)=='S2' 
                                       OR TRIM(trans_type)=='D'
                                       OR TRIM(trans_type)=='B'
                                       OR TRIM(trans_type)=='H'
                                       OR TRIM(trans_type)=='G' 
                                       OR TRIM(trans_type)=='G1'
                                       OR TRIM(trans_type)=='G2'
                                       ),
                       resub OTHERWISE;

resub_bypass_enforced_schema = enforce_schema(resub_bypass); 


resub_filter_Y = FILTER resub BY (TRIM(resub_flg) == '1');
resub_filter_N = FILTER resub BY NOT(TRIM(resub_flg) == '1');

resub_filter_Y_group = GROUP resub_filter_Y BY (bill_id);

--Picking the latest balance_id for a given bill.
--AbInitio doesn't have a specific logic to pick the latest balance_id deterministically. 
--This may cause a difference in the output. 
resub_filter_Y_dups = FOREACH resub_filter_Y_group {
                            sorted = ORDER resub_filter_Y BY balance_id DESC; --Picking the latest balance_id for a given bill.
                            top_rec = LIMIT sorted 1;
                            GENERATE FLATTEN(top_rec);
                           };                        
                                                    
resub_join = JOIN resub_filter_N BY (bill_id) FULL OUTER, resub_filter_Y_dups BY (bill_id);  

-- Case 1 - Match  
resub_join_match = FILTER resub_join BY (resub_filter_N::bill_id IS NOT NULL AND resub_filter_Y_dups::top_rec::bill_id IS NOT NULL);
resub_join_match_xform = FOREACH resub_join_match GENERATE
							resub_filter_N::bid AS bid,						
							resub_filter_N::sid AS sid,
							resub_filter_N::global_acct_id AS global_acct_id,
							resub_filter_N::obi_acct AS obi_acct,
							resub_filter_N::ar_payment_method_id AS ar_payment_method_id,
							resub_filter_N::pm_id AS pm_id,
							resub_filter_N::pm_acct_num AS pm_acct_num,
							resub_filter_N::routing_number AS routing_number,
							resub_filter_N::cycle AS cycle,
							resub_filter_N::billed_dt AS billed_dt,
							resub_filter_N::trans_type AS trans_type,
							resub_filter_N::retry_count AS retry_cnt,
							resub_filter_N::result_code AS result_code,
							COALESCE(TRIM(resub_filter_N::reason_code),'100') AS reason_code,
							resub_filter_N::lineitem AS lineitem,
							resub_filter_N::charge_amount AS charge_amount,
							resub_filter_N::bill_amount AS bill_amount,
							resub_filter_N::paid_amount AS paid_amount,
							resub_filter_N::trans_amount AS trans_amount,
							resub_filter_N::tax_amount AS tax_amount,
							resub_filter_N::trans_source_id AS trans_source_id,
							resub_filter_N::ppv_id AS ppv_id,
							resub_filter_N::nb_reason_code AS nb_reason_code,
							resub_filter_N::nb_amount AS nb_amount,
							resub_filter_N::processing_division AS processing_division,
							resub_filter_N::trans_apply_dt AS trans_apply_dt,
							resub_filter_N::adjust_amount AS adjust_amount,
							resub_filter_N::balance_id AS balance_id,
							resub_filter_N::bill_id AS bill_id,
						  	resub_filter_Y_dups::top_rec::resub_flg AS resub_flg,
							TRIM(resub_filter_N::write_off_source) AS write_off_source,
							TRIM(resub_filter_N::write_off_reason_cd) AS write_off_reason_cd,
							resub_filter_N::city AS city,
							resub_filter_N::state AS state,
							resub_filter_N::zipcode AS zipcode,
							resub_filter_N::country_code AS country_code,
							resub_filter_N::iso_currency_id AS iso_currency_id,
							resub_filter_N::origin_of_sales AS origin_of_sales,
							resub_filter_N::paid_dt AS paid_dt,
							resub_filter_N::first_name AS first_name,
							resub_filter_N::last_name AS last_name,
							resub_filter_N::email_address AS email_address,
							resub_filter_N::phone_number AS phone_number,
							resub_filter_N::street AS street,
							resub_filter_N::payment_ref_id AS payment_ref_id,
							resub_filter_N::ext_transaction_id AS ext_transaction_id,
							resub_filter_N::ext_transaction_init_dt AS ext_transaction_init_dt;

-- Case 2 -- Unused0
resub_join_unused0 = FILTER resub_join BY (resub_filter_N::bill_id IS NOT NULL AND resub_filter_Y_dups::top_rec::bill_id IS NULL);
resub_join_unused0_xform = FOREACH resub_join_unused0 GENERATE
							resub_filter_N::bid AS bid,						
							resub_filter_N::sid AS sid,
							resub_filter_N::global_acct_id AS global_acct_id,
							resub_filter_N::obi_acct AS obi_acct,
							resub_filter_N::ar_payment_method_id AS ar_payment_method_id,
							resub_filter_N::pm_id AS pm_id,
							resub_filter_N::pm_acct_num AS pm_acct_num,
							resub_filter_N::routing_number AS routing_number,
							resub_filter_N::cycle AS cycle,
							resub_filter_N::billed_dt AS billed_dt,
							resub_filter_N::trans_type AS trans_type,
							resub_filter_N::retry_count AS retry_cnt,
							resub_filter_N::result_code AS result_code,
							COALESCE(TRIM(resub_filter_N::reason_code),'100') AS reason_code,							
							resub_filter_N::lineitem AS lineitem,
							resub_filter_N::charge_amount AS charge_amount,
							resub_filter_N::bill_amount AS bill_amount,
							resub_filter_N::paid_amount AS paid_amount,
							resub_filter_N::trans_amount AS trans_amount,
							resub_filter_N::tax_amount AS tax_amount,
							resub_filter_N::trans_source_id AS trans_source_id,
							resub_filter_N::ppv_id AS ppv_id,
							resub_filter_N::nb_reason_code AS nb_reason_code,
							resub_filter_N::nb_amount AS nb_amount,
							resub_filter_N::processing_division AS processing_division,
							resub_filter_N::trans_apply_dt AS trans_apply_dt,
							resub_filter_N::adjust_amount AS adjust_amount,
							resub_filter_N::balance_id AS balance_id,
							resub_filter_N::bill_id AS bill_id,
						  	'N' AS resub_flg,
							TRIM(resub_filter_N::write_off_source) AS write_off_source,
							TRIM(resub_filter_N::write_off_reason_cd) AS write_off_reason_cd,
							resub_filter_N::city AS city,
							resub_filter_N::state AS state,
							resub_filter_N::zipcode AS zipcode,
							resub_filter_N::country_code AS country_code,
							resub_filter_N::iso_currency_id AS iso_currency_id,
							resub_filter_N::origin_of_sales AS origin_of_sales,
							resub_filter_N::paid_dt AS paid_dt,
							resub_filter_N::first_name AS first_name,
							resub_filter_N::last_name AS last_name,
							resub_filter_N::email_address AS email_address,
							resub_filter_N::phone_number AS phone_number,
							resub_filter_N::street AS street,
							resub_filter_N::payment_ref_id AS payment_ref_id,
							resub_filter_N::ext_transaction_id AS ext_transaction_id,
							resub_filter_N::ext_transaction_init_dt AS ext_transaction_init_dt;

-- Case 3 -- Unused1
resub_join_unused1 = FILTER resub_join BY (resub_filter_N::bill_id IS NULL AND resub_filter_Y_dups::top_rec::bill_id IS NOT NULL);
resub_join_unused1_xform = FOREACH resub_join_unused1 GENERATE
							resub_filter_Y_dups::top_rec::bid AS bid,						
							resub_filter_Y_dups::top_rec::sid AS sid,
							resub_filter_Y_dups::top_rec::global_acct_id AS global_acct_id,
							resub_filter_Y_dups::top_rec::obi_acct AS obi_acct,
							resub_filter_Y_dups::top_rec::ar_payment_method_id AS ar_payment_method_id,
							resub_filter_Y_dups::top_rec::pm_id AS pm_id,
							resub_filter_Y_dups::top_rec::pm_acct_num AS pm_acct_num,
							resub_filter_Y_dups::top_rec::routing_number AS routing_number,
							resub_filter_Y_dups::top_rec::cycle AS cycle,
							resub_filter_Y_dups::top_rec::billed_dt AS billed_dt,
							resub_filter_Y_dups::top_rec::trans_type AS trans_type,
							resub_filter_Y_dups::top_rec::retry_count AS retry_cnt,
							resub_filter_Y_dups::top_rec::result_code AS result_code,
							COALESCE(TRIM(resub_filter_Y_dups::top_rec::reason_code),'100') AS reason_code,
							resub_filter_Y_dups::top_rec::lineitem AS lineitem,
							resub_filter_Y_dups::top_rec::charge_amount AS charge_amount,
							resub_filter_Y_dups::top_rec::bill_amount AS bill_amount,
							resub_filter_Y_dups::top_rec::paid_amount AS paid_amount,
							
							/* Modified as part of RRT Discrepancy Fix - Start */							
							(
							  (
							   resub_filter_Y_dups::top_rec::trans_type == 'P' 
							   OR 
							   resub_filter_Y_dups::top_rec::trans_type == 'E' 
							   OR 
							   resub_filter_Y_dups::top_rec::trans_type == 'E2' 
							   OR 
							   resub_filter_Y_dups::top_rec::trans_type == 'E3'
							   ) ?  resub_filter_Y_dups::top_rec::trans_amount 
							     : resub_filter_Y_dups::top_rec::bill_amount
							  
							) AS trans_amount,
							/* Modified as part of RRT Discrepancy Fix - End */
							
							resub_filter_Y_dups::top_rec::tax_amount AS tax_amount,
							resub_filter_Y_dups::top_rec::trans_source_id AS trans_source_id,
							resub_filter_Y_dups::top_rec::ppv_id AS ppv_id,
							resub_filter_Y_dups::top_rec::nb_reason_code AS nb_reason_code,
							resub_filter_Y_dups::top_rec::nb_amount AS nb_amount,
							resub_filter_Y_dups::top_rec::processing_division AS processing_division,
							resub_filter_Y_dups::top_rec::trans_apply_dt AS trans_apply_dt,
							resub_filter_Y_dups::top_rec::adjust_amount AS adjust_amount,
							resub_filter_Y_dups::top_rec::balance_id AS balance_id,
							resub_filter_Y_dups::top_rec::bill_id AS bill_id,
						  	resub_filter_Y_dups::top_rec::resub_flg AS resub_flg,
							TRIM(resub_filter_Y_dups::top_rec::write_off_source) AS write_off_source,
							TRIM(resub_filter_Y_dups::top_rec::write_off_reason_cd) AS write_off_reason_cd,
							resub_filter_Y_dups::top_rec::city AS city,
							resub_filter_Y_dups::top_rec::state AS state,
							resub_filter_Y_dups::top_rec::zipcode AS zipcode,
							resub_filter_Y_dups::top_rec::country_code AS country_code,
							resub_filter_Y_dups::top_rec::iso_currency_id AS iso_currency_id,
							resub_filter_Y_dups::top_rec::origin_of_sales AS origin_of_sales,
							resub_filter_Y_dups::top_rec::paid_dt AS paid_dt,
							resub_filter_Y_dups::top_rec::first_name AS first_name,
							resub_filter_Y_dups::top_rec::last_name AS last_name,
							resub_filter_Y_dups::top_rec::email_address AS email_address,
							resub_filter_Y_dups::top_rec::phone_number AS phone_number,
							resub_filter_Y_dups::top_rec::street AS street,
							resub_filter_Y_dups::top_rec::payment_ref_id AS payment_ref_id,
							resub_filter_Y_dups::top_rec::ext_transaction_id AS ext_transaction_id,
							resub_filter_Y_dups::top_rec::ext_transaction_init_dt AS ext_transaction_init_dt;

resub_join_match_xform_enforced_schema = enforce_schema(resub_join_match_xform);
resub_join_unused0_xform_enforced_schema = enforce_schema(resub_join_unused0_xform);
resub_join_unused1_xform_enforced_schema = enforce_schema(resub_join_unused1_xform);       											

resub_final = UNION ONSCHEMA resub_join_match_xform_enforced_schema, 
                             resub_join_unused0_xform_enforced_schema, 
                             resub_join_unused1_xform_enforced_schema,
                             resub_bypass_enforced_schema
                             ;
                             
--Datset resub_final is the final output of AR flow. 
STORE resub_final INTO '$brrLineItemInterimDir/LI_14_process_ar_resub_filter' USING PigStorage('$delimiter');