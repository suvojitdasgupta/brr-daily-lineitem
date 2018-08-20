/*
            Script Name : LI_10_process_ar_crypto.pig
 jUnit Test Script Name : PigUnit10ProcessArCrypto.java
                Purpose : pig script to process main AR flow. 
             Dependency : LI_09_process_ar_crypto.pig                         
*/

SET pig.exec.mapPartAgg '$pig_exec_mappart_agg_enabled';
SET pig.user.cache.enabled '$pig_user_cache_mode_enabled';
SET pig.auto.local.enabled '$pig_auto_local_mode_enabled';
SET pig.local.io.sort.mb '$pig_auto_local_io_sort_mb';
SET pig.auto.local.input.maxbytes '$pig_auto_local_input_maxbytes';
SET mapreduce.reduce.memory.mb 3240;
SET pig.exec.reducers.bytes.per.reducer 100000000;

DEFINE BagConcat datafu.pig.bags.BagConcat();
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
                                 (chararray)$11 AS retry_count,
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
                                 (chararray)$27 AS balance_id,
                                 (chararray)$28 AS bill_id,
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

decrypt_pm = LOAD '$brrLineItemInterimDir/LI_09_process_ar_crypto/part*' using PigStorage('$delimiter') AS
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
						nb_amount:chararray,
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
						ext_transaction_id:chararray,
						ext_transaction_init_dt:chararray
					  );

balance_impact_tax_amt = LOAD '$brrLineItemSqoopDir/tax_non_refunds_balance_impact/part*' using PigStorage('$delimiter') AS 
					 (
					   balance_id:chararray,
					   tax_amount:double
					  );
				  
sub_t_offer_subscription_id = LOAD '$brrLineItemSqoopDir/offer_sub_ids/part*' using PigStorage('$delimiter') AS
                      (
                        balance_id:chararray,
                        offer_subscription_id:chararray
                      );				  

payment_method_token = LOAD '$brrLineItemSqoopDir/ar_payment_token/part*' using PigStorage('$delimiter') AS
                      (
                        ar_payment_method_id:chararray,
                        payment_token:chararray
                      );
				 					  
decrypt_pm_filter_Y = FILTER decrypt_pm BY (TRIM(trans_type)=='RT');

decrypt_pm_filter_N = FILTER decrypt_pm BY NOT(TRIM(trans_type)=='RT'); 
decrypt_pm_filter_N_join = JOIN decrypt_pm_filter_N BY (balance_id) LEFT OUTER, balance_impact_tax_amt BY (balance_id);
decrypt_pm_filter_N_xform = FOREACH decrypt_pm_filter_N_join GENERATE
								decrypt_pm_filter_N::bid AS bid,
								decrypt_pm_filter_N::sid AS sid,
								decrypt_pm_filter_N::global_acct_id AS global_acct_id,
								decrypt_pm_filter_N::obi_acct AS obi_acct,
								decrypt_pm_filter_N::ar_payment_method_id AS ar_payment_method_id,
								decrypt_pm_filter_N::pm_id AS pm_id,
								decrypt_pm_filter_N::pm_acct_num AS pm_acct_num,
								decrypt_pm_filter_N::routing_number AS routing_number,
								decrypt_pm_filter_N::cycle AS cycle,
								decrypt_pm_filter_N::billed_dt AS billed_dt,
								decrypt_pm_filter_N::trans_type AS trans_type,
								decrypt_pm_filter_N::retry_count AS retry_count,
								decrypt_pm_filter_N::result_code AS result_code,
								decrypt_pm_filter_N::reason_code AS reason_code,
								decrypt_pm_filter_N::lineitem AS lineitem,
								decrypt_pm_filter_N::charge_amount AS charge_amount,
								decrypt_pm_filter_N::bill_amount AS bill_amount,
								decrypt_pm_filter_N::paid_amount AS paid_amount,
								decrypt_pm_filter_N::trans_amount AS trans_amount,
								COALESCE(balance_impact_tax_amt::tax_amount,0.0) AS tax_amount,
								decrypt_pm_filter_N::trans_source_id AS trans_source_id,
								decrypt_pm_filter_N::ppv_id AS ppv_id,
								decrypt_pm_filter_N::nb_reason_code AS nb_reason_code,
								decrypt_pm_filter_N::nb_amount AS nb_amount,
								decrypt_pm_filter_N::processing_division AS processing_division,
								decrypt_pm_filter_N::trans_apply_dt AS trans_apply_dt,
								decrypt_pm_filter_N::adjust_amount AS adjust_amount,
								decrypt_pm_filter_N::balance_id AS balance_id,
								decrypt_pm_filter_N::bill_id AS bill_id,
								((decrypt_pm_filter_N::resub_flg == '0') ? 'N' : decrypt_pm_filter_N::resub_flg) AS resub_flg,
								decrypt_pm_filter_N::write_off_source AS write_off_source,
								decrypt_pm_filter_N::write_off_reason_cd AS write_off_reason_cd,
								decrypt_pm_filter_N::city AS city,
								decrypt_pm_filter_N::state AS state,
								decrypt_pm_filter_N::zipcode AS zipcode,
								decrypt_pm_filter_N::country_code AS country_code,
								decrypt_pm_filter_N::iso_currency_id AS iso_currency_id,
								decrypt_pm_filter_N::origin_of_sales AS origin_of_sales,
								decrypt_pm_filter_N::paid_dt AS paid_dt,
								decrypt_pm_filter_N::first_name AS first_name,
								decrypt_pm_filter_N::last_name AS last_name,
								decrypt_pm_filter_N::email_address AS email_address,
								decrypt_pm_filter_N::phone_number AS phone_number,
								decrypt_pm_filter_N::street AS street,
								decrypt_pm_filter_N::payment_ref_id AS payment_ref_id,
								decrypt_pm_filter_N::ext_transaction_id AS ext_transaction_id,
								decrypt_pm_filter_N::ext_transaction_init_dt AS ext_transaction_init_dt;

decrypt_pm_filter_Y_enforced_schema = enforce_schema(decrypt_pm_filter_Y);
decrypt_pm_filter_N_xform_enforced_schema = enforce_schema(decrypt_pm_filter_N_xform);
decrypt_pm_union = UNION ONSCHEMA decrypt_pm_filter_Y_enforced_schema,
                                  decrypt_pm_filter_N_xform_enforced_schema;

decrypt_pm_union_join = JOIN decrypt_pm_union BY (balance_id) LEFT OUTER, sub_t_offer_subscription_id BY (balance_id);	

--Only the needed fields are taken so the DISTINCT operation can work correctly.
decrypt_pm_union_join_clean = FOREACH decrypt_pm_union_join GENERATE 
                                      decrypt_pm_union::balance_id AS balance_id,
                                      sub_t_offer_subscription_id::offer_subscription_id AS offer_subscription_id;                                      
decrypt_pm_union_group = GROUP decrypt_pm_union_join_clean BY (balance_id);
                           
decrypt_pm_union_denormalize = FOREACH decrypt_pm_union_group {
                               unique_data = DISTINCT decrypt_pm_union_join_clean;
                               concatenate = FOREACH unique_data GENERATE
                                             CONCAT(offer_subscription_id,'');
                               -- The Bag is flattened to String with delimiter as # which is different than the overall 
                               -- delimiter for the application defined by the $delimiter variable(QA=|,PROD=^E).
                               -- Comma can't be used as PIG used comma internally.              
                               GENERATE FLATTEN(group) AS balance_id,  BagToString(concatenate,'#') AS payment_reference_ids;             
                               };                           
                                                      
decrypt_pm_with_payref = JOIN decrypt_pm_union BY (balance_id), decrypt_pm_union_denormalize BY (balance_id);
decrypt_pm_token = JOIN decrypt_pm_with_payref BY (decrypt_pm_union::ar_payment_method_id) LEFT OUTER, payment_method_token BY (ar_payment_method_id);

decrypt_pm_token_xform = FOREACH decrypt_pm_token GENERATE 
                                 decrypt_pm_with_payref::decrypt_pm_union::bid AS bid,
                                 decrypt_pm_with_payref::decrypt_pm_union::sid AS sid,
                                 decrypt_pm_with_payref::decrypt_pm_union::global_acct_id AS global_acct_id,
                                 decrypt_pm_with_payref::decrypt_pm_union::obi_acct AS obi_acct,
                                 decrypt_pm_with_payref::decrypt_pm_union::ar_payment_method_id AS ar_payment_method_id,
                                 decrypt_pm_with_payref::decrypt_pm_union::pm_id AS pm_id,
                                 decrypt_pm_with_payref::decrypt_pm_union::pm_acct_num AS pm_acct_num,
                                 decrypt_pm_with_payref::decrypt_pm_union::routing_number AS routing_number,
                                 decrypt_pm_with_payref::decrypt_pm_union::cycle AS cycle,
                                 decrypt_pm_with_payref::decrypt_pm_union::billed_dt AS billed_dt,
                                 decrypt_pm_with_payref::decrypt_pm_union::trans_type AS trans_type ,
                                 decrypt_pm_with_payref::decrypt_pm_union::retry_count AS retry_count,
                                 decrypt_pm_with_payref::decrypt_pm_union::result_code AS result_code,
                                 COALESCE(TRIM(decrypt_pm_with_payref::decrypt_pm_union::reason_code),'100') AS reason_code,
                                 decrypt_pm_with_payref::decrypt_pm_union::lineitem AS lineitem,
                                 decrypt_pm_with_payref::decrypt_pm_union::charge_amount AS charge_amount,
                                 decrypt_pm_with_payref::decrypt_pm_union::bill_amount AS bill_amount,
                                 decrypt_pm_with_payref::decrypt_pm_union::paid_amount AS paid_amount,
                                 decrypt_pm_with_payref::decrypt_pm_union::trans_amount AS trans_amount,
                                 decrypt_pm_with_payref::decrypt_pm_union::tax_amount AS tax_amount,
                                 COALESCE(TRIM(decrypt_pm_with_payref::decrypt_pm_union::trans_source_id),'OBI_AR') AS trans_source_id,
                                 decrypt_pm_with_payref::decrypt_pm_union::ppv_id AS ppv_id,
                                 decrypt_pm_with_payref::decrypt_pm_union::nb_reason_code AS nb_reason_code,
                                 decrypt_pm_with_payref::decrypt_pm_union::nb_amount AS nb_amount,
                                 decrypt_pm_with_payref::decrypt_pm_union::processing_division AS processing_division,
                                 decrypt_pm_with_payref::decrypt_pm_union::trans_apply_dt AS trans_apply_dt,
                                 decrypt_pm_with_payref::decrypt_pm_union::adjust_amount AS adjust_amount,
                                 decrypt_pm_with_payref::decrypt_pm_union::balance_id AS balance_id,
                                 decrypt_pm_with_payref::decrypt_pm_union::bill_id AS bill_id,
                                 decrypt_pm_with_payref::decrypt_pm_union::resub_flg AS resub_flg,
                                 TRIM(decrypt_pm_with_payref::decrypt_pm_union::write_off_source) AS write_off_source,
                                 TRIM(decrypt_pm_with_payref::decrypt_pm_union::write_off_reason_cd) AS write_off_reason_cd,
                                 decrypt_pm_with_payref::decrypt_pm_union::city AS city,
                                 decrypt_pm_with_payref::decrypt_pm_union::state AS state,
                                 decrypt_pm_with_payref::decrypt_pm_union::zipcode AS zipcode,
                                 decrypt_pm_with_payref::decrypt_pm_union::country_code AS country_code,
                                 decrypt_pm_with_payref::decrypt_pm_union::iso_currency_id AS iso_currency_id,
                                 COALESCE(TRIM(decrypt_pm_with_payref::decrypt_pm_union::origin_of_sales),'-1') AS origin_of_sales,
                                 decrypt_pm_with_payref::decrypt_pm_union::paid_dt AS paid_dt,
                                 decrypt_pm_with_payref::decrypt_pm_union::first_name AS first_name,
                                 decrypt_pm_with_payref::decrypt_pm_union::last_name AS last_name,
                                 decrypt_pm_with_payref::decrypt_pm_union::email_address AS email_address,
                                 decrypt_pm_with_payref::decrypt_pm_union::phone_number AS phone_number,
                                 decrypt_pm_with_payref::decrypt_pm_union::street AS street,
                                 --This field is an individual TUPLE by itself and will be in the format: 
                                 --|VALUE#VALUE#VALUE#...|                           
                                 decrypt_pm_with_payref::decrypt_pm_union_denormalize::payment_reference_ids AS payment_reference_ids,
                                 payment_method_token::payment_token AS payment_token;
                                 
STORE decrypt_pm_token_xform INTO '$brrLineItemInterimDir/LI_10_process_ar_crypto' USING PigStorage('$delimiter'); 