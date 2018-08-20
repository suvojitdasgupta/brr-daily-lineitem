/*
            Script Name : LI_09_process_ar_crypto.pig
 jUnit Test Script Name : PigUnit09ProcessArCrypto.java
                Purpose : pig script for processing the AR data from OBI.
                          The script processes takes multiple inputs , consolidates then, adds address info 
                          and moves the data for futher processing.
             Dependency : LI_08_process_ar_gather.pig.                          
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
%DECLARE EMPTY ''

DEFINE enforce_schema_address(input_records) RETURNS output_records {

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
                                 (chararray)$12 AS result_cd,
                                 (chararray)$13 AS reason_cd,
                                 (chararray)$14 AS lineitem,
                                 (double)$15 AS charge_amount,
                                 (double)$16 AS bill_amount,
                                 (double)$17 AS paid_amount,
                                 (double)$18 AS trans_amount,
                                 (double)$19 AS tax_amount,
                                 (chararray)$20 AS trans_source_id,
                                 (chararray)$21 AS ppv_id,
                                 (chararray)$22 AS nb_reason_cd,
                                 (double)$23 AS nb_amount,
                                 (chararray)$24 AS processing_division,
                                 (chararray)$25 AS trans_apply_dt,
                                 (double)$26 AS adjust_amount,
                                 (chararray)$27 AS line_item_balance_id,
                                 (chararray)$28 AS line_item_bill_id,
                                 (chararray)$29 AS resub_flg,
                                 (chararray)$30 AS write_off_source,
                                 (chararray)$31 AS write_off_reason_cd,
                                 (chararray)$32 AS city,
                                 (chararray)$33 AS state,
                                 (chararray)$34 AS zipcode,
                                 (chararray)$35 AS country_cd,
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

DEFINE enforce_schema_ALL(input_records) RETURNS output_records {
       
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
                                 (chararray)$12 AS result_cd,
                                 (chararray)$13 AS reason_cd,
                                 (chararray)$14 AS lineitem,
                                 (double)$15 AS charge_amount,
                                 (double)$16 AS bill_amount,
                                 (double)$17 AS paid_amount,
                                 (double)$18 AS trans_amount,
                                 (double)$19 AS tax_amount,
                                 (chararray)$20 AS trans_source_id,
                                 (chararray)$21 AS ppv_id,
                                 (chararray)$22 AS nb_reason_cd,
                                 (double)$23 AS nb_amount,
                                 (chararray)$24 AS processing_division,
                                 (chararray)$25 AS trans_apply_dt,
                                 (double)$26 AS adjust_amount,
                                 (chararray)$27 AS line_item_balance_id,
                                 (chararray)$28 AS line_item_bill_id,
                                 (chararray)$29 AS resub_flg,
                                 (chararray)$30 AS write_off_source,
                                 (chararray)$31 AS write_off_reason_cd,
                                 (chararray)$32 AS city,
                                 (chararray)$33 AS state,
                                 (chararray)$34 AS zipcode,
                                 (chararray)$35 AS country_cd,
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

ar_consolidated = LOAD '$brrLineItemInterimDir/LI_08_process_ar_gather/part*' using PigStorage('$delimiter') AS
                        (
                        bill_applied_date:chararray,
						line_item_balance_id:chararray,
						cash_credit_id:chararray,
						payment_processor_activity:chararray,
						trans_type:chararray,
						retry_count:chararray,
						result_cd:chararray,
						acct_id:chararray,
						trans_date:chararray,
						pay_method:chararray,
						lineitem:chararray,
						seq_no:chararray,
						reason_cd:chararray,
						trans_amount:double,
						sid:chararray,
						bid:chararray,
						global_acct_id:chararray,
						processing_division:chararray,
						payment_processor:chararray,
						campaign_id:chararray,
						line_item_bill_id:chararray,
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
					   
address_payref = LOAD '$brrLineItemInterimDir/LI_02A_process_address/part*' using PigStorage('$delimiter') AS
                        (
                          line_item_bill_id:chararray,
                          payment_reference:chararray,
                          last_transaction_amount:double,
                          adjustment_amount:double,
                          city:chararray,
                          state_province:chararray,
                          zip_postal_cd:chararray,
                          country_cd:chararray,
                          first_name:chararray,
                          last_name:chararray,
                          email_address:chararray,
                          phone_number:chararray,
                          street:chararray,
                          apt_suite:chararray,
                          payment_token:chararray,
                          routing_number:chararray
                         );
                       
address = LOAD '$brrLineItemSqoopDir/address_non_payref/part*' using PigStorage('$delimiter') AS 
                   (
					master_acct_num:chararray,
					email_address:chararray,
					phone_number:chararray,
					first_name:chararray,
					last_name:chararray,
					street:chararray,
					apt_suite:chararray,
					city:chararray,
					state_province:chararray,
					zip_postal_cd:chararray,
					country_cd:chararray,
					created_dt:chararray,
					payment_token:chararray,
					routing_number:chararray
				   );				   

non_bill_trans = LOAD '$brrLineItemSqoopDir/non_bill_transactions/part*' using PigStorage('$delimiter') AS 
						(
						global_acct_id:chararray,
						acct_id:chararray,
						bid:chararray,
						sid:chararray,
						actual_bill_dt:chararray,
						line_item_balance_id:chararray,
						line_item_bill_id:chararray,
						bill_status:chararray,
						balance_status:chararray,
						bal_created_dt:chararray,
						pm_id:chararray,
						lineitem:chararray,
						ar_payment_method_id:chararray,
						gross_amount:double,
						paid_amount:double,
						bill_amount:double,
						adjustment_amount:double,
						last_modified_by:chararray,
						retry_count:chararray,
						bill_paid_amount:double,
						credited_amount:double,
						payment_reference:chararray,
						terminate_status:chararray,
						payment_processor_activity_id:chararray,
						activity_amount:double,
						ar_result:chararray,
						ppv_id:chararray,
						processing_division:chararray,
						reason_cd:chararray,
						reason_text:chararray,
						email_address:chararray,
						phone_number:chararray,
						first_name:chararray,
						last_name:chararray,
						street:chararray,
						apt_suite:chararray,
						city:chararray,
						state_province:chararray,
						zip_postal_cd:chararray,
						country_cd:chararray,
						payment_token:chararray,
						routing_number:chararray
						);

				   				  
ar_address_payref = JOIN ar_consolidated BY (line_item_bill_id) LEFT OUTER, address_payref BY (line_item_bill_id);	

/*Unmatch Processing - Start*/
ar_address_payref_unused0 = FILTER ar_address_payref BY (address_payref::line_item_bill_id IS NULL);

master_acct_number_unused0 = FOREACH ar_address_payref_unused0 GENERATE 
                                     ar_consolidated::master_acct_num AS master_acct_num;                                  
master_acct_number_dedup = DISTINCT master_acct_number_unused0;
ar_address_only =  JOIN  master_acct_number_dedup BY (master_acct_num) LEFT OUTER ,  address BY (master_acct_num);     
ar_address_only_group = GROUP ar_address_only BY (master_acct_number_dedup::master_acct_num);
ar_address_only_dedup = FOREACH ar_address_only_group {
                                    sorted = ORDER ar_address_only BY address::created_dt DESC;
                                    top_rec = LIMIT  sorted 1;
                                    GENERATE FLATTEN(top_rec);
                                   };
                                   
                                                                                                     
ar_address_unused_with_address = JOIN  ar_address_payref_unused0 BY (ar_consolidated::master_acct_num) ,  
                                       ar_address_only_dedup BY (top_rec::master_acct_number_dedup::master_acct_num);                                                                          
ar_address_unused_with_address_with_currency = JOIN ar_address_unused_with_address BY (ar_address_payref_unused0::ar_consolidated::bid,
                                                                                       ar_address_payref_unused0::ar_consolidated::sid), 
                                                    currency_id BY (bid,sid);
                                                   
ar_address_unused_xform = FOREACH ar_address_unused_with_address_with_currency GENERATE 
                                    ar_address_unused_with_address::ar_address_payref_unused0::ar_consolidated::bid AS bid,
                                    ar_address_unused_with_address::ar_address_payref_unused0::ar_consolidated::sid AS sid,
                                    ar_address_unused_with_address::ar_address_payref_unused0::ar_consolidated::global_acct_id AS global_acct_id,
                                    '' AS obi_acct,
                                    ar_address_unused_with_address::ar_address_payref_unused0::ar_consolidated::payment_method_id AS ar_payment_method_id,
                                    ar_address_unused_with_address::ar_address_payref_unused0::ar_consolidated::pay_method AS pm_id,
                                    
                                    ar_address_unused_with_address::ar_address_payref_unused0::ar_consolidated::pm_acct_num AS pm_acct_num,
                                    
                                    ar_address_unused_with_address::ar_address_only_dedup::top_rec::address::routing_number AS routing_number,
                                    SUBSTRING(ar_address_unused_with_address::ar_address_payref_unused0::ar_consolidated::bill_date,8,10) AS cycle,
                                    ar_address_unused_with_address::ar_address_payref_unused0::ar_consolidated::bill_date AS billed_dt,
                                    (                                  
                                      ( (ar_address_unused_with_address::ar_address_payref_unused0::ar_consolidated::trans_type == 'ME') AND 
                                        ( 
                                          (ar_address_unused_with_address::ar_address_payref_unused0::ar_consolidated::reason_cd == '951') OR 
                                          (ar_address_unused_with_address::ar_address_payref_unused0::ar_consolidated::reason_cd == '954')
                                        )
                                      ) 
                                     ? 'WN'
                                    : ar_address_unused_with_address::ar_address_payref_unused0::ar_consolidated::trans_type
   
                                    ) AS trans_type,                                   
                                    ar_address_unused_with_address::ar_address_payref_unused0::ar_consolidated::retry_count AS retry_count,
                                    ar_address_unused_with_address::ar_address_payref_unused0::ar_consolidated::result_cd AS result_cd,
                                    ar_address_unused_with_address::ar_address_payref_unused0::ar_consolidated::reason_cd AS reason_cd,
                                    ar_address_unused_with_address::ar_address_payref_unused0::ar_consolidated::lineitem AS lineitem,
                                    ar_address_unused_with_address::ar_address_payref_unused0::ar_consolidated::charge_amount AS charge_amount,
                                    ar_address_unused_with_address::ar_address_payref_unused0::ar_consolidated::bill_amount AS bill_amount,
                                    0.0 AS paid_amount,
                                    ar_address_unused_with_address::ar_address_payref_unused0::ar_consolidated::trans_amount AS trans_amount,
                                    0.0 AS tax_amount,
                                    'OBI_AR' AS trans_source_id,
                                    ar_address_unused_with_address::ar_address_payref_unused0::ar_consolidated::pay_method AS ppv_id,
                                    '' AS nb_reason_cd,
                                    0.0 AS nb_amount,
                                    
                                    (
                                      ( ar_address_unused_with_address::ar_address_payref_unused0::ar_consolidated::processing_division IS NOT NULL AND 
                                        ar_address_unused_with_address::ar_address_payref_unused0::ar_consolidated::processing_division != '0'
                                      ) ? processing_division : '12345'
                                    ),
                                    
                                    ar_address_unused_with_address::ar_address_payref_unused0::ar_consolidated::trans_date AS trans_apply_dt,
                                    0.0 AS adjust_amount,
                                    ar_address_unused_with_address::ar_address_payref_unused0::ar_consolidated::line_item_balance_id AS line_item_balance_id,
                                    ar_address_unused_with_address::ar_address_payref_unused0::ar_consolidated::line_item_bill_id AS line_item_bill_id,
                                    ar_address_unused_with_address::ar_address_payref_unused0::ar_consolidated::resub_flg AS resub_flg,
                                    ar_address_unused_with_address::ar_address_payref_unused0::ar_consolidated::write_off_source AS write_off_source,
                                    '' AS write_off_reason_cd,
                                    ar_address_unused_with_address::ar_address_only_dedup::top_rec::address::city AS city,
                                    ar_address_unused_with_address::ar_address_only_dedup::top_rec::address::state_province AS state,
                                    ar_address_unused_with_address::ar_address_only_dedup::top_rec::address::zip_postal_cd AS zipcode,
                                    ar_address_unused_with_address::ar_address_only_dedup::top_rec::address::country_cd AS country_cd,
                                    currency_id::iso_currency_id AS iso_currency_id,
                                    '-1' AS origin_of_sales,
                                    ar_address_unused_with_address::ar_address_payref_unused0::ar_consolidated::trans_date AS paid_dt,
                                    ar_address_unused_with_address::ar_address_only_dedup::top_rec::address::first_name AS first_name,
                                    ar_address_unused_with_address::ar_address_only_dedup::top_rec::address::last_name AS last_name,
                                    ar_address_unused_with_address::ar_address_only_dedup::top_rec::address::email_address AS email_address,
                                    ar_address_unused_with_address::ar_address_only_dedup::top_rec::address::phone_number AS phone_number,
                                    ar_address_unused_with_address::ar_address_only_dedup::top_rec::address::street AS street,
                                    '' AS payment_ref_id,
                                    '' AS ext_transaction_id,
                                    '' AS ext_transaction_init_dt;

---------------------------------------------------------------------------------------------------------------------
--This dataset will go to GATHER 
--Dataset Name - ar_address_unused_xform
---------------------------------------------------------------------------------------------------------------------

ar_address_payref_match = FILTER ar_address_payref BY (ar_consolidated::line_item_bill_id IS NOT NULL AND address_payref::line_item_bill_id IS NOT NULL);

---------------------------------------------------------------------------------------------------
-- get_ppv_id implemention - Starts 
---------------------------------------------------------------------------------------------------
get_ppv_id_1 = JOIN ar_address_payref_match BY (ar_consolidated::bid,
                                                ar_consolidated::sid,
                                                ar_consolidated::pay_method),  
                    cdb_pay_info BY (bid,sid,pm_id);  


-- 1st fallout - Start
get_ppv_id_LO1 = JOIN ar_address_payref_match BY (ar_consolidated::bid,
                                                  ar_consolidated::sid,
                                                  ar_consolidated::pay_method) LEFT OUTER,  
                      cdb_pay_info BY (bid,sid,pm_id) USING 'replicated';                                          
get_ppv_id_LO1_F = FILTER get_ppv_id_LO1 BY (cdb_pay_info::bid IS NULL AND
                                             cdb_pay_info::sid IS NULL AND
                                             cdb_pay_info::pm_id IS NULL);
get_ppv_id_LO1_FX = FOREACH get_ppv_id_LO1_F GENERATE 
                    ar_address_payref_match::ar_consolidated::bill_applied_date ..
                    ar_address_payref_match::address_payref::routing_number;                                           
 -- 1st fallout - End   
                                            
get_ppv_id_2 = JOIN get_ppv_id_LO1_FX BY (ar_consolidated::bid,
                                         '-1',
                                         ar_consolidated::pay_method),  
                        cdb_pay_info BY (bid,sid,pm_id);                                                                   
                                                
-- 2nd fallout - Start
get_ppv_id_LO2 = JOIN get_ppv_id_LO1_FX BY (ar_consolidated::bid,
                                           '-1',
                                           ar_consolidated::pay_method) LEFT OUTER,  
                          cdb_pay_info BY (bid,sid,pm_id) USING 'replicated';                                                                   
get_ppv_id_LO2_F = FILTER  get_ppv_id_LO2 BY (cdb_pay_info::bid IS NULL AND
                                              cdb_pay_info::sid IS NULL AND
                                              cdb_pay_info::pm_id IS NULL); 
get_ppv_id_LO2_FX = FOREACH get_ppv_id_LO2_F GENERATE 
                    get_ppv_id_LO1_FX::ar_address_payref_match::ar_consolidated::bill_applied_date ..
                    get_ppv_id_LO1_FX::ar_address_payref_match::address_payref::routing_number;
-- 2nd fallout - End  

get_ppv_id_3 = JOIN get_ppv_id_LO2_FX BY ('-1',
                                          ar_consolidated::sid,
                                          ar_consolidated::pay_method),  
                         cdb_pay_info BY (bid,sid,pm_id);   

-- 3rd fallout - Start
get_ppv_id_LO3 = JOIN get_ppv_id_LO2_FX BY ('-1',
                                         ar_consolidated::sid,
                                         ar_consolidated::pay_method) LEFT OUTER, 
                        cdb_pay_info BY (bid,sid,pm_id) USING 'replicated';   
get_ppv_id_LO3_F = FILTER  get_ppv_id_LO3 BY (cdb_pay_info::bid IS NULL AND
                                              cdb_pay_info::sid IS NULL AND
                                              cdb_pay_info::pm_id IS NULL);                                   
get_ppv_id_LO3_FX = FOREACH get_ppv_id_LO3_F GENERATE
                    get_ppv_id_LO2_FX::get_ppv_id_LO1_FX::ar_address_payref_match::ar_consolidated::bill_applied_date ..
                    get_ppv_id_LO2_FX::get_ppv_id_LO1_FX::ar_address_payref_match::address_payref::routing_number;                
-- 3rd fallout - End 

get_ppv_id_4 = JOIN get_ppv_id_LO3_FX BY ('-1',
                                         '-1',
                                         ar_consolidated::pay_method),  
                        cdb_pay_info BY (bid,sid,pm_id);  
                        
 -- 4th fallout - Start
get_ppv_id_LO4 = JOIN get_ppv_id_LO3_FX BY ('-1',
                                           '-1',
                                           ar_consolidated::pay_method) LEFT OUTER,  
                      cdb_pay_info BY (bid,sid,pm_id) USING 'replicated'; 
get_ppv_id_LO4_F = FILTER  get_ppv_id_LO4 BY (cdb_pay_info::bid IS NULL AND
                                              cdb_pay_info::sid IS NULL AND
                                              cdb_pay_info::pm_id IS NULL);                                              
get_ppv_id_LO4_FX = FOREACH get_ppv_id_LO4_F GENERATE
                    get_ppv_id_LO3_FX::get_ppv_id_LO2_FX::get_ppv_id_LO1_FX::ar_address_payref_match::ar_consolidated::bill_applied_date ..
                    get_ppv_id_LO3_FX::get_ppv_id_LO2_FX::get_ppv_id_LO1_FX::ar_address_payref_match::address_payref::routing_number;                                                 
 -- 4th fallout - End                       

get_ppv_id_5 = JOIN get_ppv_id_LO4_FX BY (ar_consolidated::bid,
                                         ar_consolidated::sid,
                                         '-1'),  
                        cdb_pay_info BY (bid,sid,pm_id);

 -- 5th fallout - Start
get_ppv_id_LO5 = JOIN get_ppv_id_LO4_FX BY (ar_consolidated::bid,
                                         ar_consolidated::sid,
                                         '-1') LEFT OUTER,  
                        cdb_pay_info BY (bid,sid,pm_id) USING 'replicated';
 
get_ppv_id_LO5_F = FILTER  get_ppv_id_LO5 BY (cdb_pay_info::bid IS NULL AND
                                              cdb_pay_info::sid IS NULL AND
                                              cdb_pay_info::pm_id IS NULL);                                              
get_ppv_id_LO5_FX = FOREACH get_ppv_id_LO5_F GENERATE
                    get_ppv_id_LO4_FX::get_ppv_id_LO3_FX::get_ppv_id_LO2_FX::get_ppv_id_LO1_FX::ar_address_payref_match::ar_consolidated::bill_applied_date ..
                    get_ppv_id_LO4_FX::get_ppv_id_LO3_FX::get_ppv_id_LO2_FX::get_ppv_id_LO1_FX::ar_address_payref_match::address_payref::routing_number;                                                 
 -- 5th fallout - End 
 
get_ppv_id_6 = JOIN get_ppv_id_LO5_FX BY (ar_consolidated::bid,
                                         '-1',
                                         '-1'),  
                         cdb_pay_info BY (bid,sid,pm_id);
                         
 -- 6th fallout - Start
get_ppv_id_LO6 = JOIN get_ppv_id_LO5_FX BY (ar_consolidated::bid,
                                         '-1',
                                         '-1') LEFT OUTER,  
                        cdb_pay_info BY (bid,sid,pm_id) USING 'replicated';
get_ppv_id_LO6_F = FILTER  get_ppv_id_LO6 BY (cdb_pay_info::bid IS NULL AND
                                              cdb_pay_info::sid IS NULL AND
                                              cdb_pay_info::pm_id IS NULL);  
get_ppv_id_LO6_FX = FOREACH get_ppv_id_LO6_F GENERATE
                    get_ppv_id_LO5_FX::get_ppv_id_LO4_FX::get_ppv_id_LO3_FX::get_ppv_id_LO2_FX::get_ppv_id_LO1_FX::ar_address_payref_match::ar_consolidated::bill_applied_date ..
                    get_ppv_id_LO5_FX::get_ppv_id_LO4_FX::get_ppv_id_LO3_FX::get_ppv_id_LO2_FX::get_ppv_id_LO1_FX::ar_address_payref_match::address_payref::routing_number;                                                                       
 -- 6th fallout - End

get_ppv_id_7 = JOIN get_ppv_id_LO6_FX BY ('-1',
                                         ar_consolidated::sid,
                                         '-1'),  
                         cdb_pay_info BY (bid,sid,pm_id);
                       
 -- 7th fallout - Start
get_ppv_id_LO7 = JOIN get_ppv_id_LO6_FX BY ('-1',
                                         ar_consolidated::sid,
                                         '-1') LEFT OUTER,  
                        cdb_pay_info BY (bid,sid,pm_id) USING 'replicated';
 
get_ppv_id_LO7_F = FILTER  get_ppv_id_LO7 BY (cdb_pay_info::bid IS NULL AND
                                              cdb_pay_info::sid IS NULL AND
                                              cdb_pay_info::pm_id IS NULL);  
get_ppv_id_LO7_FX = FOREACH get_ppv_id_LO7_F GENERATE
                    get_ppv_id_LO6_FX::get_ppv_id_LO5_FX::get_ppv_id_LO4_FX::get_ppv_id_LO3_FX::get_ppv_id_LO2_FX::get_ppv_id_LO1_FX::ar_address_payref_match::ar_consolidated::bill_applied_date ..
                    get_ppv_id_LO6_FX::get_ppv_id_LO5_FX::get_ppv_id_LO4_FX::get_ppv_id_LO3_FX::get_ppv_id_LO2_FX::get_ppv_id_LO1_FX::ar_address_payref_match::address_payref::routing_number;                                                                                                                    
 -- 7th fallout - End
 
get_ppv_id_8 = JOIN get_ppv_id_LO7_FX BY ('-1',
                                          '-1',
                                          '-1'),  
                     cdb_pay_info BY (bid,sid,pm_id);

ar_ppv_id = UNION get_ppv_id_1, 
                  get_ppv_id_2, 
                  get_ppv_id_3, 
                  get_ppv_id_4, 
                  get_ppv_id_5, 
                  get_ppv_id_6, 
                  get_ppv_id_7, 
                  get_ppv_id_8 ;
                                   
ar_ppv_id_defineSchema = FOREACH ar_ppv_id GENERATE
                                  $0 AS ar_bill_applied_date: chararray,
                                  $1 AS ar_line_item_balance_id: chararray,
                                  $2 AS ar_cash_credit_id: chararray,
                                  $3 AS ar_payment_processor_activity: chararray,
                                  $4 AS ar_trans_type: chararray,
                                  $5 AS ar_retry_count: chararray,
                                  $6 AS ar_result_cd: chararray,
                                  $7 AS ar_acct_id: chararray,
                                  $8 AS ar_trans_date: chararray,
                                  $9 AS ar_pay_method: chararray,
                                  $10 AS ar_lineitem: chararray,
                                  $11 AS ar_seq_no: chararray,
                                  $12 AS ar_reason_cd: chararray,
                                  $13 AS ar_trans_amount: double,
                                  $14 AS ar_sid: chararray,
                                  $15 AS ar_bid: chararray,
                                  $16 AS ar_global_acct_id: chararray,
                                  $17 AS ar_processing_division: chararray,
                                  $18 AS ar_payment_processor: chararray,
                                  $19 AS ar_campaign_id: chararray,
                                  $20 AS ar_line_item_bill_id: chararray,
                                  $21 AS ar_bill_cycle: chararray,
                                  $22 AS ar_bill_date: chararray,
                                  $23 AS ar_write_off_source: chararray,
                                  $24 AS ar_master_acct_num: chararray,
                                  $25 AS ar_bill_applied_amount: double,
                                  $26 AS ar_charge_amount: double,
                                  $27 AS ar_bill_amount: double,
                                  $28 AS ar_resub_flg: chararray,
                                  $29 AS ar_brandi: chararray,
                                  $30 AS ar_payment_method_id: chararray,
                                  $31 AS ar_pm_acct_num: chararray,
                                  $32 AS address_payref_line_item_bill_id: chararray,
                                  $33 AS address_payref_payment_reference: chararray,
                                  $34 AS address_payref_last_transaction_amount: double,
                                  $35 AS address_payref_adjustment_amount: double,
                                  $36 AS address_payref_city: chararray,
                                  $37 AS address_payref_state_province: chararray,
                                  $38 AS address_payref_zip_postal_cd: chararray,
                                  $39 AS address_payref_country_cd: chararray,
                                  $40 AS address_payref_first_name: chararray,
                                  $41 AS address_payref_last_name: chararray,
                                  $42 AS address_payref_email_address: chararray,
                                  $43 AS address_payref_phone_number: chararray,
                                  $44 AS address_payref_street: chararray,
                                  $45 AS address_payref_apt_suite: chararray,
                                  $46 AS address_payref_payment_token: chararray,
                                  $47 AS address_payref_routing_number: chararray,
                                  $51 AS cdb_pay_info_ppv_id: chararray;                                            
-------------------------------------------------------
-- Dataset Name - ar_ppv_id_defineSchema; 
-- get_ppv_id implemention - Ends 
-------------------------------------------------------   

-------------------------------------------------------
-- getDivisionID implemention - Starts 
-------------------------------------------------------
ar_division_id_join = JOIN ar_ppv_id_defineSchema BY (cdb_pay_info_ppv_id, 
                                                 ar_bid, 
                                                 ar_sid, 
                                                 ar_lineitem) LEFT OUTER, 
                                  vendor_merchant BY (ppv_id,
                                                      bid,
                                                      sid,
                                                      lineitem) USING 'replicated';
                                                                                             
-------------------------------------------------------
-- Dataset Name - ar_division_id_join; 
-- getDivisionID implemention - End 
-------------------------------------------------------

ar_currency_join = JOIN ar_division_id_join BY (ar_ppv_id_defineSchema::ar_bid,
                                                ar_ppv_id_defineSchema::ar_sid), 
                                currency_id BY (bid,sid);

ar_address_payref_match_flatten = FOREACH ar_currency_join GENERATE 
                                          ar_ppv_id_defineSchema::ar_bill_applied_date AS ar_bill_applied_date,
                                          ar_ppv_id_defineSchema::ar_line_item_balance_id AS ar_line_item_balance_id,
                                          ar_ppv_id_defineSchema::ar_cash_credit_id AS ar_cash_credit_id,
                                          ar_ppv_id_defineSchema::ar_payment_processor_activity AS ar_payment_processor_activity,
                                          ar_ppv_id_defineSchema::ar_trans_type AS ar_trans_type,
                                          ar_ppv_id_defineSchema::ar_retry_count AS ar_retry_count,
                                          ar_ppv_id_defineSchema::ar_result_cd AS ar_result_cd,
                                          ar_ppv_id_defineSchema::ar_acct_id AS ar_acct_id,
                                          ar_ppv_id_defineSchema::ar_trans_date AS ar_trans_date,
                                          ar_ppv_id_defineSchema::ar_pay_method AS ar_pay_method,
                                          ar_ppv_id_defineSchema::ar_lineitem AS ar_lineitem,
                                          ar_ppv_id_defineSchema::ar_seq_no AS ar_seq_no,
                                          ar_ppv_id_defineSchema::ar_reason_cd AS ar_reason_cd,
                                          ar_ppv_id_defineSchema::ar_trans_amount AS ar_trans_amount,
                                          ar_ppv_id_defineSchema::ar_sid AS ar_sid,
                                          ar_ppv_id_defineSchema::ar_bid AS ar_bid,
                                          ar_ppv_id_defineSchema::ar_global_acct_id AS ar_global_acct_id,
                                          ar_ppv_id_defineSchema::ar_processing_division AS ar_processing_division,
                                          ar_ppv_id_defineSchema::ar_payment_processor AS ar_payment_processor,
                                          ar_ppv_id_defineSchema::ar_campaign_id AS ar_campaign_id,
                                          ar_ppv_id_defineSchema::ar_line_item_bill_id AS ar_line_item_bill_id,
                                          ar_ppv_id_defineSchema::ar_bill_cycle AS ar_bill_cycle,
                                          ar_ppv_id_defineSchema::ar_bill_date AS ar_bill_date,
                                          ar_ppv_id_defineSchema::ar_write_off_source AS ar_write_off_source,
                                          ar_ppv_id_defineSchema::ar_master_acct_num AS ar_master_acct_num,
                                          ar_ppv_id_defineSchema::ar_bill_applied_amount AS ar_bill_applied_amount,
                                          ar_ppv_id_defineSchema::ar_charge_amount AS ar_charge_amount,
                                          ar_ppv_id_defineSchema::ar_bill_amount AS ar_bill_amount,
                                          ar_ppv_id_defineSchema::ar_resub_flg AS ar_resub_flg,
                                          ar_ppv_id_defineSchema::ar_brandi AS ar_brandi,
                                          ar_ppv_id_defineSchema::ar_payment_method_id AS ar_payment_method_id,
                                          ar_ppv_id_defineSchema::ar_pm_acct_num AS ar_pm_acct_num,
                                          ar_ppv_id_defineSchema::address_payref_line_item_bill_id AS address_payref_line_item_bill_id,
                                          ar_ppv_id_defineSchema::address_payref_payment_reference AS address_payref_payment_reference,
                                          ar_ppv_id_defineSchema::address_payref_last_transaction_amount AS address_payref_last_transaction_am,
                                          ar_ppv_id_defineSchema::address_payref_adjustment_amount AS address_payref_adjustment_amount,
                                          ar_ppv_id_defineSchema::address_payref_city AS address_payref_city,
                                          ar_ppv_id_defineSchema::address_payref_state_province AS address_payref_state_province,
                                          ar_ppv_id_defineSchema::address_payref_zip_postal_cd AS address_payref_zip_postal_cd,
                                          ar_ppv_id_defineSchema::address_payref_country_cd AS address_payref_country_cd,
                                          ar_ppv_id_defineSchema::address_payref_first_name AS address_payref_first_name,
                                          ar_ppv_id_defineSchema::address_payref_last_name AS address_payref_last_name,
                                          ar_ppv_id_defineSchema::address_payref_email_address AS address_payref_email_address,
                                          ar_ppv_id_defineSchema::address_payref_phone_number AS address_payref_phone_number,
                                          ar_ppv_id_defineSchema::address_payref_street AS address_payref_street,
                                          ar_ppv_id_defineSchema::address_payref_apt_suite AS address_payref_apt_suite,
                                          ar_ppv_id_defineSchema::address_payref_payment_token AS address_payref_payment_token,
                                          ar_ppv_id_defineSchema::address_payref_routing_number AS address_payref_routing_number,
                                          ar_ppv_id_defineSchema::cdb_pay_info_ppv_id AS cdb_pay_info_ppv_id,
                                          (
                                            (vendor_merchant::merchant IS NULL) ? '-1' : vendor_merchant::merchant
                                          ) AS vendor_merchant_division_id,
                                          currency_id::iso_currency_id AS currency_id_iso_currency_id;

                              
ar_address_payref_match_final = FOREACH ar_address_payref_match_flatten GENERATE
								ar_bid AS bid,
								ar_sid AS sid,
								ar_global_acct_id AS global_acct_id,		
								'' AS obi_acct,
								ar_payment_method_id AS ar_payment_method_id,							
								ar_pay_method AS pm_id,
								ar_pm_acct_num AS pm_acct_num,
								address_payref_routing_number AS routing_number,
								(
								(ar_bill_cycle IS NOT NULL AND (int)ar_bill_cycle > 0) ? ar_bill_cycle 
								                                                       : (chararray)(int)SUBSTRING(ar_bill_date,8,10)
								) AS cycle,
	
								ar_bill_date AS billed_dt,
		                        (
		                          (ar_trans_type == 'ME' AND (ar_reason_cd == '951' OR ar_reason_cd == '954')) ? 'WN' : ar_trans_type
		                        ) AS trans_type,
	                        
								ar_retry_count AS retry_count,
								ar_result_cd AS result_cd,
								ar_reason_cd AS reason_cd,
								ar_lineitem AS lineitem,
								ar_charge_amount AS charge_amount,
								ar_bill_amount AS bill_amount,
								ar_bill_applied_amount AS paid_amount,
								ar_trans_amount AS trans_amount,
								0.0 AS tax_amount,
								'' AS trans_source_id,
								cdb_pay_info_ppv_id AS ppv_id,
								'' AS nb_reason_cd,
								0.0 AS nb_amount,
								
								(
								 (cdb_pay_info_ppv_id == '28') ? 
								                              '-1' 
								                                : 
								                                ( ((int)ar_processing_division != 0) ? 
								                                                                     ar_processing_division 
								                                                                     : 
								                                                                      ( ((int)vendor_merchant_division_id>0) ? 
								                                                                                                                   vendor_merchant_division_id 
								                                                                                                                   : 
								                                                                                                                   '-1'
								                                                                      )
								                                       
								                                       
								                                )
								) AS processing_division,
																	
								ar_trans_date AS trans_apply_dt,
								address_payref_adjustment_amount AS adjust_amount,
								ar_line_item_balance_id AS line_item_balance_id,
								ar_line_item_bill_id AS line_item_bill_id,
								ar_resub_flg AS resub_flg,
								ar_write_off_source AS write_off_source,
								(
								  (TRIM(ar_write_off_source)=='AR') ? ar_reason_cd : ar_write_off_source
								) AS write_off_reason_cd,								
								address_payref_city AS city,
								address_payref_state_province AS state,
								address_payref_zip_postal_cd AS zipcode,
								address_payref_country_cd AS country_cd,
								currency_id_iso_currency_id AS iso_currency_id,
								'' AS origin_of_sales,
								'' AS paid_dt,
								address_payref_first_name AS first_name,
								address_payref_last_name AS last_name,
								address_payref_email_address AS email_address,
								address_payref_phone_number AS phone_number,
								-- street = steet + apt_suite
								(
								 
	                             REPLACE(
								         TRIM(CONCAT(COALESCE(TRIM(address_payref_street),''), ',', COALESCE(TRIM(address_payref_apt_suite),''))),',,,|^,$|^,|,$',''
								        )
								        
								)
                                AS street,															
								'' AS payment_ref_id,
								'' AS ext_transaction_id,
								'' AS ext_transaction_init_dt;
								
---------------------------------------------------------------------------------------------------------------------
--This dataset will go to GATHER 
--Dataset Name - ar_address_payref_match_final;
---------------------------------------------------------------------------------------------------------------------   

ar_address_unused_xform_enforced_schema = enforce_schema_address(ar_address_unused_xform);
ar_address_payref_match_final_enforced_schema = enforce_schema_address(ar_address_payref_match_final);
ar_address_all = UNION ONSCHEMA  ar_address_unused_xform_enforced_schema,
                                 ar_address_payref_match_final_enforced_schema;

---------------------------------------------------------------------------------------------------------------------
--This dataset will go to JOIN
--Dataset Name - ar_address_all;
---------------------------------------------------------------------------------------------------------------------

non_bill_trans_join = JOIN non_bill_trans BY (bid,sid), currency_id BY (bid,sid);
non_bill_trans_xform = FOREACH non_bill_trans_join GENERATE 
                               non_bill_trans::bid AS bid,
                               non_bill_trans::sid AS sid,
                               non_bill_trans::global_acct_id AS global_acct_id,
                               '' AS obi_acct,
                               non_bill_trans::ar_payment_method_id AS ar_payment_method_id,
                               non_bill_trans::pm_id AS pm_id,
                               non_bill_trans::payment_token AS payment_token,
                               non_bill_trans::routing_number AS routing_number,
                               SUBSTRING(non_bill_trans::actual_bill_dt,8,10) AS cycle,
                               non_bill_trans::actual_bill_dt AS billed_dt,
                               '0' AS trans_type,
                               non_bill_trans::retry_count AS retry_count,
                               'NADD' AS result_cd,                                
                                non_bill_trans::reason_cd AS reason_cd,
                                non_bill_trans::lineitem AS line_item,                             
                                (non_bill_trans::bill_amount - non_bill_trans::adjustment_amount  - non_bill_trans::credited_amount) AS charge_amount,
                                non_bill_trans::bill_amount AS bill_amount,
                                non_bill_trans::bill_paid_amount AS paid_amount,
                                non_bill_trans::activity_amount AS trans_amount,
                                0.0 AS tax_amount,
                                'OBI_AR' AS trans_source_id,
                                non_bill_trans::ppv_id AS ppv_id,
                                '990' AS nb_reason_cd,
                                non_bill_trans::activity_amount AS nb_amount,
                                ((non_bill_trans::ppv_id =='28') ? '-1' : non_bill_trans::processing_division ) AS processing_division,
                                non_bill_trans::actual_bill_dt AS trans_apply_dt,
                                non_bill_trans::adjustment_amount AS adjust_amount,
                                non_bill_trans::line_item_balance_id AS line_item_balance_id,
                                non_bill_trans::line_item_bill_id AS line_item_bill_id,
                                'N' AS resub_flg,
                                '' AS write_off_source,
                                '' AS write_off_reason_cd,
                                non_bill_trans::city AS city,
                                non_bill_trans::state_province AS state,
                                non_bill_trans::zip_postal_cd AS zipcode,
                                non_bill_trans::country_cd AS country_cd,
                                currency_id::iso_currency_id AS iso_currency_id,
                                '-1' AS origin_of_sales,
                                non_bill_trans::actual_bill_dt AS paid_dt,
                                non_bill_trans::first_name AS first_name,
                                non_bill_trans::last_name AS last_name,
                                non_bill_trans::email_address AS email_address,
                                non_bill_trans::phone_number AS phone_number,
                                non_bill_trans::street AS street,
                                '' AS payment_ref_id,
                                '' AS ext_transaction_id,
                                '' AS ext_transaction_init_dt; 

ar_consolidated_non_bill_trans = JOIN ar_address_all BY (line_item_bill_id)  LEFT OUTER , non_bill_trans_xform BY (line_item_bill_id); 

ar_consolidated_non_bill_trans_match = FILTER ar_consolidated_non_bill_trans BY (ar_address_all::line_item_bill_id IS NOT NULL AND  
                                                                                 non_bill_trans_xform::line_item_bill_id IS NOT NULL);
ar_consolidated_non_bill_trans_match_xform = FOREACH ar_consolidated_non_bill_trans_match GENERATE 
                                                     ar_address_all::bid AS bid,
                                                     ar_address_all::sid AS sid,
                                                     ar_address_all::global_acct_id AS global_acct_id,
                                                     ar_address_all::obi_acct AS obi_acct,
                                                     ar_address_all::ar_payment_method_id AS ar_payment_method_id,
                                                     ar_address_all::pm_id AS pm_id,
                                                     ar_address_all::pm_acct_num AS pm_acct_num,
                                                     ar_address_all::routing_number AS routing_number,
                                                     ar_address_all::cycle AS cycle,
                                                     ar_address_all::billed_dt AS billed_dt,
                                                     (
                                                      (ar_address_all::trans_type == 'BL') ?  non_bill_trans_xform::trans_type : ar_address_all::trans_type
                                                     ) AS trans_type,
                                                     ar_address_all::retry_count AS retry_count,
                                                     (
                                                       (ar_address_all::trans_type == 'BL') ? non_bill_trans_xform::result_cd : ar_address_all::result_cd
                                                     ) AS result_cd,
                                                     ar_address_all::reason_cd AS reason_cd,
                                                     ar_address_all::lineitem AS lineitem,
                                                     ar_address_all::charge_amount AS charge_amount,
                                                     ar_address_all::bill_amount AS bill_amount,
                                                     ar_address_all::paid_amount AS paid_amount,
                                                     ar_address_all::trans_amount AS trans_amount,
                                                     ar_address_all::tax_amount AS tax_amount,
                                                     COALESCE(ar_address_all::trans_source_id,'OBI_AR') AS trans_source_id,
                                                     non_bill_trans_xform::ppv_id AS ppv_id,
                                                     (
                                                      (ar_address_all::trans_type == 'BL') ? non_bill_trans_xform::nb_reason_cd : ar_address_all::nb_reason_cd
                                                     ) AS nb_reason_cd,
                                                     (
                                                       (ar_address_all::trans_type == 'BL') ? non_bill_trans_xform::nb_amount : ar_address_all::nb_amount
                                                     ) AS nb_amount,
                                                     ar_address_all::processing_division AS processing_division,
                                                     ar_address_all::trans_apply_dt AS trans_apply_dt,
                                                     ar_address_all::adjust_amount AS adjust_amount,
                                                     ar_address_all::line_item_balance_id AS line_item_balance_id,
                                                     ar_address_all::line_item_bill_id AS line_item_bill_id,
                                                     (
                                                       (ar_address_all::resub_flg == '0' OR ar_address_all::resub_flg IS NULL) ? 'N' : ar_address_all::resub_flg
                                                     ) AS resub_flg,
                                                     ar_address_all::write_off_source AS write_off_source,
                                                     ar_address_all::write_off_reason_cd AS write_off_reason_cd,
                                                     ar_address_all::city AS city,
                                                     ar_address_all::state AS state,
                                                     ar_address_all::zipcode AS zipcode,
                                                     ar_address_all::country_cd AS country_cd,
                                                     ar_address_all::iso_currency_id AS iso_currency_id,
                                                     COALESCE(ar_address_all::origin_of_sales,'-1') AS origin_of_sales,
                                                     ar_address_all::paid_dt AS paid_dt,
                                                     ar_address_all::first_name AS first_name,
                                                     ar_address_all::last_name AS last_name,
                                                     ar_address_all::email_address AS email_address,
                                                     ar_address_all::phone_number AS phone_number,
                                                     REPLACE(ar_address_all::street,'\\|','') AS street,
                                                     ar_address_all::payment_ref_id AS payment_ref_id,
                                                     ar_address_all::ext_transaction_id AS ext_transaction_id,
                                                     ar_address_all::ext_transaction_init_dt AS ext_transaction_init_dt;
                                                     
ar_consolidated_non_bill_trans_unused0 = FILTER ar_consolidated_non_bill_trans BY (non_bill_trans_xform::line_item_bill_id IS NULL);

ar_consolidated_non_bill_trans_unused0_xform = FOREACH ar_consolidated_non_bill_trans_unused0 GENERATE 
                                                       ar_address_all::bid AS bid,
                                                       ar_address_all::sid AS sid,
                                                       ar_address_all::global_acct_id AS global_acct_id,
                                                       ar_address_all::obi_acct AS obi_acct,
                                                       ar_address_all::ar_payment_method_id AS ar_payment_method_id,
                                                       ar_address_all::pm_id AS pm_id,
                                                       ar_address_all::pm_acct_num AS pm_acct_num,
                                                       ar_address_all::routing_number AS routing_number,
                                                       ar_address_all::cycle AS cycle,
                                                       ar_address_all::billed_dt AS billed_dt,
                                                       ar_address_all::trans_type AS trans_type,
                                                       ar_address_all::retry_count AS retry_count,
                                                       ar_address_all::result_cd AS result_cd,
                                                       ar_address_all::reason_cd AS reason_cd,
                                                       ar_address_all::lineitem AS lineitem,
                                                       ar_address_all::charge_amount AS charge_amount,
                                                       ar_address_all::bill_amount AS bill_amount,
                                                       ar_address_all::paid_amount AS paid_amount,
                                                       ar_address_all::trans_amount AS trans_amount,
                                                       ar_address_all::tax_amount AS tax_amount,
                                                       COALESCE(TRIM(ar_address_all::trans_source_id),'OBI_AR') AS trans_source_id,
                                                       ar_address_all::ppv_id AS ppv_id,
                                                       ar_address_all::nb_reason_cd AS nb_reason_cd,
                                                       ar_address_all::nb_amount AS nb_amount,
                                                       ar_address_all::processing_division AS processing_division,
                                                       ar_address_all::trans_apply_dt AS trans_apply_dt,
                                                       ar_address_all::adjust_amount AS adjust_amount,
                                                       ar_address_all::line_item_balance_id AS line_item_balance_id,
                                                       ar_address_all::line_item_bill_id AS line_item_bill_id,
                                                       ar_address_all::resub_flg AS resub_flg,
                                                       TRIM(ar_address_all::write_off_source) AS write_off_source,
                                                       TRIM(ar_address_all::write_off_reason_cd) AS write_off_reason_cd,                                                       
                                                       REPLACE(ar_address_all::city,'([^a-zA-Z0-9-\'\\s]+)','') AS city,
                                                       REPLACE(ar_address_all::state,'([^a-zA-Z0-9-\'\\s]+)','') AS state,
                                                       ar_address_all::zipcode AS zipcode,
                                                       ar_address_all::country_cd AS country_cd,
                                                       ar_address_all::iso_currency_id AS iso_currency_id,
                                                       COALESCE(TRIM(ar_address_all::origin_of_sales),'-1') AS origin_of_sales,
                                                       ar_address_all::paid_dt AS paid_dt,
                                                       ar_address_all::first_name AS first_name,
                                                       ar_address_all::last_name AS last_name,
                                                       ar_address_all::email_address AS email_address,
                                                       ar_address_all::phone_number AS phone_number,
                                                       REPLACE(ar_address_all::street,'\\|','') AS street,
                                                       ar_address_all::payment_ref_id AS payment_ref_id,
                                                       ar_address_all::ext_transaction_id AS ext_transaction_id,
                                                       ar_address_all::ext_transaction_init_dt AS ext_transaction_init_dt;

ar_consolidated_non_bill_trans_match_xform_enforced_schema = enforce_schema_ALL(ar_consolidated_non_bill_trans_match_xform);
ar_consolidated_non_bill_trans_unused0_xform_enforced_schema = enforce_schema_ALL(ar_consolidated_non_bill_trans_unused0_xform);
ar_consolidated_non_bill_trans_union = UNION ONSCHEMA  ar_consolidated_non_bill_trans_match_xform_enforced_schema,  
                                                       ar_consolidated_non_bill_trans_unused0_xform_enforced_schema;
--This is the final dataset for this script
--Dataset Name - ar_consolidated_non_bill_trans_union
STORE ar_consolidated_non_bill_trans_union INTO '$brrLineItemInterimDir/LI_09_process_ar_crypto' USING PigStorage('$delimiter');