/*
            Script Name : LI_18_process_brr_rejects.pig
 jUnit Test Script Name : PigUnit18ProcessBrrRejects.java
                Purpose : pig script to identify the rejected data from the BRR main flow and  create reject and recycle file.
             Dependency : LI_16_process_brr.pig     
       High level logic : The script LI_16_process_brr.pig processes the OBI extract and creates the exp and merge file. 
                          The block for code marked by **Component : 250_RFMT_Data - Start**  and **Component : 250_RFMT_Data - End**  
                          does all the necessary lookups. If a match is not found in the lookups, the data is rejected. 
                          We write the pre and post data from that block of code, 
                    
                          In this script, we identify the records which are in pre dataset and are not in post dataset based omn the 
                          unique_id. This comprises the reject data set.We impelemnt logic on this reject dataset to identify the reason 
                          and create a recycle , reject and error files.                     
*/

SET pig.exec.mapPartAgg '$pig_exec_mappart_agg_enabled';
SET pig.user.cache.enabled '$pig_user_cache_mode_enabled';
SET pig.auto.local.enabled '$pig_auto_local_mode_enabled';
SET pig.local.io.sort.mb '$pig_auto_local_io_sort_mb';
SET pig.auto.local.input.maxbytes '$pig_auto_local_input_maxbytes';
SET mapreduce.reduce.memory.mb 3240;
SET pig.exec.reducers.bytes.per.reducer 100000000;

IMPORT '$brrLineItemPigDir/LI_01_ingest_dimensions.pig';

main_flow_input = LOAD '$brrLineItemInterimDir/LI_16_process_brr_rfmt_in/part*' using PigStorage('$delimiter') AS
	                (
	                segment_dt:chararray,
	                unique_id:chararray,
					bid:chararray,
					sid:chararray,
					global_acct_id:chararray,
					payment_acct:chararray,
					ar_payment_method_id:chararray,
					payment_method_id:chararray,
					cycle:chararray,
					billed_dt:chararray,
					transaction_type:chararray,
					retry_count:chararray,
					result_code:chararray,
					reason_code:chararray,
					lineitem:chararray,
					origin_of_sales:chararray,
					pm_id:chararray,
					charge_amount:double,
					billed_amount:double,
					trans_amount:double,
					trans_source_id:chararray,
					ppv_id:chararray,
					nb_reason_code_id:chararray,
					nb_amount:double,
					processing_division:chararray,
					transaction_applied_dt:chararray,
					adjust_amount:double,
					line_item_balance_id:chararray,
					line_item_bill_id:chararray,
					write_off_source:chararray,
					write_off_reason_code:chararray,
					resub_flag:chararray,
					paid_amount:double,
					tax_amount:double,
					currency_id:chararray,
					error_cd:chararray,
					recycle_count:chararray,
					city:chararray,
					state:chararray,
					zip:chararray,
					country_code:chararray,
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

main_flow_output = LOAD '$brrLineItemInterimDir/LI_16_process_brr_rfmt_out/part*' using PigStorage('$delimiter') AS
	                (
	                segment_dt:chararray,
	                unique_id:chararray,
					global_acct_id:chararray,
					payment_acct:chararray,
					ar_payment_method_id:chararray,
					payment_method_id:chararray,
					billkey:chararray,
					billed_dt:chararray,
					transaction_type_code:chararray,
					retry_count:chararray,
					return_code_key:chararray,
					reason_code_id:chararray,
					line_item_key:chararray,
					origin_of_sales:chararray,
					pm_id:chararray,
					charge_amount:double,
					billed_amount:double,
					trans_amount:double,
					trans_source_id:chararray,
					ppv_id:chararray,
					nb_reason_code_id:chararray,
					nb_amount:double,
					processing_division:chararray,
					transaction_applied_dt:chararray,
					adjust_amount:double,
					line_item_balance_id:chararray,
					line_item_bill_id:chararray,
					write_off_source:chararray,
					write_off_reason_code_key:chararray,
					resub_flag:chararray,
					merchant_account:chararray,
					glid:chararray,
					gl_segment:chararray,
					paid_dt:chararray,
					paid_amount:double,
					tax_amount:double,
					currency_id:chararray,
					recycle_count:chararray,
					city:chararray,
					state:chararray,
					zip:chararray,
					country_code:chararray,
					first_name:chararray,
					last_name:chararray,
					email_address:chararray,
					phone_number:chararray,
					street:chararray,
					payment_ref_id:chararray,
					ext_transaction_id:chararray,
					ext_transaction_init_dt:chararray	
	                );				

rejects =  JOIN main_flow_input BY unique_id LEFT OUTER ,main_flow_output BY unique_id;	                           
rejects_unused_0 = FILTER rejects BY (main_flow_output::unique_id IS NULL);

--Base data for all error processing
--This is the recycle data ss well
--Dataset Name - rejects_unused_0

---------------------------------------------------------------------------
-- Recycle data - Start
---------------------------------------------------------------------------
recycle_data = FOREACH  rejects_unused_0 GENERATE 
							main_flow_input::segment_dt AS segment_dt,
							main_flow_input::bid AS bid,
							main_flow_input::sid AS sid,
							main_flow_input::global_acct_id AS global_acct_id,
							main_flow_input::payment_acct AS payment_acct,
							main_flow_input::ar_payment_method_id AS ar_payment_method_id,
							main_flow_input::payment_method_id AS payment_method_id,
							main_flow_input::cycle AS cycle,
							main_flow_input::billed_dt AS billed_dt,
							main_flow_input::transaction_type AS transaction_type,
							main_flow_input::retry_count AS retry_count,
							main_flow_input::result_code AS result_code,
							main_flow_input::reason_code AS reason_code,
							main_flow_input::lineitem AS lineitem,
							main_flow_input::origin_of_sales AS origin_of_sales,
							main_flow_input::pm_id AS pm_id,
							main_flow_input::charge_amount AS charge_amount,
							main_flow_input::billed_amount AS billed_amount,
							main_flow_input::trans_amount AS trans_amount,
							main_flow_input::trans_source_id AS trans_source_id,
							main_flow_input::ppv_id AS ppv_id,
							main_flow_input::nb_reason_code_id AS nb_reason_code_id,
							main_flow_input::nb_amount AS nb_amount,
							main_flow_input::processing_division AS processing_division,
							main_flow_input::transaction_applied_dt AS transaction_applied_dt,
							main_flow_input::adjust_amount AS adjust_amount,
							main_flow_input::line_item_balance_id AS line_item_balance_id,
							main_flow_input::line_item_bill_id AS line_item_bill_id,
							main_flow_input::write_off_source AS write_off_source,
							main_flow_input::write_off_reason_code AS write_off_reason_code,
							main_flow_input::resub_flag AS resub_flag,
							main_flow_input::paid_amount AS paid_amount,
							main_flow_input::tax_amount AS tax_amount,
							main_flow_input::currency_id AS currency_id,
							(DaysBetween(ToDate('$segmentDate', 'yyyy-MM-dd'), ToDate(main_flow_input::segment_dt, 'yyyy-MM-dd')) + 1) AS recycle_count,
							main_flow_input::city AS city,
							main_flow_input::state AS state,
							main_flow_input::zip AS zip,
							main_flow_input::country_code AS country_code,
							main_flow_input::paid_dt AS paid_dt,
							main_flow_input::first_name AS first_name,
							main_flow_input::last_name AS last_name,
							main_flow_input::email_address AS email_address,
							main_flow_input::phone_number AS phone_number,
							main_flow_input::street AS street,
							REPLACE(REPLACE(main_flow_input::payment_ref_id,'#',','),'null','') AS payment_ref_id,
							main_flow_input::ext_transaction_id AS ext_transaction_id,
							main_flow_input::ext_transaction_init_dt AS ext_transaction_init_dt
                            ;                           
---------------------------------------------------------------------------
-- Dataset Name - recycle_data
-- Recycle data - end
---------------------------------------------------------------------------                            

---------------------------------------------------------------------------
-- Identification of Error Code - Start
---------------------------------------------------------------------------

line_item_map_j = JOIN rejects_unused_0 BY (main_flow_input::result_code, main_flow_input::transaction_type) LEFT OUTER, line_item_map BY (result_code, trans_type) USING 'replicated';
line_item_map_f = FILTER line_item_map_j BY (line_item_map::result_code IS NULL AND line_item_map::trans_type IS NULL);
line_item_map_x = FOREACH line_item_map_f GENERATE 
                                   rejects_unused_0::main_flow_input::segment_dt..rejects_unused_0::main_flow_input::ext_transaction_init_dt,
                                   '99' AS error_code,
                                   'line_item_map lookup mismatch' AS error_desc;
--Dataset Name - line_item_map_x


rejects_unused0_post = FILTER line_item_map_j BY NOT(line_item_map::result_code IS NULL AND line_item_map::trans_type IS NULL);

trans_type_j = JOIN rejects_unused0_post BY (main_flow_input::transaction_type) LEFT OUTER, trans_type BY (trans_type) USING 'replicated';
trans_type_f = FILTER trans_type_j BY (trans_type::trans_type IS NULL);
trans_type_x = FOREACH trans_type_f GENERATE 
                                    rejects_unused_0::main_flow_input::segment_dt..rejects_unused_0::main_flow_input::ext_transaction_init_dt,
                                   '11' AS error_code,
                                   'trans_type lookup mismatch' AS error_desc;
--Dataset Name - trans_type_x

return_code_j = JOIN rejects_unused0_post BY (main_flow_input::result_code) LEFT OUTER, return_code BY (return_code) USING 'replicated';
return_code_range = FOREACH return_code_j GENERATE 
	                         main_flow_input::segment_dt..,
	                         (
							   (
							    DaysBetween(ToDate(main_flow_input::transaction_applied_dt, 'yyyy-MM-dd'), ToDate(return_code::effective_date, 'yyyy-MM-dd')) >= (long)'0'
							     and DaysBetween(ToDate(main_flow_input::transaction_applied_dt, 'yyyy-MM-dd'), ToDate(return_code::expiration_date, 'yyyy-MM-dd')) < (long)'0'
							   ) ? 
							         (chararray)'return_code_Y' : (chararray)'return_code_N'
							 ) AS field_selector;

return_code_range_f = FILTER return_code_range BY field_selector == 'return_code_Y';

return_code_n = JOIN rejects_unused0_post BY (main_flow_input::segment_dt..main_flow_input::ext_transaction_init_dt) LEFT OUTER, 
                    return_code_range_f BY (main_flow_input::segment_dt..main_flow_input::ext_transaction_init_dt) USING 'replicated'; 
return_code_nf = FILTER return_code_n BY (return_code_range_f::rejects_unused0_post::rejects_unused_0::main_flow_input::segment_dt IS NULL);                                        
return_code_x = FOREACH return_code_nf GENERATE 
                        rejects_unused0_post::rejects_unused_0::main_flow_input::segment_dt..
                                 rejects_unused0_post::rejects_unused_0::main_flow_input::ext_transaction_init_dt,
                        '12' AS error_code,
                        'return_code lookup mismatch' AS error_desc;
--Dataset Name - return_code_x 

lineitem_j = JOIN rejects_unused0_post BY (main_flow_input::lineitem,main_flow_input::bid,main_flow_input::sid), lineitem BY (lineitem,bid,sid);
lineitem_range = FOREACH lineitem_j  GENERATE 
	                         main_flow_input::segment_dt..,
	                         (
							   (
							     DaysBetween(ToDate(main_flow_input::transaction_applied_dt, 'yyyy-MM-dd'), ToDate(lineitem::effective_date, 'yyyy-MM-dd')) >= (long)'0'
							       and DaysBetween(ToDate(main_flow_input::transaction_applied_dt, 'yyyy-MM-dd'), ToDate(lineitem::expiration_date, 'yyyy-MM-dd')) < (long)'0'
							   ) ? 
							         (chararray)'lineitem_Y' : (chararray)'lineitem_N'
							 ) AS field_selector;							 
lineitem_range_f = FILTER lineitem_range BY (field_selector == 'lineitem_Y'); 

lineitem_n = JOIN rejects_unused0_post BY (main_flow_input::segment_dt..main_flow_input::ext_transaction_init_dt) LEFT OUTER, 
                      lineitem_range_f BY (main_flow_input::segment_dt..main_flow_input::ext_transaction_init_dt) USING 'replicated';                      
lineitem_nf = FILTER lineitem_n BY (lineitem_range_f::rejects_unused0_post::rejects_unused_0::main_flow_input::segment_dt IS NULL);                                    
lineitem_x = FOREACH lineitem_nf GENERATE 
                     rejects_unused0_post::rejects_unused_0::main_flow_input::segment_dt..
                        rejects_unused0_post::rejects_unused_0::main_flow_input::ext_transaction_init_dt,
                     '13' AS error_code,
                     'lineitem lookup mismatch' AS error_desc;
--Dataset Name - lineitem_x                     

tranx_source_j = JOIN rejects_unused0_post BY (main_flow_input::trans_source_id) LEFT OUTER, tranx_source BY (trans_source_desc) USING 'replicated';
tranx_source_f = FILTER tranx_source_j BY (tranx_source::trans_source_desc IS NULL);   
tranx_source_x = FOREACH tranx_source_f GENERATE
                         rejects_unused_0::main_flow_input::segment_dt..rejects_unused_0::main_flow_input::ext_transaction_init_dt,
                         '14' AS error_code,
                         'tranx_source lookup mismatch' AS error_desc;
--Dataset Name - tranx_source_x                        

payment_method_j = JOIN rejects_unused0_post BY (main_flow_input::payment_method_id), payment_method BY (payment_method_id);
payment_method_range = FOREACH payment_method_j GENERATE 
	                         main_flow_input::segment_dt..,
	                         (
							   (
							     DaysBetween(ToDate(main_flow_input::transaction_applied_dt, 'yyyy-MM-dd'), ToDate(payment_method::effective_date, 'yyyy-MM-dd')) >= (long)'0'
							      and DaysBetween(ToDate(main_flow_input::transaction_applied_dt, 'yyyy-MM-dd'), ToDate(payment_method::expiration_date, 'yyyy-MM-dd')) < (long)'0'
							   ) ? 
							         (chararray)'payment_method_Y' : (chararray)'payment_method_N'
							 ) AS field_selector; 
payment_method_range_f = FILTER payment_method_range BY (field_selector == 'payment_method_Y'); 

payment_method_n = JOIN rejects_unused0_post BY (main_flow_input::segment_dt..main_flow_input::ext_transaction_init_dt) LEFT OUTER, 
                        payment_method_range_f BY (main_flow_input::segment_dt..main_flow_input::ext_transaction_init_dt) USING 'replicated';			 							 
payment_method_nf = FILTER payment_method_n BY (payment_method_range_f::rejects_unused0_post::rejects_unused_0::main_flow_input::segment_dt IS NULL);
payment_method_x = FOREACH payment_method_nf GENERATE 
                     rejects_unused0_post::rejects_unused_0::main_flow_input::segment_dt..
                           rejects_unused0_post::rejects_unused_0::main_flow_input::ext_transaction_init_dt,
                     '15' AS error_code,
                     'payment_method lookup mismatch' AS error_desc;							                        
--Dataset Name - payment_method_x

payment_vendor_j = JOIN rejects_unused0_post BY (main_flow_input::ppv_id), payment_vendor BY (ppv_id);
payment_vendor_range = FOREACH payment_vendor_j GENERATE 
	                         main_flow_input::segment_dt..,
	                         (
							   (
							    DaysBetween(ToDate(main_flow_input::transaction_applied_dt, 'yyyy-MM-dd'), ToDate(payment_vendor::effective_date, 'yyyy-MM-dd')) >= (long)'0'
							     and DaysBetween(ToDate(main_flow_input::transaction_applied_dt, 'yyyy-MM-dd'), ToDate(payment_vendor::expiration_date, 'yyyy-MM-dd')) < (long)'0'
							   ) ? 
							         (chararray)'payment_vendor_Y' : (chararray)'payment_vendor_N'
							 ) AS field_selector;

payment_vendor_range_f = FILTER payment_vendor_range BY field_selector == 'payment_vendor_Y'; 

payment_vendor_n = JOIN rejects_unused0_post BY (main_flow_input::segment_dt..main_flow_input::ext_transaction_init_dt) LEFT OUTER, 
                      payment_vendor_range_f BY (main_flow_input::segment_dt..main_flow_input::ext_transaction_init_dt) USING 'replicated';			 							 
payment_vendor_nf = FILTER payment_vendor_n BY (payment_vendor_range_f::rejects_unused0_post::rejects_unused_0::main_flow_input::segment_dt IS NULL);
payment_vendor_x = FOREACH payment_vendor_nf GENERATE 
                     rejects_unused0_post::rejects_unused_0::main_flow_input::segment_dt..
                         rejects_unused0_post::rejects_unused_0::main_flow_input::ext_transaction_init_dt,
                     '16' AS error_code,
                     'payment_vendor lookup mismatch' AS error_desc;							                        
--Dataset Name - payment_vendor_x

nb_reason_code_j = JOIN rejects_unused0_post BY (main_flow_input::nb_reason_code_id), nb_reason_code BY (nb_reason_code_id);
nb_reason_code_range = FOREACH nb_reason_code_j GENERATE 
	                         main_flow_input::segment_dt..,
	                         (
							   (
							    DaysBetween(ToDate(main_flow_input::transaction_applied_dt, 'yyyy-MM-dd'), ToDate(nb_reason_code::effective_date, 'yyyy-MM-dd'))  >= (long)'0'
							     and DaysBetween(ToDate(main_flow_input::transaction_applied_dt, 'yyyy-MM-dd'), ToDate(nb_reason_code::expiration_date, 'yyyy-MM-dd')) < (long)'0'
							   ) ? 
							         (chararray)'nb_reason_code_Y' : (chararray)'nb_reason_code_N'
							 ) AS field_selector;

nb_reason_code_range_f = FILTER nb_reason_code_range BY field_selector == 'nb_reason_code_Y';

nb_reason_code_n = JOIN rejects_unused0_post BY (main_flow_input::segment_dt..main_flow_input::ext_transaction_init_dt) LEFT OUTER, 
                        nb_reason_code_range_f BY (main_flow_input::segment_dt..main_flow_input::ext_transaction_init_dt) USING 'replicated';			 							 
nb_reason_code_nf = FILTER nb_reason_code_n BY (nb_reason_code_range_f::rejects_unused0_post::rejects_unused_0::main_flow_input::segment_dt IS NULL);
nb_reason_code_x = FOREACH nb_reason_code_nf GENERATE 
                     rejects_unused0_post::rejects_unused_0::main_flow_input::segment_dt..
                        rejects_unused0_post::rejects_unused_0::main_flow_input::ext_transaction_init_dt,
                     '17' AS error_code,
                     'nb_reason_code lookup mismatch' AS error_desc;							                        
--Dataset Name - nb_reason_code_x

pd_to_ma_map_j = JOIN rejects_unused0_post BY (main_flow_input::processing_division), pd_to_ma_map BY (processing_division);
pd_to_ma_map_range = FOREACH pd_to_ma_map_j GENERATE 
	                         main_flow_input::segment_dt..,
	                         (
							   (
							    DaysBetween(ToDate(main_flow_input::transaction_applied_dt, 'yyyy-MM-dd'), ToDate(pd_to_ma_map::effective_date, 'yyyy-MM-dd')) >= (long)'0'
							      and DaysBetween(ToDate(main_flow_input::transaction_applied_dt, 'yyyy-MM-dd'), ToDate(pd_to_ma_map::expiration_date, 'yyyy-MM-dd')) < (long)'0'
							   ) ? 
							         (chararray)'pd_to_ma_map_Y' : (chararray)'pd_to_ma_map_N'
							 ) AS field_selector;

pd_to_ma_map_range_f = FILTER pd_to_ma_map_range BY field_selector == 'pd_to_ma_map_Y';	

pd_to_ma_map_n = JOIN rejects_unused0_post BY (main_flow_input::segment_dt..main_flow_input::ext_transaction_init_dt) LEFT OUTER, 
                      pd_to_ma_map_range_f BY (main_flow_input::segment_dt..rejects_unused_0::main_flow_input::ext_transaction_init_dt) USING 'replicated';			 							 
pd_to_ma_map_nf = FILTER pd_to_ma_map_n BY (pd_to_ma_map_range_f::rejects_unused0_post::rejects_unused_0::main_flow_input::segment_dt IS NULL);
pd_to_ma_map_x = FOREACH pd_to_ma_map_nf GENERATE 
                     rejects_unused0_post::rejects_unused_0::main_flow_input::segment_dt..
                         rejects_unused0_post::rejects_unused_0::main_flow_input::ext_transaction_init_dt,
                     '18' AS error_code,
                     'pd_to_ma_map lookup mismatch' AS error_desc;							                        
--Dataset Name - pd_to_ma_map_x

line_item_glid_map_j = JOIN rejects_unused0_post BY (
													main_flow_input::payment_method_id, 
													main_flow_input::lineitem, 
													main_flow_input::transaction_type, 
													main_flow_input::processing_division, 
													main_flow_input::bid, 
													main_flow_input::sid
												), 
					      line_item_glid_map BY (
							                        pm_id,
							                        line_item_id,
							                        transaction_type,
							                        processing_division,
							                        business_id,
							                        service_id
							                    );						                    
line_item_glid_map_range = FOREACH line_item_glid_map_j GENERATE 
	                         main_flow_input::segment_dt..,
	                         (
							   (
							    DaysBetween(ToDate(main_flow_input::transaction_applied_dt, 'yyyy-MM-dd'), ToDate(line_item_glid_map::effective_date, 'yyyy-MM-dd')) >= (long)'0'
							      and DaysBetween(ToDate(main_flow_input::transaction_applied_dt, 'yyyy-MM-dd'), ToDate(line_item_glid_map::expiration_date, 'yyyy-MM-dd')) < (long)'0'
							   ) ? 
							         (chararray)'line_item_glid_map_Y' : (chararray)'line_item_glid_map_N'
							 ) AS field_selector;

line_item_glid_map_range_f = FILTER line_item_glid_map_range BY field_selector == 'line_item_glid_map_Y';	

line_item_glid_map_n = JOIN rejects_unused0_post BY (main_flow_input::segment_dt..main_flow_input::ext_transaction_init_dt) LEFT OUTER, 
                      line_item_glid_map_range_f BY (main_flow_input::segment_dt..main_flow_input::ext_transaction_init_dt) USING 'replicated';			 							 
line_item_glid_map_nf = FILTER line_item_glid_map_n BY (line_item_glid_map_range_f::rejects_unused0_post::rejects_unused_0::main_flow_input::segment_dt IS NULL);
line_item_glid_map_x = FOREACH line_item_glid_map_nf GENERATE 
                               rejects_unused0_post::rejects_unused_0::main_flow_input::segment_dt..rejects_unused0_post::rejects_unused_0::main_flow_input::ext_transaction_init_dt,
                               '19' AS error_code,
                               'line_item_glid_map lookup mismatch' AS error_desc;							                        
--Dataset Name - line_item_glid_map_x 

rejects_unused_0_valid = FILTER rejects_unused0_post BY (main_flow_input::write_off_reason_code IS NOT NULL AND main_flow_input::write_off_reason_code != '-1');
writeoff_reason_code_j = JOIN rejects_unused_0_valid BY (main_flow_input::write_off_reason_code) LEFT OUTER, writeoff_reason_code BY (adjustment_type_id) USING 'replicated';
writeoff_reason_code_f = FILTER writeoff_reason_code_j BY (writeoff_reason_code::adjustment_type_id IS NULL);   
writeoff_reason_code_x = FOREACH writeoff_reason_code_f GENERATE
                         rejects_unused_0::main_flow_input::segment_dt..rejects_unused_0::main_flow_input::ext_transaction_init_dt,
                         '20' AS error_code,
                         'writeoff_reason_code lookup mismatch' AS error_desc;
--Dataset Name - writeoff_reason_code_x

currency_j = JOIN rejects_unused0_post BY (main_flow_input::currency_id) LEFT OUTER, currency BY (currency_id) USING 'replicated';
currency_f =  FILTER currency_j BY (currency::currency_id IS NULL);  
currency_x = FOREACH currency_f GENERATE
                     rejects_unused0_post::rejects_unused_0::main_flow_input::segment_dt..
                          rejects_unused0_post::rejects_unused_0::main_flow_input::ext_transaction_init_dt,
                     '21' AS error_code,
                     'currency lookup mismatch' AS error_desc;
--Dataset Name - currency_x

billing_config_j = JOIN rejects_unused0_post BY (main_flow_input::lineitem, main_flow_input::bid, main_flow_input::sid), billing_config BY (lineitem,bid,sid);
billing_config_range = FOREACH billing_config_j GENERATE 
	                         main_flow_input::segment_dt..,
	                         (
							   (
							     DaysBetween(ToDate(main_flow_input::transaction_applied_dt, 'yyyy-MM-dd'), ToDate(billing_config::effective_date, 'yyyy-MM-dd')) >= (long)'0'
							       and DaysBetween(ToDate(main_flow_input::transaction_applied_dt, 'yyyy-MM-dd'), ToDate(billing_config::expiration_date, 'yyyy-MM-dd')) < (long)'0'
							   ) ? 
							         (chararray)'billing_config_Y' : (chararray)'billing_config_N'
							 ) AS field_selector;

billing_config_range_f = FILTER billing_config_range BY field_selector == 'billing_config_Y';   

billing_config_n = JOIN rejects_unused0_post BY (main_flow_input::segment_dt..main_flow_input::ext_transaction_init_dt) LEFT OUTER, 
                            billing_config_range_f BY (main_flow_input::segment_dt..main_flow_input::ext_transaction_init_dt) USING 'replicated';			 							 
billing_config_nf = FILTER billing_config_n BY (billing_config_range_f::rejects_unused0_post::rejects_unused_0::main_flow_input::segment_dt IS NULL);
billing_config_x = FOREACH billing_config_nf GENERATE 
                               rejects_unused0_post::rejects_unused_0::main_flow_input::segment_dt..
                                    rejects_unused0_post::rejects_unused_0::main_flow_input::ext_transaction_init_dt,
                               '30' AS error_code,
                               'line_item_glid_map lookup mismatch' AS error_desc;							                        
--Dataset Name - billing_config_x 


---------------------------------------------------------------------------
-- Identification of Error Code - Ends
---------------------------------------------------------------------------

-- Reject records + Error code logic
union_all_issues = UNION line_item_map_x, trans_type_x, return_code_x, lineitem_x, tranx_source_x, payment_method_x, payment_vendor_x, nb_reason_code_x,
                         pd_to_ma_map_x, line_item_glid_map_x, writeoff_reason_code_x, currency_x, billing_config_x;                                   

reject_data = FOREACH  union_all_issues GENERATE 
							main_flow_input::segment_dt AS segment_dt,
							main_flow_input::unique_id AS unique_id,
							main_flow_input::bid AS bid,
							main_flow_input::sid AS sid,
							main_flow_input::global_acct_id AS global_acct_id,
							main_flow_input::payment_acct AS payment_acct,
							main_flow_input::ar_payment_method_id AS ar_payment_method_id,
							main_flow_input::payment_method_id AS payment_method_id,
							main_flow_input::cycle AS cycle,
							main_flow_input::billed_dt AS billed_dt,
							main_flow_input::transaction_type AS transaction_type,
							main_flow_input::retry_count AS retry_count,
							main_flow_input::result_code AS result_code,
							main_flow_input::reason_code AS reason_code,
							main_flow_input::lineitem AS lineitem,
							main_flow_input::origin_of_sales AS origin_of_sales,
							main_flow_input::pm_id AS pm_id,
							main_flow_input::charge_amount AS charge_amount,
							main_flow_input::billed_amount AS billed_amount,
							main_flow_input::trans_amount AS trans_amount,
							main_flow_input::trans_source_id AS trans_source_id,
							main_flow_input::ppv_id AS ppv_id,
							main_flow_input::nb_reason_code_id AS nb_reason_code_id,
							main_flow_input::nb_amount AS nb_amount,
							main_flow_input::processing_division AS processing_division,
							main_flow_input::transaction_applied_dt AS transaction_applied_dt,
							main_flow_input::adjust_amount AS adjust_amount,
							main_flow_input::line_item_balance_id AS line_item_balance_id,
							main_flow_input::line_item_bill_id AS line_item_bill_id,
							main_flow_input::write_off_source AS write_off_source,
							main_flow_input::write_off_reason_code AS write_off_reason_code,
							main_flow_input::resub_flag AS resub_flag,
							main_flow_input::paid_amount AS paid_amount,
							main_flow_input::tax_amount AS tax_amount,
							main_flow_input::currency_id AS currency_id,
							error_code AS error_code,
							error_desc AS error_desc,
                            (DaysBetween(ToDate('$segmentDate', 'yyyy-MM-dd'), ToDate(main_flow_input::segment_dt, 'yyyy-MM-dd')) + 1) AS recycle_count,                                                        							
							main_flow_input::city AS city,
							main_flow_input::state AS state,
							main_flow_input::zip AS zip,
							main_flow_input::country_code AS country_code,
							main_flow_input::paid_dt AS paid_dt,
							main_flow_input::first_name AS first_name,
							main_flow_input::last_name AS last_name,
							main_flow_input::email_address AS email_address,
							main_flow_input::phone_number AS phone_number,
							main_flow_input::street AS street,
							REPLACE(REPLACE(main_flow_input::payment_ref_id,'#',','),'null','') AS payment_ref_id,
							main_flow_input::ext_transaction_id AS ext_transaction_id,
							main_flow_input::ext_transaction_init_dt AS ext_transaction_init_dt;
						             
reject_data_sorted = ORDER reject_data BY segment_dt,bid,sid,global_acct_id;

-- Lookup error logic
error_code_99_data_g = GROUP line_item_map_x BY (rejects_unused_0::main_flow_input::segment_dt, 
                                                 rejects_unused_0::main_flow_input::transaction_type, 
                                                 rejects_unused_0::main_flow_input::lineitem,
                                                 rejects_unused_0::main_flow_input::result_code);                                           
error_code_99_data_x = FOREACH error_code_99_data_g GENERATE
                               FLATTEN(group),
                               (SUM(line_item_map_x.trans_amount)/100) AS total_trans_amount,
                               COUNT(line_item_map_x.segment_dt) AS total_record_count;  
                                            
STORE recycle_data INTO '$brrLineItemOutputDir/LI_18_process_brr_rejects_recycle' USING PigStorage('$delimiter');   
STORE reject_data_sorted INTO '$brrLineItemRejectsDir/LI_18_process_brr_rejects_rejects' USING PigStorage('$delimiter');                             
STORE error_code_99_data_x  INTO '$brrLineItemRejectsDir/LI_18_process_brr_rejects_lookup_error' USING PigStorage('$delimiter');                                                                                                                                			