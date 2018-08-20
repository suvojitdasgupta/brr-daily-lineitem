/*
            Script Name : LI_02A_process_address.pig
 jUnit Test Script Name : PigUnit02AProcessAddress.java
                Purpose : pig script for processing Address data for downstream processing.
             Dependency : LI_01_ingest_dimensions.pig                          
*/

SET pig.exec.mapPartAgg '$pig_exec_mappart_agg_enabled';
SET pig.user.cache.enabled '$pig_user_cache_mode_enabled';
SET pig.auto.local.enabled '$pig_auto_local_mode_enabled';
SET pig.local.io.sort.mb '$pig_auto_local_io_sort_mb';
SET pig.auto.local.input.maxbytes '$pig_auto_local_input_maxbytes';
SET mapreduce.reduce.memory.mb 3240;
SET pig.exec.reducers.bytes.per.reducer 300000000;
SET output.compression.enabled '$pig_compress_enabled';
SET output.compression.codec org.apache.hadoop.io.compress.BZip2Codec;

--SQL for line_item_bill joining with account_* set of tables was split into two separate dataset
--for performance reason. The join is carried out in pig. 					   					   
   
line_item_bill = LOAD '$brrLineItemSqoopDir/line_item_bill/part*' using PigStorage('$delimiter') AS 
                       (
                        line_item_bill_id:chararray,
                        payment_reference:chararray,
						last_transaction_amount:double,
						adjustment_amount:double,                   
                        acct_id:chararray,
                        payment_method_id:chararray 
						);					   

payment_ids = LOAD '$brrLineItemSqoopDir/payment_ids/part*' using PigStorage('$delimiter') AS 
                       (
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
						routing_number:chararray,
						acct_id:chararray,
						payment_method_id:chararray
						);
						
address_payref_join = JOIN line_item_bill BY (acct_id,payment_method_id), payment_ids BY (acct_id,payment_method_id);
address_payref = FOREACH address_payref_join GENERATE
                         line_item_bill::line_item_bill_id AS line_item_bill_id,
                         line_item_bill::payment_reference AS payment_reference,
						 line_item_bill::last_transaction_amount AS last_transaction_amount,
						 line_item_bill::adjustment_amount AS adjustment_amount,
						 payment_ids::city AS city,
						 payment_ids::state_province AS state_province,
						 payment_ids::zip_postal_cd AS zip_postal_cd,
						 payment_ids::country_cd AS country_cd,
						 payment_ids::first_name AS first_name,
						 payment_ids::last_name AS last_name,
						 payment_ids::email_address AS email_address,
						 payment_ids::phone_number AS phone_number,
						 payment_ids::street AS street,
						 payment_ids::apt_suite AS apt_suite,
						 payment_ids::payment_token AS payment_token,
						 payment_ids::routing_number AS routing_number;
						 
STORE address_payref INTO '$brrLineItemInterimDir/LI_02A_process_address' USING PigStorage('$delimiter');		 