/*
            Script Name : LI_08_process_ar_gather.pig
 jUnit Test Script Name : PigUnit08ProcessArGather.java
                Purpose : pig script reads the output of all the previous jobs and creates one consolidated file.  
             Dependency : LI_03B_process_ar_refunds.pig
                          LI_03C_process_ar_writeoff.pig
                          LI_03D_process_ar_bill.pig
                          LI_07_process_ar_trans_type.pig
                                                    
*/

SET pig.exec.mapPartAgg '$pig_exec_mappart_agg_enabled';
SET pig.user.cache.enabled '$pig_user_cache_mode_enabled';
SET pig.auto.local.enabled '$pig_auto_local_mode_enabled';
SET pig.local.io.sort.mb '$pig_auto_local_io_sort_mb';
SET pig.auto.local.input.maxbytes '$pig_auto_local_input_maxbytes';

DEFINE load_data(input_path,delimiter) RETURNS output_records {
       $output_records = LOAD '$input_path' using PigStorage('$delimiter') AS
                               (
                                bill_applied_date:chararray,
                                line_item_balance_id:chararray,
                                cash_credit_id:chararray,
                                payment_processor_activity:chararray,
                                trans_type:chararray,
                                retry_count::chararray,
                                result_code:chararray,
                                account_no:chararray,
                                trans_date:chararray,
                                pay_method:chararray,
                                lineitem:chararray,
                                seq_no:chararray,
                                reason_code:chararray,
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
                              };
-- Read the data files.
refunds = load_data('$brrLineItemInterimDir/LI_03B_process_ar_refunds/part*','$delimiter');
writeoff = load_data('$brrLineItemInterimDir/LI_03C_process_ar_writeoff/part*','$delimiter');
bill = load_data('$brrLineItemInterimDir/LI_03D_process_ar_bill/part*','$delimiter');
credits = load_data('$brrLineItemInterimDir/LI_07_process_ar_trans_type/part*','$delimiter');

transactions_all = UNION ONSCHEMA refunds, 
                                  writeoff, 
                                  bill, 
                                  credits;

STORE transactions_all INTO '$brrLineItemInterimDir/LI_08_process_ar_gather' USING PigStorage('$delimiter');
