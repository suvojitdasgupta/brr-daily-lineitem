/*
            Script Name : LI_15_process_dvt_metrics.pig
 jUnit Test Script Name : PigUnit15ProcessDvtMetrics.java
                Purpose : pig script for processing all AR/BPS data and create the DVT and Metrics files.
             Dependency : LI_02B_process_bps.pig
                          LI_14_process_ar_resub_filter.pig                          
*/

SET pig.exec.mapPartAgg '$pig_exec_mappart_agg_enabled';
SET pig.user.cache.enabled '$pig_user_cache_mode_enabled';
SET pig.auto.local.enabled '$pig_auto_local_mode_enabled';
SET pig.local.io.sort.mb '$pig_auto_local_io_sort_mb';
SET pig.auto.local.input.maxbytes '$pig_auto_local_input_maxbytes';
SET mapreduce.reduce.memory.mb 3240;
SET pig.exec.reducers.bytes.per.reducer 100000000;

IMPORT '$brrLineItemPigDir/LI_01_ingest_dimensions.pig';

DEFINE load_data(input_path,delimiter) RETURNS output_records {
       $output_records = LOAD '$input_path' using PigStorage('$delimiter') AS
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
                                charge_amt:double,
                                bill_amt:double,
                                paid_amt:double,
                                trans_amt:double,
                                tax_amt:double,
                                trans_source_id:chararray,
                                ppv_id:chararray,
                                nb_reason_code:chararray,
                                nb_amt:double,
                                processing_division:chararray,
                                trans_apply_dt:chararray,
                                adjust_amt:double,
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
                              };
                              
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
            								 (double)$15 AS charge_amt,
            								 (double)$16 AS bill_amt,
            								 (double)$17 AS paid_amt,
            								 (double)$18 AS trans_amt,
            								 (double)$19 AS tax_amt,
            								 (chararray)$20 AS trans_source_id,
            								 (chararray)$21 AS ppv_id,
            								 (chararray)$22 AS nb_reason_code,
            								 (double)$23 AS nb_amt,
            								 (chararray)$24 AS processing_division,
            								 (chararray)$25 AS trans_apply_dt,
            								 (double)$26 AS adjust_amt,
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
-- Read the data files. 
AR_raw = load_data('$brrLineItemInterimDir/LI_14_process_ar_resub_filter/part*','$delimiter');
BPS_raw = load_data('$brrLineItemInterimDir/LI_02B_process_bps/part*','$delimiter');

bps_ar = UNION ONSCHEMA BPS_raw, AR_raw;

bps_ar_filterout_test = FILTER bps_ar BY NOT(UPPER(global_acct_id) MATCHES '.*OBIPROD.*') ;   
 
bps_ar_go90 = FILTER bps_ar_filterout_test BY (lineitem=='117' AND trans_type=='R');
bps_ar_otherthango90 = FILTER bps_ar_filterout_test BY NOT(lineitem=='117' AND trans_type=='R'); 
                   
bps_ar_go90_rfmt = FOREACH bps_ar_go90 GENERATE                    
                           bid..trans_amt,
                           0.0 AS tax_amt,
                           trans_source_id..ext_transaction_init_dt;                   

--Report file creation (for DVT validation)
--Dataset Name - bps_ar_rfmt                                                   
bps_ar_go90_rfmt_enforced_schema = enforce_schema(bps_ar_go90_rfmt);
bps_ar_otherthango90_enforced_schema = enforce_schema(bps_ar_otherthango90);
bps_ar_rfmt_with_hash_delimter = UNION ONSCHEMA bps_ar_go90_rfmt_enforced_schema, bps_ar_otherthango90_enforced_schema; 
bps_ar_rfmt = FOREACH bps_ar_rfmt_with_hash_delimter GENERATE 
              bid .. street,
              REPLACE(REPLACE(payment_ref_id,'#',','),'null','') AS payment_ref_id,   
              ext_transaction_id,
              ext_transaction_init_dt;

--Done file creation 
--Dataset Name - bps_ar_rfmt_done_count
bps_ar_rfmt_done_group = GROUP bps_ar_rfmt ALL; 
bps_ar_rfmt_done_count = FOREACH bps_ar_rfmt_done_group GENERATE 
                         '$segmentDate' AS run_dt,
                         COUNT(bps_ar_rfmt.global_acct_id) AS row_count;
                                                
--Summary file creation
--Dataset Name - bps_ar_rfmt_summary_count
bps_ar_rfmt_summary_group = GROUP bps_ar_rfmt BY (lineitem, trans_type); 
bps_ar_rfmt_summary_count = FOREACH bps_ar_rfmt_summary_group GENERATE 
                            '$segmentDate' AS run_dt,
                            FLATTEN(group) AS (lineitem:chararray,trans_type: chararray),
                            COUNT(bps_ar_rfmt.global_acct_id) AS row_count,
                            (SUM(bps_ar_rfmt.trans_amt)/100) AS sum_trans_amt;                          
                         

STORE bps_ar_rfmt_done_count INTO '$brrLineItemOutputDir/LI_15_process_dvt_metrics_done' USING PigStorage('$delimiter'); 
STORE bps_ar_rfmt_summary_count INTO '$brrLineItemOutputDir/LI_15_process_dvt_metrics_report' USING PigStorage('$delimiter'); 
STORE bps_ar_rfmt INTO '$brrLineItemOutputDir/LI_15_process_dvt_metrics_dvt' USING PigStorage('$delimiter');