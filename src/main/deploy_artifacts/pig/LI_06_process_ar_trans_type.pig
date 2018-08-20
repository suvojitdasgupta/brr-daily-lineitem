/*
            Script Name : LI_06_process_ar_trans_type.pig
 jUnit Test Script Name : PigUnit06ProcessArTransType.java
                Purpose : pig script to to populate  Transaction Amount for AR data.
             Dependency : LI_05_process_ar_trans_type.pig                          
*/

SET pig.exec.mapPartAgg '$pig_exec_mappart_agg_enabled';
SET pig.user.cache.enabled '$pig_user_cache_mode_enabled';
SET pig.auto.local.enabled '$pig_auto_local_mode_enabled';
SET pig.local.io.sort.mb '$pig_auto_local_io_sort_mb';
SET pig.auto.local.input.maxbytes '$pig_auto_local_input_maxbytes';

DEFINE Over org.apache.pig.piggybank.evaluation.Over('int');
DEFINE Stitch org.apache.pig.piggybank.evaluation.Stitch();
IMPORT '$brrLineItemPigDir/LI_01_ingest_dimensions.pig';

DEFINE enforce_schema_AO(input_records) RETURNS output_records {

       $output_records = FOREACH $input_records GENERATE 
                                 (chararray)$0 AS bill_applied_date,
                                 (long)$1 AS line_item_balance_id,
                                 (chararray)$2 AS cash_credit_id,
                                 (chararray)$3 AS payment_processor_activity,
                                 (chararray)$4 AS trans_type,
                                 (chararray)$5 AS retry_count,
                                 (chararray)$6 AS result_code,
                                 (chararray)$7 AS account_no,
                                 (chararray)$8 AS trans_date,
                                 (chararray)$9 AS pay_method,
                                 (chararray)$10 AS line_item,
                                 (chararray)$11 AS seq_no,
                                 (chararray)$12 AS reason_code,
                                 (double)$13 AS trans_amount,
                                 (chararray)$14 AS sid,
                                 (chararray)$15 AS bid,
                                 (chararray)$16 AS global_acct_id,
                                 (chararray)$17 AS processing_division,
                                 (chararray)$18 AS payment_processor,
                                 (chararray)$19 AS campaign_id,
                                 (long)$20 AS bill_id,
                                 (chararray)$21 AS bill_cycle,
                                 (chararray)$22 AS bill_date,
                                 (chararray)$23 AS write_off_source,
                                 (chararray)$24 AS master_acct_num,
                                 (double)$25 AS bill_applied_amount,
                                 (double)$26 AS charge_amount,
                                 (double)$27 AS bill_amount,
                                 (chararray)$28 AS resub_flg,
                                 (chararray)$29 AS brandi,
                                 (chararray)$30 AS payment_method_id,
                                 (chararray)$31 AS pm_acct_num;
                                 };

DEFINE enforce_schema_ALL(input_records) RETURNS output_records {

       $output_records = FOREACH $input_records GENERATE 
                                 (chararray)$0 AS bill_applied_date,
                                 (long)$1 AS line_item_balance_id,
                                 (chararray)$2 AS cash_credit_id,
                                 (chararray)$3 AS payment_processor_activity,
                                 (chararray)$4 AS trans_type,
                                 (chararray)$5 AS retry_count,
                                 (chararray)$6 AS result_code,
                                 (chararray)$7 AS account_no,
                                 (chararray)$8 AS trans_date,
                                 (chararray)$9 AS pay_method,
                                 (chararray)$10 AS line_item,
                                 (chararray)$11 AS seq_no,
                                 (chararray)$12 AS reason_code,
                                 (double)$13 AS trans_amount,
                                 (chararray)$14 AS sid,
                                 (chararray)$15 AS bid,
                                 (chararray)$16 AS global_acct_id,
                                 (chararray)$17 AS processing_division,
                                 (chararray)$18 AS payment_processor,
                                 (chararray)$19 AS campaign_id,
                                 (long)$20 AS bill_id,
                                 (chararray)$21 AS bill_cycle,
                                 (chararray)$22 AS bill_date,
                                 (chararray)$23 AS write_off_source,
                                 (chararray)$24 AS master_acct_num,
                                 (double)$25 AS bill_applied_amount,
                                 (double)$26 AS charge_amount,
                                 (double)$27 AS bill_amount,
                                 (chararray)$28 AS resub_flg,
                                 (chararray)$29 AS brandi,
                                 (chararray)$30 AS payment_method_id,
                                 (chararray)$31 AS pm_acct_num;
                                };

trans_amount_input = LOAD '$brrLineItemInterimDir/LI_05_process_ar_trans_type/part*' using PigStorage('$delimiter') AS
	                       (
							bill_applied_date:chararray,
							line_item_balance_id:long,
							cash_credit_id:chararray,
							payment_processor_activity:chararray,
							trans_type:chararray,
							retry_count:chararray,
							result_code:chararray,
							account_no:chararray,
							trans_date:chararray,
							pay_method:chararray,
							line_item:chararray,
							seq_no:chararray,
							reason_code:chararray,
							trans_amount:double,
							sid:chararray,
							bid:chararray,
							global_acct_id:chararray,
							processing_division:chararray,
							payment_processor:chararray,
							campaign_id:chararray,
							bill_id:long,
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
 
                   
trans_amount_drop_future_bills = FILTER trans_amount_input BY (
                                                               DaysBetween( (datetime)ToDate((chararray)SUBSTRING(bill_date,0,10),'yyyy-MM-dd'),
                                                                            (datetime)ToDate('${segmentDate}', 'yyyy-MM-dd')
                                                                          ) < (long)1
                                                              );
                                                            
trans_amount_group = GROUP trans_amount_drop_future_bills BY (line_item_balance_id,payment_processor_activity);  
trans_amount_dedup = FOREACH trans_amount_group { 
                     sorted = ORDER trans_amount_drop_future_bills BY trans_date,bill_date DESC;
                     top_rec = LIMIT sorted 1;
                     GENERATE FLATTEN(top_rec);
                     };
                   
trans_amount_filterAO_Y = FILTER  trans_amount_dedup BY (trans_type=='AO'); 
trans_amount_filterAO_Y_group = GROUP trans_amount_filterAO_Y BY (bill_id);

trans_amount_filterAO_Y_dedup = FOREACH trans_amount_filterAO_Y_group {
                                 sorted = ORDER trans_amount_filterAO_Y BY bill_id DESC;
                                 top_rec = LIMIT sorted 1;
                                 GENERATE FLATTEN(top_rec);
                                };
                                
trans_amount_filterAO_Y_rollup = FOREACH trans_amount_filterAO_Y_group GENERATE 
                                    group AS bill_id,
									SUM(trans_amount_filterAO_Y.trans_amount) AS trans_amount,                                                                                                   
									SUM(trans_amount_filterAO_Y.bill_applied_amount) AS bill_applied_amount,
									SUM(trans_amount_filterAO_Y.charge_amount) AS charge_amount,
									SUM(trans_amount_filterAO_Y.bill_amount) AS bill_amount;

trans_amount_filterAO_Y_rollup_AI = JOIN trans_amount_filterAO_Y_dedup BY (top_rec::top_rec::bill_id) LEFT OUTER , trans_amount_filterAO_Y_rollup BY (bill_id);
trans_amount_filterAO_Y_amount_adjusted = FOREACH  trans_amount_filterAO_Y_rollup_AI GENERATE 
                                            top_rec::bill_applied_date AS bill_applied_date,
											top_rec::line_item_balance_id AS line_item_balance_id,
											top_rec::cash_credit_id AS cash_credit_id,
											top_rec::payment_processor_activity AS payment_processor_activity,
											top_rec::trans_type AS trans_type,
											top_rec::retry_count AS retry_count,
											top_rec::result_code AS result_code,
											top_rec::account_no AS account_no,
											top_rec::trans_date AS trans_date,
											top_rec::pay_method AS pay_method,
											top_rec::line_item AS line_item,
											top_rec::seq_no AS seq_no,
											top_rec::reason_code AS reason_code,
											trans_amount_filterAO_Y_rollup::bill_applied_amount AS trans_amount,
											top_rec::sid AS sid,
											top_rec::bid AS bid,
											top_rec::global_acct_id AS global_acct_id,
											top_rec::processing_division AS processing_division,
											top_rec::payment_processor AS payment_processor,
											top_rec::campaign_id AS campaign_id,
											top_rec::bill_id AS bill_id,
											top_rec::bill_cycle AS bill_cycle,
											top_rec::bill_date AS bill_date,
											top_rec::write_off_source AS write_off_source,
											top_rec::master_acct_num AS master_acct_num,
											top_rec::bill_applied_amount AS bill_applied_amount,
											(
											  ( trans_amount_filterAO_Y_rollup::bill_amount == trans_amount_filterAO_Y_rollup::bill_applied_amount) ? trans_amount_filterAO_Y_rollup::charge_amount
											                                                                                                        : 0
											) AS charge_amount,
											(
											  ( trans_amount_filterAO_Y_rollup::bill_amount == trans_amount_filterAO_Y_rollup::bill_applied_amount) ? trans_amount_filterAO_Y_rollup::bill_amount
											                                                                                                        : 0
											) AS bill_amount,
											top_rec::resub_flg AS resub_flg,
											top_rec::brandi AS brandi, 
											top_rec::payment_method_id AS payment_method_id,
											top_rec::pm_acct_num AS pm_acct_num;

--Branch Filter AO Trans Type - Deselect
--Dataset Name - trans_amount_filterAO_Y_amount_adjusted

trans_amount_filterAO_N = FILTER  trans_amount_dedup BY NOT(trans_type=='AO');
    trans_amount_filterAO_declines_Y = FILTER trans_amount_filterAO_N BY (bill_applied_amount > 0.0);   
		trans_amount_filterAO_declines_Y_xform = FOREACH trans_amount_filterAO_declines_Y GENERATE
		                                            top_rec::bill_applied_date AS bill_applied_date,
													top_rec::line_item_balance_id AS line_item_balance_id,
													top_rec::cash_credit_id AS cash_credit_id,
													top_rec::payment_processor_activity AS payment_processor_activity,
													top_rec::trans_type AS trans_type,
													top_rec::retry_count AS retry_count,
													top_rec::result_code AS result_code,
													top_rec::account_no AS account_no,
													top_rec::trans_date AS trans_date,
													top_rec::pay_method AS pay_method,
													top_rec::line_item AS line_item,
													top_rec::seq_no AS seq_no,
													top_rec::reason_code AS reason_code,
													top_rec::bill_applied_amount AS trans_amount,
													top_rec::sid AS sid,
													top_rec::bid AS bid,
													top_rec::global_acct_id AS global_acct_id,
													top_rec::processing_division AS processing_division,
													top_rec::payment_processor AS payment_processor,
													top_rec::campaign_id AS campaign_id,
													top_rec::bill_id AS bill_id,
													top_rec::bill_cycle AS bill_cycle,
													top_rec::bill_date AS bill_date,
													top_rec::write_off_source AS write_off_source,
													top_rec::master_acct_num AS master_acct_num,
													top_rec::bill_applied_amount AS bill_applied_amount,
													top_rec::charge_amount AS charge_amount,
													top_rec::bill_amount AS bill_amount,
													top_rec::resub_flg AS resub_flg,
													top_rec::brandi AS brandi,
													top_rec::payment_method_id AS payment_method_id,
													top_rec::pm_acct_num AS pm_acct_num;
		

		
		--Branch Filter Declined - Select
		--Dataset Name - trans_amount_filterAO_declines_Y_xform
		
    trans_amount_filterAO_declines_N = FILTER trans_amount_filterAO_N BY NOT(bill_applied_amount > 0.0);  
    
		  trans_amount_filterAO_declines_N_retries_Y = FILTER trans_amount_filterAO_declines_N BY ((long)retry_count != 0);
			  trans_amount_filterAO_declines_N_retries_Y_group = GROUP trans_amount_filterAO_declines_N_retries_Y BY (account_no,bill_id,trans_amount);
			  trans_amount_filterAO_declines_N_retries_Y_dedup = FOREACH trans_amount_filterAO_declines_N_retries_Y_group { 
			                                                     sorted = ORDER trans_amount_filterAO_declines_N_retries_Y BY line_item_balance_id DESC; 
			                                                     top_rec = LIMIT sorted 1;
			                                                     GENERATE FLATTEN(top_rec);
			                                                      };
		  
		  trans_amount_filterAO_declines_N_retries_N = FILTER trans_amount_filterAO_declines_N BY NOT((long)retry_count != 0);

		  trans_amount_filterAO_declines_N_retries_Y_dedup_enforced_schema = enforce_schema_AO(trans_amount_filterAO_declines_N_retries_Y_dedup);
		  trans_amount_filterAO_declines_N_retries_N_enforced_schema = enforce_schema_AO(trans_amount_filterAO_declines_N_retries_N);
		  trans_amount_filterAO_declines_retries_all_declutterschema = UNION ONSCHEMA trans_amount_filterAO_declines_N_retries_Y_dedup_enforced_schema,
		                                                                              trans_amount_filterAO_declines_N_retries_N_enforced_schema;	    
		  -- Implement Scan - Start																		  
		  trans_amount_filterAO_declines_retries_all_scang0 = GROUP trans_amount_filterAO_declines_retries_all_declutterschema BY (account_no,bill_id); 
		  trans_amount_filterAO_declines_retries_all_scang1 = FOREACH trans_amount_filterAO_declines_retries_all_scang0 {
		                                                      sorted = ORDER trans_amount_filterAO_declines_retries_all_declutterschema BY account_no,bill_id,resub_flg,line_item_balance_id;
		
		                                                      GENERATE FLATTEN (Stitch(sorted, Over(sorted.account_no, 'row_number')));
		
		                                                     };
		                                                     	                                                    	                                               
		  trans_amount_filterAO_declines_retries_all_scan = FOREACH trans_amount_filterAO_declines_retries_all_scang1 GENERATE 
															stitched::bill_applied_date AS bill_applied_date,
															stitched::line_item_balance_id AS line_item_balance_id,
															stitched::cash_credit_id AS cash_credit_id,
															stitched::payment_processor_activity AS payment_processor_activity,
															stitched::trans_type AS trans_type,
															stitched::retry_count AS retry_count,
															stitched::result_code AS result_code,
															stitched::account_no AS account_no,
															stitched::trans_date AS trans_date,
															stitched::pay_method AS pay_method,
															stitched::line_item AS line_item,
															stitched::seq_no AS seq_no,
															stitched::reason_code AS reason_code,
															--Populate trans_amount on first bill only
															--Zero out trans_amount on all resubmits
		                                                    (
		                                                      ($32 == 1 ) ? stitched::trans_amount : 0
		                                                    ) AS trans_amount,
															stitched::sid AS sid,
															stitched::bid AS bid,
															stitched::global_acct_id AS global_acct_id,
															stitched::processing_division AS processing_division,
															stitched::payment_processor AS payment_processor,
															stitched::campaign_id AS campaign_id,
															stitched::bill_id AS bill_id,
															stitched::bill_cycle AS bill_cycle,
															stitched::bill_date AS bill_date,
															stitched::write_off_source AS write_off_source,
															stitched::master_acct_num AS master_acct_num,
															stitched::bill_applied_amount AS bill_applied_amount,
															stitched::charge_amount AS charge_amount,
															stitched::bill_amount AS bill_amount,
															stitched::resub_flg AS resub_flg,
															stitched::brandi AS brandi,
															stitched::payment_method_id AS payment_method_id,
															stitched::pm_acct_num AS pm_acct_num;
		 -- Implement Scan - End
		 
		 --Branch Filter Retries - Select + Deselect
		 --Dataset Name - trans_amount_filterAO_declines_retries_all_scan

 trans_amount_filterAO_Y_amount_adjusted_enforced_schema = enforce_schema_ALL(trans_amount_filterAO_Y_amount_adjusted);
 trans_amount_filterAO_declines_Y_xform_enforced_schema = enforce_schema_ALL(trans_amount_filterAO_declines_Y_xform);
 trans_amount_filterAO_declines_retries_all_scan_enforced_schema = enforce_schema_ALL(trans_amount_filterAO_declines_retries_all_scan);
 	
 trans_amount_gather_all = UNION ONSCHEMA trans_amount_filterAO_Y_amount_adjusted_enforced_schema, 
                                          trans_amount_filterAO_declines_Y_xform_enforced_schema, 
                                          trans_amount_filterAO_declines_retries_all_scan_enforced_schema;                                         
 --Dataset Name - trans_amount_gather_all 
 
 --Datset trans_amount_gather_all to be stored
STORE trans_amount_gather_all INTO '$brrLineItemInterimDir/LI_06_process_ar_trans_type' USING PigStorage('$delimiter');                      
                                                              