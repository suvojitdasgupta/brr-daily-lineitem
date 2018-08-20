/*
            Script Name : LI_03A_process_ar_credits.pig
 jUnit Test Script Name : PigUnit03AProcessArCredits.java
                Purpose : pig script for processing the AR data from OBI.
                          The script processes bills and refunds. 
                          All dimension data needs to be inported in this script.
             Dependency : LI_01_ingest_dimensions.pig.                          
*/

IMPORT '$brrLineItemPigDir/LI_01_ingest_dimensions.pig';


DEFINE enforce_schema(input_records) RETURNS output_records {
       
       $output_records = FOREACH $input_records GENERATE 
                                                (chararray)$0 AS approve,
                                                (chararray)$1 AS forced_trans_flag,
                                                (chararray)$2 AS ar_result,
                                                (chararray)$3 AS acct_id,
                                                (chararray)$4 AS payment_method_id,
                                                (chararray)$5 AS bill_activity_type,
                                                (chararray)$6 AS bill_dt,
                                                (chararray)$7 AS ppa_created_dt,
                                                (chararray)$8 AS pm_id,
                                                (chararray)$9 AS lineitem,
                                                (chararray)$10 AS trxn_pm_id,
                                                (chararray)$11 AS retry_count,
                                                (chararray)$12 AS action_code,
                                                (chararray)$13 AS reason_code,
                                                (double)$14 AS processed_amount,
                                                (chararray)$15 AS sid,
                                                (chararray)$16 AS bid,
                                                (chararray)$17 AS global_acct_id,
                                                (chararray)$18 AS processing_division,
                                                (chararray)$19 AS ppv_id,
                                                (chararray)$20 AS campaign_id,
                                                (chararray)$21 AS line_item_bill_id,
                                                (chararray)$22 AS cycle,
                                                (chararray)$23 AS payment_processor_activity_id,
                                                (chararray)$24 AS libh_created_dt,
                                                (double)$25 AS activity_amount,
                                                (double)$26 AS net_amount,
                                                (chararray)$27 AS first_bill_flag,
                                                (double)$28 AS bill_amount,
                                                (chararray)$29 AS line_item_balance_id,
                                                (chararray)$30 AS brandi,
                                                (chararray)$31 AS prior_osb_bucket,
                                                (chararray)$32 AS merchant_order_number,
                                                (chararray)$33 AS fw_id,
                                                (chararray)$34 AS fw_created_dt,
                                                (chararray)$35 AS pm_acct_num;
                                              };
%declare EMPTY ''

credit_balance_bill = LOAD '$brrLineItemSqoopDir/credit_balance_bill/part*' using PigStorage('$delimiter') AS
	                      (
	                       payment_processor_activity_id:chararray,
	                       cb_created_dt:chararray
	                      );
	                      
line_item_balance_history_1 = LOAD '$brrLineItemSqoopDir/line_item_balance_history_1/part*' using PigStorage('$delimiter') AS
                                    (
                                     approve:chararray,
                                     forced_trans_flag:chararray,
                                     ar_result:chararray,
                                     acct_id:chararray,
                                     payment_method_id:chararray,
                                     bill_activity_type:chararray,
                                     bill_dt:chararray,
                                     ppa_created_dt:chararray,
                                     pm_id:chararray,
                                     lineitem:chararray,
                                     trxn_pm_id:chararray,
                                     retry_count:chararray,
                                     action_code:chararray,
                                     reason_code:chararray,
                                     processed_amount:double,
                                     sid:chararray,
                                     bid:chararray,
                                     global_acct_id:chararray,
                                     processing_division:chararray,
                                     ppv_id:chararray,
                                     campaign_id:chararray,
                                     line_item_bill_id:chararray,
                                     cycle:chararray,
                                     payment_processor_activity_id:chararray,
                                     libh_created_dt:chararray,
                                     activity_amount:double,
                                     net_amount:double,
                                     first_bill_flag:chararray,
                                     bill_amount:double,
                                     line_item_balance_id:chararray,
                                     brandi:chararray,
                                     prior_osb_bucket:chararray,
                                     merchant_order_number:chararray,
                                     fw_id:chararray,
                                     fw_created_dt:chararray,
                                     pm_acct_num:chararray
                                    );
                      
line_item_balance_history_2 = LOAD '$brrLineItemSqoopDir/line_item_balance_history_2/part*' using PigStorage('$delimiter') AS
                                    (
                                     approve:chararray,
                                     forced_trans_flag:chararray,
                                     ar_result:chararray,
                                     acct_id:chararray,
                                     payment_method_id:chararray,
                                     bill_activity_type:chararray,
                                     bill_dt:chararray,
                                     ppa_created_dt:chararray,
                                     pm_id:chararray,
                                     lineitem:chararray,
                                     trxn_pm_id:chararray,
                                     retry_count:chararray,
                                     action_code:chararray,
                                     reason_code:chararray,
                                     processed_amount:double,
                                     sid:chararray,
                                     bid:chararray,
                                     global_acct_id:chararray,
                                     processing_division:chararray,
                                     ppv_id:chararray,
                                     campaign_id:chararray,
                                     line_item_bill_id:chararray,
                                     cycle:chararray,
                                     payment_processor_activity_id:chararray,
                                     libh_created_dt:chararray,
                                     activity_amount:double,
                                     net_amount:double,
                                     first_bill_flag:chararray,
                                     bill_amount:double,
                                     line_item_balance_id:chararray,
                                     brandi:chararray,
                                     prior_osb_bucket:chararray,
                                     merchant_order_number:chararray,
                                     fw_id:chararray,
                                     fw_created_dt:chararray,
                                     pm_acct_num:chararray
                                    );                      	 

                     
balance_status_history = LOAD '$brrLineItemSqoopDir/balance_status_history/part*' using PigStorage('$delimiter') AS
	                      (
	                       line_item_balance_id:chararray,
	                       bsh_status:chararray
	                      );
	                      

line_item_balance_history_1_enforced_schema = enforce_schema(line_item_balance_history_1);
line_item_balance_history_2_enforced_schema = enforce_schema(line_item_balance_history_2);	
                      
line_item_balance_history = UNION ONSCHEMA 	line_item_balance_history_1_enforced_schema, 
                                            line_item_balance_history_2_enforced_schema; 
                                                                                                          	                                         	                      	                      
join1 = JOIN credit_balance_bill BY (payment_processor_activity_id) RIGHT OUTER, line_item_balance_history BY (payment_processor_activity_id);
bill_credit = FOREACH join1 GENERATE
                 line_item_balance_history::approve AS approve,
                 line_item_balance_history::forced_trans_flag AS forced_trans_flag,
                 line_item_balance_history::ar_result AS ar_result,
                 line_item_balance_history::acct_id AS acct_id,
                 line_item_balance_history::payment_method_id AS payment_method_id,
                 line_item_balance_history::bill_activity_type AS bill_activity_type,
                 line_item_balance_history::bill_dt AS bill_dt,
                 line_item_balance_history::ppa_created_dt AS ppa_created_dt,
                 ((line_item_balance_history::trxn_pm_id IS NULL ) ? line_item_balance_history::pm_id : line_item_balance_history::trxn_pm_id)  AS pm_id,
                 line_item_balance_history::lineitem AS lineitem,
                 line_item_balance_history::retry_count AS retry_count,
                 line_item_balance_history::action_code AS action_code,
                 line_item_balance_history::reason_code AS reason_code,
                 line_item_balance_history::processed_amount AS processed_amount,
                 line_item_balance_history::sid AS sid,
                 line_item_balance_history::bid AS bid,
                 line_item_balance_history::global_acct_id AS global_acct_id,
                 line_item_balance_history::processing_division AS processing_division,
                 line_item_balance_history::ppv_id AS ppv_id,
                 line_item_balance_history::campaign_id AS campaign_id,
                 line_item_balance_history::line_item_bill_id AS line_item_bill_id,
                 line_item_balance_history::cycle AS cycle,
                 line_item_balance_history::payment_processor_activity_id AS payment_processor_activity_id,
                 line_item_balance_history::libh_created_dt AS libh_created_dt,
                 line_item_balance_history::activity_amount AS activity_amount,
                 line_item_balance_history::net_amount AS net_amount,
                 line_item_balance_history::first_bill_flag AS first_bill_flag,
                 line_item_balance_history::bill_amount AS bill_amount,
                 line_item_balance_history::line_item_balance_id AS line_item_balance_id,
                 credit_balance_bill::cb_created_dt AS cb_created_dt,
                 line_item_balance_history::brandi AS brandi,
                 line_item_balance_history::prior_osb_bucket AS prior_osb_bucket,
                 line_item_balance_history::merchant_order_number AS merchant_order_number,
                 /*
                 (  ( (line_item_balance_history::fw_id IS NOT NULL) 
                       AND MilliSecondsBetween(ToDate(line_item_balance_history::fw_created_dt,'yyyy-MM-dd HH:mm:ss'), ToDate('$segmentDate 23:59:59','yyyy-MM-dd HH:mm:ss')) <= (long)'0' 
                    )  ? '105' : line_item_balance_history::fw_id
                 ) AS fw_id,
                 */
                 (  ( (line_item_balance_history::fw_id IS NOT NULL) 
                       AND MilliSecondsBetween(ToDate(line_item_balance_history::fw_created_dt,'yyyy-MM-dd HH:mm:ss'), ToDate('$segmentDate 23:59:59','yyyy-MM-dd HH:mm:ss')) <= (long)'0' 
                    )  ? '105' : '$EMPTY'
                 ) AS fw_id,
                 line_item_balance_history::pm_acct_num AS pm_acct_num;	

join2 = JOIN bill_credit BY (line_item_balance_id) LEFT OUTER, balance_status_history BY (line_item_balance_id);

bill_credit_history = FOREACH join2 GENERATE
                              bill_credit::line_item_balance_id AS line_item_balance_id,
                              bill_credit::approve  AS approve,
                              bill_credit::forced_trans_flag AS forced_trans_flag,
                              bill_credit::ar_result AS ar_result,
                              bill_credit::acct_id AS acct_id,
                              bill_credit::payment_method_id AS payment_method_id,
                              bill_credit::bill_activity_type AS bill_activity_type,
                              bill_credit::bill_dt AS bill_dt,
                              bill_credit::ppa_created_dt  AS ppa_created_dt,
                              bill_credit::pm_id AS pm_id,
                              bill_credit::lineitem AS lineitem,
                              bill_credit::retry_count AS retry_count,
                              bill_credit::action_code AS action_code,
                              bill_credit::reason_code AS reason_code,
                              bill_credit::processed_amount AS processed_amount,
                              bill_credit::sid AS sid,
                              bill_credit::bid AS bid,
                              bill_credit::global_acct_id AS global_acct_id,
                              bill_credit::processing_division AS processing_division,
                              bill_credit::ppv_id  AS ppv_id,
                              bill_credit::campaign_id AS campaign_id,
                              bill_credit::line_item_bill_id AS line_item_bill_id,
                              bill_credit::cycle AS cycle,
                              bill_credit::payment_processor_activity_id AS payment_processor_activity_id,
                              bill_credit::libh_created_dt AS libh_created_dt, 
                              bill_credit::activity_amount AS activity_amount,
                              bill_credit::net_amount AS net_amount,
                              bill_credit::first_bill_flag  AS first_bill_flag, 
                              bill_credit::bill_amount AS bill_amount,
                              bill_credit::cb_created_dt AS cb_created_dt,
                              (
                              (TRIM(bill_credit::fw_id) IS NOT NULL AND TRIM(bill_credit::fw_id) != '$EMPTY') 
                                ? TRIM(bill_credit::fw_id) 
                                : TRIM(balance_status_history::bsh_status)
                              ) AS bsh_status,
                              bill_credit::brandi AS brandi,
                              bill_credit::prior_osb_bucket AS prior_osb_bucket,
                              bill_credit::merchant_order_number AS merchant_order_number,
                              bill_credit::pm_acct_num AS pm_acct_num;                  
 
-----------------------------------------------------------------------------------------------------------------                                        
-- Dataset Name - bill_credit_history 
-- Datset bill_credit_history feed will be the input to "determine trans_types" subgraph
----------------------------------------------------------------------------------------------------------------   

--Datset bill_credit_history will be the input to "determine trans_types" subgraph
STORE bill_credit_history INTO '$brrLineItemInterimDir/LI_03A_process_ar_credits' USING PigStorage('$delimiter');    
                                                             