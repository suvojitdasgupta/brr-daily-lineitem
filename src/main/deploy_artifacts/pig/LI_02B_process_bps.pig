/*
            Script Name : LI_02B_process_bps.pig
 jUnit Test Script Name : PigUnit02BProcessBps.java
                Purpose : pig script for processing all BPS data needed for lineitem daily processing.
                          All required dimension data was sourced using the import statement.
             Dependency : LI_01_ingest_dimensions.pig                          
*/

SET pig.exec.mapPartAgg '$pig_exec_mappart_agg_enabled';
SET pig.user.cache.enabled '$pig_user_cache_mode_enabled';
SET pig.auto.local.enabled '$pig_auto_local_mode_enabled';
SET pig.local.io.sort.mb '$pig_auto_local_io_sort_mb';
SET pig.auto.local.input.maxbytes '$pig_auto_local_input_maxbytes';

DEFINE COALESCE datafu.pig.util.Coalesce();
IMPORT '$brrLineItemPigDir/LI_01_ingest_dimensions.pig';

DEFINE enforce_schema_ALL_BUT_SG(input_records) RETURNS output_records {
                    
       $output_records = FOREACH $input_records GENERATE 
                                                (chararray)$0 AS global_acct_id,
                                                (long)$1 AS offer_id,
                                                (chararray)$2 AS obi_acct,
                                                (chararray)$3 AS payment_reference_id,
                                                (chararray)$4 AS payment_method_id,
                                                (chararray)$5 AS transaction_type,
                                                (chararray)$6 AS billed_dt,
                                                (double)$7 AS charge_amount,
                                                (double)$8 AS billed_amount,
                                                (double)$9 AS paid_amount,
                                                (double)$10 AS trans_amount,
                                                (double)$11 AS tax_amount,
                                                (chararray)$12 AS ppv_id,
                                                (chararray)$13 AS trans_applied_dt,
                                                (chararray)$14 AS bill_id,
                                                (chararray)$15 AS business_unit_id,
                                                (chararray)$16 AS paid_dt;
                                               };

DEFINE enforce_schema_ALL(input_records) RETURNS output_records {

        $output_records = FOREACH $input_records GENERATE 
                                                 (chararray)$0 AS guid,
                                                 (long)$1 AS offer_id,
                                                 (chararray)$2 AS obi_acct,
                                                 (chararray)$3 AS bid,
                                                 (chararray)$4 AS sid,
                                                 (chararray)$5 AS iso_currency_id,
                                                 (chararray)$6 AS payment_ref_id,
                                                 (chararray)$7 AS ar_payment_method_id,
                                                 (chararray)$8 AS pm_id,
                                                 (chararray)$9 AS cycle,
                                                 (chararray)$10 AS billed_dt,
                                                 (chararray)$11 AS trans_type,
                                                 (chararray)$12 AS retry_cnt,
                                                 (chararray)$13 AS result_code,
                                                 (chararray)$14 AS reason_code,
                                                 (chararray)$15 AS lineitem,
                                                 (double)$16 AS charge_amt,
                                                 (double)$17 AS bill_amt,
                                                 (double)$18 AS paid_amt,
                                                 (double)$19 AS trans_amt,
                                                 (double)$20 AS tax_amt,
                                                 (chararray)$21 AS trans_source_id,
                                                 (chararray)$22 AS ppv_id,
                                                 (chararray)$23 AS nb_reason_code,
                                                 (double)$24 AS nb_amt,
                                                 (chararray)$25 AS processing_division,
                                                 (chararray)$26 AS trans_apply_dt,
                                                 (double)$27 AS adjust_amt,
                                                 (chararray)$28 AS balance_id,
                                                 (chararray)$29 AS bill_id,
                                                 (chararray)$30 AS resub_flg,
                                                 (chararray)$31 AS write_off_source,
                                                 (chararray)$32 AS write_off_reason_cd,
                                                 (chararray)$33 AS city,
                                                 (chararray)$34 AS state,
                                                 (chararray)$35 AS zipcode,
                                                 (chararray)$36 AS country_code,
                                                 (chararray)$37 AS paid_dt,
                                                 (chararray)$38 AS first_name,
                                                 (chararray)$39 AS last_name,
                                                 (chararray)$40 AS email_address,
                                                 (chararray)$41 AS phone_number,
                                                 (chararray)$42 AS street;
                                                };

                                     
SB_raw = LOAD '$brrLineItemSqoopDir/SB/part*' using PigStorage('$delimiter') AS
          (
           global_acct_id:chararray,
           offer_id:long,
           obi_acct:chararray,
           busybid:chararray,
           iso_currency_id:chararray,
           billed_dt:chararray,
           charge_amt:double,
           bill_amt:double,
           trans_amt:double,
           trans_apply_dt:chararray,
           bill_id:chararray,
           balance_id:chararray,
           trans_type:chararray
          );


bps_synsdication_address = LOAD '$brrLineItemSqoopDir/bps_syndication_address/part*' using PigStorage('$delimiter') AS
                      (
                       global_acct_id:chararray,
                       payment_type:chararray,
                       offer_id:chararray,
                       first_name:chararray,
                       last_name:chararray,
                       email_address:chararray,
                       phone_number:chararray,
                       street:chararray,
                       city:chararray,
                       state:chararray,
                       zipcode:chararray,
                       country_code:chararray
                      );

SG_raw = LOAD '$brrLineItemSqoopDir/SG/part*' using PigStorage('$delimiter') AS
          (
          guid:chararray,
          offer_id:long,
          obi_acct:chararray,
          payment_method_id:chararray,
          billed_dt:chararray,
          trans_type:chararray,
          trans_amt:double,
          ppv_id:chararray,
          trans_apply_dt:chararray,
          adjust_amt:double,
          bill_id:chararray,
          business_unit_id:chararray,
          paid_dt:chararray
          );
                            
SP1_raw = LOAD '$brrLineItemSqoopDir/SP1/part*' using PigStorage('$delimiter') AS
	         (
	          global_acct_id:chararray,
	          offer_id:long,
	          obi_acct:chararray,
	          payment_reference_id:chararray,
	          payment_method_id:chararray,
	          transaction_type:chararray,
	          billed_dt:chararray,
	          charge_amount:double,
	          billed_amount:double,
	          paid_amount:double,
	          trans_amount:double,
	          tax_amount:double,
	          ppv_id:chararray,
	          trans_applied_dt:chararray,
	          bill_id:chararray,
	          business_unit_id:chararray,
	          paid_dt:chararray
	         );  
	         
SP2_raw = LOAD '$brrLineItemSqoopDir/SP2/part*' using PigStorage('$delimiter') AS	
          (
          global_acct_id:chararray,
          offer_id:long,
          obi_acct:chararray,
          payment_reference_id:chararray,
          payment_method_id:chararray,
          transaction_type:chararray,
          billed_dt:chararray,
          charge_amount:double,
          billed_amount:double,
          paid_amount:double,
          trans_amount:double,
          tax_amount:double,
          ppv_id:chararray,
          trans_applied_dt:chararray,
          bill_id:chararray,
          business_unit_id:chararray,
          paid_dt:chararray
         );
         
SP3_raw = LOAD '$brrLineItemSqoopDir/SP3/part*' using PigStorage('$delimiter') AS
          (
          global_acct_id:chararray,
          offer_id:long,
          obi_acct:chararray,
          payment_reference_id:chararray,
          payment_method_id:chararray,
          transaction_type:chararray,
          billed_dt:chararray,
          charge_amount:double,
          billed_amount:double,
          paid_amount:double,
          trans_amount:double,
          tax_amount:double,
          ppv_id:chararray,
          trans_applied_dt:chararray,
          bill_id:chararray,
          business_unit_id:chararray,
          paid_dt:chararray
         );
         
 SX_raw = LOAD '$brrLineItemSqoopDir/SX/part*' using PigStorage('$delimiter') AS
          (
          global_acct_id:chararray,
          offer_id:long,
          obi_acct:chararray,
          payment_reference_id:chararray,
          payment_method_id:chararray,
          transaction_type:chararray,
          billed_dt:chararray,
          charge_amount:double,
          billed_amount:double,
          paid_amount:double,
          trans_amount:double,
          tax_amount:double,
          ppv_id:chararray,
          trans_applied_dt:chararray,
          bill_id:chararray,
          business_unit_id:chararray,
          paid_dt:chararray
         ); 
         
SD_raw = LOAD '$brrLineItemSqoopDir/SD/part*' using PigStorage('$delimiter') AS
         (
          global_acct_id:chararray,
          offer_id:long,
          obi_acct:chararray,
          payment_reference_id:chararray,
          payment_method_id:chararray,
          transaction_type:chararray,
          billed_dt:chararray,
          charge_amount:double,
          billed_amount:double,
          paid_amount:double,
          trans_amount:double,
          tax_amount:double,
          ppv_id:chararray,
          trans_applied_dt:chararray,
          bill_id:chararray,
          business_unit_id:chararray,
          paid_dt:chararray
         );  
         
SE_raw = LOAD '$brrLineItemSqoopDir/SE/part*' using PigStorage('$delimiter') AS  
         (
          global_acct_id:chararray,
          offer_id:long,
          obi_acct:chararray,
          payment_reference_id:chararray,
          payment_method_id:chararray,
          transaction_type:chararray,
          billed_dt:chararray,
          charge_amount:double,
          billed_amount:double,
          paid_amount:double,
          trans_amount:double,
          tax_amount:double,
          ppv_id:chararray,
          trans_applied_dt:chararray,
          bill_id:chararray,
          business_unit_id:chararray,
          paid_dt:chararray
         );                                        


bps_payable_address = LOAD '$brrLineItemSqoopDir/bps_payable_address/part*' using PigStorage('$delimiter') AS
                      (
                       global_acct_id:chararray,
                       first_name:chararray,
                       last_name:chararray,
                       phone_number:chararray,
                       email_address:chararray,
                       street_address:chararray,
                       city:chararray,
                       state:chararray,
                       zip:chararray,
                       country_code:chararray
                      );
                     
bps_brd_currency = LOAD '$brrLineItemSqoopDir/bps_brd_currency/part*' using PigStorage('$delimiter') AS
                    (
                     offer_id:long,
                     iso_currency_id:chararray,
                     country_code:chararray
                    );                     
                     
----------------------------------------------
-- Component/Subgraph : SB Data - Start
----------------------------------------------

SB_address_join = JOIN SB_raw BY (global_acct_id), bps_synsdication_address BY (global_acct_id);
SB_rfmt1 = JOIN SB_address_join BY (bps_synsdication_address::payment_type), pm_mapping BY (infinys_pm_id);
SB_rfmt2 = JOIN SB_rfmt1 BY (SB_address_join::bps_synsdication_address::offer_id), offer_line_item BY (offer_id);
SB_filter = FILTER SB_rfmt2 BY SB_rfmt1::SB_address_join::SB_raw::bill_amt != 0;

SB_rfmt = FOREACH SB_filter GENERATE
                       SB_rfmt1::SB_address_join::SB_raw::global_acct_id AS guid,
                       SB_rfmt1::SB_address_join::SB_raw::offer_id AS offer_id,
                       SB_rfmt1::SB_address_join::SB_raw::obi_acct AS obi_acct,
                       '1' AS bid,
                       '2' AS sid,                                  
                       COALESCE(TRIM(SB_rfmt1::SB_address_join::SB_raw::iso_currency_id),'840')  AS  iso_currency_id,          
                       '' AS payment_ref_id,
                       '-1' AS ar_payment_method_id,
                       SB_rfmt1::pm_mapping::aol_pm_id AS pm_id,  
                       SUBSTRING(SB_rfmt1::SB_address_join::SB_raw::billed_dt,8,10)  AS cycle,
                       SB_rfmt1::SB_address_join::SB_raw::billed_dt AS billed_dt,
                       SB_rfmt1::SB_address_join::SB_raw::trans_type AS trans_type,
                       '0' AS retry_cnt,
                       'A' AS result_code,
                       '100' AS reason_code,
                       offer_line_item::line_item_id AS lineitem,
                       SB_rfmt1::SB_address_join::SB_raw::charge_amt AS charge_amt,
                       SB_rfmt1::SB_address_join::SB_raw::bill_amt AS bill_amt,
                       0.0 AS paid_amt,
                       SB_rfmt1::SB_address_join::SB_raw::trans_amt AS trans_amt,
                       0.0 AS tax_amt,
                       'OBI_AR' AS trans_source_id,
                       ((SB_rfmt1::SB_address_join::bps_synsdication_address::payment_type == '8' ) ? '24' : '21') AS ppv_id,
                       '' AS nb_reason_code,
                       0.0 AS nb_amt,
                       '-1' AS processing_division,
                       SB_rfmt1::SB_address_join::SB_raw::trans_apply_dt AS trans_apply_dt,
                       0.0  AS adjust_amt,
                       SB_rfmt1::SB_address_join::SB_raw::balance_id AS balance_id,
                       SB_rfmt1::SB_address_join::SB_raw::bill_id AS bill_id,                  
                       'N' AS resub_flg,                       
                       '' AS write_off_source,
                       '-1' AS write_off_reason_cd,
                       SB_rfmt1::SB_address_join::bps_synsdication_address::city AS city,
                       SB_rfmt1::SB_address_join::bps_synsdication_address::state AS state,
                       SB_rfmt1::SB_address_join::bps_synsdication_address::zipcode AS zipcode,
                       SB_rfmt1::SB_address_join::bps_synsdication_address::country_code AS country_code,
                       '' AS paid_dt,
                       SB_rfmt1::SB_address_join::bps_synsdication_address::first_name AS first_name,
                       SB_rfmt1::SB_address_join::bps_synsdication_address::last_name AS last_name,
                       SB_rfmt1::SB_address_join::bps_synsdication_address::email_address AS email_address,
                       SB_rfmt1::SB_address_join::bps_synsdication_address::phone_number AS phone_number,
                       SB_rfmt1::SB_address_join::bps_synsdication_address::street AS street;
                      
----------------------------------------------
-- Component/Subgraph : SB Data - End
----------------------------------------------

----------------------------------------------
-- Component/Subgraph : Data from BPS - Start
----------------------------------------------

-- Modified the initial lookup data to remove any dups based on the keys. 
-- The modified lookup is used by all non 'SB' transaction types. 
pm_mapping_group = GROUP  pm_mapping BY (description);
pm_mapping_dedup = FOREACH pm_mapping_group {
                     sorted = ORDER pm_mapping BY aol_pm_id DESC; --Keep it as DESC sort order.
                     top_rec = LIMIT sorted 1;
                     GENERATE FLATTEN(top_rec); 
                   }
         
SG_rfmt1 = JOIN SG_raw BY (business_unit_id), bu_line_item BY (business_unit_id);
SG_rfmt2 = JOIN SG_rfmt1 BY (payment_method_id), pm_mapping_dedup BY (top_rec::description);	
SG_rfmt = FOREACH SG_rfmt2 GENERATE
                  SG_rfmt1::SG_raw::guid AS guid,
                  SG_rfmt1::SG_raw::offer_id AS offer_id,
                  SG_rfmt1::SG_raw::obi_acct AS obi_acct,
                  '1' AS bid,
                  '2' AS sid,
                  '840' AS iso_currency_id,
                  '' AS payment_ref_id,
                  '-1' AS ar_payment_method_id,
                  pm_mapping_dedup::top_rec::aol_pm_id AS pm_id,
                  SUBSTRING(SG_rfmt1::SG_raw::billed_dt,8,10)  AS cycle,
                  SG_rfmt1::SG_raw::billed_dt AS billed_dt,
                  SG_rfmt1::SG_raw::trans_type AS trans_type,
                  '0' AS retry_cnt,
                  'A' AS result_code,
                  '100' AS reason_code,
                  SG_rfmt1::bu_line_item::lineitem AS lineitem,
                  0.0 AS charge_amt,
                  0.0 AS bill_amt,
                  0.0 AS paid_amt,
                  SG_rfmt1::SG_raw::trans_amt AS trans_amt,
                  0.0 AS tax_amt,
                  'OBI_AR' AS trans_source_id,
                  SG_rfmt1::SG_raw::ppv_id AS ppv_id,
                  '' AS nb_reason_code,
                  0.0 AS nb_amt,
                  '-1' AS processing_division,
                  SG_rfmt1::SG_raw::trans_apply_dt AS trans_apply_dt,
                  SG_rfmt1::SG_raw::adjust_amt AS adjust_amt,
                  '-1' AS balance_id,
                  SG_rfmt1::SG_raw::bill_id AS bill_id,
                  'N' AS resub_flg,
                  '' AS write_off_source,
                  '-1' AS write_off_reason_cd,
                  '' AS city,
                  '' AS state,
                  '' AS zipcode,
                  '' AS country_code,
                  SG_rfmt1::SG_raw::paid_dt AS paid_dt,
                  '' AS first_name,
                  '' AS last_name,
                  '' AS email_address,
                  '' AS phone_number,
                  '' AS street;
 
SP1_raw_enforced_schema = enforce_schema_ALL_BUT_SG(SP1_raw);
SP2_raw_enforced_schema = enforce_schema_ALL_BUT_SG(SP2_raw);
SP3_raw_enforced_schema = enforce_schema_ALL_BUT_SG(SP3_raw);
SX_raw_enforced_schema = enforce_schema_ALL_BUT_SG(SX_raw);
SD_raw_enforced_schema = enforce_schema_ALL_BUT_SG(SD_raw);
SE_raw_enforced_schema = enforce_schema_ALL_BUT_SG(SE_raw); 
         
union_ALL_BUT_SG = UNION ONSCHEMA SP1_raw_enforced_schema,
                                  SP2_raw_enforced_schema,
                                  SP3_raw_enforced_schema,
                                  SX_raw_enforced_schema,
                                  SD_raw_enforced_schema,
                                  SE_raw_enforced_schema;

ALL_BUT_SG_rfmt1 = JOIN union_ALL_BUT_SG BY (business_unit_id), bu_line_item BY (business_unit_id);
ALL_BUT_SG_rfmt2 = JOIN ALL_BUT_SG_rfmt1 BY (payment_method_id), pm_mapping_dedup BY (top_rec::description);

ALL_BUT_SG_rfmt = FOREACH ALL_BUT_SG_rfmt2 GENERATE
                          ALL_BUT_SG_rfmt1::union_ALL_BUT_SG::global_acct_id AS guid,
                          ALL_BUT_SG_rfmt1::union_ALL_BUT_SG::offer_id AS offer_id,
                          ALL_BUT_SG_rfmt1::union_ALL_BUT_SG::obi_acct AS obi_acct,
                          '1' AS bid,
                          '2' AS sid,
                          '840' AS iso_currency_id,
                          ALL_BUT_SG_rfmt1::union_ALL_BUT_SG::payment_reference_id AS payment_ref_id,
                          '-1' AS ar_payment_method_id,
                          pm_mapping_dedup::top_rec::aol_pm_id AS pm_id,
                          '-1' AS cycle,
                          ALL_BUT_SG_rfmt1::union_ALL_BUT_SG::billed_dt AS billed_dt,
                          ALL_BUT_SG_rfmt1::union_ALL_BUT_SG::transaction_type AS trans_type,
                          '0' AS retry_cnt,
                          'A' AS result_code,
                          '100' AS reason_code,
                          ALL_BUT_SG_rfmt1::bu_line_item::lineitem AS lineitem,
                          ALL_BUT_SG_rfmt1::union_ALL_BUT_SG::charge_amount/10 AS charge_amt,
                          ALL_BUT_SG_rfmt1::union_ALL_BUT_SG::billed_amount/10 AS bill_amt,
                          ALL_BUT_SG_rfmt1::union_ALL_BUT_SG::paid_amount/10 AS paid_amt,
                          ALL_BUT_SG_rfmt1::union_ALL_BUT_SG::trans_amount/10 AS trans_amt,
                          ALL_BUT_SG_rfmt1::union_ALL_BUT_SG::tax_amount/10 AS tax_amt,
                          'OBI_AR' AS trans_source_id,
                          ALL_BUT_SG_rfmt1::union_ALL_BUT_SG::ppv_id AS ppv_id,
                          '' AS nb_reason_code,
                          0.0 AS nb_amt,
                          '-1' AS processing_division,
                          ALL_BUT_SG_rfmt1::union_ALL_BUT_SG::trans_applied_dt AS trans_apply_dt,
                          0.0 AS adjust_amt,
                          '-1' AS balance_id,
                          ALL_BUT_SG_rfmt1::union_ALL_BUT_SG::bill_id AS bill_id,
                          'N' AS resub_flg,
                          '' AS write_off_source,
                          '-1' AS write_off_reason_cd,
                          '' AS city,
                          '' AS state,
                          '' AS zipcode,
                          '' AS country_code,
                          ALL_BUT_SG_rfmt1::union_ALL_BUT_SG::paid_dt AS paid_dt,
                          '' AS first_name,
                          '' AS last_name,
                          '' AS email_address,
                          '' AS phone_number,
                          '' AS street;                                            
--------------------------------------------
-- Component/Subgraph : Data from BPS - End
-------------------------------------------- 
SB_rfmt_enforced_schema = enforce_schema_ALL(SB_rfmt);
SG_rfmt_enforced_schema = enforce_schema_ALL(SG_rfmt);
ALL_BUT_SG_rfmt_enforced_schema = enforce_schema_ALL(ALL_BUT_SG_rfmt);

union_ALL = UNION ONSCHEMA SB_rfmt_enforced_schema, 
                           SG_rfmt_enforced_schema, 
                           ALL_BUT_SG_rfmt_enforced_schema;

union_ALL_grouped = GROUP union_ALL BY 
	                              (
	                              obi_acct,
	                              trans_type,
	                              pm_id,
                                  billed_dt,
	                              result_code,
	                              reason_code,
	                              lineitem
	                              );

bps_transactions_ALL_deduped = FOREACH union_ALL_grouped {  
                                 sorted = ORDER union_ALL BY trans_apply_dt ASC; --Matching AbInitio's default Sort order.
                                 top_rec = LIMIT sorted 1;
                                 GENERATE FLATTEN(top_rec);
                               }


bps_transactions_ALL_rollup =  FOREACH union_ALL_grouped GENERATE
                               FLATTEN(group) AS (obi_acct: chararray,trans_type: chararray,pm_id: chararray,billed_dt: chararray,result_code: chararray,reason_code: chararray,lineitem: chararray),
                               SUM(union_ALL.charge_amt) AS charge_amt,                    
                               SUM(union_ALL.bill_amt) AS bill_amt,
                               SUM(union_ALL.paid_amt) AS paid_amt,
                               SUM(union_ALL.trans_amt) AS trans_amt,
                               SUM(union_ALL.tax_amt) AS tax_amt,
                               SUM(union_ALL.adjust_amt) AS adjust_amt;

bps_transactions_ALL_rollup_AI = JOIN bps_transactions_ALL_deduped BY (top_rec::obi_acct,
                                                                       top_rec::trans_type,
	                                                                   top_rec::pm_id,
                                                                       top_rec::billed_dt,
	                                                                   top_rec::result_code,
	                                                                   top_rec::reason_code,
	                                                                   top_rec::lineitem) LEFT OUTER, 
                                      bps_transactions_ALL_rollup BY (obi_acct,
                                                                      trans_type,
	                                                                  pm_id,
                                                                      billed_dt,
	                                                                  result_code,
	                                                                  reason_code,
                                                                      lineitem);


bps_transactions_get_address = JOIN bps_transactions_ALL_rollup_AI BY (bps_transactions_ALL_deduped::top_rec::guid) LEFT OUTER, 
                                                                       bps_payable_address BY (global_acct_id);
                                                                                                                                           
bps_transactions_invert_sign = FOREACH bps_transactions_get_address GENERATE 
                                       bps_transactions_ALL_rollup_AI::bps_transactions_ALL_deduped::top_rec::bid AS bid,
                                       bps_transactions_ALL_rollup_AI::bps_transactions_ALL_deduped::top_rec::sid AS sid,
                                       bps_transactions_ALL_rollup_AI::bps_transactions_ALL_deduped::top_rec::guid AS guid,
                                       bps_transactions_ALL_rollup_AI::bps_transactions_ALL_deduped::top_rec::offer_id AS offer_id,
                                       bps_transactions_ALL_rollup_AI::bps_transactions_ALL_deduped::top_rec::obi_acct AS obi_acct,
                                       COALESCE(bps_transactions_ALL_rollup_AI::bps_transactions_ALL_deduped::top_rec::ar_payment_method_id,'-1') AS ar_payment_method_id,
                                       bps_transactions_ALL_rollup_AI::bps_transactions_ALL_deduped::top_rec::pm_id AS pm_id,
                                       '111111' AS pm_acct_num,
                                       '-1' AS routing_number,
                                       (int)bps_transactions_ALL_rollup_AI::bps_transactions_ALL_deduped::top_rec::cycle AS cycle,
                                       bps_transactions_ALL_rollup_AI::bps_transactions_ALL_deduped::top_rec::billed_dt AS billed_dt,
                                       bps_transactions_ALL_rollup_AI::bps_transactions_ALL_deduped::top_rec::trans_type AS trans_type,
                                       bps_transactions_ALL_rollup_AI::bps_transactions_ALL_deduped::top_rec::retry_cnt AS retry_cnt,
                                       bps_transactions_ALL_rollup_AI::bps_transactions_ALL_deduped::top_rec::result_code  AS result_code,
                                       (
                                       
                                       bps_transactions_ALL_rollup_AI::bps_transactions_ALL_deduped::top_rec::reason_code == '-1'
                                         ? ''
                                         : bps_transactions_ALL_rollup_AI::bps_transactions_ALL_deduped::top_rec::reason_code
                                       
                                       ) AS reason_code,
                                       
                                       bps_transactions_ALL_rollup_AI::bps_transactions_ALL_deduped::top_rec::lineitem AS lineitem,
                                       bps_transactions_ALL_rollup_AI::bps_transactions_ALL_rollup::charge_amt AS charge_amt,
                                       bps_transactions_ALL_rollup_AI::bps_transactions_ALL_rollup::bill_amt AS bill_amt,
                                       ABS(bps_transactions_ALL_rollup_AI::bps_transactions_ALL_rollup::paid_amt) AS paid_amt,
                                       ABS(bps_transactions_ALL_rollup_AI::bps_transactions_ALL_rollup::trans_amt) AS trans_amt,
                                       ABS(bps_transactions_ALL_rollup_AI::bps_transactions_ALL_rollup::tax_amt) AS tax_amt,
                                       COALESCE(TRIM(bps_transactions_ALL_rollup_AI::bps_transactions_ALL_deduped::top_rec::trans_source_id),'OBI_AR') AS trans_source_id,
                                       (
                                       
                                       bps_transactions_ALL_rollup_AI::bps_transactions_ALL_deduped::top_rec::ppv_id == '-1' 
                                         ? '0' 
                                         :  bps_transactions_ALL_rollup_AI::bps_transactions_ALL_deduped::top_rec::ppv_id
                                       
                                       ) AS ppv_id,
                                       (
                                       
                                       bps_transactions_ALL_rollup_AI::bps_transactions_ALL_deduped::top_rec::nb_reason_code == '-1' 
                                         ? '' 
                                         : bps_transactions_ALL_rollup_AI::bps_transactions_ALL_deduped::top_rec::nb_reason_code
                                       
                                       ) AS nb_reason_code,
                                       ABS(bps_transactions_ALL_rollup_AI::bps_transactions_ALL_deduped::top_rec::nb_amt) AS nb_amt,
                                       bps_transactions_ALL_rollup_AI::bps_transactions_ALL_deduped::top_rec::processing_division AS processing_division,
                                       bps_transactions_ALL_rollup_AI::bps_transactions_ALL_deduped::top_rec::trans_apply_dt AS trans_apply_dt,                                    
                                       ABS(bps_transactions_ALL_rollup_AI::bps_transactions_ALL_rollup::adjust_amt) AS adjust_amt,                                       
                                       bps_transactions_ALL_rollup_AI::bps_transactions_ALL_deduped::top_rec::balance_id AS balance_id,
                                       bps_transactions_ALL_rollup_AI::bps_transactions_ALL_deduped::top_rec::bill_id AS bill_id,
                                       bps_transactions_ALL_rollup_AI::bps_transactions_ALL_deduped::top_rec::resub_flg AS resub_flg,
                                       bps_transactions_ALL_rollup_AI::bps_transactions_ALL_deduped::top_rec::write_off_source AS write_off_source,
                                       (
                                       
                                       bps_transactions_ALL_rollup_AI::bps_transactions_ALL_deduped::top_rec::write_off_reason_cd == '-1' 
                                         ? '' 
                                         : bps_transactions_ALL_rollup_AI::bps_transactions_ALL_deduped::top_rec::write_off_reason_cd
                                       
                                       ) AS write_off_reason_cd,
                                       
                                       TRIM(bps_payable_address::city) AS city,
                                       TRIM(bps_payable_address::state) AS state,
                                       bps_payable_address::zip AS zipcode,
                                       TRIM(bps_payable_address::country_code) AS country_code,
                                       bps_transactions_ALL_rollup_AI::bps_transactions_ALL_deduped::top_rec::iso_currency_id AS iso_currency_id,
                                       '-1' AS origin_of_sales,
                                       bps_transactions_ALL_rollup_AI::bps_transactions_ALL_deduped::top_rec::paid_dt AS paid_dt,
                                       TRIM(bps_payable_address::first_name) AS first_name,
                                       TRIM(bps_payable_address::last_name) AS last_name,
                                       TRIM(bps_payable_address::email_address) AS email_address,                                    
                                       COALESCE(bps_payable_address::phone_number,'') AS phone_number,
                                       TRIM(bps_payable_address::street_address) AS street,
                                       bps_transactions_ALL_rollup_AI::bps_transactions_ALL_deduped::top_rec::payment_ref_id AS payment_ref_id,
                                       '' AS ext_transaction_id,
                                       '' AS ext_transaction_init_dt;
                                                                                                                                                                                         

bps_transactions_get_iso_currency_id = JOIN bps_transactions_invert_sign BY (offer_id) LEFT OUTER,
                                            bps_brd_currency BY (offer_id);

bps_transactions_final = FOREACH bps_transactions_get_iso_currency_id GENERATE 
                                 (TRIM(bps_brd_currency::country_code) == 'GB' ? '3' : '1') AS bid,
                                 bps_transactions_invert_sign::sid AS sid,
                                 bps_transactions_invert_sign::guid AS guid,
                                 bps_transactions_invert_sign::obi_acct AS obi_acct,
                                 bps_transactions_invert_sign::ar_payment_method_id AS ar_payment_method_id,
                                 bps_transactions_invert_sign::pm_id AS pm_id,
                                 bps_transactions_invert_sign::pm_acct_num AS pm_acct_num,
                                 bps_transactions_invert_sign::routing_number AS routing_number,
                                 bps_transactions_invert_sign::cycle AS cycle,
                                 bps_transactions_invert_sign::billed_dt AS billed_dt,
                                 bps_transactions_invert_sign::trans_type AS trans_type,
                                 bps_transactions_invert_sign::retry_cnt AS retry_cnt,
                                 bps_transactions_invert_sign::result_code AS result_code,
                                 bps_transactions_invert_sign::reason_code AS reason_code,
                                 bps_transactions_invert_sign::lineitem AS lineitem,
                                 bps_transactions_invert_sign::charge_amt AS charge_amt,
                                 bps_transactions_invert_sign::bill_amt AS bill_amt,
                                 bps_transactions_invert_sign::paid_amt AS paid_amt,
                                 bps_transactions_invert_sign::trans_amt AS trans_amt,
                                 bps_transactions_invert_sign::tax_amt AS tax_amt,
                                 bps_transactions_invert_sign::trans_source_id AS trans_source_id,
                                 bps_transactions_invert_sign::ppv_id AS ppv_id,
                                 bps_transactions_invert_sign::nb_reason_code AS nb_reason_code,
                                 bps_transactions_invert_sign::nb_amt AS nb_amt,
                                 bps_transactions_invert_sign::processing_division AS processing_division,
                                 bps_transactions_invert_sign::trans_apply_dt AS trans_apply_dt,
                                 bps_transactions_invert_sign::adjust_amt AS adjust_amt,
                                 bps_transactions_invert_sign::balance_id AS balance_id,
                                 bps_transactions_invert_sign::bill_id AS bill_id,
                                 bps_transactions_invert_sign::resub_flg AS resub_flg,
                                 bps_transactions_invert_sign::write_off_source AS write_off_source,
                                 bps_transactions_invert_sign::write_off_reason_cd AS write_off_reason_cd,
                                 bps_transactions_invert_sign::city AS city,
                                 bps_transactions_invert_sign::state AS state,
                                 bps_transactions_invert_sign::zipcode AS zipcode,
                                 bps_transactions_invert_sign::country_code AS country_code,
                                 bps_brd_currency::iso_currency_id AS iso_currency_id,
                                 bps_transactions_invert_sign::origin_of_sales AS origin_of_sales,
                                 bps_transactions_invert_sign::paid_dt AS paid_dt,
                                 bps_transactions_invert_sign::first_name AS first_name,
                                 bps_transactions_invert_sign::last_name AS last_name,
                                 bps_transactions_invert_sign::email_address AS email_address,
                                 bps_transactions_invert_sign::phone_number AS phone_number,
                                 bps_transactions_invert_sign::street AS street,
                                 bps_transactions_invert_sign::payment_ref_id AS payment_ref_id,
                                 bps_transactions_invert_sign::ext_transaction_id AS ext_transaction_id,
                                 bps_transactions_invert_sign::ext_transaction_init_dt AS ext_transaction_init_dt;
                                            
--Store Datset
STORE bps_transactions_final INTO '$brrLineItemInterimDir/LI_02B_process_bps' USING PigStorage('$delimiter');                           