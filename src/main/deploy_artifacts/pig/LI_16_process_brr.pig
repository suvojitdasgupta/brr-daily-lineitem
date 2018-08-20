/*
            Script Name : LI_16_process_brr.pig
 jUnit Test Script Name : PigUnit16ProcessBrr.java
                Purpose : pig script for processing all OBI data and create load ready files for BRR.
             Dependency : LI_15_process_dvt_metrics.pig                         
*/

SET pig.exec.mapPartAgg '$pig_exec_mappart_agg_enabled';
SET pig.user.cache.enabled '$pig_user_cache_mode_enabled';
SET pig.auto.local.enabled '$pig_auto_local_mode_enabled';
SET pig.local.io.sort.mb '$pig_auto_local_io_sort_mb';
SET pig.auto.local.input.maxbytes '$pig_auto_local_input_maxbytes';
SET mapreduce.reduce.memory.mb 3240;
SET pig.exec.reducers.bytes.per.reducer 100000000;

DEFINE COALESCE datafu.pig.util.Coalesce();
DEFINE LENGTH org.apache.pig.piggybank.evaluation.string.LENGTH();
IMPORT '$brrLineItemPigDir/LI_01_ingest_dimensions.pig';

DEFINE enforce_schema_input(input_records) RETURNS output_records {
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
                                 (chararray)$27 AS line_item_balance_id,
                                 (chararray)$28 AS line_item_bill_id,
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
                                 
DEFINE enforce_schema_glid(input_records) RETURNS output_records {
       $output_records = FOREACH $input_records GENERATE 
                                 -- main
                                 (chararray)$0 AS rfmt_data_segment_dt,
                                 (chararray)$1 AS rfmt_data_unique_id,
                                 (chararray)$2 AS rfmt_data_bid,
                                 (chararray)$3 AS rfmt_data_sid,
                                 (chararray)$4 AS rfmt_data_global_acct_id,
                                 (chararray)$5 AS rfmt_data_payment_acct,
                                 (chararray)$6 AS rfmt_data_ar_payment_method_id,
                                 (chararray)$7 AS rfmt_data_payment_method_id,
                                 (chararray)$8 AS rfmt_data_cycle,
                                 (chararray)$9 AS rfmt_data_billed_dt,
                                 (chararray)$10 AS rfmt_data_trans_type,
                                 (chararray)$11 AS rfmt_data_retry_count,
                                 (chararray)$12 AS rfmt_data_result_code,
                                 (chararray)$13 AS rfmt_data_reason_code,
                                 (chararray)$14 AS rfmt_data_lineitem,
                                 (chararray)$15 AS rfmt_data_origin_of_sales,
                                 (chararray)$16 AS rfmt_data_pm_id,
                                 (double)$17 AS rfmt_data_charge_amount,
                                 (double)$18 AS rfmt_data_billed_amount,
                                 (double)$19 AS rfmt_data_trans_amount,
                                 (chararray)$20 AS rfmt_data_trans_source_id,
                                 (chararray)$21 AS rfmt_data_ppv_id,
                                 (chararray)$22 AS rfmt_data_nb_reason_code_id,
                                 (double)$23 AS rfmt_data_nb_amount,
                                 (chararray)$24 AS rfmt_data_processing_division,
                                 (chararray)$25 AS rfmt_data_transaction_applied_dt,
                                 (double)$26 AS rfmt_data_adjust_amount,
                                 (chararray)$27 AS rfmt_data_line_item_balance_id,
                                 (chararray)$28 AS rfmt_data_line_item_bill_id,
                                 (chararray)$29 AS rfmt_data_write_off_source,
                                 (chararray)$30 AS rfmt_data_write_off_reason_code,
                                 (chararray)$31 AS rfmt_data_resub_flag,
                                 (double)$32 AS rfmt_data_paid_amount,
                                 (double)$33 AS rfmt_data_tax_amount,
                                 (chararray)$34 AS rfmt_data_currency_id,
                                 (chararray)$35 AS rfmt_data_error_cd,
                                 (chararray)$36 AS rfmt_data_recycle_count,
                                 (chararray)$37 AS rfmt_data_city,
                                 (chararray)$38 AS rfmt_data_state,
                                 (chararray)$39 AS rfmt_data_zip,
                                 (chararray)$40 AS rfmt_data_country_code,
                                 (chararray)$41 AS rfmt_data_paid_dt,
                                 (chararray)$42 AS rfmt_data_first_name,
                                 (chararray)$43 AS rfmt_data_last_name,
                                 (chararray)$44 AS rfmt_data_email_address,
                                 (chararray)$45 AS rfmt_data_phone_number,
                                 (chararray)$46 AS rfmt_data_street,
                                 (chararray)$47 AS rfmt_data_payment_ref_id,
                                 (chararray)$48 AS rfmt_data_ext_transaction_id,
                                 (chararray)$49 AS rfmt_data_ext_transaction_init_dt,
                                 -- currency
                                 (chararray)$50 AS currency_currency_id,
                                 (chararray)$51 AS currency_currency_cd,
                                 (chararray)$52 AS currency_currency_desc,
                                 (chararray)$53 AS currency_status,
                                 -- line_item_map
                                 (chararray)$54 AS line_item_map_result_code,
                                 (chararray)$55 AS line_item_map_trans_type,
                                 (double)$56 AS line_item_map_billed_factor,
                                 (double)$57 AS line_item_map_paid_factor,
                                 (double)$58 AS line_item_map_charge_factor,
                                 (double)$59 AS line_item_map_adjust_factor,
                                 (chararray)$60 AS line_item_map_active,
                                 (chararray)$61 AS line_item_map_trans_status,
                                 (chararray)$62 AS line_item_map_effective_date,
                                 (chararray)$63 AS line_item_map_expiration_date,
                                 -- tranx_source
                                 (chararray)$64 AS tranx_source_trans_source_id,
                                 (chararray)$65 AS tranx_source_trans_source_desc,
                                 (chararray)$66 AS tranx_source_active,
                                 (chararray)$67 AS tranx_source_effective_date,
                                 (chararray)$68 AS tranx_source_expiration_date,
                                 -- trans_type
                                 (chararray)$69 AS trans_type_trans_type,
                                 (chararray)$70 AS trans_type_descr,
                                 (chararray)$71 AS trans_type_trans_type_token,
                                 (chararray)$72 AS trans_type_is_tax,
                                 (chararray)$73 AS trans_type_is_taxable,
                                 (chararray)$74 AS trans_type_forecast_method,
                                 (chararray)$75 AS trans_type_active,
                                 (chararray)$76 AS trans_type_effective_date,
                                 (chararray)$77 AS trans_type_expiration_date,
                                 (chararray)$78 AS trans_type_bill_flg,
                                 -- pd_to_ma_map
                                 (chararray)$79 AS pd_to_ma_map_reformatted_processing_division,
                                 (chararray)$80 AS pd_to_ma_map_reformatted_merchant_account,
                                 (chararray)$81 AS pd_to_ma_map_reformatted_description,
                                 (chararray)$82 AS pd_to_ma_map_reformatted_status,
                                 (chararray)$83 AS pd_to_ma_map_reformatted_effective_date,
                                 (chararray)$84 AS pd_to_ma_map_reformatted_expiration_date,
                                 (chararray)$85 AS pd_to_ma_map_reformatted_line_item_id,
                                 (chararray)$86 AS rfmt_data_pd_to_ma_map_field_selector,
                                 -- nb_reason_code
                                 (chararray)$87 AS nb_reason_code_nb_reason_code_id,
                                 (chararray)$88 AS nb_reason_code_descr,
                                 (chararray)$89 AS nb_reason_code_active,
                                 (chararray)$90 AS nb_reason_code_effective_date,
                                 (chararray)$91 AS nb_reason_code_expiration_date,
                                 (chararray)$92 AS rfmt_data_nb_reason_code_field_selector,
                                 -- payment_vendor
                                 (chararray)$93 AS payment_vendor_ppv_id,
                                 (chararray)$94 AS payment_vendor_ppv_code,
                                 (chararray)$95 AS payment_vendor_ppv_name,
                                 (chararray)$96 AS payment_vendor_ppv_dir_name,
                                 (chararray)$97 AS payment_vendor_ppv_id_name,
                                 (chararray)$98 AS payment_vendor_active,
                                 (chararray)$99 AS payment_vendor_effective_date,
                                 (chararray)$100 AS payment_vendor_expiration_date,
                                 (chararray)$101 AS rfmt_data_payment_vendor_field_selector,
                                 -- payment_method
                                 (chararray)$102 AS payment_method_payment_method_id,
                                 (chararray)$103 AS payment_method_short_name,
                                 (double)$104 AS payment_method_payment_group_id,
                                 (chararray)$105 AS payment_method_active,
                                 (chararray)$106 AS payment_method_effective_date,
                                 (chararray)$107 AS payment_method_expiration_date,
                                 (chararray)$108 AS payment_method_cash_basis_flg,
                                 (chararray)$109 AS rfmt_data_payment_method_field_selector,
                                 -- lineitem
                                 (chararray)$110 AS lineitem_sk,
                                 (chararray)$111 AS lineitem_lineitem,
                                 (chararray)$112 AS lineitem_bid,
                                 (chararray)$113 AS lineitem_sid,
                                 (chararray)$114 AS lineitem_title,
                                 (chararray)$115 AS lineitem_active,
                                 (chararray)$116 AS lineitem_effective_date,
                                 (chararray)$117 AS lineitem_expiration_date,
                                 (chararray)$118 AS lineitem_cash_basis_flg,
                                 (chararray)$119 AS lineitem_is_tax,
                                 (chararray)$120 AS lineitem_is_taxable,
                                 (chararray)$121 AS lineitem_cash_frcst_mthd,
                                 (chararray)$122 AS lineitem_vendor_id,
                                 (chararray)$123 AS lineitem_paid_service_flg,
                                 (chararray)$124 AS lineitem_paid_service_bundle_flg,
                                 (chararray)$125 AS rfmt_data_lineitem_field_selector,
                                 -- return_code
                                 (chararray)$126 AS return_code_sk,
                                 (chararray)$127 AS return_code_return_code,
                                 (chararray)$128 AS return_code_descr,
                                 (chararray)$129 AS return_code_active,
                                 (chararray)$130 AS return_code_effective_date,
                                 (chararray)$131 AS return_code_expiration_date,
                                 (chararray)$132 AS rfmt_data_return_code_field_selector,
                                 -- billing_config
                                 (chararray)$133 AS billing_config_billing_config_key,
                                 (chararray)$134 AS billing_config_bid,
                                 (chararray)$135 AS billing_config_sid,
                                 (chararray)$136 AS billing_config_lineitem,
                                 (chararray)$137 AS billing_config_subscription_ind,
                                 (chararray)$138 AS billing_config_effective_dt,
                                 (chararray)$139 AS billing_config_expiration_dt,
                                 (chararray)$140 AS billing_config_update_dt,
                                 (chararray)$141 AS billing_config_status,
                                 (chararray)$142 AS rfmt_data_billing_config_field_selector,
                                 -- glid
                                 (chararray)$143 AS glid;
                                 };

brr_input = LOAD '$brrLineItemOutputDir/LI_15_process_dvt_metrics_dvt/part*' using PigStorage('$delimiter') AS
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
					line_item_balance_id:chararray,
					line_item_bill_id:chararray,
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

-- $brrLineItemRecycleDir will point to PREVIOUS DAY's $brrLineItemOutputDir.
brr_recycle = LOAD '$brrLineItemRecycleDir/LI_18_process_brr_rejects_recycle/part*' using PigStorage('$delimiter') AS
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
					line_item_balance_id:chararray,
					line_item_bill_id:chararray,
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
	                
-- Modify the original lookup file to remove leading zeros from field if any
pd_to_ma_map_reformatted = FOREACH pd_to_ma_map GENERATE 
                                   REPLACE(processing_division,'^0+', '') AS processing_division,          --Removes leading zeros if any.
                                   merchant_account AS merchant_account,
                                   description AS description,
                                   status AS status,
                                   effective_date AS effective_date,
                                   expiration_date AS expiration_date,
                                   line_item_id AS  line_item_id;
                                   
line_item_glid_map_reformatted = FOREACH line_item_glid_map GENERATE 
						                 line_item_glid_key AS line_item_glid_key,
						                 pm_id AS pm_id,
						                 line_item_id AS line_item_id,
						                 transaction_type AS transaction_type,
						                 business_id AS business_id,
						                 service_id AS service_id,
						                 
						                 (
						                   ( TRIM(processing_division) == '0') ?
						                                                       TRIM(processing_division)
						                                                       :
                                                                               REPLACE(processing_division,'^0+', '') --Removes leading zeros if any.
                                         ) AS processing_division,   						                 
						                 
						                 base_glid AS base_glid,
						                 glid_offset AS glid_offset,
						                 glid AS glid,
						                 glid_desc AS glid_desc,
						                 trans_status AS trans_status,
						                 status AS status,
						                 effective_date AS effective_date,
						                 expiration_date AS expiration_date,
						                 gl_seg_desc AS gl_seg_desc;                                   

-- Modify the original lookup file to drop decimal points (.0) on sk field. 
lineitem_reformatted = FOREACH lineitem GENERATE 
                       (int)sk AS sk,
			           lineitem .. paid_service_bundle_flg;
			           
return_code_reformatted = FOREACH return_code GENERATE 				
                          (int)sk AS sk,
				          return_code .. expiration_date;		
				          
				          				          	          		                                             
brr_input_enforced_schema = enforce_schema_input(brr_input);
brr_recycle_enforced_schema = enforce_schema_input(brr_recycle); 
curr_recycle_data = UNION ONSCHEMA brr_input_enforced_schema, brr_recycle_enforced_schema;
            
rfmt_data = FOREACH curr_recycle_data GENERATE			
            '${segmentDate}' AS segment_dt,
            UniqueID()  AS unique_id,                         -- Generate Unique Id; Takes the form "index-sequence"
            bid AS bid,
            sid AS sid,
            TRIM(global_acct_id) AS global_acct_id,
            COALESCE(TRIM(obi_acct),'-1') AS payment_acct,
            ar_payment_method_id AS ar_payment_method_id,
            pm_id AS payment_method_id,
            cycle AS cycle,
            SUBSTRING(billed_dt,0,10) AS billed_dt,            -- Date Format - YYYY-MM-DD
            TRIM(trans_type) AS trans_type,
            retry_count AS retry_count,
            result_code AS result_code,
            reason_code AS reason_code,
            lineitem AS lineitem,
            origin_of_sales AS origin_of_sales,
            
            /* Modified as part of RRT Discrepancy Fix - Start */
            (
              (
                TRIM(routing_number) == '-1' 
                OR 
                TRIM(routing_number) IS NULL 
                OR
                TRIM(routing_number) == ''
              )   
              ? pm_acct_num 
              : routing_number
            ) AS pm_id,
            /* Modified as part of RRT Discrepancy Fix - End */
            
            charge_amount AS charge_amount,
            bill_amount AS billed_amount,
            trans_amount AS trans_amount,
            COALESCE(TRIM(trans_source_id),'OBI_AR') AS trans_source_id,
            ppv_id AS ppv_id,
            COALESCE(TRIM(nb_reason_code),'-1') AS nb_reason_code_id,
            nb_amount AS nb_amount,
            REPLACE(processing_division,'^0+', '') AS processing_division,          --Removes leading zeros if any.
            SUBSTRING(trans_apply_dt,0,10) AS transaction_applied_dt,               -- Date Format - YYYY-MM-DD
            adjust_amount AS adjust_amount,
            line_item_balance_id AS line_item_balance_id,
            line_item_bill_id AS line_item_bill_id,
            COALESCE(TRIM(write_off_source),'-1') AS write_off_source,
            COALESCE(TRIM(write_off_reason_cd),'-1') AS write_off_reason_code,
            resub_flg AS resub_flag,
            paid_amount AS paid_amount,
            tax_amount AS tax_amount,
            iso_currency_id AS currency_id,
            '0' AS error_cd,
            '0' AS recycle_count,
            TRIM(city) AS city,
            TRIM(state) AS state,
            TRIM(zipcode) AS zip, 
            COALESCE(TRIM(country_code),'-1') AS country_code,
            SUBSTRING(paid_dt,0,10) AS paid_dt,                                     -- Date Format - YYYY-MM-DD
            TRIM(REPLACE(first_name,'\n','')) AS first_name,
            TRIM(REPLACE(last_name,'\n','')) AS last_name,
            email_address AS email_address,
            phone_number AS phone_number,
            
            --TRIM(REPLACE(street,'\n','')) AS street,
            -- Removes 1 trailing comma.
            (
              ENDSWITH (street,',') ? TRIM(REPLACE(SUBSTRING(street,0,LENGTH(street)-1),'\n','')) : TRIM(REPLACE(street,'\n',''))
            ) AS street,
            
            payment_ref_id AS payment_ref_id,
            ext_transaction_id AS ext_transaction_id,
            ext_transaction_init_dt AS ext_transaction_init_dt                      -- Date Format - YYYY-MM-DD HH24:MI:SS
			;
			
-------------------------------------					
-- Component : 250_RFMT_Data - Start
-------------------------------------						
rfmt_data_currency = JOIN rfmt_data BY (currency_id), currency BY (currency_id);
rfmt_data_line_item_map = JOIN rfmt_data_currency BY (result_code,trans_type), line_item_map BY (result_code,trans_type);
rfmt_data_tranx_source = JOIN rfmt_data_line_item_map BY (trans_source_id), tranx_source BY (trans_source_desc);
rfmt_data_trans_type = JOIN rfmt_data_tranx_source BY (rfmt_data::trans_type) , trans_type BY (trans_type); 

rfmt_data_pd_to_ma_map_1 = JOIN rfmt_data_trans_type BY (processing_division), pd_to_ma_map_reformatted BY (processing_division);
rfmt_data_pd_to_ma_map_2 = FOREACH rfmt_data_pd_to_ma_map_1 GENERATE 
	                         segment_dt..,
	                         (
							   ( 
							     DaysBetween(ToDate(rfmt_data::transaction_applied_dt, 'yyyy-MM-dd'), ToDate(pd_to_ma_map_reformatted::effective_date, 'yyyy-MM-dd')) >= (long)'0'
							     and DaysBetween(ToDate(rfmt_data::transaction_applied_dt, 'yyyy-MM-dd'), ToDate(pd_to_ma_map_reformatted::expiration_date, 'yyyy-MM-dd')) < (long)'0'
							   ) ? 
							         (chararray)'pd_to_ma_map_Y' : (chararray)'pd_to_ma_map_N'
							 ) AS field_selector;
rfmt_data_pd_to_ma_map = FILTER rfmt_data_pd_to_ma_map_2 BY field_selector == 'pd_to_ma_map_Y';	

rfmt_data_nb_reason_code_1 = JOIN rfmt_data_pd_to_ma_map BY (nb_reason_code_id), nb_reason_code BY (nb_reason_code_id);
rfmt_data_nb_reason_code_2 = FOREACH rfmt_data_nb_reason_code_1 GENERATE 
	                         segment_dt..,
	                         (
							   (
							      DaysBetween(ToDate(rfmt_data::transaction_applied_dt, 'yyyy-MM-dd'), ToDate(nb_reason_code::effective_date, 'yyyy-MM-dd')) >= (long)'0'
							       and DaysBetween(ToDate(rfmt_data::transaction_applied_dt, 'yyyy-MM-dd'), ToDate(nb_reason_code::expiration_date, 'yyyy-MM-dd')) < (long)'0'
							   ) ? 
							         (chararray)'nb_reason_code_Y' : (chararray)'nb_reason_code_N'
							 ) AS field_selector;

rfmt_data_nb_reason_code = FILTER rfmt_data_nb_reason_code_2 BY field_selector == 'nb_reason_code_Y';

rfmt_data_payment_vendor_1 = JOIN rfmt_data_nb_reason_code BY (ppv_id), payment_vendor BY (ppv_id);
rfmt_data_payment_vendor_2 = FOREACH rfmt_data_payment_vendor_1 GENERATE 
	                         segment_dt..,
	                         (
							   (
							     DaysBetween(ToDate(rfmt_data::transaction_applied_dt, 'yyyy-MM-dd'), ToDate(payment_vendor::effective_date, 'yyyy-MM-dd')) >= (long)'0'
							      and DaysBetween(ToDate(rfmt_data::transaction_applied_dt, 'yyyy-MM-dd'), ToDate(payment_vendor::expiration_date, 'yyyy-MM-dd')) < (long)'0'
							   ) ? 
							         (chararray)'payment_vendor_Y' : (chararray)'payment_vendor_N'
							 ) AS field_selector;

rfmt_data_payment_vendor = FILTER rfmt_data_payment_vendor_2 BY field_selector == 'payment_vendor_Y';

rfmt_data_payment_method_1 = JOIN rfmt_data_payment_vendor BY (payment_method_id), payment_method BY (payment_method_id);
rfmt_data_payment_method_2 = FOREACH rfmt_data_payment_method_1 GENERATE 
	                         segment_dt..,
	                         (
							   (
							    DaysBetween(ToDate(rfmt_data::transaction_applied_dt, 'yyyy-MM-dd'), ToDate(payment_method::effective_date, 'yyyy-MM-dd')) >= (long)'0'
							      and DaysBetween(ToDate(rfmt_data::transaction_applied_dt, 'yyyy-MM-dd'), ToDate(payment_method::expiration_date, 'yyyy-MM-dd')) < (long)'0'
							   ) ? 
							         (chararray)'payment_method_Y' : (chararray)'payment_method_N'
							 ) AS field_selector;

rfmt_data_payment_method = FILTER rfmt_data_payment_method_2 BY field_selector == 'payment_method_Y';

rfmt_data_lineitem_1 = JOIN rfmt_data_payment_method BY (lineitem,bid,sid), lineitem_reformatted BY (lineitem,bid,sid);
rfmt_data_lineitem_2 = FOREACH rfmt_data_lineitem_1 GENERATE 
	                         segment_dt..,
	                         (
							   (
							    DaysBetween(ToDate(rfmt_data::transaction_applied_dt, 'yyyy-MM-dd'), ToDate(lineitem_reformatted::effective_date, 'yyyy-MM-dd')) >= (long)'0'
							      and DaysBetween(ToDate(rfmt_data::transaction_applied_dt, 'yyyy-MM-dd'), ToDate(lineitem_reformatted::expiration_date, 'yyyy-MM-dd')) < (long)'0'
							   ) ? 
							         (chararray)'lineitem_Y' : (chararray)'lineitem_N'
							 ) AS field_selector;

rfmt_data_lineitem = FILTER rfmt_data_lineitem_2 BY field_selector == 'lineitem_Y';

rfmt_data_return_code_1 = JOIN rfmt_data_lineitem BY (rfmt_data::result_code), return_code_reformatted BY (return_code);
rfmt_data_return_code_2 = FOREACH rfmt_data_return_code_1 GENERATE 
	                         segment_dt..,
	                         (
							   (
							    DaysBetween(ToDate(rfmt_data::transaction_applied_dt, 'yyyy-MM-dd'), ToDate(return_code_reformatted::effective_date, 'yyyy-MM-dd')) >= (long)'0'
							      and DaysBetween(ToDate(rfmt_data::transaction_applied_dt, 'yyyy-MM-dd'), ToDate(return_code_reformatted::expiration_date, 'yyyy-MM-dd')) < (long)'0'
							   ) ? 
							         (chararray)'return_code_Y' : (chararray)'return_code_N'
							 ) AS field_selector;

rfmt_data_return_code = FILTER rfmt_data_return_code_2 BY field_selector == 'return_code_Y';

rfmt_data_billing_config_1 = JOIN rfmt_data_return_code BY (rfmt_data::lineitem, rfmt_data::bid, rfmt_data::sid), billing_config BY (lineitem,bid,sid);
rfmt_data_billing_config_2 = FOREACH rfmt_data_billing_config_1 GENERATE 
	                         segment_dt..,
	                         (
							   (
							    DaysBetween(ToDate(rfmt_data::transaction_applied_dt, 'yyyy-MM-dd'), ToDate(billing_config::effective_date, 'yyyy-MM-dd')) >= (long)'0'
							      and DaysBetween(ToDate(rfmt_data::transaction_applied_dt, 'yyyy-MM-dd'), ToDate(billing_config::expiration_date, 'yyyy-MM-dd')) < (long)'0'
							   ) ? 
							         (chararray)'billing_config_Y' : (chararray)'billing_config_N'
							 ) AS field_selector;

rfmt_data_billing_config = FILTER rfmt_data_billing_config_2 BY field_selector == 'billing_config_Y';

-- Implementation to capture GLID - Starts 
-- Pass 1
glid_l1_1 = JOIN rfmt_data_billing_config BY (
                                               rfmt_data::payment_method_id,
                                               rfmt_data::lineitem,
                                               rfmt_data::trans_type,
                                               rfmt_data::processing_division,
                                               rfmt_data::bid,
                                               rfmt_data::sid,
                                               '1'
                                              ), 
            line_item_glid_map_reformatted BY (
                                                pm_id, 
                                                line_item_id, 
                                                transaction_type, 
                                                processing_division, 
                                                business_id, 
                                                service_id, 
                                                trans_status
                                               );

glid_l1_2 = FOREACH glid_l1_1 GENERATE 
            segment_dt..,
            (
			  (
				DaysBetween(ToDate(rfmt_data::transaction_applied_dt, 'yyyy-MM-dd'), ToDate(line_item_glid_map_reformatted::effective_date, 'yyyy-MM-dd')) >= (long)'0'
							      and DaysBetween(ToDate(rfmt_data::transaction_applied_dt, 'yyyy-MM-dd'), ToDate(line_item_glid_map_reformatted::expiration_date, 'yyyy-MM-dd')) < (long)'0'
			  ) ? 
				  (chararray)'glid_l1_Y' : (chararray)'glid_l1_N'
			) AS field_selector;
            
glid_l1_Y = FILTER glid_l1_2 BY field_selector == 'glid_l1_Y'; 
glid_l1_Y_propagate = FOREACH glid_l1_Y GENERATE 
                      rfmt_data::segment_dt .. rfmt_data_billing_config::field_selector,
                      line_item_glid_map_reformatted::glid AS glid; 
glid_l1_Y_propagate_schemaEnforced = enforce_schema_glid(glid_l1_Y_propagate);    

glid_l1_fallout_join = JOIN rfmt_data_billing_config BY (rfmt_data::unique_id) LEFT OUTER, glid_l1_Y_propagate_schemaEnforced BY (rfmt_data_unique_id);
glid_l1_fallout = FILTER glid_l1_fallout_join BY glid_l1_Y_propagate_schemaEnforced::rfmt_data_unique_id IS NULL;
    
--Pass 2            
glid_l2_1 = JOIN glid_l1_fallout BY (
                                      rfmt_data::payment_method_id,
                                      rfmt_data::lineitem,
                                      '-1',
                                      rfmt_data::processing_division,
                                      rfmt_data::bid,
                                      rfmt_data::sid,
                                      '1'
                                     ), 
   line_item_glid_map_reformatted BY (
                                      pm_id, 
                                      line_item_id, 
                                      transaction_type, 
                                      processing_division, 
                                      business_id, 
                                      service_id, 
                                      trans_status
                                    );

glid_l2_2 = FOREACH glid_l2_1 GENERATE 
            segment_dt..,
            (
			  (
				DaysBetween(ToDate(rfmt_data::transaction_applied_dt, 'yyyy-MM-dd'), ToDate(line_item_glid_map_reformatted::effective_date, 'yyyy-MM-dd')) >= (long)'0'
							      and DaysBetween(ToDate(rfmt_data::transaction_applied_dt, 'yyyy-MM-dd'), ToDate(line_item_glid_map_reformatted::expiration_date, 'yyyy-MM-dd')) < (long)'0'
			  ) ? 
				  (chararray)'glid_l2_Y' : (chararray)'glid_l2_N'
			) AS field_selector;
			            
glid_l2_Y = FILTER glid_l2_2 BY field_selector == 'glid_l2_Y';  
glid_l2_Y_propagate = FOREACH glid_l2_Y GENERATE 
                      rfmt_data::segment_dt .. rfmt_data_billing_config::field_selector,
                      line_item_glid_map_reformatted::glid AS glid;  
glid_l2_Y_propagate_schemaEnforced = enforce_schema_glid(glid_l2_Y_propagate);                        

glid_l2_fallout_join = JOIN glid_l1_fallout BY (rfmt_data::unique_id) LEFT OUTER, glid_l2_Y_propagate_schemaEnforced BY (rfmt_data_unique_id);
glid_l2_fallout = FILTER glid_l2_fallout_join BY glid_l2_Y_propagate_schemaEnforced::rfmt_data_unique_id IS NULL;

--Pass 3           
glid_l3_1 = JOIN glid_l2_fallout BY (
                                      '0',
                                      '0',
                                      '0',
                                      '0',
                                      '0',
                                      '0',
                                      '0'
                                    ), 
  line_item_glid_map_reformatted BY (
                                      pm_id, 
                                      line_item_id, 
                                      transaction_type, 
                                      processing_division, 
                                      business_id, 
                                      service_id, 
                                      trans_status
                                     );

glid_l3_2 = FOREACH glid_l3_1 GENERATE 
            segment_dt..,
            (
			  (
				DaysBetween(ToDate(rfmt_data::transaction_applied_dt, 'yyyy-MM-dd'), ToDate(line_item_glid_map_reformatted::effective_date, 'yyyy-MM-dd')) >= (long)'0'
							      and DaysBetween(ToDate(rfmt_data::transaction_applied_dt, 'yyyy-MM-dd'), ToDate(line_item_glid_map_reformatted::expiration_date, 'yyyy-MM-dd')) < (long)'0'
			  ) ? 
				  (chararray)'glid_l3_Y' : (chararray)'glid_l3_N'
			) AS field_selector;
            
glid_l3_Y = FILTER glid_l3_2 BY field_selector == 'glid_l3_Y';  
glid_l3_Y_propagate = FOREACH glid_l3_Y GENERATE 
                      rfmt_data::segment_dt .. rfmt_data_billing_config::field_selector,
                      line_item_glid_map_reformatted::glid AS glid;
glid_l3_Y_propagate_schemaEnforced = enforce_schema_glid(glid_l3_Y_propagate);                         
          
rfmt_data_ALL = UNION ONSCHEMA 
                      glid_l1_Y_propagate_schemaEnforced,
                      glid_l2_Y_propagate_schemaEnforced,
                      glid_l3_Y_propagate_schemaEnforced;
-- Implementation to capture GLID - Ends

rfmt_data_OUT = FOREACH rfmt_data_ALL GENERATE
				 rfmt_data_segment_dt AS segment_dt,
				 rfmt_data_unique_id AS unique_id,
				 rfmt_data_global_acct_id AS global_acct_id,
				 rfmt_data_payment_acct AS payment_acct,
				 rfmt_data_ar_payment_method_id AS ar_payment_method_id,
				 payment_method_payment_method_id AS payment_method_id, 
				 (
				 (TRIM(billing_config_subscription_ind) == '1') ? (long) ToString(SubtractDuration(ToDate((chararray)SUBSTRING(rfmt_data_billed_dt,0,10),'yyyy-MM-dd'),'P1D'),'yyyyMM') * 100 
                                                                       + (long) rfmt_data_cycle
                                                                  : (long)ToString(ToDate((chararray)SUBSTRING(rfmt_data_billed_dt,0,10),'yyyy-MM-dd'),'yyyyMMdd') 
				 
				 
				 ) AS billkey,
				 	 
				 rfmt_data_billed_dt AS billed_dt,
				 trans_type_trans_type AS transaction_type_code,
				 rfmt_data_retry_count AS retry_count,
				 return_code_sk AS return_code_key,
				 rfmt_data_reason_code AS reason_code_id,
				 lineitem_sk AS line_item_key,
				 rfmt_data_origin_of_sales AS origin_of_sales,
				 rfmt_data_pm_id AS pm_id,
				 (line_item_map_charge_factor * (rfmt_data_charge_amount/100)) AS charge_amount,
				 (line_item_map_billed_factor * (rfmt_data_billed_amount/100)) AS billed_amount,
				 rfmt_data_trans_amount/100 AS trans_amount,
				 tranx_source_trans_source_id AS trans_source_id,
				 payment_vendor_ppv_id AS ppv_id,
				 nb_reason_code_nb_reason_code_id AS nb_reason_code_id,
				 rfmt_data_nb_amount/100 AS nb_amount,
				 rfmt_data_processing_division AS processing_division,
				 rfmt_data_transaction_applied_dt AS transaction_applied_dt,
				 (line_item_map_adjust_factor * (rfmt_data_adjust_amount/100)) AS adjust_amount,
				 rfmt_data_line_item_balance_id AS line_item_balance_id,
				 rfmt_data_line_item_bill_id AS line_item_bill_id,
				 rfmt_data_write_off_source AS write_off_source,
				 -- Instead of implementing a lookup_match to find if there is a match(1) or no match(0), I used if-else comparison.
				 -- Below is the original AbInitio code. out.WRITE_OFF_REASON_CODE_KEY gets one of the values -> 1,0,-1.
				 -- out.WRITE_OFF_REASON_CODE_KEY :: if(!(is_null(in.WRITE_OFF_REASON_CODE))||!(is_blank(in.WRITE_OFF_REASON_CODE))) lookup_match("writeoff_reason_code", in.WRITE_OFF_REASON_CODE)else -1; 
				 (
				   (rfmt_data_write_off_reason_code =='951' OR  rfmt_data_write_off_reason_code =='954') ? '1' : '0' 
				 ) AS write_off_reason_code_key,
				 
				 rfmt_data_resub_flag AS resub_flag,
				 pd_to_ma_map_reformatted_merchant_account AS merchant_account,
				 glid AS glid, -- GLID
				                 
				 (
				   ( (TRIM(rfmt_data_lineitem) =='1' OR TRIM(rfmt_data_lineitem) == '6') AND (TRIM(rfmt_data_sid) == '2' AND TRIM(rfmt_data_bid) == '1')
				   ) ? '2001' :
                       ( (TRIM(rfmt_data_sid) == '2' AND TRIM(rfmt_data_bid) == '1') ? '492' : 
                                                                  (chararray)(  ( (int)rfmt_data_sid*1000 ) + ( (int)rfmt_data_bid) )
                       
                       )
                 ) AS gl_segment,                
                           
				 ((rfmt_data_paid_dt IS NULL) ? rfmt_data_transaction_applied_dt : rfmt_data_paid_dt) AS paid_dt,
				 (line_item_map_paid_factor * (rfmt_data_trans_amount/100)) AS paid_amount,
				 (rfmt_data_tax_amount/100) AS tax_amount,
				 currency_currency_id AS currency_id,
				 rfmt_data_recycle_count AS recycle_count,
				 rfmt_data_city AS city,
				 rfmt_data_state AS state,
				 rfmt_data_zip AS zip,
				 rfmt_data_country_code AS country_code,
				 rfmt_data_first_name AS first_name,
				 rfmt_data_last_name AS last_name,
				 rfmt_data_email_address AS email_address,
				 rfmt_data_phone_number AS phone_number,
				 rfmt_data_street AS street,
				 rfmt_data_payment_ref_id AS payment_ref_id,
				 rfmt_data_ext_transaction_id AS ext_transaction_id,
				 rfmt_data_ext_transaction_init_dt AS ext_transaction_init_dt
				;	
-------------------------------------
-- Component : 250_RFMT_Data - End
-------------------------------------
-- The below datasets are used for identifying the lookup rejects. 
STORE rfmt_data INTO '$brrLineItemInterimDir/LI_16_process_brr_rfmt_in' USING PigStorage('$delimiter');
STORE rfmt_data_OUT INTO '$brrLineItemInterimDir/LI_16_process_brr_rfmt_out' USING PigStorage('$delimiter');

brr_data_group =  GROUP rfmt_data_OUT BY (segment_dt, global_acct_id, payment_acct, transaction_type_code, return_code_key, reason_code_id, line_item_balance_id, line_item_bill_id, gl_segment);  
brr_data_sum = FOREACH brr_data_group GENERATE
                      FLATTEN(group),
					  SUM(rfmt_data_OUT.charge_amount) AS charge_amount,
					  SUM(rfmt_data_OUT.billed_amount) AS billed_amount,
					  SUM(rfmt_data_OUT.trans_amount) AS trans_amount,
					  SUM(rfmt_data_OUT.nb_amount) AS nb_amount,
					  SUM(rfmt_data_OUT.adjust_amount) AS adjust_amount,
					  SUM(rfmt_data_OUT.paid_amount) AS paid_amount,
					  SUM(rfmt_data_OUT.tax_amount) AS tax_amount;
					  				  
brr_data_dedup = FOREACH brr_data_group {
                         sorted = ORDER rfmt_data_OUT BY segment_dt DESC,billed_dt DESC, transaction_applied_dt DESC, paid_dt DESC;
                         top_rec = LIMIT sorted 1;
                         GENERATE FLATTEN(top_rec);
                 };					   

brr_data_rollup = JOIN brr_data_dedup BY (top_rec::segment_dt,
                                          top_rec::global_acct_id,
                                          top_rec::payment_acct,
                                          top_rec::transaction_type_code,
                                          top_rec::return_code_key,
                                          top_rec::reason_code_id,
                                          top_rec::line_item_balance_id,
                                          top_rec::line_item_bill_id,
                                          top_rec::gl_segment) , 
                        brr_data_sum BY (group::segment_dt,
                                         group::global_acct_id,
                                         group::payment_acct,
                                         group::transaction_type_code,
                                         group::return_code_key,
                                         group::reason_code_id,
                                         group::line_item_balance_id,
                                         group::line_item_bill_id,
                                         group::gl_segment);

brr_data_rollup_xform = FOREACH brr_data_rollup GENERATE 
                                                brr_data_dedup::top_rec::segment_dt AS segment_dt,
                                                brr_data_dedup::top_rec::global_acct_id AS global_acct_id,
                                                brr_data_dedup::top_rec::payment_acct AS payment_acct, 
                                                brr_data_dedup::top_rec::ar_payment_method_id AS ar_payment_method_id,
                                                brr_data_dedup::top_rec::payment_method_id AS payment_method_id, 
                                                brr_data_dedup::top_rec::billkey AS billkey,
                                                brr_data_dedup::top_rec::billed_dt AS billed_dt,
                                                brr_data_dedup::top_rec::transaction_type_code AS transaction_type_code,
                                                brr_data_dedup::top_rec::retry_count AS retry_count,
                                                brr_data_dedup::top_rec::return_code_key AS return_code_key,
                                                brr_data_dedup::top_rec::reason_code_id AS reason_code_id,
                                                brr_data_dedup::top_rec::line_item_key AS line_item_key,
                                                brr_data_dedup::top_rec::origin_of_sales AS origin_of_sales,
                                                brr_data_dedup::top_rec::pm_id AS pm_id,
                                                ROUND_TO(brr_data_sum::charge_amount,2) AS charge_amount,
                                                ROUND_TO(brr_data_sum::billed_amount,2) AS billed_amount,
                                                ROUND_TO(brr_data_sum::trans_amount,2) AS trans_amount,
                                                brr_data_dedup::top_rec::trans_source_id AS trans_source_id, 
                                                brr_data_dedup::top_rec::ppv_id AS ppv_id,
                                                brr_data_dedup::top_rec::nb_reason_code_id AS nb_reason_code_id,
                                                ROUND_TO(brr_data_sum::nb_amount,2) AS nb_amount,
                                                brr_data_dedup::top_rec::processing_division AS processing_division,
                                                brr_data_dedup::top_rec::transaction_applied_dt AS transaction_applied_dt,
                                                ROUND_TO(brr_data_sum::adjust_amount,2) AS adjust_amount,
                                                brr_data_dedup::top_rec::line_item_balance_id AS line_item_balance_id,
                                                brr_data_dedup::top_rec::line_item_bill_id AS line_item_bill_id,
                                                brr_data_dedup::top_rec::write_off_source AS write_off_source,
                                                COALESCE(TRIM(brr_data_dedup::top_rec::write_off_reason_code_key),'0') AS write_off_reason_code_key,
                                                brr_data_dedup::top_rec::resub_flag AS resub_flag,
                                                brr_data_dedup::top_rec::merchant_account AS merchant_account,
                                                brr_data_dedup::top_rec::glid AS glid,
                                                brr_data_dedup::top_rec::gl_segment AS gl_segment,
                                                brr_data_dedup::top_rec::paid_dt AS paid_dt,
                                                ROUND_TO(brr_data_sum::paid_amount,2) AS paid_amount,
                                                ROUND_TO(brr_data_sum::tax_amount,2) AS tax_amount,
                                                brr_data_dedup::top_rec::currency_id AS currency_id,
                                                brr_data_dedup::top_rec::recycle_count AS recycle_count,
                                                brr_data_dedup::top_rec::city AS city,
                                                brr_data_dedup::top_rec::state AS state,
                                                brr_data_dedup::top_rec::zip AS zip,
                                                brr_data_dedup::top_rec::country_code AS country_code,
                                                brr_data_dedup::top_rec::first_name AS first_name,
                                                brr_data_dedup::top_rec::last_name AS last_name,
                                                brr_data_dedup::top_rec::email_address AS email_address,
                                                brr_data_dedup::top_rec::phone_number AS phone_number,
                                                brr_data_dedup::top_rec::street AS street,
                                                brr_data_dedup::top_rec::payment_ref_id AS payment_ref_id,
                                                brr_data_dedup::top_rec::ext_transaction_id AS ext_transaction_id,
                                                brr_data_dedup::top_rec::ext_transaction_init_dt AS ext_transaction_init_dt,
                                                '' AS original_bill_id;

SPLIT brr_data_rollup_xform INTO 
 exp_output_records_with_recycle_count IF ((long)recycle_count == (long)0),
 merge_output_records_with_recycle_count IF ((long)recycle_count != (long)0);
 
-- Dropping the recycle_count field from the datasets.  
exp_output_records = FOREACH exp_output_records_with_recycle_count GENERATE 
                             segment_dt .. currency_id, 
                             city .. street,
                             REPLACE(REPLACE(payment_ref_id,'#',','),'null','') AS payment_ref_id,                            
                             ext_transaction_id .. original_bill_id;

merge_output_records = FOREACH merge_output_records_with_recycle_count GENERATE 
                               segment_dt .. currency_id, 
                               city .. street,
                               REPLACE(REPLACE(payment_ref_id,'#',','),'null','') AS payment_ref_id,                            
                               ext_transaction_id .. original_bill_id;

STORE exp_output_records INTO '$brrLineItemOutputDir/LI_16_process_brr_exp' USING PigStorage('$delimiter');
STORE merge_output_records INTO '$brrLineItemOutputDir/LI_16_process_brr_merge' USING PigStorage('$delimiter');                                                  