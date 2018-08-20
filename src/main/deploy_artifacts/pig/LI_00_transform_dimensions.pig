/*
            Script Name : LI_00_transform_dimensions.pig
 jUnit Test Script Name : PigUnit00TransformDimensions.java
                Purpose : pig script for reading all dimension data needed for lineitem daily processing
                          and reformatting to uniform formal for downstream ingestion..
             Dependency : External dependency.                          
*/

SET pig.exec.mapPartAgg '$pig_exec_mappart_agg_enabled';
SET pig.user.cache.enabled '$pig_user_cache_mode_enabled';
SET pig.auto.local.enabled '$pig_auto_local_mode_enabled';
SET pig.local.io.sort.mb '$pig_auto_local_io_sort_mb';
SET pig.auto.local.input.maxbytes '$pig_auto_local_input_maxbytes';

billing_config = LOAD '$brrLineItemExternalDir/obi_billing_config.nss' using PigStorage('$delimiter') AS
					(
					billing_config_key:chararray,
					bid:chararray,
					sid:chararray,
					lineitem:chararray,
					subscription_ind:chararray,
					effective_dt:chararray, --YYYY-MM-DD
					expiration_dt:chararray, --YYYY-MM-DD
					update_dt:chararray,
					status:chararray
					);
                        					
line_item_glid_map_raw = LOAD '$brrLineItemExternalDir/line_item_glid_map.nss' using PigStorage('$delimiter') AS
						(
						line_item_glid_key:chararray,
						pm_id:chararray,
						line_item_id:chararray,
						transaction_type:chararray,
						business_id:chararray,
						service_id:chararray,
						processing_division:chararray,
						base_glid:chararray,
						glid_offset:chararray,
						glid:chararray,
						glid_desc:chararray,
						trans_status:chararray,
						status:chararray,
						effective_date:chararray,
						expiration_date:chararray,
						gl_seg_desc:chararray
						);

line_item_glid_map = FOREACH line_item_glid_map_raw GENERATE 
						     line_item_glid_key AS line_item_glid_key,
						     pm_id AS pm_id,
						     line_item_id AS line_item_id,
						     transaction_type AS transaction_type,
						     business_id AS business_id,
						     service_id AS service_id,
						     processing_division AS processing_division,
						     base_glid AS base_glid,
						     glid_offset AS glid_offset,
						     glid AS glid,
						     glid_desc AS glid_desc,
						     trans_status AS trans_status,
						     status AS status,
						     (chararray)ToString(ToDate(effective_date,'yyyyMMdd'),'yyyy-MM-dd') AS effective_date,
						     (chararray)ToString(ToDate(expiration_date,'yyyyMMdd'),'yyyy-MM-dd') AS expiration_date,
						     gl_seg_desc AS gl_seg_desc;
        		
trans_type_raw = LOAD '$brrLineItemExternalDir/transtype.nss' using PigStorage('$delimiter') AS
				(
				trans_type:chararray,
				descr:chararray,
				trans_type_token:chararray,
				is_tax:chararray,
				is_taxable:chararray,
				forecast_method:chararray,
				active:chararray,
				effective_date:chararray,
				expiration_date:chararray,
				bill_flg:chararray
				);

trans_type = FOREACH trans_type_raw GENERATE 
				     trans_type AS trans_type,
				     descr AS descr,
				     trans_type_token AS trans_type_token,
				     is_tax AS is_tax,
				     is_taxable AS is_taxable,
				     forecast_method AS forecast_method,
				     active AS active,
				     (chararray)ToString(ToDate(effective_date,'yyyyMMdd'),'yyyy-MM-dd') AS effective_date,
				     (chararray)ToString(ToDate(expiration_date,'yyyyMMdd'),'yyyy-MM-dd') AS expiration_date,
				     bill_flg AS bill_flg;
				
return_code_raw = LOAD '$brrLineItemExternalDir/returncode.nss' using PigStorage('$delimiter') AS
				(
				sk:double,
				return_code:chararray,
				descr:chararray,
				active:chararray,
				effective_date:chararray,
				expiration_date:chararray
				);

return_code = FOREACH return_code_raw GENERATE 
				      sk AS sk,
				      return_code AS return_code,
				      descr AS descr,
				      active AS active,
					  (chararray)ToString(ToDate(effective_date,'yyyyMMdd'),'yyyy-MM-dd') AS effective_date,
					  (chararray)ToString(ToDate(expiration_date,'yyyyMMdd'),'yyyy-MM-dd') AS expiration_date;
				
lineitem_raw = LOAD '$brrLineItemExternalDir/lineitem.nss' using PigStorage('$delimiter') AS
			(
			sk:double,
			lineitem:chararray,
			bid:chararray,
			sid:chararray,
			title:chararray,
			active:chararray,
			effective_date:chararray,
			expiration_date:chararray,
			cash_basis_flg:chararray,
			is_tax:chararray,
			is_taxable:chararray,
			cash_frcst_mthd:chararray,
			vendor_id:double,
			paid_service_flg:chararray,
			paid_service_bundle_flg:chararray,
			cash_flow_type:chararray
			);
			
lineitem = FOREACH lineitem_raw GENERATE
			       sk AS sk,
			       lineitem AS lineitem,
			       bid AS bid,
			       sid AS sid,
			       title AS title,
			       active AS active,
				   (chararray)ToString(ToDate(effective_date,'yyyyMMdd'),'yyyy-MM-dd') AS effective_date,
				   (chararray)ToString(ToDate(expiration_date,'yyyyMMdd'),'yyyy-MM-dd') AS expiration_date,
			       cash_basis_flg AS cash_basis_flg,
			       is_tax AS is_tax,
			       is_taxable AS is_taxable,
			       cash_frcst_mthd AS cash_frcst_mthd,
			       vendor_id AS vendor_id,
			       paid_service_flg AS paid_service_flg,
			       paid_service_bundle_flg AS paid_service_bundle_flg; 		
			
line_item_map_raw = LOAD '$brrLineItemExternalDir/line_item_map.nss' using PigStorage('$delimiter') AS
				(
				result_code:chararray,
				trans_type:chararray,
				billed_factor:double,
				paid_factor:double,
				charge_factor:double,
				adjust_factor:double,
				active:chararray,
				trans_status:chararray,
				effective_date:chararray,
				expiration_date:chararray
				);

line_item_map = FOREACH line_item_map_raw GENERATE
				        result_code AS result_code,
				        trans_type AS trans_type,
				        billed_factor AS billed_factor,
				        paid_factor AS paid_factor,
				        charge_factor AS charge_factor,
				        adjust_factor AS adjust_factor,
				        active AS active,
				        trans_status AS trans_status,
					    (chararray)ToString(ToDate(effective_date,'yyyyMMdd'),'yyyy-MM-dd') AS effective_date,
					    (chararray)ToString(ToDate(expiration_date,'yyyyMMdd'),'yyyy-MM-dd') AS expiration_date;
						
tranx_source_raw = LOAD '$brrLineItemExternalDir/tranxsource.nss' using PigStorage('$delimiter') AS
				(
				trans_source_id:chararray,
				trans_source_desc:chararray,
				active:chararray,
				effective_date:chararray,
				expiration_date:chararray
				);

tranx_source = FOREACH tranx_source_raw GENERATE
				       trans_source_id AS trans_source_id,
				       trans_source_desc AS trans_source_desc,
				       active AS active,
					   (chararray)ToString(ToDate(effective_date,'yyyyMMdd'),'yyyy-MM-dd') AS effective_date,
					   (chararray)ToString(ToDate(expiration_date,'yyyyMMdd'),'yyyy-MM-dd') AS expiration_date;
 
payment_method_raw = LOAD '$brrLineItemExternalDir/paymentmethod.nss' using PigStorage('$delimiter') AS
					(
					payment_method_id:chararray,
					short_name:chararray,
					payment_group_id:double,
					active:chararray,
					effective_date:chararray,
					expiration_date:chararray,
					cash_basis_flg:chararray
					);	
					
payment_method = FOREACH payment_method_raw GENERATE
					     payment_method_id AS payment_method_id,
					     short_name AS short_name,
					     payment_group_id AS payment_group_id,
					     active AS active,
					     (chararray)ToString(ToDate(effective_date,'yyyyMMdd'),'yyyy-MM-dd') AS effective_date,
					     (chararray)ToString(ToDate(expiration_date,'yyyyMMdd'),'yyyy-MM-dd') AS expiration_date,
					     cash_basis_flg AS cash_basis_flg;
								
payment_vendor_raw = LOAD '$brrLineItemExternalDir/paymentvendor.nss' using PigStorage('$delimiter') AS
					(
					ppv_id:chararray,
					ppv_code:chararray,
					ppv_name:chararray,
					ppv_dir_name:chararray,
					ppv_id_name:chararray,
					active:chararray,
					effective_date:chararray,
					expiration_date:chararray
					);

payment_vendor = FOREACH payment_vendor_raw GENERATE
					     ppv_id AS ppv_id,
					     ppv_code AS ppv_code,
					     ppv_name AS ppv_name,
					     ppv_dir_name AS ppv_dir_name,
					     ppv_id_name AS ppv_id_name,
					     active AS active,
					     (chararray)ToString(ToDate(effective_date,'yyyyMMdd'),'yyyy-MM-dd') AS effective_date,
					     (chararray)ToString(ToDate(expiration_date,'yyyyMMdd'),'yyyy-MM-dd') AS expiration_date;
					
nb_reason_code_raw = LOAD '$brrLineItemExternalDir/nb_reason_code.nss' using PigStorage('$delimiter') AS
					(
					nb_reason_code_id:chararray,
					descr:chararray,
					active:chararray,
					effective_date:chararray,
					expiration_date:chararray
					);

nb_reason_code = FOREACH nb_reason_code_raw GENERATE
					     nb_reason_code_id AS nb_reason_code_id,
					     descr AS descr,
					     active AS active,
					     (chararray)ToString(ToDate(effective_date,'yyyyMMdd'),'yyyy-MM-dd') AS effective_date,
					     (chararray)ToString(ToDate(expiration_date,'yyyyMMdd'),'yyyy-MM-dd') AS expiration_date;
 					
pd_to_ma_map_raw = LOAD '$brrLineItemExternalDir/pd_to_ma_map.nss' using PigStorage('$delimiter') AS
				(
				processing_division:chararray,
				merchant_account:chararray, 
				description:chararray,
				status:chararray,
				effective_date:chararray,
				expiration_date:chararray,
				line_item_id:chararray
				);	

pd_to_ma_map = FOREACH pd_to_ma_map_raw GENERATE
				       processing_division AS processing_division,
				       merchant_account AS merchant_account, 
				       description AS description,
				       status AS status,
					   (chararray)ToString(ToDate(effective_date,'yyyyMMdd'),'yyyy-MM-dd') AS effective_date,
					   (chararray)ToString(ToDate(expiration_date,'yyyyMMdd'),'yyyy-MM-dd') AS expiration_date,
				       line_item_id AS line_item_id;
		
writeoff_reason_code = LOAD '$brrLineItemExternalDir/obi_writeoff_reason_code.nss' using PigStorage('$delimiter') AS 	
						(
						adjustment_type_id:chararray,
						adjustment_type_name:chararray,
						adjustment_type_desc:chararray,
						update_dt:chararray --YYYY-MM-DD
						);


-- No change to the below lookups as the lookups don't have any date field. 
currency = LOAD '$brrLineItemExternalDir/currency.nss' using PigStorage('$delimiter') AS
			(
			currency_id:chararray,
			currency_cd:chararray,
			currency_desc:chararray,
			status:chararray
			);
			
-- Original AI file name - currency_id.lkup			
currency_id = LOAD '$brrLineItemExternalDir/J5_iso_currency/part*' using PigStorage('$delimiter') AS
					(
					bid:chararray,
					sid:chararray,
					iso_currency_id:chararray
					);	

-- Original AI file name - cdb_pay_info.lkup
cdb_pay_info = LOAD '$brrLineItemExternalDir/J11_cdb_pay_info/part*' using PigStorage('$delimiter') AS 
					(
					pm_id:chararray,
					bid:chararray,
					sid:chararray,
					ppv_id:chararray,
					sibling_pm:chararray,
					long_name:chararray,
					language:chararray,
					reg_bill_argument:chararray,
					reg_bill_token:chararray,
					edit_rule:chararray,
					pm_actn_id:chararray,
					authorize_amount:double,
					monetary_cap:chararray,
					long_addr_format:chararray,
					show_in_cris:chararray,
					show_in_autorep:chararray,
					cvv2:chararray,
					get_bank_info:chararray,
					pm_code:chararray

					);
-- Original AI file name - vendor_merchant.lkup
vendor_merchant = LOAD '$brrLineItemExternalDir/J12_vendor_merchant/part*' using PigStorage('$delimiter') AS 
						(
						ppv_id:chararray,
						lineitem:chararray,
						bid:chararray,
						sid:chararray,
						merchant:chararray,
						settlement:chararray
						);	

STORE billing_config INTO '$brrLineItemInterimDir/LI_00_billing_config' USING PigStorage('$delimiter'); 
STORE line_item_glid_map INTO '$brrLineItemInterimDir/LI_00_line_item_glid_map' USING PigStorage('$delimiter'); 
STORE trans_type INTO '$brrLineItemInterimDir/LI_00_trans_type' USING PigStorage('$delimiter'); 
STORE return_code INTO '$brrLineItemInterimDir/LI_00_return_code' USING PigStorage('$delimiter'); 
STORE lineitem INTO '$brrLineItemInterimDir/LI_00_lineitem' USING PigStorage('$delimiter'); 
STORE line_item_map INTO '$brrLineItemInterimDir/LI_00_line_item_map' USING PigStorage('$delimiter'); 
STORE tranx_source INTO '$brrLineItemInterimDir/LI_00_tranx_source' USING PigStorage('$delimiter'); 
STORE payment_method INTO '$brrLineItemInterimDir/LI_00_payment_method' USING PigStorage('$delimiter'); 
STORE payment_vendor INTO '$brrLineItemInterimDir/LI_00_payment_vendor' USING PigStorage('$delimiter'); 
STORE nb_reason_code INTO '$brrLineItemInterimDir/LI_00_nb_reason_code' USING PigStorage('$delimiter'); 
STORE pd_to_ma_map INTO '$brrLineItemInterimDir/LI_00_pd_to_ma_map' USING PigStorage('$delimiter'); 
STORE writeoff_reason_code INTO '$brrLineItemInterimDir/LI_00_writeoff_reason_code' USING PigStorage('$delimiter'); 
STORE currency INTO '$brrLineItemInterimDir/LI_00_currency' USING PigStorage('$delimiter'); 
STORE currency_id INTO '$brrLineItemInterimDir/LI_00_currency_id' USING PigStorage('$delimiter'); 
STORE cdb_pay_info INTO '$brrLineItemInterimDir/LI_00_cdb_pay_info' USING PigStorage('$delimiter'); 
STORE vendor_merchant INTO '$brrLineItemInterimDir/LI_00_vendor_merchant' USING PigStorage('$delimiter');
                                                                        										