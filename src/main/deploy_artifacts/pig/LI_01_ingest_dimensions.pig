/*
            Script Name : LI_01_ingest_dimensions.pig
 jUnit Test Script Name : PigUnit01IngestDimensions.java
                Purpose : pig script for reading all dimension data needed for lineitem daily processing.
                          All dimension data needs to be defined in this script.
             Dependency : External AbInitio Job to copy all the files.                          
*/

billing_config = LOAD '$brrLineItemInterimDir/LI_00_billing_config' using PigStorage('$delimiter') AS
					(
					billing_config_key:chararray,
					bid:chararray,
					sid:chararray,
					lineitem:chararray,
					subscription_ind:chararray,
					effective_date:chararray,
					expiration_date:chararray,
					update_dt:chararray,
					status:chararray
					);

					
line_item_glid_map = LOAD '$brrLineItemInterimDir/LI_00_line_item_glid_map' using PigStorage('$delimiter') AS
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
			
trans_type = LOAD '$brrLineItemInterimDir/LI_00_trans_type' using PigStorage('$delimiter') AS
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
				
return_code = LOAD '$brrLineItemInterimDir/LI_00_return_code' using PigStorage('$delimiter') AS
				(
				sk:chararray,
				return_code:chararray,
				descr:chararray,
				active:chararray,
				effective_date:chararray,
				expiration_date:chararray
				);
				
lineitem = LOAD '$brrLineItemInterimDir/LI_00_lineitem' using PigStorage('$delimiter') AS
			(
			sk:chararray,
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
			paid_service_bundle_flg:chararray
			);
			
line_item_map = LOAD '$brrLineItemInterimDir/LI_00_line_item_map' using PigStorage('$delimiter') AS
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
						
tranx_source = LOAD '$brrLineItemInterimDir/LI_00_tranx_source' using PigStorage('$delimiter') AS
				(
				trans_source_id:chararray,
				trans_source_desc:chararray,
				active:chararray,
				effective_date:chararray,
				expiration_date:chararray
				);

payment_method = LOAD '$brrLineItemInterimDir/LI_00_payment_method' using PigStorage('$delimiter') AS
					(
					payment_method_id:chararray,
					short_name:chararray,
					payment_group_id:double,
					active:chararray,
					effective_date:chararray,
					expiration_date:chararray,
					cash_basis_flg:chararray
					);	
					
payment_vendor = LOAD '$brrLineItemInterimDir/LI_00_payment_vendor' using PigStorage('$delimiter') AS
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
					
nb_reason_code = LOAD '$brrLineItemInterimDir/LI_00_nb_reason_code' using PigStorage('$delimiter') AS
					(
					nb_reason_code_id:chararray,
					descr:chararray,
					active:chararray,
					effective_date:chararray,
					expiration_date:chararray
					);
					
pd_to_ma_map = LOAD '$brrLineItemInterimDir/LI_00_pd_to_ma_map' using PigStorage('$delimiter') AS
				(
				processing_division:chararray,
				merchant_account:chararray, 
				description:chararray,
				status:chararray,
				effective_date:chararray,
				expiration_date:chararray,
				line_item_id:chararray
				);	

currency = LOAD '$brrLineItemInterimDir/LI_00_currency' using PigStorage('$delimiter') AS
			(
			currency_id:chararray,
			currency_cd:chararray,
			currency_desc:chararray,
			status:chararray
			);
			
currency_id = LOAD '$brrLineItemInterimDir/LI_00_currency_id' using PigStorage('$delimiter') AS
					(
					bid:chararray,
					sid:chararray,
					iso_currency_id:chararray
					);			
			
writeoff_reason_code = LOAD '$brrLineItemInterimDir/LI_00_writeoff_reason_code' using PigStorage('$delimiter') AS 	
						(
						adjustment_type_id:chararray,
						adjustment_type_name:chararray,
						adjustment_type_desc:chararray,
						update_dt:chararray
						);

cdb_pay_info = LOAD '$brrLineItemInterimDir/LI_00_cdb_pay_info' using PigStorage('$delimiter') AS 
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
vendor_merchant = LOAD '$brrLineItemInterimDir/LI_00_vendor_merchant' using PigStorage('$delimiter') AS 
						(
						ppv_id:chararray,
						lineitem:chararray,
						bid:chararray,
						sid:chararray,
						merchant:chararray,
						settlement:chararray
						);	


offer_line_item = LOAD '$brrLineItemSqoopDir/offer_line_item' using PigStorage('$delimiter') AS 
                      (
                       offer_id:chararray,
                       line_item_id:chararray,
                       description:chararray
                      );
						
bu_line_item = 	LOAD '$brrLineItemSqoopDir/bu_line_item' using PigStorage('$delimiter') AS 
                     (
                      business_unit_id:chararray,
                      lineitem:chararray
                     );
                     
pm_mapping = LOAD '$brrLineItemSqoopDir/pm_mapping' using PigStorage('$delimiter') AS
                      (
                       infinys_pm_id:chararray,
                       aol_pm_id:long,
                       description:chararray
                      );                                                                                 										