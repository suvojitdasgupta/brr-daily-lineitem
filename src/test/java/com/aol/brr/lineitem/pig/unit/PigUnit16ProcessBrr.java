package com.aol.brr.lineitem.pig.unit;

import static com.aol.brr.lineitem.pig.unit.PigUnitUtil.createPigTest;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.pig.tools.parameters.ParseException;
import org.junit.Test;

import com.aol.brr.lineitem.pig.unit.PigUnitBaseTest;

public class PigUnit16ProcessBrr extends PigUnitBaseTest {

	@Test
	public void validateProcessBrr() throws IOException, ParseException {

		String pigScriptPath = PIG_SCRIPT_BASEPATH + "/LI_16_process_brr.pig";
		String[] args = { "brrLineItemPigDir=" + PIG_SCRIPT_BASEPATH,
		                  "brrLineItemExternalDir=" + BASE_TMP_PATH,
		                  "brrLineItemSqoopDir=" + BASE_TMP_PATH,
		                  "brrLineItemInterimDir=" + BASE_TMP_PATH,
		                  "brrLineItemOutputDir=" + BASE_TMP_PATH,
		                  "brrLineItemRejectsDir=" + BASE_TMP_PATH,
		                  /*$brrLineItemRecycleDir will point to PREVIOUS DAY's $brrLineItemOutputDir*/
		                  "brrLineItemRecycleDir=" + BASE_TMP_PATH,   
				          "segmentDate=2015-01-15",
				          "delimiter=|",
						  "pig_exec_mappart_agg_enabled=false",
						  "pig_user_cache_mode_enabled=false",
						  "pig_auto_local_mode_enabled=false",
						  "pig_auto_local_io_sort_mb=10",
						  "pig_auto_local_input_maxbytes=10000" };
		
		String mockDataBasePath = MOCK_DATA_BASEPATH + "/";

		/* Input Data Test */
		
		initBeforeEachTest();
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_billing_config"),
				new Path(BASE_TMP_PATH + "/LI_00_billing_config"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_line_item_glid_map"),
				new Path(BASE_TMP_PATH + "/LI_00_line_item_glid_map"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_trans_type"),
				new Path(BASE_TMP_PATH + "/LI_00_trans_type"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_return_code"),
				new Path(BASE_TMP_PATH + "/LI_00_return_code"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_lineitem"),
				new Path(BASE_TMP_PATH + "/LI_00_lineitem"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_line_item_map"),
				new Path(BASE_TMP_PATH + "/LI_00_line_item_map"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_tranx_source"),
				new Path(BASE_TMP_PATH + "/LI_00_tranx_source"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_tranx_source"),
				new Path(BASE_TMP_PATH + "/LI_00_tranx_source"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_payment_method"),
				new Path(BASE_TMP_PATH + "/LI_00_payment_method"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_payment_vendor"),
				new Path(BASE_TMP_PATH + "/LI_00_payment_vendor"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_nb_reason_code"),
				new Path(BASE_TMP_PATH + "/LI_00_nb_reason_code"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_pd_to_ma_map"),
				new Path(BASE_TMP_PATH + "/LI_00_pd_to_ma_map"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_currency"),
				new Path(BASE_TMP_PATH + "/LI_00_currency"));	
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_writeoff_reason_code"),
				new Path(BASE_TMP_PATH + "/LI_00_writeoff_reason_code"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_cdb_pay_info"),
				new Path(BASE_TMP_PATH + "/LI_00_cdb_pay_info/part0"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_vendor_merchant"),
				new Path(BASE_TMP_PATH + "/LI_00_vendor_merchant/part0"));
		
		cluster.update(new Path(mockDataBasePath + "/brr/input/LI_16_brr.txt"),
				new Path(BASE_TMP_PATH + "/LI_15_process_dvt_metrics_dvt/part0"));
		cluster.update(new Path(mockDataBasePath + "/brr/input/LI_16_recycle.txt"),
				new Path(BASE_TMP_PATH + "/LI_18_process_brr_rejects_recycle/part0"));
		
		test = createPigTest(pigServer, cluster, pigScriptPath, args);	
		assertOutputAnyOrder("rfmt_data", mockDataBasePath + "/brr/output/LI_16_rfmt_in.txt");
		
		
		/* Post Reformat(Lookups)Data Test */
		
		initBeforeEachTest();
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_billing_config"),
				new Path(BASE_TMP_PATH + "/LI_00_billing_config"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_line_item_glid_map"),
				new Path(BASE_TMP_PATH + "/LI_00_line_item_glid_map"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_trans_type"),
				new Path(BASE_TMP_PATH + "/LI_00_trans_type"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_return_code"),
				new Path(BASE_TMP_PATH + "/LI_00_return_code"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_lineitem"),
				new Path(BASE_TMP_PATH + "/LI_00_lineitem"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_line_item_map"),
				new Path(BASE_TMP_PATH + "/LI_00_line_item_map"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_tranx_source"),
				new Path(BASE_TMP_PATH + "/LI_00_tranx_source"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_tranx_source"),
				new Path(BASE_TMP_PATH + "/LI_00_tranx_source"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_payment_method"),
				new Path(BASE_TMP_PATH + "/LI_00_payment_method"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_payment_vendor"),
				new Path(BASE_TMP_PATH + "/LI_00_payment_vendor"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_nb_reason_code"),
				new Path(BASE_TMP_PATH + "/LI_00_nb_reason_code"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_pd_to_ma_map"),
				new Path(BASE_TMP_PATH + "/LI_00_pd_to_ma_map"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_currency"),
				new Path(BASE_TMP_PATH + "/LI_00_currency"));	
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_writeoff_reason_code"),
				new Path(BASE_TMP_PATH + "/LI_00_writeoff_reason_code"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_cdb_pay_info"),
				new Path(BASE_TMP_PATH + "/LI_00_cdb_pay_info/part0"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_vendor_merchant"),
				new Path(BASE_TMP_PATH + "/LI_00_vendor_merchant/part0"));
		
		cluster.update(new Path(mockDataBasePath + "/brr/input/LI_16_brr.txt"),
				new Path(BASE_TMP_PATH + "/LI_15_process_dvt_metrics_dvt/part0"));
		cluster.update(new Path(mockDataBasePath + "/brr/input/LI_16_recycle.txt"),
				new Path(BASE_TMP_PATH + "/LI_18_process_brr_rejects_recycle/part0"));
		
		test = createPigTest(pigServer, cluster, pigScriptPath, args);
		assertOutputAnyOrder("rfmt_data_OUT", mockDataBasePath + "/brr/output/LI_16_rfmt_out.txt");
		
		
		/* Exp File Data Test */
		
		initBeforeEachTest();
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_billing_config"),
				new Path(BASE_TMP_PATH + "/LI_00_billing_config"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_line_item_glid_map"),
				new Path(BASE_TMP_PATH + "/LI_00_line_item_glid_map"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_trans_type"),
				new Path(BASE_TMP_PATH + "/LI_00_trans_type"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_return_code"),
				new Path(BASE_TMP_PATH + "/LI_00_return_code"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_lineitem"),
				new Path(BASE_TMP_PATH + "/LI_00_lineitem"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_line_item_map"),
				new Path(BASE_TMP_PATH + "/LI_00_line_item_map"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_tranx_source"),
				new Path(BASE_TMP_PATH + "/LI_00_tranx_source"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_tranx_source"),
				new Path(BASE_TMP_PATH + "/LI_00_tranx_source"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_payment_method"),
				new Path(BASE_TMP_PATH + "/LI_00_payment_method"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_payment_vendor"),
				new Path(BASE_TMP_PATH + "/LI_00_payment_vendor"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_nb_reason_code"),
				new Path(BASE_TMP_PATH + "/LI_00_nb_reason_code"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_pd_to_ma_map"),
				new Path(BASE_TMP_PATH + "/LI_00_pd_to_ma_map"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_currency"),
				new Path(BASE_TMP_PATH + "/LI_00_currency"));	
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_writeoff_reason_code"),
				new Path(BASE_TMP_PATH + "/LI_00_writeoff_reason_code"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_cdb_pay_info"),
				new Path(BASE_TMP_PATH + "/LI_00_cdb_pay_info/part0"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_vendor_merchant"),
				new Path(BASE_TMP_PATH + "/LI_00_vendor_merchant/part0"));
		
		cluster.update(new Path(mockDataBasePath + "/brr/input/LI_16_brr.txt"),
				new Path(BASE_TMP_PATH + "/LI_15_process_dvt_metrics_dvt/part0"));
		cluster.update(new Path(mockDataBasePath + "/brr/input/LI_16_recycle.txt"),
				new Path(BASE_TMP_PATH + "/LI_18_process_brr_rejects_recycle/part0"));
		
		test = createPigTest(pigServer, cluster, pigScriptPath, args);
		assertOutputAnyOrder("exp_output_records", mockDataBasePath + "/brr/output/LI_16_exp.txt");
		
		/* Merge File Data Test */
	    
		initBeforeEachTest();
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_billing_config"),
				new Path(BASE_TMP_PATH + "/LI_00_billing_config"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_line_item_glid_map"),
				new Path(BASE_TMP_PATH + "/LI_00_line_item_glid_map"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_trans_type"),
				new Path(BASE_TMP_PATH + "/LI_00_trans_type"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_return_code"),
				new Path(BASE_TMP_PATH + "/LI_00_return_code"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_lineitem"),
				new Path(BASE_TMP_PATH + "/LI_00_lineitem"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_line_item_map"),
				new Path(BASE_TMP_PATH + "/LI_00_line_item_map"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_tranx_source"),
				new Path(BASE_TMP_PATH + "/LI_00_tranx_source"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_tranx_source"),
				new Path(BASE_TMP_PATH + "/LI_00_tranx_source"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_payment_method"),
				new Path(BASE_TMP_PATH + "/LI_00_payment_method"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_payment_vendor"),
				new Path(BASE_TMP_PATH + "/LI_00_payment_vendor"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_nb_reason_code"),
				new Path(BASE_TMP_PATH + "/LI_00_nb_reason_code"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_pd_to_ma_map"),
				new Path(BASE_TMP_PATH + "/LI_00_pd_to_ma_map"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_currency"),
				new Path(BASE_TMP_PATH + "/LI_00_currency"));	
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_writeoff_reason_code"),
				new Path(BASE_TMP_PATH + "/LI_00_writeoff_reason_code"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_cdb_pay_info"),
				new Path(BASE_TMP_PATH + "/LI_00_cdb_pay_info/part0"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_vendor_merchant"),
				new Path(BASE_TMP_PATH + "/LI_00_vendor_merchant/part0"));
		
		cluster.update(new Path(mockDataBasePath + "/brr/input/LI_16_brr.txt"),
				new Path(BASE_TMP_PATH + "/LI_15_process_dvt_metrics_dvt/part0"));
		cluster.update(new Path(mockDataBasePath + "/brr/input/LI_16_recycle.txt"),
				new Path(BASE_TMP_PATH + "/LI_18_process_brr_rejects_recycle/part0"));
		
		test = createPigTest(pigServer, cluster, pigScriptPath, args);
		assertOutputAnyOrder("merge_output_records", mockDataBasePath + "/brr/output/LI_16_merge.txt");		
        
	}
}
