package com.aol.brr.lineitem.pig.unit;

import static com.aol.brr.lineitem.pig.unit.PigUnitUtil.createPigTest;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.pig.tools.parameters.ParseException;
import org.junit.Test;

import com.aol.brr.lineitem.pig.unit.PigUnitBaseTest;

public class PigUnit18ProcessBrrRejects extends PigUnitBaseTest {

	@Test
	public void validateProcessBrrRejects() throws IOException, ParseException {

		String pigScriptPath = PIG_SCRIPT_BASEPATH + "/LI_18_process_brr_rejects.pig";
		String[] args = { "brrLineItemPigDir=" + PIG_SCRIPT_BASEPATH,
		                  "brrLineItemExternalDir=" + BASE_TMP_PATH,
		                  "brrLineItemSqoopDir=" + BASE_TMP_PATH,
		                  "brrLineItemInterimDir=" + BASE_TMP_PATH,
		                  "brrLineItemOutputDir=" + BASE_TMP_PATH,
		                  "brrLineItemRejectsDir=" + BASE_TMP_PATH,
				          "delimiter=|",
				          "segmentDate=2016-01-12",
						  "pig_exec_mappart_agg_enabled=false",
						  "pig_user_cache_mode_enabled=false",
						  "pig_auto_local_mode_enabled=false",
						  "pig_auto_local_io_sort_mb=10",
						  "pig_auto_local_input_maxbytes=10000" };
		
		String mockDataBasePath = MOCK_DATA_BASEPATH + "/";
		
		initBeforeEachTest();
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_line_item_map"),
				new Path(BASE_TMP_PATH + "/LI_00_line_item_map/part0"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_trans_type"),
				new Path(BASE_TMP_PATH + "/LI_00_trans_type/part0"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_return_code"),
				new Path(BASE_TMP_PATH + "/LI_00_return_code/part0"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_lineitem"),
				new Path(BASE_TMP_PATH + "/LI_00_lineitem/part0"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_tranx_source"),
				new Path(BASE_TMP_PATH + "/LI_00_tranx_source/part0"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_payment_method"),
				new Path(BASE_TMP_PATH + "/LI_00_payment_method/part0"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_payment_vendor"),
				new Path(BASE_TMP_PATH + "/LI_00_payment_vendor/part0"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_nb_reason_code"),
				new Path(BASE_TMP_PATH + "/LI_00_nb_reason_code/part0"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_pd_to_ma_map"),
				new Path(BASE_TMP_PATH + "/LI_00_pd_to_ma_map/part0"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_line_item_glid_map"),
				new Path(BASE_TMP_PATH + "/LI_00_line_item_glid_map/part0"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_writeoff_reason_code"),
				new Path(BASE_TMP_PATH + "/LI_00_writeoff_reason_code/part0"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_currency"),
				new Path(BASE_TMP_PATH + "/LI_00_currency/part0"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_billing_config"),
				new Path(BASE_TMP_PATH + "/LI_00_billing_config/part0"));
		
		/*These twho files represents the input and output data files for the component - 250_RFMT_Data in AbInitio*/
		cluster.update(new Path(mockDataBasePath + "/brr/input/LI_18_250_RFMT_Data_input.txt"),
				new Path(BASE_TMP_PATH + "/LI_16_process_brr_rfmt_in/part0"));
		cluster.update(new Path(mockDataBasePath + "/brr/input/LI_18_250_RFMT_Data_output.txt"),
				new Path(BASE_TMP_PATH + "/LI_16_process_brr_rfmt_out/part0"));
		
		test = createPigTest(pigServer, cluster, pigScriptPath, args);
		
		assertOutputAnyOrder("recycle_data", mockDataBasePath + "/brr/output/LI_18_recycle.txt");
		assertOutputAnyOrder("reject_data_sorted", mockDataBasePath + "/brr/output/LI_18_reject.txt");
		assertOutputAnyOrder("error_code_99_data_x", mockDataBasePath + "/brr/output/LI_18_lookup_error.txt");

	}
}