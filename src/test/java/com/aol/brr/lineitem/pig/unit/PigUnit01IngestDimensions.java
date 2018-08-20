package com.aol.brr.lineitem.pig.unit;

import static com.aol.brr.lineitem.pig.unit.PigUnitUtil.createPigTest;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.pig.tools.parameters.ParseException;
import org.junit.Test;

import com.aol.brr.lineitem.pig.unit.PigUnitBaseTest;

public class PigUnit01IngestDimensions extends PigUnitBaseTest {

	@Test
	public void validateIngestDimensions() throws IOException, ParseException {

		String pigScriptPath = PIG_SCRIPT_BASEPATH + "/LI_01_ingest_dimensions.pig";
		String[] args = { "brrLineItemPigDir=" + PIG_SCRIPT_BASEPATH,
				          "brrLineItemExternalDir=" + BASE_TMP_PATH,
				          "brrLineItemSqoopDir=" + BASE_TMP_PATH,
				          "brrLineItemInterimDir=" + BASE_TMP_PATH,
				          "brrLineItemOutputDir=" + BASE_TMP_PATH,
				          "brrLineItemRejectsDir=" + BASE_TMP_PATH,
				          "delimiter=|",
						  "pig_exec_mappart_agg_enabled=false",
						  "pig_user_cache_mode_enabled=false",
						  "pig_auto_local_mode_enabled=false",
						  "pig_auto_local_io_sort_mb=10",
						  "pig_auto_local_input_maxbytes=10000" };
		
		String mockDataBasePath = MOCK_DATA_BASEPATH + "/";
		
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
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_currency_id"),
				new Path(BASE_TMP_PATH + "/LI_00_currency_id"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_writeoff_reason_code"),
				new Path(BASE_TMP_PATH + "/LI_00_writeoff_reason_code"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_cdb_pay_info"),
				new Path(BASE_TMP_PATH + "/LI_00_cdb_pay_info"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_vendor_merchant"),
				new Path(BASE_TMP_PATH + "/LI_00_vendor_merchant"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/offer_line_item"),
				new Path(BASE_TMP_PATH + "/offer_line_item"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/bu_line_item"),
				new Path(BASE_TMP_PATH + "/bu_line_item"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/pm_mapping"),
				new Path(BASE_TMP_PATH + "/pm_mapping"));
	
		test = createPigTest(pigServer, cluster, pigScriptPath, args);
		
		assertOutputAnyOrder("billing_config", mockDataBasePath + "/lookups/output/LI_01_billing_config");
		/*assertOutputAnyOrder("line_item_glid_map", mockDataBasePath + "/lookups/output/LI_01_line_item_glid_map");*/
		assertOutputAnyOrder("trans_type", mockDataBasePath + "/lookups/output/LI_01_trans_type");
		assertOutputAnyOrder("return_code", mockDataBasePath + "/lookups/output/LI_01_return_code");
		assertOutputAnyOrder("lineitem", mockDataBasePath + "/lookups/output/LI_01_lineitem");
		assertOutputAnyOrder("line_item_map", mockDataBasePath + "/lookups/output/LI_01_line_item_map");
		assertOutputAnyOrder("tranx_source", mockDataBasePath + "/lookups/output/LI_01_tranx_source");
	    assertOutputAnyOrder("payment_method", mockDataBasePath + "/lookups/output/LI_01_payment_method");
		assertOutputAnyOrder("payment_vendor", mockDataBasePath + "/lookups/output/LI_01_payment_vendor");
		assertOutputAnyOrder("nb_reason_code", mockDataBasePath + "/lookups/output/LI_01_nb_reason_code");
		assertOutputAnyOrder("pd_to_ma_map", mockDataBasePath + "/lookups/output/LI_01_pd_to_ma_map");
		assertOutputAnyOrder("currency", mockDataBasePath + "/lookups/output/LI_01_currency");
		assertOutputAnyOrder("currency_id", mockDataBasePath + "/lookups/output/LI_01_currency_id");
		assertOutputAnyOrder("writeoff_reason_code", mockDataBasePath + "/lookups/output/LI_01_writeoff_reason_code");
		assertOutputAnyOrder("cdb_pay_info", mockDataBasePath + "/lookups/output/LI_01_cdb_pay_info");
		assertOutputAnyOrder("vendor_merchant", mockDataBasePath + "/lookups/output/LI_01_vendor_merchant");
		assertOutputAnyOrder("offer_line_item", mockDataBasePath + "/lookups/output/LI_01_offer_line_item");
	    assertOutputAnyOrder("bu_line_item", mockDataBasePath + "/lookups/output/LI_01_bu_line_item");
	    assertOutputAnyOrder("pm_mapping", mockDataBasePath + "/lookups/output/LI_01_pm_mapping");
	}
}