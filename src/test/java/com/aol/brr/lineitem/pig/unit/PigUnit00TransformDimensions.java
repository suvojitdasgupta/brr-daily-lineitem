package com.aol.brr.lineitem.pig.unit;

import static com.aol.brr.lineitem.pig.unit.PigUnitUtil.createPigTest;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.pig.tools.parameters.ParseException;
import org.junit.Test;

import com.aol.brr.lineitem.pig.unit.PigUnitBaseTest;

public class PigUnit00TransformDimensions extends PigUnitBaseTest {

	@Test
	public void validateTransformDimensions() throws IOException, ParseException {

		String pigScriptPath = PIG_SCRIPT_BASEPATH + "/LI_00_transform_dimensions.pig";
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
						  "pig_auto_local_input_maxbytes=10000"};
		
		String mockDataBasePath = MOCK_DATA_BASEPATH + "/";
		
		initBeforeEachTest();
		cluster.update(new Path(mockDataBasePath + "/external/input/obi_billing_config.nss"),
				new Path(BASE_TMP_PATH + "/obi_billing_config.nss"));
		cluster.update(new Path(mockDataBasePath + "/external/input/line_item_glid_map.nss"),
				new Path(BASE_TMP_PATH + "/line_item_glid_map.nss"));
		cluster.update(new Path(mockDataBasePath + "/external/input/transtype.nss"),
				new Path(BASE_TMP_PATH + "/transtype.nss"));
		cluster.update(new Path(mockDataBasePath + "/external/input/returncode.nss"),
				new Path(BASE_TMP_PATH + "/returncode.nss"));
		cluster.update(new Path(mockDataBasePath + "/external/input/lineitem.nss"),
				new Path(BASE_TMP_PATH + "/lineitem.nss"));
		cluster.update(new Path(mockDataBasePath + "/external/input/line_item_map.nss"),
				new Path(BASE_TMP_PATH + "/line_item_map.nss"));
		cluster.update(new Path(mockDataBasePath + "/external/input/tranxsource.nss"),
				new Path(BASE_TMP_PATH + "/tranxsource.nss"));
		cluster.update(new Path(mockDataBasePath + "/external/input/paymentmethod.nss"),
				new Path(BASE_TMP_PATH + "/paymentmethod.nss"));
		cluster.update(new Path(mockDataBasePath + "/external/input/paymentvendor.nss"),
				new Path(BASE_TMP_PATH + "/paymentvendor.nss"));
		cluster.update(new Path(mockDataBasePath + "/external/input/nb_reason_code.nss"),
				new Path(BASE_TMP_PATH + "/nb_reason_code.nss"));
		cluster.update(new Path(mockDataBasePath + "/external/input/pd_to_ma_map.nss"),
				new Path(BASE_TMP_PATH + "/pd_to_ma_map.nss"));
		cluster.update(new Path(mockDataBasePath + "/external/input/obi_writeoff_reason_code.nss"),
				new Path(BASE_TMP_PATH + "/obi_writeoff_reason_code.nss"));
		cluster.update(new Path(mockDataBasePath + "/external/input/currency.nss"),
				new Path(BASE_TMP_PATH + "/currency.nss"));	
		cluster.update(new Path(mockDataBasePath + "/external/input/currency_id.lkup"),
				new Path(BASE_TMP_PATH + "/J5_iso_currency/part*"));
		cluster.update(new Path(mockDataBasePath + "/external/input/cdb_pay_info.lkup"),
				new Path(BASE_TMP_PATH + "/J11_cdb_pay_info/part*"));
		cluster.update(new Path(mockDataBasePath + "/external/input/vendor_merchant.lkup"),
				new Path(BASE_TMP_PATH + "/J12_vendor_merchant/part*"));


		
		test = createPigTest(pigServer, cluster, pigScriptPath, args);
		
		assertOutputAnyOrder("billing_config", mockDataBasePath + "/external/output/LI_00_billing_config");
		assertOutputAnyOrder("line_item_glid_map", mockDataBasePath + "/external/output/LI_00_line_item_glid_map");
		assertOutputAnyOrder("trans_type", mockDataBasePath + "/external/output/LI_00_trans_type");
		assertOutputAnyOrder("return_code", mockDataBasePath + "/external/output/LI_00_return_code");
		assertOutputAnyOrder("lineitem", mockDataBasePath + "/external/output/LI_00_lineitem");
		assertOutputAnyOrder("line_item_map", mockDataBasePath + "/external/output/LI_00_line_item_map");
		assertOutputAnyOrder("tranx_source", mockDataBasePath + "/external/output/LI_00_tranx_source");
		assertOutputAnyOrder("payment_method", mockDataBasePath + "/external/output/LI_00_payment_method");
		assertOutputAnyOrder("payment_vendor", mockDataBasePath + "/external/output/LI_00_payment_vendor");
		assertOutputAnyOrder("nb_reason_code", mockDataBasePath + "/external/output/LI_00_nb_reason_code");
		assertOutputAnyOrder("pd_to_ma_map", mockDataBasePath + "/external/output/LI_00_pd_to_ma_map");
		assertOutputAnyOrder("writeoff_reason_code", mockDataBasePath + "/external/output/LI_00_writeoff_reason_code");
		assertOutputAnyOrder("currency", mockDataBasePath + "/external/output/LI_00_currency");
		assertOutputAnyOrder("currency_id", mockDataBasePath + "/external/output/LI_00_currency_id");
		assertOutputAnyOrder("cdb_pay_info", mockDataBasePath + "/external/output/LI_00_cdb_pay_info");
		assertOutputAnyOrder("vendor_merchant", mockDataBasePath + "/external/output/LI_00_vendor_merchant");

	}
}