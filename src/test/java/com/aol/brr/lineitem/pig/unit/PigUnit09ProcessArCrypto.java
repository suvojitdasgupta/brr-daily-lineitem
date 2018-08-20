package com.aol.brr.lineitem.pig.unit;

import static com.aol.brr.lineitem.pig.unit.PigUnitUtil.createPigTest;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.pig.tools.parameters.ParseException;
import org.junit.Test;

import com.aol.brr.lineitem.pig.unit.PigUnitBaseTest;

public class PigUnit09ProcessArCrypto extends PigUnitBaseTest {

	@Test
	public void validateProcessArCrypto() throws IOException, ParseException {

		String pigScriptPath = PIG_SCRIPT_BASEPATH + "/LI_09_process_ar_crypto.pig";
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
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_currency"),
				new Path(BASE_TMP_PATH + "/LI_00_currency/part0"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_currency_id"),
				new Path(BASE_TMP_PATH + "/LI_00_currency_id/part0"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_cdb_pay_info"),
				new Path(BASE_TMP_PATH + "/LI_00_cdb_pay_info/part0"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/LI_00_vendor_merchant"),
				new Path(BASE_TMP_PATH + "/LI_00_vendor_merchant/part0"));
		
		cluster.update(new Path(mockDataBasePath + "/ar_crypto/input/LI_09_ar_main.txt"),
				new Path(BASE_TMP_PATH + "/LI_08_process_ar_gather/part0"));	
		cluster.update(new Path(mockDataBasePath + "/ar_crypto/input/LI_02A_address.txt"),
				new Path(BASE_TMP_PATH + "/LI_02A_process_address/part0"));
		cluster.update(new Path(mockDataBasePath + "/ar_crypto/input/address.txt"),
				new Path(BASE_TMP_PATH + "/address_non_payref/part0"));
		cluster.update(new Path(mockDataBasePath + "/ar_crypto/input/non_bill_trans.txt"),
				new Path(BASE_TMP_PATH + "/non_bill_transactions/part0"));

		test = createPigTest(pigServer, cluster, pigScriptPath, args);

		assertOutputAnyOrder("ar_consolidated_non_bill_trans_union", mockDataBasePath + "/ar_crypto/output/LI_09_ar.txt");
	}
}