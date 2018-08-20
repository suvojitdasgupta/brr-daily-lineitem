package com.aol.brr.lineitem.pig.unit;

import static com.aol.brr.lineitem.pig.unit.PigUnitUtil.createPigTest;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.pig.tools.parameters.ParseException;
import org.junit.Test;

import com.aol.brr.lineitem.pig.unit.PigUnitBaseTest;

public class PigUnit13ProcessArCrypto extends PigUnitBaseTest {

	@Test
	public void validateProcessArCrypto() throws IOException, ParseException {

		String pigScriptPath = PIG_SCRIPT_BASEPATH + "/LI_13_process_ar_crypto.pig";
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
		cluster.update(new Path(mockDataBasePath + "/ar_crypto/input/LI_13_ar_decripted.txt"),
				new Path(BASE_TMP_PATH + "/LI_12_process_ar_crypto"));
		cluster.update(new Path(mockDataBasePath + "/ar_crypto/input/LI_13_ar_main.txt"),
				new Path(BASE_TMP_PATH + "/LI_10_process_ar_crypto/part0"));
		cluster.update(new Path(mockDataBasePath + "/ar_crypto/input/paypal_trans.txt"),
				new Path(BASE_TMP_PATH + "/paypal_transactions/part0"));
		cluster.update(new Path(mockDataBasePath + "/ar_crypto/input/acct_pm_bin.txt"),
				new Path(BASE_TMP_PATH + "/acct_pm_bin/part0"));
				
		test = createPigTest(pigServer, cluster, pigScriptPath, args);
		
		assertOutputAnyOrder("ar_main_paypal_xform", mockDataBasePath + "/ar_crypto/output/LI_13_ar.txt");
      }
}