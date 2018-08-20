package com.aol.brr.lineitem.pig.unit;

import static com.aol.brr.lineitem.pig.unit.PigUnitUtil.createPigTest;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.pig.tools.parameters.ParseException;
import org.junit.Test;

import com.aol.brr.lineitem.pig.unit.PigUnitBaseTest;

public class PigUnit02AProcessAddress extends PigUnitBaseTest {

	@Test
	public void validateProcessAddress() throws IOException, ParseException {

		String pigScriptPath = PIG_SCRIPT_BASEPATH + "/LI_02A_process_address.pig";
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
						  "pig_auto_local_input_maxbytes=10000",
						  "pig_compress_enabled=false"};
		
		String mockDataBasePath = MOCK_DATA_BASEPATH + "/";
		
		initBeforeEachTest();
		
		cluster.update(new Path(mockDataBasePath + "/ar/input/line_item_bill.txt"),
				new Path(BASE_TMP_PATH + "/line_item_bill/part0"));
		cluster.update(new Path(mockDataBasePath + "/ar/input/payment_ids.txt"),
				new Path(BASE_TMP_PATH + "/payment_ids/part0"));
		
		test = createPigTest(pigServer, cluster, pigScriptPath, args);

		test.assertOutput("address_payref", new File(mockDataBasePath + "/ar/output/LI_02A_address.txt"));
	}
}