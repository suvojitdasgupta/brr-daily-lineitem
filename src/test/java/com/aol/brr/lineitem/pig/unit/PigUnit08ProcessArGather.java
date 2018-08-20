package com.aol.brr.lineitem.pig.unit;

import static com.aol.brr.lineitem.pig.unit.PigUnitUtil.createPigTest;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.pig.tools.parameters.ParseException;
import org.junit.Test;

import com.aol.brr.lineitem.pig.unit.PigUnitBaseTest;

public class PigUnit08ProcessArGather extends PigUnitBaseTest {

	@Test
	public void validateProcessArGather() throws IOException, ParseException {

		String pigScriptPath = PIG_SCRIPT_BASEPATH + "/LI_08_process_ar_gather.pig";
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
		cluster.update(new Path(mockDataBasePath + "/ar_gather/input/LI_03B_refund_trans.txt"),
				new Path(BASE_TMP_PATH + "/LI_03B_process_ar_refunds/part0"));
		cluster.update(new Path(mockDataBasePath + "/ar_gather/input/LI_03C_financial_writeoff.txt"),
				new Path(BASE_TMP_PATH + "/LI_03C_process_ar_writeoff/part0"));
		cluster.update(new Path(mockDataBasePath + "/ar_gather/input/LI_03D_bill_transactions.txt"),
				new Path(BASE_TMP_PATH + "/LI_03D_process_ar_bill/part0"));
		cluster.update(new Path(mockDataBasePath + "/ar_gather/input/LI_07_ar_trans_type.txt"),
				new Path(BASE_TMP_PATH + "/LI_07_process_ar_trans_type/part0"));
		
		test = createPigTest(pigServer, cluster, pigScriptPath, args);

		assertOutputAnyOrder("transactions_all", mockDataBasePath + "/ar_gather/output/LI_08_gather.txt");
	}
}