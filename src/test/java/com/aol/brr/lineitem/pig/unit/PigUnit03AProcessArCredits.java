package com.aol.brr.lineitem.pig.unit;

import static com.aol.brr.lineitem.pig.unit.PigUnitUtil.createPigTest;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.pig.tools.parameters.ParseException;
import org.junit.Test;

import com.aol.brr.lineitem.pig.unit.PigUnitBaseTest;

public class PigUnit03AProcessArCredits extends PigUnitBaseTest {

	@Test
	public void validateProcessArCredits() throws IOException, ParseException {

		String pigScriptPath = PIG_SCRIPT_BASEPATH + "/LI_03A_process_ar_credits.pig";
		String[] args = { "brrLineItemPigDir=" + PIG_SCRIPT_BASEPATH,
		                  "brrLineItemExternalDir=" + BASE_TMP_PATH,
		                  "brrLineItemSqoopDir=" + BASE_TMP_PATH,
		                  "brrLineItemInterimDir=" + BASE_TMP_PATH,
		                  "brrLineItemOutputDir=" + BASE_TMP_PATH,
		                  "brrLineItemRejectsDir=" + BASE_TMP_PATH,
		                  "segmentDate=2016-01-02",
				          "delimiter=|",
						  "pig_exec_mappart_agg_enabled=false",
						  "pig_user_cache_mode_enabled=false",
						  "pig_auto_local_mode_enabled=false",
						  "pig_auto_local_io_sort_mb=10",
						  "pig_auto_local_input_maxbytes=10000" };
		
		String mockDataBasePath = MOCK_DATA_BASEPATH + "/";
		
		initBeforeEachTest();
		cluster.update(new Path(mockDataBasePath + "/ar/input/credit_balance_bill.txt"),
				new Path(BASE_TMP_PATH + "/credit_balance_bill/part0"));
		cluster.update(new Path(mockDataBasePath + "/ar/input/line_item_balance_history_1.txt"),
				new Path(BASE_TMP_PATH + "/line_item_balance_history_1/part0"));
		cluster.update(new Path(mockDataBasePath + "/ar/input/line_item_balance_history_2.txt"),
				new Path(BASE_TMP_PATH + "/line_item_balance_history_2/part0"));
		cluster.update(new Path(mockDataBasePath + "/ar/input/balance_status_history.txt"),
				new Path(BASE_TMP_PATH + "/balance_status_history/part0"));		
		
		test = createPigTest(pigServer, cluster, pigScriptPath, args);
		
		assertOutputAnyOrder("bill_credit_history", mockDataBasePath + "/ar/output/LI_03A_bill_credit_history.txt");

	}
}