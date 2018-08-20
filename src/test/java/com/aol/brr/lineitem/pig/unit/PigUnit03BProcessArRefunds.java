package com.aol.brr.lineitem.pig.unit;

import static com.aol.brr.lineitem.pig.unit.PigUnitUtil.createPigTest;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.pig.tools.parameters.ParseException;
import org.junit.Test;

import com.aol.brr.lineitem.pig.unit.PigUnitBaseTest;

public class PigUnit03BProcessArRefunds extends PigUnitBaseTest {

	@Test
	public void validateProcessArRefunds() throws IOException, ParseException {

		String pigScriptPath = PIG_SCRIPT_BASEPATH + "/LI_03B_process_ar_refunds.pig";
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
		cluster.update(new Path(mockDataBasePath + "/ar/input/cash_credit_history.txt"),
				new Path(BASE_TMP_PATH + "/cash_credit_history/part0"));

		cluster.update(new Path(mockDataBasePath + "/ar/input/line_item_balance.txt"),
				new Path(BASE_TMP_PATH + "/tax_line_item_balance/part0"));		
		cluster.update(new Path(mockDataBasePath + "/ar/input/balance_impact.txt"),
				new Path(BASE_TMP_PATH + "/tax_balance_impact/part0"));
		
		test = createPigTest(pigServer, cluster, pigScriptPath, args);
		
		assertOutputAnyOrder("refund_trans_final", mockDataBasePath + "/ar/output/LI_03B_refund_trans.txt");
		assertOutputAnyOrder("refund_error", mockDataBasePath + "/ar/output/LI_03B_refund_error.txt");
	}
}