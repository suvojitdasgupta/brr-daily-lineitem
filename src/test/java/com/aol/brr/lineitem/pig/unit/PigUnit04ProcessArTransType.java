package com.aol.brr.lineitem.pig.unit;

import static com.aol.brr.lineitem.pig.unit.PigUnitUtil.createPigTest;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.pig.tools.parameters.ParseException;
import org.junit.Test;

public class PigUnit04ProcessArTransType extends PigUnitBaseTest {
	@Test
	public void validateProcessArTransType() throws IOException, ParseException {

		String pigScriptPath = PIG_SCRIPT_BASEPATH + "/LI_04_process_ar_trans_type.pig";
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
		cluster.update(new Path(mockDataBasePath + "/ar_trans_type/input/LI_04.txt"),
				new Path(BASE_TMP_PATH + "/LI_03A_process_ar_credits/part0"));
		cluster.update(new Path(mockDataBasePath + "/ar_trans_type/input/bad_debt_writeoff.txt"),
				new Path(BASE_TMP_PATH + "/bad_debt_writeoff_counts/part0"));

		test = createPigTest(pigServer, cluster, pigScriptPath, args);
				
		assertOutputAnyOrder("trans_type_xform", mockDataBasePath + "/ar_trans_type/output/LI_04_main.txt");
		assertOutputAnyOrder("trans_type_filter_Y", mockDataBasePath + "/ar_trans_type/output/LI_04_zero_dolar.txt");
		
		
	}

}