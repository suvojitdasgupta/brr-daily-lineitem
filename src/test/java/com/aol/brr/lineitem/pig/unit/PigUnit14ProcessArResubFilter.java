package com.aol.brr.lineitem.pig.unit;

import static com.aol.brr.lineitem.pig.unit.PigUnitUtil.createPigTest;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.pig.tools.parameters.ParseException;
import org.junit.Test;

import com.aol.brr.lineitem.pig.unit.PigUnitBaseTest;

public class PigUnit14ProcessArResubFilter extends PigUnitBaseTest {

	@Test
	public void validateProcessArResubFilter() throws IOException, ParseException {

		String pigScriptPath = PIG_SCRIPT_BASEPATH + "/LI_14_process_ar_resub_filter.pig";
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
		cluster.update(new Path(mockDataBasePath + "/ar_resub/input/LI_14_ar_resub.txt"),
				new Path(BASE_TMP_PATH + "/LI_13_process_ar_crypto/part0"));

		test = createPigTest(pigServer, cluster, pigScriptPath, args);

		assertOutputAnyOrder("resub_final", mockDataBasePath + "/ar_resub/output/LI_14_ar_resub.txt");
	}
}