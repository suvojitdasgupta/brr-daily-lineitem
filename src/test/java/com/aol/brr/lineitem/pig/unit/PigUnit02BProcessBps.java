package com.aol.brr.lineitem.pig.unit;

import static com.aol.brr.lineitem.pig.unit.PigUnitUtil.createPigTest;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.pig.tools.parameters.ParseException;
import org.junit.Test;

import com.aol.brr.lineitem.pig.unit.PigUnitBaseTest;

public class PigUnit02BProcessBps extends PigUnitBaseTest {

	@Test
	public void validateProcessBps() throws IOException, ParseException {

		String pigScriptPath = PIG_SCRIPT_BASEPATH + "/LI_02B_process_bps.pig";
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
		/* Lookup Files */
		cluster.update(new Path(mockDataBasePath + "/lookups/input/offer_line_item"),
				new Path(BASE_TMP_PATH + "/offer_line_item/part0"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/bu_line_item"),
				new Path(BASE_TMP_PATH + "/bu_line_item/part0"));
		cluster.update(new Path(mockDataBasePath + "/lookups/input/pm_mapping"),
				new Path(BASE_TMP_PATH + "/pm_mapping/part0"));
		
		/* Input Data */
		cluster.update(new Path(mockDataBasePath + "/bps/input/SB.txt"),
				new Path(BASE_TMP_PATH + "/SB/part0"));
		cluster.update(new Path(mockDataBasePath + "/bps/input/bps_syndication_address.txt"),
				new Path(BASE_TMP_PATH + "/bps_syndication_address/part0"));
		cluster.update(new Path(mockDataBasePath + "/bps/input/bps_payable_address.txt"),
				new Path(BASE_TMP_PATH + "/bps_payable_address/part0"));
		cluster.update(new Path(mockDataBasePath + "/bps/input/bps_brd_currency.txt"),
				new Path(BASE_TMP_PATH + "/bps_brd_currency/part0"));
		
		
		cluster.update(new Path(mockDataBasePath + "/bps/input/SG.txt"),
				new Path(BASE_TMP_PATH + "/SG/part0"));
		cluster.update(new Path(mockDataBasePath + "/bps/input/SP_1.txt"),
				new Path(BASE_TMP_PATH + "/SP1/part0"));
		cluster.update(new Path(mockDataBasePath + "/bps/input/SP_2.txt"),
				new Path(BASE_TMP_PATH + "/SP2/part0"));
		cluster.update(new Path(mockDataBasePath + "/bps/input/SP_3.txt"),
				new Path(BASE_TMP_PATH + "/SP3/part0"));
		cluster.update(new Path(mockDataBasePath + "/bps/input/SX.txt"),
				new Path(BASE_TMP_PATH + "/SX/part0"));
		cluster.update(new Path(mockDataBasePath + "/bps/input/SD.txt"),
				new Path(BASE_TMP_PATH + "/SD/part0"));
		cluster.update(new Path(mockDataBasePath + "/bps/input/SE.txt"),
				new Path(BASE_TMP_PATH + "/SE/part0"));
		
		test = createPigTest(pigServer, cluster, pigScriptPath, args);

		test.assertOutput("bps_transactions_final", new File(mockDataBasePath + "/bps/output/LI_02_bps.txt"));
	}
}