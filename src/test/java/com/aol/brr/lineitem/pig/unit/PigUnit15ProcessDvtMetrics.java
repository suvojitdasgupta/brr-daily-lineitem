package com.aol.brr.lineitem.pig.unit;

import static com.aol.brr.lineitem.pig.unit.PigUnitUtil.createPigTest;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.aol.brr.lineitem.pig.unit.PigUnitBaseTest;

public class PigUnit15ProcessDvtMetrics extends PigUnitBaseTest {
	
	@Test
	public void validateProcessDvtMetrics() throws Exception {
		
		String pigScriptPath = PIG_SCRIPT_BASEPATH + "/LI_15_process_dvt_metrics.pig";
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
		cluster.update(new Path(mockDataBasePath + "/dvt_metrics/input/LI_15_bps.txt"),
				new Path(BASE_TMP_PATH + "/LI_02B_process_bps/part0"));
		cluster.update(new Path(mockDataBasePath + "/dvt_metrics/input/LI_15_ar.txt"),
				new Path(BASE_TMP_PATH + "/LI_14_process_ar_resub_filter/part0"));
		test = createPigTest(pigServer, cluster, pigScriptPath, args);
		assertOutputAnyOrder("bps_ar_rfmt_done_count", mockDataBasePath + "/dvt_metrics/output/LI_15_done.txt");
		
		initBeforeEachTest();
		cluster.update(new Path(mockDataBasePath + "/dvt_metrics/input/LI_15_bps.txt"),
				new Path(BASE_TMP_PATH + "/LI_02B_process_bps/part0"));
		cluster.update(new Path(mockDataBasePath + "/dvt_metrics/input/LI_15_ar.txt"),
				new Path(BASE_TMP_PATH + "/LI_14_process_ar_resub_filter/part0"));
		test = createPigTest(pigServer, cluster, pigScriptPath, args);
		assertOutputAnyOrder("bps_ar_rfmt", mockDataBasePath + "/dvt_metrics/output/LI_15_dvt_report.txt");
		
		initBeforeEachTest();
		cluster.update(new Path(mockDataBasePath + "/dvt_metrics/input/LI_15_bps.txt"),
				new Path(BASE_TMP_PATH + "/LI_02B_process_bps/part0"));
		cluster.update(new Path(mockDataBasePath + "/dvt_metrics/input/LI_15_ar.txt"),
				new Path(BASE_TMP_PATH + "/LI_14_process_ar_resub_filter/part0"));
		test = createPigTest(pigServer, cluster, pigScriptPath, args);
		assertOutputAnyOrder("bps_ar_rfmt_summary_count", mockDataBasePath + "/dvt_metrics/output/LI_15_summary.txt");		
	}
}