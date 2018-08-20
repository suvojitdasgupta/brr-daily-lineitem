package com.aol.brr.lineitem.pig.unit;

import static com.aol.brr.lineitem.pig.unit.PigUnitUtil.createPigCluster;
import static com.aol.brr.lineitem.pig.unit.PigUnitUtil.createPigServer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.pig.PigException;
import org.apache.pig.pigunit.Cluster;
import org.apache.pig.pigunit.PigTest;
import org.apache.pig.pigunit.pig.PigServer;
import org.apache.pig.tools.parameters.ParseException;
import org.junit.After;
import org.junit.Ignore;

@Ignore
public class PigUnitBaseTest {
	
		protected static final String MOCK_DATA_BASEPATH = "src/test/resources/pig_unit";
		protected static final String BASE_TMP_PATH = "pigunit_tmp";
		protected static final String PIG_SCRIPT_BASEPATH = "src/main/deploy_artifacts/pig";

		protected Cluster cluster;
		protected PigServer pigServer;
		protected PigTest test;

		public void initBeforeEachTest() throws PigException {
			this.pigServer = createPigServer();
			this.cluster = createPigCluster(pigServer);

		}

		@After
		public void initAfterAllTests() throws Exception {
			try {
				FileUtils.deleteDirectory(new File(BASE_TMP_PATH));
			} catch (IOException ex) {
				// Do Nothing
			}
		}

		protected void assertOutputAnyOrder(String aliasToAssert, String expectedOutputFilePath)
				throws IOException, FileNotFoundException, ParseException {
			List<String> lines = IOUtils.readLines(new FileInputStream(expectedOutputFilePath));
			String[] linesArray = lines.toArray(new String[lines.size()]);
			test.assertOutputAnyOrder(aliasToAssert, linesArray);
		}
		
		public static String[] getArgsWith(List<String> existingArgsList, String additionalArg) {
			ArrayList<String> argsList = new ArrayList<String>(existingArgsList);
			argsList.add(additionalArg);
			String[] argsArray = argsList.toArray(new String[argsList.size()]);
			return argsArray;
		}


	}
