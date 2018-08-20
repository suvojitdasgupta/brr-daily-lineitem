package com.aol.brr.lineitem.pig.unit;

import org.apache.pig.ExecTypeProvider;
import org.apache.pig.PigException;
import org.apache.pig.pigunit.Cluster;
import org.apache.pig.pigunit.PigTest;
import org.apache.pig.pigunit.pig.PigServer;

import com.google.common.base.Strings;
public class PigUnitUtil {

	private static final String DEFAULT_JUNIT_PIG_EXECUTION_MODE = "local";

	public static String getPigExecutionModeBasedOnMavenProfile() {
		String executionModeBasedOnMavenProfile = System.getProperty("junit_pig_mode_property");
		if (Strings.isNullOrEmpty(executionModeBasedOnMavenProfile)) {
			executionModeBasedOnMavenProfile = DEFAULT_JUNIT_PIG_EXECUTION_MODE;
			System.err.println("Value for junit_pig_mode_property not set; Using DEFAULT_JUNIT_PIG_EXECUTION_MODE : "
					+ DEFAULT_JUNIT_PIG_EXECUTION_MODE + " as executionModeBasedOnMavenProfile");
		}
		return executionModeBasedOnMavenProfile;
	}

	public static PigTest createPigTest(PigServer pigServer, Cluster pigCluster, String scriptFile, String[] inputs)
			throws PigException {
		try {
			return new PigTest(scriptFile, inputs, pigServer, pigCluster);
		} catch (Exception ex) {
			throw new PigException("Failed to create PigTest instance. ", ex);
		}
	}

	public static Cluster createPigCluster(PigServer pigServer) throws PigException {
		try {

			return new Cluster(pigServer.getPigContext());
		} catch (Exception ex) {
			ex.printStackTrace();
			throw new PigException("Failed to create Cluster instance. ", ex);
		}
	}

	public static PigServer createPigServer() throws PigException {
		try {
			String executionModeForUnitTesting = getPigExecutionModeBasedOnMavenProfile();
			System.out.println(
					"Execution-Mode for unit testing set ! executionModeForUnitTesting=" + executionModeForUnitTesting);
			return new PigServer(ExecTypeProvider.fromString(executionModeForUnitTesting));
		} catch (Exception ex) {
			throw new PigException("Failed to create PigServer instance. ", ex);
		}
	}
}
