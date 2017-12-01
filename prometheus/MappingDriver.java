



public class MappingDriver {
	
	
	private HashMap<String, String> parameters;
	
	
	public MappingDriver (HashMap<String, String> parameters) {
		this.parameters = parameters;
		//initialize();
		process();
	}
	
	private void process ()  {
		
		
		ArrayList<String[]> workQueue = InputManager.getInputFileNamesAndObjectNames(parameters);
		
		for (String[] sPair : workQueue) {
			
			log.info("Starting server for file " + sPair[0] + " creating " + sPair[1]);
			ServerControl _sc = new ServerControl();
			_sc.resourceName = sPair[0];
			_sc.objectName = sPair[1];
			new MultiThreadedSocketServer(parameters, _sc).process();
		
		}
		
		
		
		
	}
	
	
	
	/**
	 * main strictly for unit testing
	 * @param args
	 */
	public static void main(String[] args) {
		
		
		
		// get parameters and make them available to all
		RunParameters rp = new RunParameters();
		
		HashMap<String, String> parameters = RunParameters.parameters;
		parameters = rp.loadParameters(args, parameterDefaults);
		
		/*
		parameters.put("configfolder", "C:\\workspace\\ECR_Analytics\\trunk\\EpisodeConstruction\\src\\");
		parameters.put("env", "prd");

		parameters.put("stepname", ProcessJobStep.STEP_MAP);
		
		
		parameters.put("jobuid", "1095");
		parameters.put("clientID", "Test");
		parameters.put("runname", "Dec50K");
		parameters.put("rundate", "20151209");
		parameters.put("jobname", "Test_Dec50K20151209");
		parameters.put("mapname", "hci3");

		parameters.put("claim_file1", "C:/input/Test/new_2015_11_05/claims_mod.csv");
		parameters.put("claim_rx_file1", "C:/input/Test/new_2015_11_05/pharmacy_claims.csv");
		parameters.put("enroll_file1", "C:/input/Test/new_2015_11_05/member.csv");
		parameters.put("member_file1", "C:/input/Test/new_2015_11_05/member.csv");
		parameters.put("provider_file1", "C:/input/Test/new_2015_11_05/provider.csv");
		*/
		
		
		
		BigKahuna bigKahuna = new BigKahuna();
		String _cf = parameters.get("configfolder");
    	
    	bigKahuna.parameters = parameters;
		
    	bigKahuna.reLoadParameters();
    	
    	// fake stuff for local testing
    	parameters.put("configfolder", _cf);
    	
    	/*MappingDriver instance = */new MappingDriver(parameters);


	}

	
	static String [][] parameterDefaults = {
		{"configfolder", "C:\\workspace\\ECR_Analytics\\trunk\\EpisodeConstruction\\src\\"}
	};
	
	
	private static org.apache.log4j.Logger log = Logger.getLogger(MappingDriver.class);
	
	

}
