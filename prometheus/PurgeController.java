






public class PurgeController {
	
	
	private HashMap<String, String> parameters;
	
	
	/**
	 * constructor - requires job parameters
	 * @param parameters
	 */
	public PurgeController (HashMap<String, String> parameters) {
		
		this.parameters = parameters;
		
		process();
		
	}
	
	
	private void process () {
		
		// drop Mongo collections
		dropMongoCollections();
		
		// get rid of all jobStepQueue entries
		cleanJobStepQueue();
		
		// drop data schema
		dropDataSchema();
				
		
	}
	
	
	
	private void dropDataSchema () {
		
		if( ! parameters.containsKey("jobname"))
			throw new IllegalStateException ("You must set the jobname parameter before invoking this method");
		
		ConnectParameters cpSource = new ConnectParameters("prd");
		cpSource.setSchema(parameters.get("jobname"));
		Connection prdConn = ConnectionHelper.getConnection(cpSource);
		Statement drop;

		try {
		
			drop = prdConn.createStatement();
			drop.execute("DROP SCHEMA IF EXISTS " + parameters.get("jobname"));
			drop.close();
			log.info("Dropped schema " + parameters.get("jobname") );
		
		} catch (SQLException e) {
			log.error("SQL Exception attempting to drop data schema " + parameters.get("jobname") + ": " + e);
		} finally{
			try{
				if(prdConn!=null)
		       		 prdConn.close();
			}catch(SQLException se){
				log.error("SQL Exception attempting to close connection after drop of data schema " + parameters.get("jobname") + ": " + se);
			}
		}
		
	}


	
	/**
	 * get rid of the mapped Mongo collections
	 */
	private void dropMongoCollections() {
		
		Cleaner _c = new Cleaner(parameters);
		_c.cleanUp(_mSteps);
		
	}
	
	private static final String [] _mSteps = {ProcessJobStep.STEP_INPUTSTORE, ProcessJobStep.STEP_MAP}; 


	
	private void cleanJobStepQueue () {
		
		if( ! parameters.containsKey("jobname"))
			throw new IllegalStateException ("You must set the jobname parameter before invoking this method");
		
		ConnectParameters cpSource = new ConnectParameters("ecr");
		Connection ecrConn = ConnectionHelper.getConnection(cpSource);
		Statement deleteJSQ, deleteJSM;

		try {
		
			deleteJSQ = ecrConn.createStatement();
			deleteJSQ.execute("DELETE FROM jobStepQueue WHERE jobUid = '" + parameters.get("jobuid") + "'");
			deleteJSQ.close();
			log.info("Job Step Queue Cleared for Job " + parameters.get("jobuid") );
			
			deleteJSM = ecrConn.createStatement();
			deleteJSM.execute("DELETE FROM processMessage WHERE jobUid = '" + parameters.get("jobuid") + "'");
			deleteJSM.close();
			log.info("Process Messages Cleared for Job " + parameters.get("jobuid") );
		
		} catch (SQLException e) {
			log.error("SQL Exception attempting to purge jobStepQueue for jobUid " + parameters.get("jobuid") + ": " + e);
		} finally{
			try{
				if(ecrConn!=null)
					ecrConn.close();
			}catch(SQLException se){
				log.error("SQL Exception attempting to close connection after purge of jobStepQueue for jobUid " + parameters.get("jobuid") + ": " + se);
			}
		}
		
	}

	

	
	private static org.apache.log4j.Logger log = Logger.getLogger(PurgeController.class);
	
	
	/**
	 * main strictly for unit testing
	 * @param args
	 */
	public static void main(String[] args) {
		
		// get parameters and make them available to all
		RunParameters rp = new RunParameters();
		HashMap<String, String> parameters = RunParameters.parameters;
		parameters = rp.loadParameters(args, parameterDefaults);
		
		
		BigKahuna bigKahuna = new BigKahuna();
		String _cf = parameters.get("configfolder");
    	
    	bigKahuna.parameters = parameters;
		
    	bigKahuna.reLoadParameters();
    	
    	// fake stuff for local testing
    	parameters.put("configfolder", _cf);		

    	new PurgeController(parameters);
		
	}

	
	
	static String [][] parameterDefaults = {
		{"configfolder", "C:\\workspace\\ECR_Analytics\\trunk\\EpisodeConstruction\\src\\"}
	};
	
	

}
