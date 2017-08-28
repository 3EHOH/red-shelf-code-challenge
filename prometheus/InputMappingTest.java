



public class InputMappingTest {
	
	private AllDataInterface di;
	private InputObjectOutputInterface oi;
	private List<ClaimServLine> svcLines;
	private Map<String, List<Enrollment>> enrollments;
	private Map<String, PlanMember> members;
	private Map<String, Provider> providers;
	private List<ClaimRx> rxClaims;
	
	/**
	 * main process for input mapping test
	 * @throws IOException
	 */
	private void process () throws IOException {

		log.info("Starting Input Mapping Test");
		
		
		svcLines = di.getAllServiceClaims();
		enrollments = di.getAllEnrollments();
		members = di.getAllPlanMembers();
		providers = di.getAllProviders();
		rxClaims = di.getAllRxClaims();
		
		oi.writeMedicalClaims(svcLines);
		oi.writeRxClaims(rxClaims);
		oi.writeMembers(members.values());
		oi.writeEnrollments(enrollments.values());
		oi.writeProviders(providers.values());
		
		log.info("Completed Input Mapping Test");
		
	}

	

	public static void main(String[] args) throws IOException {
		
		InputMappingTest instance = new InputMappingTest();
		
		// get parameters (if any)
		instance.loadParameters(args);
			
		// choose an input interface
		if (instance.parameters.get("typeinput").equalsIgnoreCase("csv"))
			instance.di = new AllDataCSV(instance.parameters);
		else if (instance.parameters.get("typeinput").equalsIgnoreCase("apcd"))
			instance.di = new AllDataAPCD(instance.parameters);
		else if (instance.parameters.get("typeinput").equalsIgnoreCase("xg"))
			instance.di = new AllDataXG(instance.parameters);
		else if (instance.parameters.get("typeinput").equalsIgnoreCase("wellpoint"))
			instance.di = new AllDataWellpoint(instance.parameters);
		else if (instance.parameters.get("typeinput").equalsIgnoreCase("pebtf"))
			instance.di = new AllDataPebtf(instance.parameters);
		else if (instance.parameters.get("typeinput").equalsIgnoreCase("hci3"))
			instance.di = new AllDataHCI3(instance.parameters);
		else
			throw new IllegalStateException ("Invalid input type parameter: " + instance.parameters.get("typeinput"));
		
		// instantiate an error manager for the run
		//ErrorManager errMgr = new ErrorManager(instance.di);
		//instance.di.setErrorManager(errMgr);
		
		// choose an output interface
		if (instance.parameters.get("typeoutput").equalsIgnoreCase("csv")) {
			instance.oi = new InputObjectOutputCSV();
		} else if (instance.parameters.get("typeoutput").equalsIgnoreCase("sql")) {
			instance.oi = new InputObjectOutputHibSQL();
		} else if (instance.parameters.get("typeoutput").equalsIgnoreCase("wjmsql")) {
			instance.oi = new InputObjectOutputSQL();
		} else {
			throw new IllegalStateException ("Invalid output type parameter: " + instance.parameters.get("typeoutput"));
		}
		
		if (instance.parameters.get("runname") == null)
			instance.parameters.put("runname", "sample");
		
		// process
		instance.process();

	}
	
	HashMap<String, String> parameters = RunParameters.parameters;
	//HashMap<String, String> parameters = new HashMap<String, String>();
	String [][] parameterDefaults = {
			{"typeoutput", "csv"}
	};
	
	/**
	 * load default parameters and 
	 * put any run arguments in the hash map as well
	 * arguments should take the form keyword=value (e.g., studybegin=20140101)
	 * @param args
	 */
	private void loadParameters (String[] args) {
		// load any default parameters from the default parameter array
		for (int i = 0; i < parameterDefaults.length; i++) {
			parameters.put(parameterDefaults[i][0], parameterDefaults[i][1]);
		}
		// overlay or add any incoming parameters
		for (int i = 0; i < args.length; i++) {
			parameters.put(args[i].substring(0, args[i].indexOf('=')), args[i].substring(args[i].indexOf('=')+1)) ;
		}
		
		
	}
	
	
	DateFormat df1 = new SimpleDateFormat("yyyyMMdd");
	DateFormat df2 = new SimpleDateFormat("yyyy-MM-dd");
	
	private static org.apache.log4j.Logger log = Logger.getLogger(InputMappingTest.class);
	
}
