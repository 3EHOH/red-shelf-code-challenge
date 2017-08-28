


public class MongoTest {
	
	
	
	
	//private AllDataInterface di;
	//private ReportInterface oi;
	Date studyBegin, studyEnd;
	
	/**
	 * main process for input validation
	 * @throws IOException
	 */
	private void process () throws IOException {
		
		Mongo mongoClient = new MongoClient( "54.213.22.179" );
		DB db = mongoClient.getDB( "mydb" );
		DBCollection coll = db.getCollection("testCollection");
		
		BasicDBObject doc = new BasicDBObject("name", "MongoDB")
        .append("type", "database")
        .append("count", 1)
        .append("info", new BasicDBObject("x", 203).append("y", 102));
		coll.insert(doc);
		
		DBObject myDoc = coll.findOne();
		System.out.println(myDoc);

		/*
		log.info("Starting Input Validation");
		oi.createValidationReports(di);
		oi.createPatientCostDistribution(di);
		oi.createGeneralDataSummary(di);
		oi.finalize(di);
		log.info("Completed Input Validation");
		*/
		
	}

	
	
	


	public static void main(String[] args) throws IOException {
		
		MongoTest instance = new MongoTest();
		
		// get parameters (if any)
		instance.loadParameters(args);
		
		// always validate for this run
		instance.parameters.put("validate", "yes");
			
		// choose an input interface
		/*
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
			*/
		
		/* instantiate an error manager for the run
		ErrorManager errMgr = new ErrorManager(instance.di);
		instance.di.setErrorManager(errMgr);
		*/
		
		// choose an output interface
		/*
		if (instance.parameters.get("typeoutput").equalsIgnoreCase("xslx"))
			instance.oi = new ReportXSLX(instance.parameters);
		else
			throw new IllegalStateException ("Invalid output type parameter: " + instance.parameters.get("typeoutput"));
		
		if (instance.parameters.get("runname") == null)
			instance.parameters.put("runname", "sample");
			*/
		
		// process
		instance.process();

	}
	
	HashMap<String, String> parameters = RunParameters.parameters;
	//HashMap<String, String> parameters = new HashMap<String, String>();
	String [][] parameterDefaults = {
			{"studybegin", "20120101"},
			{"studyend", "20121231"},
			{"typeoutput", "xslx"}
	};
	
	/**
	 * load default parameters and 
	 * put any run arguments in the hash map as well
	 * arguments should take the form keyword=value (e.e., studybegin=20140101)
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
		// set specific variables
		try {
			studyBegin = df1.parse(parameters.get("studybegin"));
			studyEnd = df1.parse(parameters.get("studyend"));
		}
		catch (Exception ex ) {
			System.out.println(ex);
		}
		
	}
	
	
	DateFormat df1 = new SimpleDateFormat("yyyyMMdd");
	DateFormat df2 = new SimpleDateFormat("yyyy-MM-dd");
	
	
	
	//private static org.apache.log4j.Logger log = Logger.getLogger(MongoTest.class);


}
