




public class MappingController {
	
	
	
	private String mapSet;
	private HashMap<String, String> parameters;
	
	private InputMap imap;
	private List<InputObjectMap> maps;
	private InputMapper mapper;
	
	private HibernateHelper h;
	private SessionFactory factory;
	private Session session;
	
	private MongoInputCollectionSet micky;
	
	/**
	 * constructor used in batch process (BigKahuna)
	 * @param parameters
	 */
	public MappingController (HashMap<String, String> parameters) {
		this.mapSet = parameters.get("mapname");
		this.parameters = parameters;
		initialize();
		process();
	}
	
	
	/**
	 * constructor used by single map request process (socket client)
	 */
	public MappingController (HashMap<String, String> parameters, MappingRequest _mr) {
		
		this.parameters = parameters;
		
		
		String sCfg = parameters.get("configfolder") == null ? null : parameters.get("configfolder");
		parameters.put("jobuid", _mr.getJobUid());
		BigKahuna bigKahuna = new BigKahuna();
    	bigKahuna.parameters = parameters;
    	bigKahuna.reLoadParameters();
    	if(sCfg != null)
    		parameters.put("configfolder", sCfg);
		
		
		//initializeForMonitorRetrieval();
	}
	
	
	/**
	 * process to handle all files (BigKahuna)
	 */
	private void process ()  {
			
		getMapSet();
		initializeForMonitorRetrieval();
		
		// find potential source files
		getMapsForFiles();
		
		for (InputObjectMap m : maps) {
			mapper = new InputMapper( m.getObjectName(), parameters );
			ArrayList<String> source = sourceList.get(m.getObjectName());
			if( source == null  ||  source.isEmpty())
				continue;
			for (String s : source) {
				mapper.addSourceMonitor(monitorRetrieval(s));
			}
			mapper.prepareAllWorkObjects(m);
		}
		
		closeMonitorRetrieval();
		
	}
	
	
	/**
	 * process a single mapping request (socket client)
	 */
	public boolean processMapRequest(MappingRequest _mr) {
		
		boolean bR = true;
		
		if(mapSet == null)				// first time check
			initializeForMapRequest(_mr);
		
		//log.info("Gwaana map: " + _mr.getSequence());
		
		for (InputObjectMap m : maps) {
			if (m.getObjectName().equals(_mr.getObjectName().substring(_mr.getObjectName().lastIndexOf('.') +1))) {
				mapper.prepareOneWorkObject(m, _mr);
			}
			
		}
		
		
		
		
		return bR;
		
	}
	
	
	private void initializeForMapRequest (MappingRequest _mr) {
		
		List<Class<?>> cList = new ArrayList<Class<?>>();
		cList.add(MapEntry.class);
		cList.add(InputMonitor.class);
		h = new HibernateHelper(cList, "ecr", "ecr");
		factory = h.getFactory("ecr", "ecr");
		
		mdi = new MapDefinitionSQL();
		mdi.getAllMapDefinitions();
		
		mapSet = _mr.getMapName();
		imap = new InputMap(mdi.getMapDefinition(mapSet).getMapContents()).getMapping();
		
		maps = imap.getObjectMaps();
		mapper = new InputMapper(_mr, parameters );
		mapper.addSourceMonitor(monitorRetrieval(_mr.getFileName()));
	}
	
	
	MapDefinitionInterface mdi;
	
	
	
	private void getMapSet ()  {
		
		mdi = new MapDefinitionSQL();
		mdi.getAllMapDefinitions();
		imap = new InputMap(mdi.getMapDefinition(mapSet).getMapContents()).getMapping();
		maps = imap.getObjectMaps();
		for (InputObjectMap m : maps) {
			
			log.info("Retrieved " + mapSet + " map for object: " + m.getObjectName());
			
			/*
			for (Map.Entry<String, String> entry : m.getFieldMappings().entrySet()) {
			    String key = entry.getKey();
			    Object value = entry.getValue();
			    log.info("Field Target: " + key + " Source: " + value);
			}
			for (Map.Entry<String, String> entry : m.getMethodMappings().entrySet()) {
			    String key = entry.getKey();
			    Object value = entry.getValue();
			    log.info("Method Target: " + key + " Source: " + value);
			}
			*/


		}
		
	}
	
	
	private void getMapsForFiles () {
		
		// search parameters for input files (parameter contains "_FILE")
		for (Entry<String, String> parm : parameters.entrySet()) {
			
			String sParm = parm.getKey();
			Matcher matcher = _FILE.matcher(sParm);
			
			if ( ! matcher.find() )
				continue;
			
			String classKey = sParm.substring(0, matcher.start());
			String sourceListKey = "";
			
			for (PatternReference entry : parmToClassName) {
				matcher = entry.pattern.matcher(sParm);
	    		if(matcher.find()) {
	    			sourceListKey = entry.value;
	    			break;
	    		}
	    	}
			
			System.out.println( "Readied class key=" + classKey + " for " + sourceListKey);
			
			// add the source file name to the list of files to be mapped to the target object
			ArrayList<String> source = sourceList.get(sourceListKey);
			if (source == null) {
				source = new ArrayList<String>();
				source.add(parm.getValue());
				sourceList.put(sourceListKey, source);
			}
			else {
				source.add(parm.getValue());
			}
	    	
			/*
			for (Entry<Pattern, Class<?>> entry : parmToClass.entrySet()) {
	    		Pattern key = entry.getKey();
	    		Class<?> value = entry.getValue();
	    		System.out.println("Found related class=" + value.getSimpleName() + " for key " + key);
	    		// ...
	    	}
	    	*/
	    
		}
		
	}
	
	private HashMap<String, ArrayList<String>> sourceList = new HashMap<String, ArrayList<String>>();
	
	
	static final Pattern _FILE = Pattern.compile("_file", Pattern.CASE_INSENSITIVE);
	
	
	/**
	 * initialize Hibernate to enable input monitor retrieval
	 */
	private void initializeForMonitorRetrieval ()  {
		
		List<Class<?>> cList = new ArrayList<Class<?>>();
		cList.add(InputMonitor.class);
		
		h = new HibernateHelper(cList, "ecr", "ecr");
		factory = h.getFactory("ecr", "ecr");
		
		session = factory.openSession();
	
	}
	
	
	/**
	 * precise get of job when uid is known
	 */
	private InputMonitor monitorRetrieval (String source) {
		
		initializeForMonitorRetrieval();
		
		Transaction tx = null;
		InputMonitor _IM = null;
		
		try {
			

			//
			
			tx = session.beginTransaction();
			

			 Query query = session.createQuery("from InputMonitor where jobuid = :jobuid AND filename = :source "); 
			 query.setParameter("jobuid", parameters.get("jobuid")); 
			 query.setParameter("source", source);
			 

			 List<?> list = query.list(); 
			 if (list.isEmpty()) {
				 log.error("Input Monitor not found for " + source + " in jobuid " + parameters.get("jobuid") + ".  Input Analysis may not have been run since file name was set.");
				 throw new IllegalStateException ("Input Monitor not found for " + source + " in jobuid " + parameters.get("jobuid") + ".  Input Analysis may not have been run since file name was set.");
			 }
			 _IM = (InputMonitor)list.get(0); 

			
			tx.commit();
			
		} catch (HibernateException e) {
			if (tx!=null) tx.rollback();
			e.printStackTrace(); 
		} finally {
			// don't close here - called from a loop
		}
		
		return _IM;
		
	}
	
	
	private void closeMonitorRetrieval () {

		session.close();
		
		try {
			h.close("prd", "ecr");
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	
	/**
	 * gets the statistics for the raw input file and formats them into a html page
	 * @return
	 */
	public String getStatsAsHTML ()  {
		
		micky = new MongoInputCollectionSet(parameters);
		StringBuffer sb = new StringBuffer();
		/*
		sb.append("<!DOCTYPE html>");
		sb.append("<html lang=\"en\">");
		sb.append("<head><title>Input File Statistics</title></head>"); 
		
		sb.append("<body>");
		*/
		
		sb.append("<table style='width:1000px' border=\"1\" cellspacing=\"1\" cellpadding=\"5\">");
		
		sb.append("<tr>");
		sb.append("<th>File Name</th>");
		sb.append("<th>Record Count</th>");
		sb.append("<th>Collection Name</th>");
		sb.append("<th>Document Count</th>");
		sb.append("</tr>");
		
		for (InputObjectMap m : maps) {
			
			//InputMapper mapper = new InputMapper( m.getObjectName(), parameters );
			ArrayList<String> source = sourceList.get(m.getObjectName());
			ArrayList<InputMonitor> sourceMonitors = new ArrayList<InputMonitor>();
			if( source == null  ||  source.isEmpty())
				continue;
			for (String s : source) {
				sourceMonitors.add(monitorRetrieval(s));
			}
			
			switch (m.getObjectName()) {
			case "ClaimServLine":
				sb.append(genInputCounts("Medical Claims", "ClaimServLine", sourceMonitors));
				sb.append(genOutputCounts("Medical Claims", "ClaimServLine", sourceMonitors));
				break;
			case "ClaimRx":
				sb.append(genInputCounts("Pharmacy Claims", "ClaimRx", sourceMonitors));
				sb.append(genOutputCounts("Pharmacy Claims", "ClaimRx", sourceMonitors));
				break;
			case "PlanMember":
				sb.append(genInputCounts("Plan Members", "PlanMember", sourceMonitors));
				sb.append(genOutputCounts("Plan Members", "PlanMember", sourceMonitors));
				break;
			case "Enrollment":
				sb.append(genInputCounts("Enrollments", "Enrollment", sourceMonitors));
				sb.append(genOutputCounts("Enrollments", "Enrollment", sourceMonitors));
				break;
			case "Provider":
				sb.append(genInputCounts("Providers", "Provider", sourceMonitors));
				sb.append(genOutputCounts("Providers", "Provider", sourceMonitors));
				break;
			}
			
		
		}
		
		
		sb.append("</table>");
		
		/*
		sb.append("</body>");
		sb.append("</html>");
		*/
		
		closeMonitorRetrieval();
		
		return sb.toString();
		
	}
	
	

	
	
	private StringBuffer genInputCounts(String objTitle, String objName, ArrayList<InputMonitor> sourceMonitors) {

		StringBuffer sb = new StringBuffer();
		
		sb.append("<tr>");
		
		sb.append("<th>" + objTitle + "</th>");
		sb.append("<th>" + "&nbsp" + "</th>");
		sb.append("<th>" + "target" + "</th>");
		sb.append("<th>" + objName + "</th>");
		sb.append("</tr>");	
		
		for (InputMonitor mon: sourceMonitors) {
			
			//sb.append("<th colspan=\"3\">" + mon.getFilename() + "</th>");
			sb.append("<tr>");
			sb.append("<td>" + mon.getFilename() + "</td>");
			sb.append("<td align=\"right\">" + mon.getRcdCount() + "</td>");
			sb.append("<td>" + "&nbsp" + "</td>");
			sb.append("<td>" + "&nbsp" + "</td>");
			sb.append("</tr>");	
			
		}
					
		
		return sb;
		
	}
	
	
	
	private StringBuffer genOutputCounts(String objTitle, String objName, ArrayList<InputMonitor> sourceMonitors) {
		
		if (!bMongoUp)
			initializeMongoForReport();
			
		StringBuffer sb = new StringBuffer();
		

		switch (objName) {
		case "ClaimServLine":
			coll = db.getCollection(micky.getsClaimColl());
			break;
		case "ClaimRx":
			coll = db.getCollection(micky.getsRxColl());
			break;
		case "PlanMember":
			coll = db.getCollection(micky.getsMemberColl());
			break;
		case "Enrollment":
			coll = db.getCollection(micky.getsEnrollColl());
			break;
		case "Provider":
			coll = db.getCollection(micky.getsProviderColl());
			break;
		}
		
		sb.append("<td>" + "&nbsp" + "</td>");
		sb.append("<td>" + "&nbsp" + "</td>");
		sb.append("<td>" + coll.getFullName() + "</td>");
		sb.append("<td align=\"right\">" + coll.getCount() + "</td>");
		//coll.getCount();
		

		return sb;
		
	}
	
	private DB db;		
	private DBCollection coll;
	private boolean bMongoUp = false;
	
	private void initializeMongoForReport() {
		
		micky = new MongoInputCollectionSet(parameters);
		db = micky.getDb();
		bMongoUp = true;
		
	}


	private void initialize() {
		
		parmToClassName.add(new PatternReference(Pattern.compile("rx"), "ClaimRx"));
		parmToClassName.add(new PatternReference(Pattern.compile("enroll"), "Enrollment"));
		parmToClassName.add(new PatternReference(Pattern.compile("member"), "PlanMember"));
		parmToClassName.add(new PatternReference(Pattern.compile("provider"), "Provider"));
		parmToClassName.add(new PatternReference(Pattern.compile("claim"), "ClaimServLine"));
		parmToClassName.add(new PatternReference(Pattern.compile("prof"), "ClaimServLine"));
		parmToClassName.add(new PatternReference(Pattern.compile("stay"), "ClaimServLine"));
		
	}

	
	public static HashMap<String, Class<?>> getClassNameToClass() {
		return classNameToClass;
	}


	public static void setClassNameToClass(HashMap<String, Class<?>> classNameToClass) {
		MappingController.classNameToClass = classNameToClass;
	}


	static ArrayList<PatternReference> parmToClassName = new ArrayList<PatternReference>(); 
	private static HashMap<String, Class<?>> classNameToClass = new HashMap<String, Class<?>>();
	  
	static {
		
		
		
		getClassNameToClass().put("ClaimRx", ClaimRx.class);
		getClassNameToClass().put("Enrollment", Enrollment.class);
		getClassNameToClass().put("PlanMember", PlanMember.class);
		getClassNameToClass().put("Provider", Provider.class);
		getClassNameToClass().put("ClaimServLine", ClaimServLine.class);
		
	}
	

	
	
	private static org.apache.log4j.Logger log = Logger.getLogger(MappingController.class);
	

	class PatternReference  {
        
		Pattern pattern;
        String value;
        
        PatternReference (Pattern pattern, String value) {
        	this.pattern = pattern;
        	this.value = value;
        }
    
	}
	
	
	/**
	 * main strictly for unit testing
	 * @param args
	 */
	public static void main(String[] args) {
		
		HashMap<String, String> parameters = RunParameters.parameters;
		//parameters.put("jobuid", "1066");
		//parameters.put("clientID", "XG");
		//parameters.put("runname", "XG_GHP_SAMPLE");
		//parameters.put("rundate", "20150820");
		//parameters.put("jobname", "XG_XG_GHP_SAMPLE20150810");
		//parameters.put("mapname", "xg_ghp");
		
		//parameters.put("jobuid", "1067");
		//parameters.put("clientID", "Test");
		//parameters.put("runname", "5_3_freeze");
		//parameters.put("rundate", "20150824");
		//parameters.put("jobname", "Test_5_3_freeze20150824");
		//parameters.put("mapname", "hci3");
		
		//parameters.put("testlimit", "200");
		parameters.put("configfolder", "C:\\workspace\\ECR_Analytics\\trunk\\EpisodeConstruction\\src\\");
		parameters.put("env", "prd");
		//parameters.put("studybegin", "20130101");
		//parameters.put("studyend", "20141231");
		
		parameters.put("stepname", ProcessJobStep.STEP_MAP);
		
		//parameters.put("claim_file2", "C:/input/XGGHP/opaclaimsx.csv");
		//parameters.put("claim_file1", "C:/input/XGGHP/ipclaimsx.csv");
		//parameters.put("claim_rx_file1", "C:/input/XGGHP/pharmclaimsx.csv");
		//parameters.put("enroll_file1", "C:/input/XGGHP/memberenrollx.csv");
		//parameters.put("member_file1", "C:/input/XGGHP/memberx.csv");
		//parameters.put("provider_file1", "C:/input/XGGHP/providersx.csv");
		
		//parameters.put("jobuid", "1067");
		//parameters.put("clientID", "Test");
		//parameters.put("runname", "5_3_freeze");
		//parameters.put("rundate", "20150824");
		//parameters.put("jobname", "Test_5_3_freeze20150824");
		//parameters.put("mapname", "hci3");
		
		//parameters.put("claim_file1", "C:/input/Test/clm_sample.csv");
		//parameters.put("claim_rx_file1", "C:/input/Test/rx_sample.csv");
		//parameters.put("enroll_file1", "C:/input/Test/member_sample.csv");
		//parameters.put("member_file1", "C:/input/Test/member_sample.csv");
		//parameters.put("provider_file1", "C:/input/Test/provider_sample.csv");

		//parameters.put("jobuid", "1082");
		//parameters.put("clientID", "XG");
		//parameters.put("runname", "");
		//parameters.put("rundate", "20150903");
		//parameters.put("jobname", "XG_GHC20150903");
		//parameters.put("mapname", "xg_ghc");

		//parameters.put("claim_file1", "C:/input/XGGHC/single_MED.dat");
		//parameters.put("claim_rx_file1", "C:/input/XGGHC/single_RX.dat");
		//parameters.put("enroll_file1", "C:/input/XGGHC/single_MBRMONTHS.dat");
		//parameters.put("member_file1", "C:/input/XGGHC/single_MBR.dat");
		//parameters.put("provider_file1", "C:/input/XGGHC/single_PRV.dat");


		//parameters.put("jobuid", "1068");
		//parameters.put("clientID", "XG");
		//parameters.put("runname", "GHP_Commercial");
		//parameters.put("rundate", "20150826");
		//parameters.put("jobname", "XG_GHP_Commercial20150826");
		//parameters.put("mapname", "xg_ghp");

		//parameters.put("claim_file1", "C:/input/XGGHP/ipclaims_commercial.csv");
		//parameters.put("claim_file2", "C:/input/XGGHP/opaclaimsx.csv");
		//parameters.put("claim_rx_file1", "C:/input/XGGHP/pharmclaims_commercial.csv");
		//parameters.put("enroll_file1", "C:/input/XGGHP/memberenroll_commercial.csv");
		//parameters.put("member_file1", "C:/input/XGGHP/member_commercial.csv");
		//parameters.put("provider_file1", "C:/input/XGGHP/providers.csv");
		
		/*
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

		
		parameters.put("jobuid", "1119");
		parameters.put("clientID", "PEBTF");
		parameters.put("runname", "PEBTF_maptest");
		parameters.put("rundate", "20160720");
		parameters.put("jobname", "PEBTF_maptest20160720");
		parameters.put("mapname", "pebtf");

		parameters.put("claim_file1", "C:/input/PEBTF/unitchk.csv");
		parameters.put("claim_rx_file1", "C:/input/PEBTF/Rx_sample.csv");
		parameters.put("enroll_file1", "C:/input/PEBTF/member_hdr.csv");
		parameters.put("member_file1", "C:/input/PEBTF/member_hdr.csv");
		parameters.put("provider_file1", "C:/input/PEBTF/unitchk.csv");
		



		JobStepManager jsm = new JobStepManager(Long.parseLong(parameters.get("jobuid")));
		MappingController _MC = new MappingController(parameters);
		
		jsm.updateReport(ProcessJobStep.STEP_MAP, _MC.getStatsAsHTML());
		
		//System.out.println("Not what I meant to do");

	}

	

}
