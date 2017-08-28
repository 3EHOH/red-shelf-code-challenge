






public class claimsByStudyPeriodReport {
	
	
	HashMap<String, String> parameters;
	
	
	Mongo mongoClient;
	DB db;
	DBCursor cursor;
	BasicDBObject query;
	DBCollection coll;
	
	String schemaName=null;
	String collName=null;
	String collNameRx=null;
	String jobUid=null;
	
	MongoInputCollectionSet micky;
	Report rpt = new Report();
	
	int resultSize=0;
	int resultSizeRX=0;
	
	boolean debugMode = false;
	
	static final String BEGINDTFIELD ="begin_date";
	static final String RXFILLDTFIELD ="rx_fill_date";
	String verifyMsg = null;
	boolean hasBeginDt = false;
	boolean hasRXFillDt = false;
	
	/**
	 * static parameter constructor, just for testing
	 */
	public claimsByStudyPeriodReport() {	}
	
	/**
	 * constructor using parameters pulled from control database
	 * @param parameters
	 */
	public claimsByStudyPeriodReport(HashMap<String, String> parameters) {
		this.parameters = parameters;
	}
	
	
	private void process () {
		
		initialize();
		if(debugMode){
			System.out.println("init done!");
		}
		
		verifyMsg = dataVerification();
		if(debugMode){
			System.out.println("dataVerification done! verifyMsg="+verifyMsg);
		}

		if(hasBeginDt){
			generateReport(null);
		}
		if(debugMode){
			System.out.println("generateReport done!");
		}

		if(hasRXFillDt){
			generateReport("RX");
		}
		if(debugMode){
			System.out.println("generateReportRX done!");
		}
		
		log.info(getStatsAsHTML());
		
		storeReport();
		if(debugMode){
			System.out.println("storeReport done!");
		}
		
	}
	
	private boolean fieldExist(String fieldName) {
	boolean fieldExist = false;
	
	BasicDBObject query = new BasicDBObject(); 
	BasicDBObject fields=null;
	int cnt = 0;
	String reqField=null;
	int nums=10;
	
	if(RXFILLDTFIELD.equalsIgnoreCase(fieldName)){
		coll = db.getCollection(collNameRx);
	}
	
	
	if(!fieldExist){
		fields = new BasicDBObject(fieldName,true).append("_id",false);;
		DBCursor curs = coll.find(query, fields);
		cnt = 0;
		reqField = null;
		while(curs.hasNext()) {
			   DBObject o = curs.next();
			   reqField =(String)o.get(fieldName);
			   if(reqField !=null){
				   fieldExist = true;
				   break;
			   }
			   cnt++;
			   if(cnt>nums){
				   break;
			   }
		}
	}
	return fieldExist;
	}
	
	private String dataVerification() {

		String errorMsg = null;
		
		hasBeginDt = fieldExist(BEGINDTFIELD);
		
		hasRXFillDt = fieldExist(RXFILLDTFIELD);
		
		if(debugMode){
			System.out.println("hasBeginDt="+hasBeginDt+" hasRXFillDt="+hasRXFillDt);
		}
		return errorMsg;
	}
		

	
	
	@SuppressWarnings("deprecation")
	private void generateReport(String rptType) {
		
		if("RX".equalsIgnoreCase(rptType)){
			coll = db.getCollection(collNameRx);
		}else{
			coll = db.getCollection(collName);
		}
		
		rpt.medicalClaimsTotalRecords = coll.getCount();
		if(debugMode){
			System.out.println("getRXMedicalClaimValues collection recCnt="+rpt.medicalClaimsTotalRecords+" rptType="+rptType);
		}
		
		//build qury
		List<Object> substrList = null;
		if("RX".equalsIgnoreCase(rptType)){
			substrList = Arrays.asList(new Object[]{"$rx_fill_date", 0, 7});
		}else{
			substrList = Arrays.asList(new Object[]{"$begin_date", 0, 7});
		}
		
        DBObject monthProjection = new BasicDBObject("$substr", substrList);
		
        DBObject projectFields = new BasicDBObject("month_year", "$master_claim_id");
        projectFields.put("month_year", monthProjection);
        DBObject project = new BasicDBObject("$project", projectFields );
        
        DBObject groupFields = new BasicDBObject( "_id", "$month_year");
	    groupFields.put("count", new BasicDBObject( "$sum", 1));
		DBObject group = new BasicDBObject("$group", groupFields );

		DBObject sortFields = new BasicDBObject("_id", 1);
	    DBObject sort = new BasicDBObject("$sort", sortFields );
	    if(debugMode){
	    	System.out.println("start aggregate count "+rptType);
	    }
		AggregationOutput output = coll.aggregate(project, group, sort);
	    if(debugMode){
	    	System.out.println("Complete aggregate count "+rptType);
	    }
	    int idx = 0;
	    for (DBObject obj : output.results()) {
			if("RX".equalsIgnoreCase(rptType)){
				rpt.monthYearsRX[idx]=(String)obj.get("_id");
				rpt.recTotalRX[idx]=Long.parseLong(obj.get("count").toString());
			}else{
				rpt.monthYears[idx]=(String)obj.get("_id");
				rpt.recTotal[idx]=Long.parseLong(obj.get("count").toString());
			}
            
    		if(debugMode){
    			System.out.println("monthYear="+rpt.monthYears[idx]+" cnt="+rpt.recTotal[idx]);
    		}
    		idx++;
	    };	    

		if("RX".equalsIgnoreCase(rptType)){
			resultSizeRX = idx;
		}else{
			resultSize = idx;
		}
	    
		//log.info(getStatsAsHTML());
		
	}

	
	
	private void storeReport() {
		
		List<Class<?>> cList = new ArrayList<Class<?>>();
        cList.add(ProcessReport.class);
		
		String env = parameters.get("env") == null ?  "prd" :  parameters.get("env");
        
        HibernateHelper h = new HibernateHelper(cList, env, schemaName);
        SessionFactory factory = h.getFactory(env, schemaName);

        Session session = factory.openSession();
        session.setCacheMode(CacheMode.IGNORE);
        session.setFlushMode(FlushMode.COMMIT);
        Transaction tx = null;
        
        try{
			tx = session.beginTransaction();
			
			String hql = "FROM ProcessReport WHERE jobuid = :jobuid AND stepname = '" + ProcessJobStep.STEP_POSTMAP_REPORT + "' and reportName='"+rpt.reportName+"'";
					
			Query query = session.createQuery(hql).setLockOptions(new LockOptions(LockMode.PESSIMISTIC_WRITE));
//			query.setParameter("jobuid", parameters.get("jobuid"));
			query.setParameter("jobuid", jobUid);

			@SuppressWarnings("unchecked")
			List<ProcessReport> results = query.list();
			ProcessReport r;
			if (results.isEmpty()) {
				r = new ProcessReport();
//				r.setJobUid(Long.parseLong(parameters.get("jobuid")));
				r.setJobUid(Long.parseLong(jobUid));
				r.setStepName(ProcessJobStep.STEP_POSTMAP_REPORT);
				r.setReportName(rpt.reportName);
			} else
				r = results.get(0);
			r.setReport(new SerialBlob(getStatsAsHTML().getBytes()));
			
			session.save(r);
			
			tx.commit();
			
		}catch (HibernateException e) {
			if (tx!=null) tx.rollback();
			e.printStackTrace(); 
		} catch (SerialException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally {
			session.close(); 

			try {
				h.close("prd", schemaName);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
        
	}

	/**
	 * gets the statistics for the raw input file and formats them into a html page
	 * @return
	 */
	private String getStatsAsHTML ()  {
		
		StringBuffer sb = new StringBuffer();
		
		sb.append("<b>Medical Claim File</b>");
		sb.append("<table class=\"sortable\" border=\"1\" cellspacing=\"1\" cellpadding=\"5\" width=\"750px\" >");
		sb.append("<tr bgcolor=\"#99CCFF\">");
		sb.append("<th width=\"200px\">Service From Date(month/year)</th>");
		sb.append("<th width=\"100px\">Number of Claims</th>");
		sb.append("</tr>");
		

		if(!hasBeginDt || resultSize ==0){
    		sb.append("<tr><td colspan='2'>Missing beign_dt field or No records found!</td></tr>");
		}else{
			for (int i=0; i<resultSize; i++) {
				sb.append("<tr>");
				sb.append("<td align=\"right\">" + rpt.monthYears[i] + "</td>"); 	
				sb.append("<td align=\"right\">" + rpt.recTotal[i] + "</td>"); 	
				sb.append("</tr>");
				if(debugMode){
					System.out.println("monthYear="+rpt.monthYears[i]+" cnt="+rpt.recTotal[i]);
				}
			};
		}

		sb.append("</table><br><br><b>RX File</b>");
		
		sb.append("<table class=\"sortable\" border=\"1\" cellspacing=\"1\" cellpadding=\"5\" width=\"750px\" >");
		sb.append("<tr bgcolor=\"#99CCFF\">");
		sb.append("<th width=\"200px\">RX Fill Date(month/year)</th>");
		sb.append("<th width=\"100px\">Number of Claims</th>");
		sb.append("</tr>");

		if(!hasRXFillDt || resultSizeRX ==0){
    		sb.append("<tr><td colspan='2'>Missing rx_fill_dt field or No records found!</td></tr>");
		}else{
			for (int i=0; i<resultSizeRX; i++) {
				sb.append("<tr>");
				sb.append("<td align=\"right\">" + rpt.monthYearsRX[i] + "</td>"); 	
				sb.append("<td align=\"right\">" + rpt.recTotalRX[i] + "</td>"); 	
				sb.append("</tr>");
				if(debugMode){
					System.out.println("monthYear="+rpt.monthYearsRX[i]+" cnt="+rpt.recTotalRX[i]);
				}
			};
		}

		sb.append("</table>");

		
		return sb.toString();
		
	}

	
	
	
	private void initialize ()  {

		micky = new MongoInputCollectionSet(parameters);
		db = micky.getDb();
		
		collName=micky.getsClaimColl();
		collNameRx = micky.getsRxColl();
		jobUid=parameters.get("jobuid");
        schemaName = parameters.get("jobname")  == null ?  "javatest" : parameters.get("jobname");
		
		//test settings
        /*collName="";
        collNameRx="";
        schemaName="";
        jobUid="";*/
                
        
		
		coll = db.getCollection(collName);
		if(debugMode){
			System.out.println("collName="+collName+" schemaName="+schemaName+"jobId="+jobUid);
		}
		
		
	}
	
	class Report {
		
		long medicalClaimsTotalRecords = 0l;
		int maxSize=300;
		String monthYears[] =new String[maxSize];
		long recTotal[] =new long[maxSize];
		
		String monthYearsRX[] =new String[maxSize];
		long recTotalRX[] =new long[maxSize];

		String reportName ="ClaimsByStudyPeriodReport";
	}
	//Arrays.fill(medicalClaimsTotalCosts, BigDecimal.ZERO);
	
	public static void main(String[] args) {
		
		log.info("Starting claimsByStudyPeriodReport Report");

		claimsByStudyPeriodReport instance = new claimsByStudyPeriodReport();
		
		// get parameters and make them available to all
		RunParameters rp = new RunParameters();
		instance.parameters = rp.loadParameters(args, parameterDefaults);
				
		String s = instance.parameters.get("runname") + "_" + instance.parameters.get("rundate");
		instance.parameters.put("jobname", s);
		
				
		RunParameters.parameters.put("configfolder", instance.parameters.get("configfolder"));
		
		instance.process();
		
		log.info("Complete claimsByStudyPeriod Report");
	}
	

	static String [][] parameterDefaults = {
		{"configfolder", "C:/workspace/ECR_Analytics/trunk/EpisodeConstruction/src/"},
		{"rundate", "20150611"},
		{"runname", "CHC"},
		{"jobuid", "1059"} //hardcode for test
	};
	
	
	private static org.apache.log4j.Logger log = Logger.getLogger(claimsByStudyPeriodReport.class);

}
