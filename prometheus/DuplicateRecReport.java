
//import java.util.Arrays.asList;




/*import com.mongodb.BasicDBObject;


public class DuplicateRecReport {
	
	
	HashMap<String, String> parameters;
	
	
	Mongo mongoClient;
	DB db;
	BasicDBObject query;
	DBCollection coll;
	
	String schemaName=null;
	String collName=null;
	String jobUid=null;
	
	MongoInputCollectionSet micky;
	Report rpt = new Report();
	Cursor aggregateOutput = null;	
	int idx=0;
    long cnt = 0l;
    int pgCnt =0;//# of pages/files
    boolean debugMode = false;
    int maxRec = 25000;//max rows per file/page
	StringBuffer[] sbs = null;     

	static final String MEMIDFIELD ="member_id";
	static final String CLAIMIDFIELD ="claim_id";
	static final String CLAIMLNIDFIELD ="claim_line_id";
	static final String BEGINDTFIELD ="begin_date";
	static final String ENDDTFIELD ="end_date";
	static final String ALLOWEDAMTFIELD ="allowed_amt";
	static final String CHARGEAMTFIELD ="charge_amt";
	static final String PROVIDERIDFIELD ="provider_id";
	String verifyMsg = null;
	boolean hasMemId = false;
	boolean hasClaimId = false;
	boolean hasClaimLnId = false;
	boolean hasAllowedAmt = false;
	boolean hasChargeAmt = false;
	boolean hasBeginDt = false;
	boolean hasEndDt = false;
	boolean hasProviderId = false;
	
	/**
	 * static parameter constructor, just for testing
	 */
	public DuplicateRecReport() {	}
	
	/**
	 * constructor using parameters pulled from control database
	 * @param parameters
	 */
	public DuplicateRecReport(HashMap<String, String> parameters) {
		this.parameters = parameters;
	}
	
	
	private void process () {
		initialize();
		log.info("init done!");

		verifyMsg = dataVerification();
		if(debugMode){
			System.out.println("dataVerification done! verifyMsg="+verifyMsg);
		}

		if(verifyMsg == null){
			generateReport();
		}
		log.info("generateReport done!");
		
		storeReport();
		log.info("storeReport done!");
		
	}
	
	
	private boolean fieldExist(String fieldName) {
	boolean fieldExist = false;
	
	BasicDBObject query = new BasicDBObject(); 
	BasicDBObject fields=null;
	int cnt = 0;
	String reqField=null;
	int nums=10;
	
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

		StringBuffer errorMsg = null;
		
		hasMemId = fieldExist(MEMIDFIELD);
		hasClaimId = fieldExist(CLAIMIDFIELD);
		hasClaimLnId = fieldExist(CLAIMLNIDFIELD);
		
		hasBeginDt = fieldExist(BEGINDTFIELD);
		hasEndDt = fieldExist(ENDDTFIELD);

		hasAllowedAmt = fieldExist(ALLOWEDAMTFIELD);
		hasChargeAmt = fieldExist(CHARGEAMTFIELD);
		hasProviderId = fieldExist(PROVIDERIDFIELD);

		if(debugMode){
			System.out.println("md="+hasMemId+" CId="+hasClaimId+" CLnId="+hasClaimLnId+" BDt="+hasBeginDt+" eDt="+hasEndDt+" AAmt="+hasAllowedAmt+"cAmt="+hasChargeAmt+" PId="+hasProviderId);
		}
		if(!hasMemId || ! hasClaimId || !hasClaimLnId || !hasBeginDt || (!hasAllowedAmt && !hasChargeAmt) || !hasProviderId){
			errorMsg=new StringBuffer();
			errorMsg.append("Missing comparision fields, can not run the report!");
			return errorMsg.toString();
		}else{
			return null;
		}
	}
		

	
	

	
	
	private void generateReport() {

		rpt.medicalClaimsTotalRecords = coll.getCount();
		if(debugMode){
			System.out.println("Collection ="+coll+" recCnt="+rpt.medicalClaimsTotalRecords);
		}
		
		//build query
		DBObject groupFields = new BasicDBObject("_id", new BasicDBObject("mem_id",  "$member_id")
        		.append("claim_id",  "$claim_id")
        		.append("claim_line_id", "$claim_line_id")
        		.append("begin_date", "$begin_date")
        		.append("end_date", "$end_date")
        		.append("allowed_amt", "$allowed_amt")
        		.append("provider_id", "$provider_id")
				);
 	    groupFields.put("count", new BasicDBObject( "$sum", 1));
		DBObject group = new BasicDBObject("$group", groupFields );
		
		DBObject match = new BasicDBObject("$match", 
				   new BasicDBObject("count",   new BasicDBObject("$gt", 1)  ));
		
		DBObject sortFields = new BasicDBObject("count", -1);
	    DBObject sort = new BasicDBObject("$sort", sortFields );
		
		List<DBObject> aggregationQuery = Arrays.<DBObject>asList(
	            group,
	            match,
	            sort	    );
		if(debugMode){
			System.out.println("qry="+aggregationQuery);
			System.out.println("start aggregate count");
		}

		if(debugMode){
			log.info("qry="+aggregationQuery);
			log.info("start aggregate count");
		}
		
	    aggregateOutput = coll.aggregate(
	            aggregationQuery,
	            AggregationOptions.builder()
	            	.outputMode(AggregationOptions.OutputMode.CURSOR)
	                .allowDiskUse(true)
	                .build()
	    );
	    
		if(debugMode){
			System.out.println("done aggregate count");
		}
//		log.info(getStatsAsHTML());
		
	}

	
	
	private void storeReport() {
		
    	getStatsAsHTML();
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
			if(pgCnt == -1){
				pgCnt = 1;
			}else{
				pgCnt++;
			}
			for(int i=0; i<pgCnt; i++){
				
				String rptName =rpt.reportName+Integer.toString(i+1);			
			
				String hql = "FROM ProcessReport WHERE jobuid = :jobuid AND stepname = '" + ProcessJobStep.STEP_POSTMAP_REPORT + "' and reportName='"+rptName+"'";
				Query query = session.createQuery(hql).setLockOptions(new LockOptions(LockMode.PESSIMISTIC_WRITE));
//			query.setParameter("jobuid", parameters.get("jobuid"));
				query.setParameter("jobuid", jobUid);

				@SuppressWarnings("unchecked")
				List<ProcessReport> results = query.list();
				ProcessReport r;
				if (results.isEmpty()) {
					r = new ProcessReport();
					r.setJobUid(Long.parseLong(jobUid));
					r.setStepName(ProcessJobStep.STEP_POSTMAP_REPORT);
					r.setReportName(rptName);
				} else
					r = results.get(0);
				//r.setReport(new SerialBlob(getStatsAsHTML().getBytes()));
				r.setReport(new SerialBlob(sbs[i].toString().getBytes()));
				if(debugMode){
					System.out.println("storeReport i="+i+" rptName="+rptName+" sz="+sbs[i].toString().length());
				}			
				session.save(r);
			}//endfor(int i=1; i<=batchCnt; i++){
			
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
	private void getStatsAsHTML ()  {
		boolean found = false;
		StringBuffer sb = new StringBuffer();
		int maxPage=50;//total report pages
		
		sbs = new StringBuffer[maxPage]; 
		for(int i=0; i<maxPage; i++){
			sbs[i] = new StringBuffer();
			sbs[i].append("<table  class=\"sortable\" border=\"1\" cellspacing=\"1\" cellpadding=\"5\" width=\"750px\" >");
			sbs[i].append("<tr bgcolor=\"#99CCFF\">");
			sbs[i].append("<th width=\"650px\">MembetId/ClaimId/ClaimLineId/BeginDt/EndDt/AllowedAmt/ProviderId</th>");
			sbs[i].append("<th width=\"100px\">Count</th>");
			sbs[i].append("</tr>");

		}		
		
		if(verifyMsg != null){
			sbs[0].append("<tr><td colspan='2'>"+verifyMsg+"</td></tr></table>");
			pgCnt =-1;
		}else{
			idx = 0;
			StringBuffer keys = new StringBuffer(); 
			while ( aggregateOutput.hasNext() ) {
				found = true;
				DBObject docObj = aggregateOutput.next();
				DBObject idObj=(DBObject)docObj.get("_id");
				keys = new StringBuffer();
				keys.append((String)idObj.get("mem_id"));
				keys.append("/").append((String)idObj.get("claim_id"));
				keys.append("/").append((String)idObj.get("claim_line_id"));
				keys.append("/").append((String)idObj.get("begin_date"));
				keys.append("/").append((String)idObj.get("end_date"));
				keys.append("/").append((String)idObj.get("allowed_amt"));
				keys.append("/").append((String)idObj.get("provider_id"));
				cnt = Long.parseLong(docObj.get("count").toString());
		
				sb.append("<tr>");
				sb.append("<td align=\"right\">" + keys + "</td>"); 	
				sb.append("<td align=\"right\">" + cnt + "</td>"); 	
				sb.append("</tr>");
				idx++;
				if(idx>maxRec){
					sb.append("</table>");
					sbs[pgCnt].append(sb);
					pgCnt++;
					idx = 0;
					sb = new StringBuffer();
				}
			} 
			if(debugMode){
				System.out.println("pgCnt="+pgCnt+"  found="+found);
			}
			if(!found){
				sbs[0].append("<tr><td colspan=3>No Duplicates Found!</td></tr></table>");
				pgCnt =-1;
			}else{
				sb.append("</table>");
				sbs[pgCnt].append(sb);				
			}
		}
		
		return ;
		
	}

	
	
	
	private void initialize ()  {

		micky = new MongoInputCollectionSet(parameters);
		db = micky.getDb();		
		collName=micky.getsClaimColl();
		jobUid=parameters.get("jobuid");
        schemaName = parameters.get("jobname")  == null ?  "javatest" : parameters.get("jobname");
		
		//test settings
        /*collName="";
        schemaName="";
        jobUid="";*/
        
		
		coll = db.getCollection(collName);
		if(debugMode){
			System.out.println("collName="+collName+" schemaName="+schemaName+" jobId="+jobUid);
		}
		
	}
	
	class Report {
		
		long medicalClaimsTotalRecords = 0l;
		BigDecimal medicalClaimsTotalAllowedAmt = new BigDecimal("0.00");
		long pharmacyClaimsTotalRecords = 0l;
		BigDecimal pharmacyClaimsTotalAllowedAmt = new BigDecimal("0.00");
		
		BigDecimal medicalClaimsTotalChargeAmt = new BigDecimal("0.00");
		
		int maxSize = 150;
		String billTypes[] =new String[maxSize];
		String billTypesDesc[] =new String[maxSize];
		long recTotal[] =new long[maxSize];
		long recNPITotal[] =new long[maxSize];
		long recDRGTotal[] =new long[maxSize];
		long recPIDTotal[] =new long[maxSize];
		float recNPITotalPerc[] =new float[maxSize];
		float recDRGTotalPerc[] =new float[maxSize];
		float recPIDTotalPerc[] =new float[maxSize];
		
		BigDecimal[] medicalClaimsTotalCosts = new BigDecimal[maxSize];
		String reportName ="DuplicateRecordsReport";
		
		
	}
	//Arrays.fill(medicalClaimsTotalCosts, BigDecimal.ZERO);
	
	public static void main(String[] args) {
		
		log.info("Starting Post Mapping Control Report");

		DuplicateRecReport instance = new DuplicateRecReport();
		
		// get parameters and make them available to all
		RunParameters rp = new RunParameters();
		instance.parameters = rp.loadParameters(args, parameterDefaults);
				
		String s = instance.parameters.get("runname") + "_" + instance.parameters.get("rundate");
		instance.parameters.put("jobname", s);
		
				
		RunParameters.parameters.put("configfolder", instance.parameters.get("configfolder"));
		
		instance.process();
		
		
		log.info("Complete Duplicate Report");
	}
	

	static String [][] parameterDefaults = {
		{"configfolder", "C:/workspace/ECR_Analytics/trunk/EpisodeConstruction/src/"},
		{"rundate", "20150611"},
		{"runname", "CHC"},
		{"jobuid", "1043"}
	};
	
	
	private static org.apache.log4j.Logger log = Logger.getLogger(DuplicateRecReport.class);

}
