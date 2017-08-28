




/*import com.mongodb.BasicDBObject;


public class PrincipalProcICD9CodeDistReport {
	
	
	HashMap<String, String> parameters;
	
	
	Mongo mongoClient;
	DB db;
	DBCursor cursor;
	BasicDBObject query;
	DBCollection coll;
	
	String schemaName=null;
	String collName=null;
	String jobUid=null;
	
	MongoInputCollectionSet micky;
	Report rpt = new Report();
	AggregationOutput output = null;
	
	boolean debugMode = false;
	static final String BILLTYPEFIELD ="type_of_bill";
	static final String CLAIMTYPEFIELD ="claim_line_type_code";
	String verifyMsg = null;
	boolean hasBillType = false;
	boolean hasClaimType = false;

	
	/**
	 * static parameter constructor, just for testing
	 */
	public PrincipalProcICD9CodeDistReport() {	}
	
	/**
	 * constructor using parameters pulled from control database
	 * @param parameters
	 */
	public PrincipalProcICD9CodeDistReport(HashMap<String, String> parameters) {
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
		
		if(verifyMsg == null){
			generateReport();
			if(debugMode){
				System.out.println("generateReport done!");
			}
		}
		
		storeReport(rpt.medicalClaimsTotalRecords11);

		
		
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
		
		hasBillType = false;
		hasBillType = fieldExist(BILLTYPEFIELD);
		
		hasClaimType = false;
		if(!hasBillType){//bilType not found, then check cliam_line_type_code
			hasClaimType = fieldExist(CLAIMTYPEFIELD);
		}
		
		if(!hasBillType && !hasClaimType){
			errorMsg ="Both "+BILLTYPEFIELD+" and "+CLAIMTYPEFIELD+" are missing, can not run the report!";
			return errorMsg;
		}

		if(debugMode){
			System.out.println("BType="+hasBillType+" CType="+hasClaimType);
		}
		return errorMsg;
	}
		
	

	@SuppressWarnings("deprecation")
	private void generateReport() {
		
		rpt.medicalClaimsTotalRecords = coll.getCount();
		
		if(debugMode){
			System.out.println("generateReport collection recCnt="+rpt.medicalClaimsTotalRecords);
		}
		
		DBObject matchType = null;
		//build qury
		if(hasBillType){
			DBObject c0 = new BasicDBObject("type_of_bill", new BasicDBObject("$eq", "110"));
			DBObject c1 = new BasicDBObject("type_of_bill", new BasicDBObject("$eq", "111"));
			DBObject c2 = new BasicDBObject("type_of_bill", new BasicDBObject("$eq", "112"));
			DBObject c3 = new BasicDBObject("type_of_bill", new BasicDBObject("$eq", "113"));
			DBObject c4 = new BasicDBObject("type_of_bill", new BasicDBObject("$eq", "114"));
			DBObject c5 = new BasicDBObject("type_of_bill", new BasicDBObject("$eq", "115"));
			DBObject c6 = new BasicDBObject("type_of_bill", new BasicDBObject("$eq", "116"));
			DBObject c7 = new BasicDBObject("type_of_bill", new BasicDBObject("$eq", "117"));
			DBObject c8 = new BasicDBObject("type_of_bill", new BasicDBObject("$eq", "118"));
			DBObject c9 = new BasicDBObject("type_of_bill", new BasicDBObject("$eq", "119"));
		
			BasicDBList orClauses = new BasicDBList();
			orClauses.add(c0);
			orClauses.add(c1);
			orClauses.add(c2);
			orClauses.add(c3);
			orClauses.add(c4);
			orClauses.add(c5);
			orClauses.add(c6);
			orClauses.add(c7);
			orClauses.add(c8);
			orClauses.add(c9);
			BasicDBObject or = new BasicDBObject("$or", orClauses);
		    matchType = new BasicDBObject("$match", or);
		}else{
		    matchType = new BasicDBObject("$match", new BasicDBObject("claim_line_type_code", "IP"));	    
		}

		  DBObject groupFields1 = new BasicDBObject( "_id", null);
		  groupFields1.put("recCount", new BasicDBObject( "$sum", 1));
	      DBObject group1 = new BasicDBObject("$group", groupFields1);
	      AggregationOutput output1 = coll.aggregate( matchType,  group1 );
	      for (DBObject obj : output1.results()) {
	            rpt.medicalClaimsTotalRecords11 = Long.parseLong(obj.get("recCount").toString());
	            if(debugMode){
	            	System.out.println("total proc 11x Rec="+rpt.medicalClaimsTotalRecords11);
	            }
	        }
		
			DBObject unwind = new BasicDBObject("$unwind", "$med_codes.med_codes");
			
			DBObject m1 = new BasicDBObject("med_codes.med_codes.nomen", new BasicDBObject("$eq", "PX"));
			DBObject m2 = new BasicDBObject("med_codes.med_codes.principal", new BasicDBObject("$eq", "1"));
			BasicDBList andClauses = new BasicDBList();
			  andClauses.add(m1);
			  andClauses.add(m2);
			BasicDBObject and = new BasicDBObject("$and", andClauses);
			DBObject matchPX = new BasicDBObject("$match", and);
			
        DBObject groupFields = new BasicDBObject( "_id", "$med_codes.med_codes.code_value");
	    groupFields.put("count", new BasicDBObject( "$sum", 1));
        
	    
        DBObject group = new BasicDBObject("$group", groupFields);
        
		DBObject sortFields = new BasicDBObject("count", -1);
	    DBObject sort = new BasicDBObject("$sort", sortFields );
        
	    if(debugMode){
	    	System.out.println("start aggregate count");
	    }
		output = coll.aggregate(matchType, unwind,  matchPX, group, sort );        
		if(debugMode){
			System.out.println("Complete aggregate count");
		}
	    
		log.info(getStatsAsHTML(rpt.medicalClaimsTotalRecords11));
		
	}

	
	/*
	private void generateReportByprincipal_proc_code() {
		
		DBCollection coll = db.getCollection(micky.getsClaimColl());
//		coll = db.getCollection("CHCClaimServLine20150611");
		
		rpt.medicalClaimsTotalRecords = coll.getCount();
		
		//build qury for 11 code
		DBObject unwind = new BasicDBObject("$unwind", "$principal_proc_code");
		
		DBObject c0 = new BasicDBObject("type_of_bill", new BasicDBObject("$eq", "110"));
		DBObject c1 = new BasicDBObject("type_of_bill", new BasicDBObject("$eq", "111"));
		DBObject c2 = new BasicDBObject("type_of_bill", new BasicDBObject("$eq", "112"));
		DBObject c3 = new BasicDBObject("type_of_bill", new BasicDBObject("$eq", "113"));
		DBObject c4 = new BasicDBObject("type_of_bill", new BasicDBObject("$eq", "114"));
		DBObject c5 = new BasicDBObject("type_of_bill", new BasicDBObject("$eq", "115"));
		DBObject c6 = new BasicDBObject("type_of_bill", new BasicDBObject("$eq", "116"));
		DBObject c7 = new BasicDBObject("type_of_bill", new BasicDBObject("$eq", "117"));
		DBObject c8 = new BasicDBObject("type_of_bill", new BasicDBObject("$eq", "118"));
		DBObject c9 = new BasicDBObject("type_of_bill", new BasicDBObject("$eq", "119"));
		
		BasicDBList orClauses = new BasicDBList();
		  orClauses.add(c0);
		  orClauses.add(c1);
		  orClauses.add(c2);
		  orClauses.add(c3);
		  orClauses.add(c4);
		  orClauses.add(c5);
		  orClauses.add(c6);
		  orClauses.add(c7);
		  orClauses.add(c8);
		  orClauses.add(c9);
		  BasicDBObject or = new BasicDBObject("$or", orClauses);
		  DBObject match = new BasicDBObject("$match", or);

	        DBObject groupFields1 = new BasicDBObject( "_id", null);
		    groupFields1.put("recCount", new BasicDBObject( "$sum", 1));
	        DBObject group1 = new BasicDBObject("$group", groupFields1);
	    	AggregationOutput output1 = coll.aggregate( match,  group1 );
	        for (DBObject obj : output1.results()) {
	            rpt.medicalClaimsTotalRecords11 = Long.parseLong(obj.get("recCount").toString());
	        }
		
		  
        DBObject groupFields = new BasicDBObject( "_id", "$principal_proc_code");
	    groupFields.put("count", new BasicDBObject( "$sum", 1));
        
	    
        DBObject group = new BasicDBObject("$group", groupFields);
        
		DBObject sortFields = new BasicDBObject("count", -1);
	    DBObject sort = new BasicDBObject("$sort", sortFields );
        

		System.out.println("start aggregate count");
        output = coll.aggregate( unwind, match,  group, sort );        
		
		System.out.println("Complete aggregate count");
	    
		log.info(getStatsAsHTML(rpt.medicalClaimsTotalRecords11));
		
	}
	*/
	
	
	
	private void storeReport(long totalRecords) {
		
		List<Class<?>> cList = new ArrayList<Class<?>>();
        cList.add(ProcessReport.class);
		
		String env = parameters.get("env") == null ?  "prd" :  parameters.get("env");
//        String schemaName = parameters.get("jobname")  == null ?  "javatest" : parameters.get("jobname");
        
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
			r.setReport(new SerialBlob(getStatsAsHTML(totalRecords).getBytes()));
			
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
	private String getStatsAsHTML (long totalRecords)  {
		
		boolean found =false;
		StringBuffer sb = new StringBuffer();
		String pProcCode=null;
		long recCnt = 0; //total records from output
		double recDispSize = 0;

        for (DBObject obj : output.results()) {
            pProcCode = (String) obj.get("_id");
            recCnt++;
        }
		recDispSize= (long)recCnt*0.2;
		recCnt = 0;
		
		sb.append("<table  class=\"sortable\" border=\"1\" cellspacing=\"1\" cellpadding=\"5\" width=\"700px\" >");
		sb.append("<tr  bgcolor=\"#99CCFF\">");
		sb.append("<th width=\"200px\">Principal Proc Code</th>");
		sb.append("<th width=\"200px\">Total Number of Records</th>");
		sb.append("<th width=\"300px\">% of Records Populated</th>");
		sb.append("</tr>");

        for (DBObject obj : output.results()) {
            pProcCode = (String) obj.get("_id");
            
        	long cnt = Long.parseLong(obj.get("count").toString());
            
            float percentAmt = (float)cnt / totalRecords * 100;
            
            recCnt++;
            
//            if(percentAmt>0.10){
              if(recCnt<recDispSize){
            	if(!found){  
            		sb.append("<tr><td colspan='3'>Top 20% Most Frequently Occur Principal Proc Codes</td></tr>");
            		found = true;
            	}
   	    		sb.append("<tr>");
        		sb.append("<td align=\"right\">" + pProcCode + "</td>"); 	
        		sb.append("<td align=\"right\">" + cnt + "</td>"); 				
        		sb.append("<td align=\"right\">"+String.format("%.6f", percentAmt)+"%</td>"); 				
        		sb.append("</tr>");
            }
        }        
        if(!found){
    		sb.append("<tr><td colspan=3>No Record Found.</td</tr>");
        }
		
		sb.append("</table>");
		
		return sb.toString();
		
	}

	
	
	
	private void initialize ()  {
		micky = new MongoInputCollectionSet(parameters);
		db = micky.getDb();		
		collName=micky.getsClaimColl();
		jobUid=parameters.get("jobuid");
        schemaName = parameters.get("jobname")  == null ?  "javatest" : parameters.get("jobname");
		
		//test settings
		/*collName ="CT_Medicaid_54002ClaimServLine20161205";
		schemaName="CT_Medicaid_5400220161205";//test
		jobUid ="1143";*/
               
        
		
		
		coll = db.getCollection(collName);
		if(debugMode){
			System.out.println("collName="+collName+" schemaName="+schemaName+"jobId="+jobUid);
		}
		
		
	}
	
	class Report {
		
		long medicalClaimsTotalRecords = 0l;
		long medicalClaimsTotalRecords11 = 0l;
		String reportName ="Dist.ofPrincipalICD9ProcCodesReport";
		
		
		
	}
	
	public static void main(String[] args) {
		
		log.info("Starting Dist.ofPrincipalICD9ProcCodes Report");

		PrincipalProcICD9CodeDistReport instance = new PrincipalProcICD9CodeDistReport();
		
		// get parameters and make them available to all
		RunParameters rp = new RunParameters();
		instance.parameters = rp.loadParameters(args, parameterDefaults);
				
		String s = instance.parameters.get("runname") + "_" + instance.parameters.get("rundate");
		instance.parameters.put("jobname", s);
		
				
		RunParameters.parameters.put("configfolder", instance.parameters.get("configfolder"));
		
		instance.process();
		
		
		log.info("Completed Dist.ofPrincipalICD9ProcCodes Report");
	}
	

	static String [][] parameterDefaults = {
		{"configfolder", "C:/workspace/ECR_Analytics/trunk/EpisodeConstruction/src/"},
		{"rundate", "20150611"},
		{"runname", "CHC"},
		{"jobuid", "1043"}
	};
	
	
	private static org.apache.log4j.Logger log = Logger.getLogger(PrincipalProcICD9CodeDistReport.class);

}
