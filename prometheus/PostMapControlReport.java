
//import java.util.Iterator;






public class PostMapControlReport {
	
	
	HashMap<String, String> parameters;
	
	
	Mongo mongoClient;
	DB db;
	DBCursor cursor;
	BasicDBObject query;
	
	MongoInputCollectionSet micky;
	DBCollection memberColl;
	DBCollection collRx;
	DBCollection coll;
	
	
	String schemaName=null;
	String collName=null;
	String collNameRx=null;
	String collNameMem=null;
	String jobUid=null;
	boolean debugMode = false;
	AggregationOutput output = null;
	static final String CHARGEAMTFIELD="charge_amt";
	static final String ALLOWEDAMTFIELD="allowed_amt";
	String verifyMsg = null;
	int maxRec = 10000; //max members per page display
	StringBuffer[] sbs = new StringBuffer[50];       //store Med reports
	StringBuffer[] sbsMed = new StringBuffer[50];       //store Med reports
	StringBuffer[] sbsRx = new StringBuffer[50];       //store Med reports
	
    int pgCntMed =0;//# of pages/files
    int pgCntRx =0;//# of pages/files
    int pgCnt =0;//# of pages/files
	
	boolean hasMemId = false;
	boolean hasChargeAmt = false;
	boolean hasAllowedAmt = false;
	boolean hasRxChargeAmt = false;
	boolean hasRxAllowedAmt = false;
	
	Report rpt = new Report();
	
	/**
	 * static parameter constructor, just for testing
	 */
	public PostMapControlReport() {	}
	
	/**
	 * constructor using parameters pulled from control database
	 * @param parameters
	 */
	public PostMapControlReport(HashMap<String, String> parameters) {
		this.parameters = parameters;
	}
	
	
	private void process () {
		
		initialize();
		
		verifyMsg = dataVerification();
		if(debugMode){
			System.out.println("dataVerification done! verifyMsg="+verifyMsg);
		}
		
		if(verifyMsg == null){
			getMedicalClaimValues();
	    	if(debugMode){
	    		System.out.println("getMedicalClaimValues complete.");
	    	}
	    	getMemberCount();
	    	if(debugMode){
	    		System.out.println("getMemberCount complete.");
	    	}
		
	    	storeReport();
		}else{
			log.info("Can not PostMapControlReport for jobId="+jobUid+" error message="+verifyMsg);
			
		}
		
	}
	
	
	
	private void getMemberCount () {
		
//		@SuppressWarnings("unchecked")
		//List<String> distinct = memberColl.distinct("member_id"); 
		/*
		for (Object obj : distinct) { 
			System.out.println(obj); 
		} 
		*/
		// count 
//		rpt.uniqueMemberCount = distinct.size(); 
		
		DBObject groupFields = null;
		groupFields = new BasicDBObject( "_id", "$member_id");
		DBObject group = new BasicDBObject("$group", groupFields );
		int memCnt = 0;
	    if(debugMode){
	    	System.out.println("before aggregate count group="+group);
	    }
		List<DBObject> aggregationQuery = Arrays.<DBObject>asList(group);
		Cursor aggregateOutput = null;	
        aggregateOutput = memberColl.aggregate(
	            aggregationQuery,
	            AggregationOptions.builder()
	            	.outputMode(AggregationOptions.OutputMode.CURSOR)
	                .allowDiskUse(true)
	                .build()
	    );
        if(debugMode){
        	System.out.println("after aggregate count..");
        }
		while ( aggregateOutput.hasNext() ) {
			memCnt++;
	    
			DBObject memObj = aggregateOutput.next();
			String memId = (String)memObj.get("_id");
		    if(debugMode && memCnt%150000 == 0){
		    	System.out.println("current unique memCnt="+memCnt+" memId="+memId);
		    }
		}
		
		
	    if(debugMode){
	    	System.out.println("complete aggregate count unique memCnt="+memCnt);
	    }
		rpt.uniqueMemberCount = memCnt; 

	}
	
	
	private boolean fieldExist(String fieldName, boolean isRx) {
	boolean fieldExist = false;

	
	BasicDBObject query = new BasicDBObject(); 
	BasicDBObject fields=null;
	DBCursor curs = null;
	int cnt = 0;
	String reqField=null;
	int nums=10;
	if(!fieldExist){
		fields = new BasicDBObject(fieldName,true).append("_id",false);;
		if(!isRx){
			curs = coll.find(query, fields);
		}else{
			curs = collRx.find(query, fields);
		}
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
		
		//check allow_amt field
		hasAllowedAmt=fieldExist(ALLOWEDAMTFIELD, false);
		hasChargeAmt = false;
		if(!hasAllowedAmt){
			//if not found allow_amt, check charge_amt
			hasChargeAmt = fieldExist(CHARGEAMTFIELD, false);
		}

		//check charge_amt field RX
		hasRxChargeAmt = fieldExist(CHARGEAMTFIELD, true);
		//if not found charge_amt, check allowed_amt
		hasRxAllowedAmt=fieldExist(ALLOWEDAMTFIELD, true);
		
		if(!hasChargeAmt && !hasAllowedAmt){
			errorMsg="Both Charge Amt and Allowed Amt are missing, can not run the report!";
			return errorMsg;
		}
		if(debugMode){
			System.out.println("hasChargeAmt="+hasChargeAmt+" hasAllowAmt="+hasAllowedAmt);
		}
		return errorMsg;
	}
		
	
	private void getMedicalClaimValues() {

		rpt.medicalClaimsTotalRecords = coll.getCount();
    	if(debugMode){
    		System.out.println("getMedicalClaimValues start total ="+rpt.medicalClaimsTotalRecords);
    	}
		
		String amtName = "allowed_amt";
		if(!hasAllowedAmt){
			amtName = "charge_amt";
		}
		
		String memName = "member_id";
		String amtValue;
		String memValue;
		double amtValD;
		// create an empty query
		BasicDBObject query = new BasicDBObject(); 
		// configure fields to be returned (true/1 or false/0 will work)
		// EXPLICITLY CONFIGURE _id TO NOT SHOW
		BasicDBObject fields = new BasicDBObject(amtName, true).append("_id", false).append(memName, true);

		// do a query specifying the fields (and print results)
		int cnt = 0;
		DBCursor curs = null;
    	if(rpt.medicalClaimsTotalRecords>0){
    		curs = coll.find(query, fields);
    		while(curs.hasNext()) {
    			DBObject o = curs.next();
    			amtValue = (String) o.get(amtName);
    			memValue = (String) o.get(memName);
		   //rpt.medicalClaimsTotalAllowedAmt.add(BigDecimal.valueOf(Double.parseDouble(amtValue.replace("$", ""))));
		   //log.info(Double.parseDouble(amtValue.replace("$", "")));
    			if (amtValue != null) {
    				amtValD = Double.parseDouble(amtValue.replace("$", ""));
    				rpt.medicalClaimsTotalAllowedAmt = rpt.medicalClaimsTotalAllowedAmt.add(BigDecimal.valueOf(amtValD));
    			}
		   
    			cnt++;
		   
		   //log.info(o.get(amtName));
		   
		   // if we haven't encountered this member, add it
    			if(!rpt.mcMembers.contains(memValue)) {
    				rpt.mcMembers.add(memValue);
    				if ( !checkForMember(memValue) )
    					rpt.mcMembersNotInMemberFile.add(memValue);
    			}
    			if(debugMode && cnt%250000 == 0){
    				System.out.println("MedicalClaim Total="+rpt.medicalClaimsTotalRecords+" memValue="+memValue+" cnt="+cnt);
    			}
    		}//endwhile
		}
		
		rpt.pharmacyClaimsTotalRecords = collRx.getCount();
    	if(debugMode){
    		System.out.println("getMedicalClaimValues start RX total="+rpt.pharmacyClaimsTotalRecords);
    	}
    	if(rpt.pharmacyClaimsTotalRecords>0){
    		curs = collRx.find(query, fields);
    		cnt = 0;
    		while(curs.hasNext()) {
    			DBObject o = curs.next();
    			amtValue = (String) o.get(amtName);
    			memValue = (String) o.get(memName);
    			cnt++;
		   //rpt.medicalClaimsTotalAllowedAmt.add(BigDecimal.valueOf(Double.parseDouble(amtValue.replace("$", ""))));
		   //log.info(Double.parseDouble(amtValue.replace("$", "")));
    			if(amtValue != null){
    				amtValD = Double.parseDouble(amtValue.replace("$", ""));
    				rpt.pharmacyClaimsTotalAllowedAmt = rpt.pharmacyClaimsTotalAllowedAmt.add(BigDecimal.valueOf(amtValD));
    			}
		   //log.info(o.get(amtName));
		   
    			if(!rpt.rxMembers.contains(memValue)) {
    				rpt.rxMembers.add(memValue);
    				if ( !checkForMember(memValue) )
    					rpt.rxMembersNotInMemberFile.add(memValue);
    			}	
    			if(debugMode && cnt%250000 == 0){
    				System.out.println("RX Total="+rpt.pharmacyClaimsTotalRecords+" memValue="+memValue+" cnt="+cnt);
    			}
    		}//endwhile
		}
		
		//log.info(getStatsAsHTML());
		
	}
	
	/**
	 * see if the member id exists in the member collection
	 * @param member_id
	 * @return
	 */
	private boolean checkForMember (String member_id) {
		
		// query to filter the document based on member_id
        DBObject query = new BasicDBObject("member_id", member_id);
        // resultant document fetched by satisfying the criteria
        DBObject d1 = memberColl.findOne(query);
        
        return d1 == null ? false : true;
        
	}
	
	
	private void storeReport() {
		
    	String contrlRptStr = getStatsAsHTML();
		doMemberList("Members in Medical Claims but Not in Member File", rpt.mcMembersNotInMemberFile,"Members in Pharmacy Claims but Not in Member File", rpt.rxMembersNotInMemberFile);
		
    	if(debugMode){
    		System.out.println("storeReport pgCnt="+pgCnt+" pgCntMed="+pgCntMed);
    	}
		
		List<Class<?>> cList = new ArrayList<Class<?>>();
        cList.add(ProcessReport.class);
		
		String env = parameters.get("env") == null ?  "prd" :  parameters.get("env");
       // String schemaName = parameters.get("jobname")  == null ?  "javatest" : parameters.get("jobname");
        HibernateHelper h = new HibernateHelper(cList, env, schemaName);
        SessionFactory factory = h.getFactory(env, schemaName);

        Session session = factory.openSession();
        session.setCacheMode(CacheMode.IGNORE);
        session.setFlushMode(FlushMode.COMMIT);
        Transaction tx = null;
        String reportName = rpt.reportNameNotInMember;
        try{
			tx = session.beginTransaction();
			if(debugMode){
				System.out.println("pg total cnt="+pgCnt);
			}
			
			for(int i=0; i<=pgCnt+1; i++){
				if( i == pgCnt+1){
					reportName = rpt.reportName;
				}else{
					reportName = rpt.reportNameNotInMember+"-"+Integer.toString(i+1);
				}
				if(debugMode){
					System.out.println("PgCnt "+i+" rptName="+reportName+" jobId="+jobUid);
				}
			
				String hql = "FROM ProcessReport WHERE jobuid = :jobuid AND stepname = '" + ProcessJobStep.STEP_POSTMAP_REPORT + "' and reportName='"+reportName+"'";
				Query query = session.createQuery(hql).setLockOptions(new LockOptions(LockMode.PESSIMISTIC_WRITE));
				query.setParameter("jobuid", jobUid);

				@SuppressWarnings("unchecked")
				List<ProcessReport> results = query.list();
				ProcessReport r;
				if (results.isEmpty()) {
					r = new ProcessReport();
					//r.setJobUid(Long.parseLong(parameters.get("jobuid")));
					r.setJobUid(Long.parseLong(jobUid));				
					r.setStepName(ProcessJobStep.STEP_POSTMAP_REPORT);
					r.setReportName(reportName);
				} else{
					r = results.get(0);
				}
//				r.setReport(new SerialBlob(getStatsAsHTML().getBytes()));
				if( i == pgCnt+1){
					r.setReport(new SerialBlob(contrlRptStr.toString().getBytes()));
				}else{
					r.setReport(new SerialBlob(sbs[i].toString().getBytes()));
					//r.setReport(new SerialBlob(contrlRptStr.toString().getBytes()));
					
				}
			
				session.save(r);
			}//endfor(int i=0; i<=pgCnt; i++){
			tx.commit();
			
		}catch (HibernateException e) {
			if (tx!=null) tx.rollback();
			e.printStackTrace(); 
		} catch (SerialException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}finally {
			session.close(); 

			try {
				h.close("prd", schemaName);
			} catch (Exception e) {
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
		/*
		sb.append("<!DOCTYPE html>");
		sb.append("<html lang=\"en\">");
		sb.append("<head><title>Mongo Collection Statistics</title></head>"); 
		
		sb.append("<body>");
		*/
		
		// report i
		
		sb.append("<div style='align:center; margin:0 auto; width:750px'><table border=\"1\" cellspacing=\"1\" cellpadding=\"5\">");
		sb.append("<tr bgcolor=\"#99CCFF\">");
		sb.append("<th width='250px'>Collection</th>");
		sb.append("<th width='250px'>Total Number of Records</th>");
		sb.append("<th width='250px'>Total Cost</th>");
		sb.append("</tr>");
		
		sb.append("<tr>");
		sb.append("<td bgcolor=\"#99CCFF\"><strong>Medical Claims File</strong></td>"); 	// column name
		sb.append("<td style=\"text-align:center\">" + NumberFormat.getNumberInstance(Locale.US).format(rpt.medicalClaimsTotalRecords) + "</td>"); 	
		sb.append("<td style=\"text-align:center\">" + NumberFormat.getCurrencyInstance().format(rpt.medicalClaimsTotalAllowedAmt) + "</td>"); 				
		sb.append("</tr>");	 
		
		sb.append("<tr>");
		sb.append("<td bgcolor=\"#99CCFF\"><strong>Pharmacy Claims File</strong></td>"); 	// column name
		sb.append("<td style=\"text-align:center\">" + NumberFormat.getNumberInstance(Locale.US).format(rpt.pharmacyClaimsTotalRecords) + "</td>"); 	
		sb.append("<td style=\"text-align:center\">" + NumberFormat.getCurrencyInstance().format(rpt.pharmacyClaimsTotalAllowedAmt) + "</td>"); 				
		sb.append("</tr></table></div>");	 
		
		sb.append("<br>");
		

		// report ii, v
		sb.append("<div style='align:center; margin:0 auto; width:820px'><table border=\"1\" cellspacing=\"1\" cellpadding=\"5\">");
		sb.append("<tr bgcolor=\"#99CCFF\">");
		sb.append("<th width='150px'>&nbsp;</th>");
		sb.append("<th width='150px'>Unique Member Count</th>"); 	// column name
		sb.append("<th width='270px'>Number of Members Not Found in Member File</th>"); 
		sb.append("<th width='250px'>% of Members Not Found in Member File</th>");  					
		sb.append("</tr>");	
		
		sb.append("<tr>");
		sb.append("<td bgcolor=\"#99CCFF\"><strong>Member File<strong></td>");
		sb.append("<td style=\"text-align:center\">" + NumberFormat.getNumberInstance(Locale.US).format(rpt.uniqueMemberCount) + "</td>"); 
		sb.append("<td style=\"text-align:center\">--</td>");
		sb.append("<td style=\"text-align:center\">--</td>");
		sb.append("</tr>");	
		
		sb.append("<tr>");
		sb.append("<td bgcolor=\"#99CCFF\"><strong>Medical Claims File</strong></td>");
		sb.append("<td style=\"text-align:center\">" + NumberFormat.getNumberInstance(Locale.US).format(rpt.mcMembers.size()) + "</td>"); 
		sb.append("<td style=\"text-align:center\">" + NumberFormat.getNumberInstance(Locale.US).format(rpt.mcMembersNotInMemberFile.size()) + "</td>");
		sb.append("<td style=\"text-align:center\">");
		sb.append(pctFormat.format(MathUtil.getPercentage(rpt.mcMembersNotInMemberFile.size(), rpt.mcMembers.size())));
		sb.append("</td>");
		sb.append("</tr>");	
		
		sb.append("<tr>");
		sb.append("<td bgcolor=\"#99CCFF\"><strong>Pharmacy Claims File</strong></td>");
		sb.append("<td style=\"text-align:center\">" + NumberFormat.getNumberInstance(Locale.US).format(rpt.rxMembers.size()) + "</td>"); 
		sb.append("<td style=\"text-align:center\">" + NumberFormat.getNumberInstance(Locale.US).format(rpt.rxMembersNotInMemberFile.size()) + "</td>");
		sb.append("<td style=\"text-align:center\">");
		sb.append(pctFormat.format(MathUtil.getPercentage(rpt.rxMembersNotInMemberFile.size(), rpt.rxMembers.size())));
		sb.append("</td>");
		sb.append("</tr>");	
		
		sb.append("</table></div>");
		
		//
		//doMemberList("Members in Medical Claims but Not in Member File", rpt.mcMembersNotInMemberFile,null);
		//doMemberList("Members in Pharmacy Claims but Not in Member File", rpt.rxMembersNotInMemberFile, "RX");
		
		//if (rpt.mcMembersNotInMemberFile.size() > 0) {
		//	sb.append(doMemberList("Members in Medical Claims but Not in Member File", rpt.mcMembersNotInMemberFile));
		//}
		//if (rpt.rxMembersNotInMemberFile.size() > 0) {
			//sb.append(doMemberList("Members in Pharmacy Claims but Not in Member File", rpt.rxMembersNotInMemberFile));
		//}
		
		/*
		sb.append("</body>");
		sb.append("</html>");
		*/
		
		return sb.toString();
		
	}
	
	
	private void doMemberList(String header, HashSet<String> list,String headerRx, HashSet<String> listRx ) {
		
		StringBuffer sb = new StringBuffer();
		int memCnt = 0;
		boolean found = false;
		
		pgCntMed = 0;
		sbsMed[pgCntMed] = new StringBuffer();	
		sbsMed[pgCntMed].append("<div style='align:center; overflow-x:hidden;overflow-y:auto; height:480px; margin:0 auto; width:400px'>");
		sbsMed[pgCntMed].append("<table class=\"sortable\" border=\"1\" cellspacing=\"1\" cellpadding=\"5\">");
		sbsMed[pgCntMed].append("<tr bgcolor=\"#99CCFF\">");
		sbsMed[pgCntMed].append("<th>" + header + "</th>"); 	// column name 					
		sbsMed[pgCntMed].append("</tr>");	
	
		if (list != null && list.size() > 0) {
			if(debugMode){
				System.out.println("doMemberList med listSize="+list.size());
			}
			found = true;
			for (String s : list) {
				sb.append("<tr>");
				sb.append("<td style=\"text-align:center\">" + s + "</td>"); 
				sb.append("</tr>");
				memCnt++;
				if(memCnt>maxRec){
					sb.append("</table></div>");
					sbsMed[pgCntMed].append(sb);
					memCnt = 0;
					sb = new StringBuffer();
					pgCntMed++;
					sbsMed[pgCntMed] = new StringBuffer();	
					sbsMed[pgCntMed].append("<div style='align:center;overflow-x:hidden;overflow-y:auto; height:480px; margin:0 auto; width:400px'>");
					sbsMed[pgCntMed].append("<table class=\"sortable\" border=\"1\" cellspacing=\"1\" cellpadding=\"5\">");
					sbsMed[pgCntMed].append("<tr bgcolor=\"#99CCFF\">");
					sbsMed[pgCntMed].append("<th>" + header + "</th>"); 	// column name 					
					sbsMed[pgCntMed].append("</tr>");	
				}
			}
		}else{
			pgCntMed = 0;
			sbsMed[0].append("<tr><td>No records found!</td></tr></table></div>");
		}
		if(found){
			sb.append("</table></div>");
			sbsMed[pgCntMed].append(sb);			
		}

		//do RX list
		sb = new StringBuffer();
		memCnt = 0;
		found = false;
		
		pgCntRx=0;
		sbsRx[pgCntRx] = new StringBuffer();	
		sbsRx[pgCntRx].append("<div style='align:center; overflow-x:hidden;overflow-y:auto; height:480px; margin:0 auto; width:400px'>");
		sbsRx[pgCntRx].append("<table class=\"sortable\" border=\"1\" cellspacing=\"1\" cellpadding=\"5\">");
		sbsRx[pgCntRx].append("<tr bgcolor=\"#99CCFF\">");
		sbsRx[pgCntRx].append("<th>" + headerRx + "</th>"); 	// column name 					
		sbsRx[pgCntRx].append("</tr>");	
	
		if (listRx != null && listRx.size() > 0) {
			if(debugMode){
				System.out.println("doMemberList RX listSize="+listRx.size());
			}
			found = true;
			for (String s : listRx) {
				sb.append("<tr>");
				sb.append("<td style=\"text-align:center\">" + s + "</td>"); 
				sb.append("</tr>");
				memCnt++;
				if(memCnt>maxRec){
					sb.append("</table></div>");
					sbsRx[pgCntRx].append(sb);
					memCnt = 0;
					sb = new StringBuffer();
					pgCntRx++;
					sbsRx[pgCntRx] = new StringBuffer();	
					sbsRx[pgCntRx].append("<div style='align:center;overflow-x:hidden;overflow-y:auto; height:480px; margin:0 auto; width:400px'>");
					sbsRx[pgCntRx].append("<table class=\"sortable\" border=\"1\" cellspacing=\"1\" cellpadding=\"5\">");
					sbsRx[pgCntRx].append("<tr bgcolor=\"#99CCFF\">");
					sbsRx[pgCntRx].append("<th>" + headerRx + "</th>"); 	// column name 					
					sbsRx[pgCntRx].append("</tr>");	
				}
			}
		}else{
			pgCntRx = 0;
			sbsRx[0].append("<tr><td>No records found!</td></tr></table></div>");
		}
		if(found){
			sb.append("</table></div>");
			sbsRx[pgCntRx].append(sb);			
		}

		
		//combine
		pgCnt = pgCntMed;
		if(pgCntRx>pgCntMed){
			pgCnt = pgCntRx;
		}
		
		if(debugMode){
			System.out.println("pgCnt="+pgCnt+" pgCntRx="+pgCntRx+"   pgCntMed="+pgCntMed);
		}
		
		for(int i=0; i<pgCnt+1; i++){
			sb = new StringBuffer();
			sb.append("<div style='width: 100%; overflow: hidden;'><div style='width:350px; float: left;'>");
			if(sbsMed[i] != null){
				sb.append(sbsMed[i]);
			}else{
				sb.append("&nbsp;");
			}
			sb.append("</div><div style='margin-left: 370px;'>" );
			if(sbsRx[i] != null){
				sb.append(sbsRx[i]);
			}else{
				sb.append("&nbsp;");
			}
			sb.append("</div></div>");
			sbs[i] = sb;
		}
		
		return ;
		
	}
	
	
	private void initialize ()  {

		jobUid=parameters.get("jobuid");
        schemaName = parameters.get("jobname")  == null ?  "javatest" : parameters.get("jobname");
		
		
		micky = new MongoInputCollectionSet(parameters);
		db = micky.getDb();	
		
		memberColl = db.getCollection(micky.getsMemberColl());
		collName=micky.getsClaimColl();
		collNameRx = micky.getsRxColl();
		
		/*collName ="";
		collNameRx ="";
		collNameMem="";
		schemaName="";
		jobUid="";*/
		
	

		coll = db.getCollection(collName);
		collRx = db.getCollection(collNameRx);
		/*
		memberColl = db.getCollection(collNameMem);
		if(debugMode){
			System.out.println("collName="+collName+" collNameMem="+collNameMem);
		}	
		*/	
		
	}
	
	class Report {
		
		long medicalClaimsTotalRecords = 0l;
		BigDecimal medicalClaimsTotalAllowedAmt = new BigDecimal("0.00");
		long pharmacyClaimsTotalRecords = 0l;
		BigDecimal pharmacyClaimsTotalAllowedAmt = new BigDecimal("0.00");
		
		long uniqueMemberCount = 0l;
		
		HashSet<String>mcMembers = new HashSet<String>();
		HashSet<String>rxMembers = new HashSet<String>();
		HashSet<String>mcMembersNotInMemberFile = new HashSet<String>();
		HashSet<String>rxMembersNotInMemberFile = new HashSet<String>();
		
		String reportName ="ControlReport";//step 7, ii, iv
		String reportNameNotInMember ="MembersInClaimFileNotInMemberFileReport";//step 7, iii
		
		
	}
	
	public static void main(String[] args) {
		
		log.info("Starting Post Mapping Control Report");

		PostMapControlReport instance = new PostMapControlReport();
		
		// get parameters and make them available to all
		RunParameters rp = new RunParameters();
		instance.parameters = rp.loadParameters(args, parameterDefaults);
				
		String s = instance.parameters.get("runname") + "_" + instance.parameters.get("rundate");
		instance.parameters.put("jobname", s);
		
				
		RunParameters.parameters.put("configfolder", instance.parameters.get("configfolder"));
		
		instance.process();
		
		
		log.info("Completed Post Mapping Control Report");
	}
	

	static String [][] parameterDefaults = {
		{"configfolder", "C:/workspace/ECR_Analytics/trunk/EpisodeConstruction/src/"},
		{"clientID", "Test"},
		{"rundate", "20150824"},
		{"runname", "5_3_freeze"},
		{"jobname", "Test_5_3_freeze20150824"},
		{"jobuid", "1067"}
	};
	
	static NumberFormat pctFormat = NumberFormat.getPercentInstance();
	static {
		pctFormat.setMinimumFractionDigits(1);
	}
	
	
	private static org.apache.log4j.Logger log = Logger.getLogger(PostMapControlReport.class);

}
