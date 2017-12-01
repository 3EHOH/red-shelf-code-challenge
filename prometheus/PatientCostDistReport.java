






@SuppressWarnings({ "rawtypes", "unchecked" })
public class PatientCostDistReport {
	
	HashMap<String, String> parameters;
		
	Report rpt = new Report();
	
	String schemaName=null;
	String jobUid=null;
	String env=null;
		
	double totalCostAll =0.0; //total cost for all members
    boolean debugMode = false;

    List<Object[]> costList= null;
    
	HashMap<Double, List<String>> duplMap = new HashMap <Double, List<String>>();

    Map<Double, String> finalTreeMap = new TreeMap<Double, String>(
			new Comparator<Double>() {
			@Override
			public int compare(Double o1, Double o2) {
				return o2.compareTo(o1);
			}

		});
    
	StringBuffer[] sbs = null; 
	int batchCnt = 1;//number of batch
	int batchLimit = 15000; //rows per web page
	int batchTotal = 0; //max batch number
	int actTotal = 0;
	int duplTotal = 0;//members has the same totalCost
	
	double percentAmt = 0.02; //top/bottom percentage cost to display
	
    
	/**
	 * static parameter constructor, just for testing
	 */
	public PatientCostDistReport() {	}
	
	/**
	 * constructor using parameters pulled from control database
	 * @param parameters
	 */
	public PatientCostDistReport(HashMap<String, String> parameters) {
		this.parameters = parameters;
	}
	
	
	private void process () {
		
		initialize();
		if(debugMode){
			System.out.println("init done!");
		}
		generateReport();
		if(debugMode){
			System.out.println("generateReport done!");
		}
		
		storeReport();
		if(debugMode){
			System.out.println("storeReport done!");
		}
		
	}
	
	
	private void generateReport() {
		List<Class<?>> cList = new ArrayList<Class<?>>();
        cList.add(ClaimsCombinedTbl.class);
		
        HibernateHelper h = new HibernateHelper(cList, env, schemaName);

		String curMemId = null;
		String prevMemId = null;
	    
        StringBuffer sb  = null;
		double totalCost =0.0;
        long IPNo = 0;
        double IPAvg=0;;
        long OPNo = 0;
        double OPAvg=0;;
        long PBNo = 0;
        double PBAvg=0;;
        long RXNo = 0;
        double RXAvg=0;;
        boolean first =true;
        
        String connUrl=null;
        String rptQry=null;
		Connection conn=null;
		ResultSet rs=null;
        
        long costListSize = 0;
        try{
       		if(debugMode){
       			System.out.println("Env="+env+ " DbDial="+h.getCurrentConfig().getConnParms().getDialect()+" schema="+schemaName+" db="+h.getCurrentConfig().getConnParms().getDatabase());
       		}
        	
            if("vertica".equalsIgnoreCase(h.getCurrentConfig().getConnParms().getDialect())){
            	connUrl="jdbc:vertica://" + h.getCurrentConfig().getConnParms().getDbUrl()+"/" + h.getCurrentConfig().getConnParms().getDatabase();
            	batchLimit=6000;
            }else{
          	    connUrl="jdbc:mysql://" + h.getCurrentConfig().getConnParms().getDbUrl()+"/ecr";
            }
       		if(debugMode){
       			System.out.println("connUrl="+connUrl);
       		}
                
    		Properties connProp = new Properties();
            connProp.put("user", h.getCurrentConfig().getConnParms().getDbUser());
            connProp.put("password", h.getCurrentConfig().getConnParms().getDbPw());
            conn = DriverManager.getConnection(connUrl, connProp);
        	
        	
       		totalCost = 0.0;
   			
   			rptQry="select cc.member_id,cc.claim_line_type_code ,sum(cc.allowed_amt) sum_amt,count(*) count, avg(cc.allowed_amt) avg_amt from "+
 					schemaName+".claims_combined cc where member_id is not null and  member_id<>'' "+
 					" group by cc.member_id, cc.claim_line_type_code order by cc.member_id, cc.claim_line_type_code";
    
       	    rs = conn.createStatement().executeQuery(rptQry);
            while(rs.next()){
            		costListSize++;
            		String claimType =rs.getString("claim_line_type_code");
            		curMemId=rs.getString("member_id");
            		if(first){
            			prevMemId = curMemId;
            			first = false;
            		}
            		if(!curMemId.equalsIgnoreCase(prevMemId)){
            			//compose one line for this member
            			sb = new StringBuffer();
            			sb.append("<tr><td>" +prevMemId  + "</td>");
            			if(PBNo == 0){
               				sb.append("<td>&nbsp;</td><td>&nbsp;</td>"); 	
            			}else{
            				sb.append("<td>" +PBNo+"</td><td>" +NumberFormat.getCurrencyInstance().format(PBAvg)+ "</td>");
            			}
            			if(OPNo == 0){
               				sb.append("<td>&nbsp;</td><td>&nbsp;</td>"); 	
            			}else{
            				sb.append("<td>" +OPNo+"</td><td>" +NumberFormat.getCurrencyInstance().format(OPAvg)+ "</td>");
            			}
            			if(IPNo == 0){
               				sb.append("<td>&nbsp;</td><td>&nbsp;</td>"); 	
            			}else{
            				sb.append("<td>" +IPNo+"</td><td>" +NumberFormat.getCurrencyInstance().format(IPAvg)+ "</td>");
            			}
            			if(RXNo == 0){
               				sb.append("<td>&nbsp;</td><td>&nbsp;</td>"); 	
            			}else{
            				sb.append("<td>" +RXNo+"</td><td>" +NumberFormat.getCurrencyInstance().format(RXAvg)+ "</td>");
            			}
            			sb.append("<td>" +NumberFormat.getCurrencyInstance().format(totalCost)+ "</td></tr>"); 
            			totalCostAll =totalCostAll+totalCost;
            			if(debugMode){
            			//	System.out.println(prevMemId+" cost="+totalCost);
            			}
            			
            			boolean keyExist = finalTreeMap.containsKey(totalCost);
            	        if(keyExist){
            	        	//check if duplList exist
            	        	List<String> duplList = new ArrayList<String>();
            	            duplList = ( List<String> )duplMap.get(totalCost);
            	            if(duplList == null){
            	            	duplList = new ArrayList<String>();
            	            	duplList.add(sb.toString());
            	            	duplMap.put(totalCost, duplList);
            	            	duplTotal++;
            	            }else{
            	/*                			if(duplList != null && duplList.size()>1)	{
            	                				System.out.println("found duplcost="+totalCost+" duplList size="+duplList.size()+" mem="+curMemId);
            	                			}*/
            	            	duplList.add(sb.toString());
            	            	duplMap.put(totalCost, duplList);
            	            	duplTotal++;
            	            }
            	        }else{
            	        	finalTreeMap.put(totalCost, sb.toString());
            	        }            						
            	        sb = new StringBuffer();
            			prevMemId = curMemId;
            			IPNo =0;
        				IPAvg =0;
        				OPNo =0;
        				OPAvg =0;
        				PBNo =0;
        				PBAvg =0;
        				RXNo =0;
        				RXAvg =0;
        				totalCost=0;
            			}//end if(!curMemId.equalsIgnoreCase(prevMemId)){
            		
            			if("IP".equalsIgnoreCase(claimType)){
        					IPNo =rs.getLong("count");
       						IPAvg =rs.getLong("avg_amt");
        				}else if("OP".equalsIgnoreCase(claimType)){
        					OPNo =rs.getLong("count");
       						OPAvg =rs.getLong("avg_amt");
        				}else if("PB".equalsIgnoreCase(claimType)){
        					PBNo =rs.getLong("count");
       						PBAvg =rs.getLong("avg_amt");
        					
        				}else if("RX".equalsIgnoreCase(claimType)){
        					RXNo =rs.getLong("count");
       						RXAvg =rs.getLong("avg_amt");
        				}            					   
//            			totalCost = totalCost+ (double)costObj[2];
            			BigDecimal bd =rs.getBigDecimal("sum_amt");
           				totalCost = totalCost+ bd.doubleValue();
            					
            }
            
            if (costListSize  > 2500000) {
            	percentAmt = 0.01;             	
            }
            
           	actTotal = finalTreeMap.size();
           	if(debugMode){
           		System.out.println("generateReport mapSize="+finalTreeMap.size()+" percentAmt="+percentAmt);
           	}
		}catch (HibernateException e) {
			e.printStackTrace(); 
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
			
		}finally {
			try {
				 if(rs !=null){rs.close();}	
				 if(conn != null){conn.close();	}
				h.close(env, schemaName);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		
	}

	
	private void generateReportHib() {
		List<Class<?>> cList = new ArrayList<Class<?>>();
        cList.add(ClaimsCombinedTbl.class);
		
        HibernateHelper h = new HibernateHelper(cList, env, schemaName);
        SessionFactory factory = h.getFactory(env, schemaName);

        Session session = factory.openSession();
        session.setCacheMode(CacheMode.IGNORE);
        session.setFlushMode(FlushMode.COMMIT);
        
        StringBuffer memCostQry = new StringBuffer();
 	    
		String curMemId = null;
		String prevMemId = null;
	    
        StringBuffer sb  = null;
		double totalCost =0.0;
        long IPNo = 0;
        double IPAvg=0;;
        long OPNo = 0;
        double OPAvg=0;;
        long PBNo = 0;
        double PBAvg=0;;
        long RXNo = 0;
        double RXAvg=0;;
        boolean first =true;
        
        String connUrl=null;
        String rptQry=null;
		Connection conn=null;
		ResultSet rs=null;
        
   
        try{
       		if(debugMode){
       			System.out.println("Env="+env+ " DbDial="+h.getCurrentConfig().getConnParms().getDialect()+" schema="+schemaName+" db="+h.getCurrentConfig().getConnParms().getDatabase());
       		}
        	
            if("vertica".equalsIgnoreCase(h.getCurrentConfig().getConnParms().getDialect())){
            	connUrl="jdbc:vertica://" + h.getCurrentConfig().getConnParms().getDbUrl()+"/" + h.getCurrentConfig().getConnParms().getDatabase();
            	batchLimit = 6000;
            }else{
          	    connUrl="jdbc:mysql://" + h.getCurrentConfig().getConnParms().getDbUrl()+"/ecr";
            }
       		if(debugMode){
       			System.out.println("connUrl="+connUrl);
       		}
                
        	
        	
       		totalCost = 0.0;
   			memCostQry = new StringBuffer();
   			memCostQry.append("select cs.member_id,cs.claim_line_type_code ,sum(cs.allowed_amt),count(*), avg(cs.allowed_amt) ");
   			memCostQry.append("from ClaimsCombinedTbl cs where member_id is not null and  member_id<>'' group by cs.member_id, cs.claim_line_type_code ");
   			memCostQry.append("order by cs.member_id, cs.claim_line_type_code ");
            			
   			Query query = session.createQuery(memCostQry.toString());
    
   			costList = (List<Object[]>)query.list();
            if (costList != null && costList.size() > 2500000) {
            	percentAmt = 0.01;             	
            }
            if (costList != null && costList.size() > 0) {			
            	for(Object[] costObj: costList){
            		String claimType =(String)costObj[1];
            		curMemId =(String)costObj[0];
//            		System.out.println("claimType="+claimType+" curMemId="+curMemId);
            		if(first){
            			prevMemId = curMemId;
            			first = false;
            		}
            		if(!curMemId.equalsIgnoreCase(prevMemId)){
            			//compose one line for this member
            			sb = new StringBuffer();
            			sb.append("<tr><td>" +prevMemId  + "</td>");
            			if(PBNo == 0){
               				sb.append("<td>&nbsp;</td><td>&nbsp;</td>"); 	
            			}else{
            				sb.append("<td>" +PBNo+"</td><td>" +NumberFormat.getCurrencyInstance().format(PBAvg)+ "</td>");
            			}
            			if(OPNo == 0){
               				sb.append("<td>&nbsp;</td><td>&nbsp;</td>"); 	
            			}else{
            				sb.append("<td>" +OPNo+"</td><td>" +NumberFormat.getCurrencyInstance().format(OPAvg)+ "</td>");
            			}
            			if(IPNo == 0){
               				sb.append("<td>&nbsp;</td><td>&nbsp;</td>"); 	
            			}else{
            				sb.append("<td>" +IPNo+"</td><td>" +NumberFormat.getCurrencyInstance().format(IPAvg)+ "</td>");
            			}
            			if(RXNo == 0){
               				sb.append("<td>&nbsp;</td><td>&nbsp;</td>"); 	
            			}else{
            				sb.append("<td>" +RXNo+"</td><td>" +NumberFormat.getCurrencyInstance().format(RXAvg)+ "</td>");
            			}
            			sb.append("<td>" +NumberFormat.getCurrencyInstance().format(totalCost)+ "</td></tr>"); 
            			totalCostAll =totalCostAll+totalCost;
            			if(debugMode){
            			//	System.out.println(prevMemId+" cost="+totalCost);
            			}
            			
            			boolean keyExist = finalTreeMap.containsKey(totalCost);
            	        if(keyExist){
            	        	//check if duplList exist
            	        	List<String> duplList = new ArrayList<String>();
            	            duplList = ( List<String> )duplMap.get(totalCost);
            	            if(duplList == null){
            	            	duplList = new ArrayList<String>();
            	            	duplList.add(sb.toString());
            	            	duplMap.put(totalCost, duplList);
            	            	duplTotal++;
            	            }else{
            	/*                			if(duplList != null && duplList.size()>1)	{
            	                				System.out.println("found duplcost="+totalCost+" duplList size="+duplList.size()+" mem="+curMemId);
            	                			}*/
            	            	duplList.add(sb.toString());
            	            	duplMap.put(totalCost, duplList);
            	            	duplTotal++;
            	            }
            	        }else{
            	        	finalTreeMap.put(totalCost, sb.toString());
            	        }            						
            	        sb = new StringBuffer();
            			prevMemId = curMemId;
            			IPNo =0;
        				IPAvg =0;
        				OPNo =0;
        				OPAvg =0;
        				PBNo =0;
        				PBAvg =0;
        				RXNo =0;
        				RXAvg =0;
        				totalCost=0;
            			}//end if(!curMemId.equalsIgnoreCase(prevMemId)){
            		
            			if("IP".equalsIgnoreCase(claimType)){
        					IPNo =(long)costObj[3];
        					if(costObj[4] != null){
        						IPAvg =(double)costObj[4];
        					}
        				}else if("OP".equalsIgnoreCase(claimType)){
        					OPNo =(long)costObj[3];
        					if(costObj[4] != null){
        						OPAvg =(double)costObj[4];
        					}
        				}else if("PB".equalsIgnoreCase(claimType)){
        					PBNo =(long)costObj[3];
        					if(costObj[4] != null){
        						PBAvg =(double)costObj[4];
        					}
        				}else if("RX".equalsIgnoreCase(claimType)){
        					RXNo =(long)costObj[3];
        					if(costObj[4] != null){
        					   RXAvg =(double)costObj[4];
        					}
        				}            					   
//            			totalCost = totalCost+ (double)costObj[2];
            			BigDecimal bd =(BigDecimal)costObj[2];
            			if(costObj[2] != null){
            				totalCost = totalCost+ bd.doubleValue();
            			}
            					
            	}//endfor(Object[] costObj: costList)
            }
           	actTotal = finalTreeMap.size();
           	if(debugMode){
           		System.out.println("generateReport no records mapSize="+finalTreeMap.size());
           	}
		}catch (HibernateException e) {
			e.printStackTrace(); 
		}finally {
	            session.close();

			try {
				h.close(env, schemaName);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	    
		log.info("rptHtml="+getStatsAsHTML());
		
	}
	
	
	private void storeReportV(String connUrl, String user, String passWd){
		Properties connProp = new Properties();
        connProp.put("user", user);
        connProp.put("password", passWd);

        String procRptTab ="processReport";
		Connection conn=null;
		
		String insertQ="insert INTO "+schemaName+"."+procRptTab+ " (jobUid,stepName,reportName,report,uid ) VALUES(?,?,?,?,?)";
		String updateQ="update "+schemaName+"."+procRptTab+" set report=? where jobUid =? AND stepname = '" +
				ProcessJobStep.STEP_POSTNORMALIZATION_REPORT  + "' and reportName=?";
		
		String nextUidQ=" select ZEROIFNULL (max(uid))+1 uid from  "+schemaName+"."+procRptTab;
		PreparedStatement pstmt=null;
		ResultSet rs=null;
		int uId=0;
		String rptName=null;
		try {
            conn = DriverManager.getConnection(connUrl, connProp);
            conn.setAutoCommit (false); 
    
		    for(int i=0; i<batchCnt; i++){
		    	rptName =rpt.reportName+Integer.toString(i+1);
		    	updateQ="update "+schemaName+"."+procRptTab+" set report=? where jobUid =? AND stepname = '" +
						ProcessJobStep.STEP_POSTNORMALIZATION_REPORT  + "' and reportName='"+rptName+"'";
				
		    	rs = conn.createStatement().executeQuery(nextUidQ);
		    	while(rs.next()){
		    		uId=rs.getInt("uid");
		    	}
            
		    	pstmt = conn.prepareStatement(updateQ);
		    	pstmt.setBytes(1,sbs[i].toString().getBytes());
		    	pstmt.setString(2,jobUid );
		    	int rtnCode= pstmt.executeUpdate();
		    	if(rtnCode ==0){
		    		pstmt = conn.prepareStatement(insertQ);
		    		pstmt.setString(1,jobUid );
		    		pstmt.setString(2, ProcessJobStep.STEP_POSTNORMALIZATION_REPORT);
		    		pstmt.setString(3,rptName);
		    		pstmt.setBytes(4,sbs[i].toString().getBytes());
		    		pstmt.setInt(5,uId );
		    	}
		    	pstmt.execute();
	    		log.info("rptHtml="+sbs[i].toString());
		    }
            conn.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			try {
			 if(rs !=null){rs.close();}	
			 if(conn != null){conn.close();	}
			} catch (Exception e) {
				e.printStackTrace();
			 
			}
		}
	}
	
	
	
	private void storeReport() {
		
		List<Class<?>> cList = new ArrayList<Class<?>>();
        cList.add(ProcessReport.class);
		
		
        HibernateHelper h = new HibernateHelper(cList, env, schemaName);
        
        String connUrl=null;
        String user=null;
        String passWd=null;
        Transaction tx = null;
        Session session = null;
        
        try{
       		if(debugMode){
       			System.out.println("Db Dialect="+h.getCurrentConfig().getConnParms().getDialect()+env+" schema="+schemaName);
       		}
            	
       		getStatsAsHTML();
            if("vertica".equalsIgnoreCase(h.getCurrentConfig().getConnParms().getDialect())){
            	connUrl="jdbc:vertica://" + h.getCurrentConfig().getConnParms().getDbUrl()+"/" + h.getCurrentConfig().getConnParms().getDatabase();
            	user=h.getCurrentConfig().getConnParms().getDbUser();
            	passWd=h.getCurrentConfig().getConnParms().getDbPw();
                storeReportV(connUrl, user, passWd);
            }else{
            	SessionFactory factory = h.getFactory(env, schemaName);
            	session = factory.openSession();
            	session.setCacheMode(CacheMode.IGNORE);
            	session.setFlushMode(FlushMode.COMMIT);
        	
			    tx = session.beginTransaction();
			
			    for(int i=0; i<batchCnt; i++){
				
				String rptName =rpt.reportName+Integer.toString(i+1);
				
	        	String hql = "FROM ProcessReport WHERE jobuid = :jobuid AND stepname = '" + ProcessJobStep.STEP_POSTNORMALIZATION_REPORT  + "' and reportName='"+rptName+"'";
	        	Query query = session.createQuery(hql).setLockOptions(new LockOptions(LockMode.PESSIMISTIC_WRITE));
	        	query.setParameter("jobuid", jobUid);

	        	List<ProcessReport> results = query.list();
	        	ProcessReport r;
	        	if (results.isEmpty()) {
	        		r = new ProcessReport();
	        		r.setJobUid(Long.parseLong(jobUid));
	        		r.setStepName(ProcessJobStep.STEP_POSTNORMALIZATION_REPORT);
	        		r.setReportName(rptName);
	        	} else
	        		r = results.get(0);
			
	        	r.setReport(new SerialBlob(sbs[i].toString().getBytes()));
	        	if(debugMode){
	        		System.out.println("storeReport i="+i+" rptName="+rptName+" sz="+sbs[i].toString().length());
	        	}
	        	session.save(r);
			}//endfor(int i=1; i<=batchCnt; i++){
			
			tx.commit();
            }
			
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
			if(session != null)
			session.close(); 
			try {
				h.close(env, schemaName);
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
		
		if(finalTreeMap.size() ==0){
			sbs = new StringBuffer[1];
			sbs[0] = new StringBuffer();
			sbs[0].append("<table class=\"sortable\" border=\"1\" cellspacing=\"1\" cellpadding=\"5\" width=\"730px\">");
			sbs[0].append("<tr bgcolor=\"#99CCFF\">");
			sbs[0].append("<th width=\"90px\">Member ID</th>");
			sbs[0].append("<th width=\"50px\"># of PB Services</th>");
			sbs[0].append("<th width=\"70px\">Average PB Cost</th>");
			sbs[0].append("<th width=\"50px\"># of OP Services</th>");
			sbs[0].append("<th width=\"70px\">Average OP Cost</th>");
			sbs[0].append("<th width=\"50px\"># of IP Services</th>");
			sbs[0].append("<th width=\"80px\">Average IP Cost</th>");
			sbs[0].append("<th width=\"50px\"># of RX Services</th>");
			sbs[0].append("<th width=\"70px\">Average RX Cost</th>");
			sbs[0].append("<th width=\"150px\">Total Cost</th>");
			sbs[0].append("</tr>");
        	sbs[0].append("<tr><td colspan='10'>No Member Cost Record Found!</td></tr></table>");
        	batchCnt = 1;
        	return null;
		};
		
		StringBuffer sb = new StringBuffer();
		double topPercent = (actTotal+duplTotal) * percentAmt;
		double botPercent = (actTotal+duplTotal) * (1-percentAmt);
		
		batchTotal = (actTotal + duplTotal)/ batchLimit + 2;
		double curRowCnt = 0;
		List<String> duplList = new ArrayList<String>();
		double duplCost =0;
		
		boolean topPercentEnd = false;
		
		if(debugMode){
			System.out.println("getStatsAsHTML batchTotal="+batchTotal+" actTotal="+actTotal+" duplTotal="+duplTotal+" topPercent="+topPercent+" botPercent="+botPercent);
		}
		
		sbs = new StringBuffer[batchTotal]; 
		for(int i=0; i<batchTotal; i++){
			sbs[i] = new StringBuffer();
			sbs[i].append("<table class=\"sortable\" border=\"1\" cellspacing=\"1\" cellpadding=\"5\" width=\"730px\">");
			sbs[i].append("<tr bgcolor=\"#99CCFF\">");
			sbs[i].append("<th width=\"90px\">Member ID</th>");
			sbs[i].append("<th width=\"50px\"># of PB Services</th>");
			sbs[i].append("<th width=\"70px\">Average PB Cost</th>");
			sbs[i].append("<th width=\"50px\"># of OP Services</th>");
			sbs[i].append("<th width=\"70px\">Average OP Cost</th>");
			sbs[i].append("<th width=\"50px\"># of IP Services</th>");
			sbs[i].append("<th width=\"80px\">Average IP Cost</th>");
			sbs[i].append("<th width=\"50px\"># of RX Services</th>");
			sbs[i].append("<th width=\"70px\">Average RX Cost</th>");
			sbs[i].append("<th width=\"150px\">Total Cost</th>");
			sbs[i].append("</tr>");
			
			if(i==0){
	        	sbs[i].append("<tr><td>Highest 2%</td><td colspan=9>&nbsp;</td></tr>");
			}
		}
		
		
		Iterator finalIt = finalTreeMap.entrySet().iterator();
		int rowCnt = 0;//rowCnt per page
		batchCnt = 1;
		double keyCost = 0;
	    while (finalIt.hasNext()) {
	    	sb = new StringBuffer();
	        Map.Entry me = (Map.Entry)finalIt.next();
    		
	        if(curRowCnt <=topPercent || curRowCnt>botPercent) { //only show top&botPercent
	        	if(!topPercentEnd && curRowCnt>botPercent ){
		        	if(debugMode){
		        		System.out.println("batchCnt="+batchCnt+" rowCnt="+rowCnt+" topPercentEnd="+topPercentEnd+" curRowCnt="+curRowCnt);
		        	};
	        	    topPercentEnd = true;
	        		rowCnt = 1;
	        		
	        		//start lowest
	        		keyCost= (double)me.getKey();

	        	}

	        	if(rowCnt>batchLimit){
		        	if(debugMode){
		        		System.out.println("batchCnt="+batchCnt+" rowCnt="+rowCnt+" topPercentEnd="+topPercentEnd+" curRowCnt="+curRowCnt);
		        	};
	        		batchCnt++;
	        		rowCnt = 1;
	        		if(!topPercentEnd){
			        	sb.append("<tr><td>Highest 2%</td><td colspan=9>&nbsp;</td></tr>");
	        		}else{
			        	topPercentEnd = true;
	        		}
	        	}
	        	
	        	sb.append(me.getValue());
	        	rowCnt++;
	        	curRowCnt++;
	        	
    			//check if duplList exist
				duplList = new ArrayList<String>();
				duplCost =(double)me.getKey();
    			duplList = ( List<String> )duplMap.get(duplCost);
    			if(duplList != null){
        			for(int k=0; k<duplList.size(); k++){
        				String duplStr=duplList.get(k);
        	        	sb.append(duplStr);
        	        	rowCnt++;
        	        	curRowCnt++;
        			}
    			}
	        	
//	        	sbs[batchCnt-1].append(sb);
	        	if(topPercentEnd){
	        		break;
	        	}else{
		        	sbs[batchCnt-1].append(sb);
	        	}
	        }else{//if(curRowCnt <=topPercent || curRowCnt>botPercent) { //only show top10 &bot10
	    		curRowCnt++;
				//check if duplList exist
				duplList = new ArrayList<String>();
				duplCost =(double)me.getKey();
				duplList = ( List<String> )duplMap.get(duplCost);
				if(duplList != null){
					curRowCnt = curRowCnt+ duplList.size();
				}
	        	
	        }
	    };
		
	    
	    
	    //build lowest
	    rowCnt = 0;
    	sb = new StringBuffer();
	    batchCnt++;
    	sb.append("<tr><td>Lowest 2%</td><td colspan=9>&nbsp;</td></tr>");
    	sbs[batchCnt-1].append(sb);
		ArrayList<Double> keys = new ArrayList<Double>(finalTreeMap.keySet());
        for(int i=keys.size()-1; i>=0;i--){
	    	sb = new StringBuffer();
            if(keys.get(i) > keyCost){
            	break;
            };
            
        	if(rowCnt>batchLimit){
	        	if(debugMode){
	        		System.out.println("batchCnt="+batchCnt+" rowCnt="+rowCnt+" topPercentEnd="+topPercentEnd+" curRowCnt="+curRowCnt);
	        	};
        		batchCnt++;
        		rowCnt = 1;
	        	sb.append("<tr><td>Lowest 2%</td><td colspan=9>&nbsp;</td></tr>");
        	}
        	
            
        	sb.append(finalTreeMap.get(keys.get(i)) );
        	rowCnt++;
        	
			//check if duplList exist
			duplList = new ArrayList<String>();
			duplCost =(double)keys.get(i);
			duplList = ( List<String> )duplMap.get(duplCost);
			if(duplList != null){
    			for(int k=0; k<duplList.size(); k++){
    				String duplStr=duplList.get(k);
    	        	sb.append(duplStr);
    	        	rowCnt++;
    	        	if(rowCnt>batchLimit){
    	            	sbs[batchCnt-1].append(sb);
    	        		
    	        		batchCnt++;
    	        		rowCnt =1;
    	    	    	sb = new StringBuffer();
    		        	sb.append("<tr><td>Lowest 2%</td><td colspan=9>&nbsp;</td></tr>");
    	        		
    	        	}
    			}
			}
        	sbs[batchCnt-1].append(sb);
        }		

        
	    for(int j=batchCnt-1; j>=0; j--){
	    	sbs[j].append("</table>");
	    }
	    if(debugMode){
	    	System.out.println("curRow="+curRowCnt+" showed rows="+rowCnt+" batchCnt="+batchCnt);
	    }
	    
		return null;
		
	}

	
	
	private void initialize ()  {
		env = parameters.get("env") == null ?  "prd" :  parameters.get("env");
		
        schemaName = parameters.get("jobname")  == null ?  "javatest" : parameters.get("jobname");

		jobUid=parameters.get("jobuid");
		
		//vertica test 
		/*jobUid="1120";
		schemaName="Warren_Vertica_8_Bench20161129";
		env="prdv";*/

/*		jobUid="1120";
		schemaName="MD_APCD_Enroll_Fix_5_420160711";
		env="prdv2";*/
		
		
		//mysql test
		/*jobUid="1143";
		schemaName="CT_Medicaid_5400220161205";
		
		jobUid="1138";
		schemaName="Horizon_54002_20160921";*/		
		
		
		if(debugMode){
			System.out.println("schemaName="+schemaName+"jobId="+jobUid);
		}
        
	}
	
	class Report {
		
		long medicalClaimsTotalRecords = 0l;
		
		String reportName ="PatientCostDistReport";
		
	}
	//Arrays.fill(medicalClaimsTotalCosts, BigDecimal.ZERO);
	
	public static void main(String[] args) {
		
		log.info("Starting DataValidation Report");

		PatientCostDistReport instance = new PatientCostDistReport();
		
		// get parameters and make them available to all
		RunParameters rp = new RunParameters();
		instance.parameters = rp.loadParameters(args, parameterDefaults);
				
		String s = instance.parameters.get("runname") + "_" + instance.parameters.get("rundate");
		instance.parameters.put("jobname", s);
		
				
		RunParameters.parameters.put("configfolder", instance.parameters.get("configfolder"));
		
		instance.process();
		
		
		log.info("Complete PatientCost Dist. Report");
		
	}
	

	static String [][] parameterDefaults = {
		{"configfolder", "C:/workspace/ECR_Analytics/trunk/EpisodeConstruction/src/"},
		{"rundate", "20150611"},
		{"runname", "CHC"},
		{"jobuid", "1043"}
	};
	
	
	private static org.apache.log4j.Logger log = Logger.getLogger(PatientCostDistReport.class);

}
