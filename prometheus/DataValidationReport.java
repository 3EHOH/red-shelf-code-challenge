





public class DataValidationReport {
	
	
	HashMap<String, String> parameters;
	
	Report rpt = new Report();
	String schemaName=null;
	String jobUid=null;
	String env=null;	
    long claimRecCnt=0;
    long memRecCnt = 0;
    long enRecCnt = 0;
    long provRecCnt = 0;
    long rxRecCnt = 0;
    
    SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");
    String minDate = null;
    String maxDate = null;
    
    long memClaimRecCnt = 0;
    int idx = 0;
    
    boolean debugMode = false;
    HibernateHelper h;
	SessionFactory factory;    
	
	/**
	 * static parameter constructor, just for testing
	 */
	public DataValidationReport() {	}
	
	/**
	 * constructor using parameters pulled from control database
	 * @param parameters
	 */
	public DataValidationReport(HashMap<String, String> parameters) {
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
	
	
	
	
	@SuppressWarnings("rawtypes")
	private void generateReport() {
		List<Class<?>> cList = new ArrayList<Class<?>>();
        cList.add(PlanMember.class);
        cList.add(Enrollment.class);
        cList.add(Provider.class);
        cList.add(ClaimRx.class);
        cList.add(ClaimsCombinedTbl.class);
		

        HibernateHelper h = new HibernateHelper(cList, env, schemaName);
        
        String connUrl=null;
        String rptQry=null;
		Connection conn=null;
		ResultSet rs=null;
        
		String clmType ="";
	    
        try{
        	
       		if(debugMode){
       			System.out.println("Env="+env+ " DbDial="+h.getCurrentConfig().getConnParms().getDialect()+" schema="+schemaName+" db="+h.getCurrentConfig().getConnParms().getDatabase());
       		}
        	
            if("vertica".equalsIgnoreCase(h.getCurrentConfig().getConnParms().getDialect())){
            	connUrl="jdbc:vertica://" + h.getCurrentConfig().getConnParms().getDbUrl()+"/" + h.getCurrentConfig().getConnParms().getDatabase();
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
       		
       		
			rptQry ="select count(*) total from "+schemaName+".claims_combined c where c.claim_line_type_code is not null and c.claim_line_type_code <>'' and c.claim_line_type_code not in ('RX') ";
       	    rs = conn.createStatement().executeQuery(rptQry);
            while(rs.next()){
		    	claimRecCnt= rs.getLong("total");
		    }
		    
			rptQry ="select count(*) total from "+schemaName+".member ";
       	    rs = conn.createStatement().executeQuery(rptQry);
            while(rs.next()){
				memRecCnt =rs.getLong("total");
			}

			rptQry ="select count(*) total from "+schemaName+".enrollment";
       	    rs = conn.createStatement().executeQuery(rptQry);
            while(rs.next()){
				enRecCnt =rs.getLong("total");
			}
			if(debugMode){
				System.out.println("clmCnt="+claimRecCnt+" memCnt="+memRecCnt+" enrollCnt="+enRecCnt);
			}
			
			rptQry ="select count(*) total from "+schemaName+".provider";
       	    rs = conn.createStatement().executeQuery(rptQry);
            while(rs.next()){
				provRecCnt =rs.getLong("total");
			}
		    
			rptQry ="select count(*) total from "+schemaName+".claims_combined c where c.claim_line_type_code='RX'";
       	    rs = conn.createStatement().executeQuery(rptQry);
            while(rs.next()){
				rxRecCnt= rs.getLong("total");
			}
			if(debugMode){
				System.out.println("prvdCnt="+provRecCnt+" rxRecCnt="+rxRecCnt);
			} 

    		rptQry="select min(begin_date) min_beg_dt,max(begin_date) max_beg_dt from "+schemaName+".claims_combined c where c.claim_line_type_code is not null and c.claim_line_type_code <>'' ";
       	    rs = conn.createStatement().executeQuery(rptQry);
            while(rs.next()){
    		    minDate=sdf.format((java.util.Date) rs.getDate("min_beg_dt"));
    		    maxDate=sdf.format((java.util.Date) rs.getDate("max_beg_dt"));
    		}
			if(debugMode){
    			System.out.println("mindDt="+ minDate+" maxDt="+maxDate);
			} 
			
			rptQry="select count(*) total from (select count(*) total from "+schemaName+".member m, "+schemaName+".claims_combined c where c.claim_line_type_code is not null and c.claim_line_type_code <>'' and m.member_id=c.member_id group by m.member_id order by m.member_id)t";
       	    rs = conn.createStatement().executeQuery(rptQry);
            while(rs.next()){
				memClaimRecCnt =rs.getLong("total");
			}
			if(debugMode){
				System.out.println(" memClaimRecCnt="+memClaimRecCnt);
			}
			
		    idx = 0;
		    rptQry="select c.claim_line_type_code,count(*) total, avg(allowed_amt) avg_amt from "+schemaName+".claims_combined c where c.claim_line_type_code is not null and c.claim_line_type_code <>'' group by  c.claim_line_type_code order by c.claim_line_type_code";
       	    rs = conn.createStatement().executeQuery(rptQry);
            while(rs.next()){
    			clmType =rs.getString("claim_line_type_code");
    			if("IP".equalsIgnoreCase(clmType)){
    				rpt.claimType[idx] ="All Inpatient Claims";
    				rpt.claimTotal[idx]= rs.getLong("total");
    				rpt.claimAmtAvg[idx]= (Double) rs.getDouble("avg_amt");
    			    rpt.claimTotal[3]= rpt.claimTotal[idx];
    			    idx++;
    			}else if("OP".equalsIgnoreCase(clmType)){
    				rpt.claimType[idx] ="All Outpatient Claims";
    				rpt.claimTotal[idx]= rs.getLong("total");
    				rpt.claimAmtAvg[idx]= (Double) rs.getDouble("avg_amt");
    			    idx++;
    			}else if("PB".equalsIgnoreCase(clmType)){
    				rpt.claimType[idx] ="All Professional Claims";
    				rpt.claimTotal[idx]= rs.getLong("total");
    				rpt.claimAmtAvg[idx]= (Double) rs.getDouble("avg_amt");
    			    idx=idx+2;
    			}else if("RX".equalsIgnoreCase(clmType)){
    				rpt.claimType[idx] ="All Pharmacy Claims";
    				rpt.claimTotal[idx]= rs.getLong("total");
    				rpt.claimAmtAvg[idx]= (Double) rs.getDouble("avg_amt");
    			}

    		}

			if(debugMode){
				System.out.println(" done calc all clmtypes");
			}
    		
    		//all services(IP,OP,PB)
		    rptQry="select count(*) total, avg(allowed_amt) avg_amt from "+schemaName+".claims_combined c where c.claim_line_type_code is not null and c.claim_line_type_code <>'' and c.claim_line_type_code<>'RX' ";
    		idx = 3;
       	    rs = conn.createStatement().executeQuery(rptQry);
            while(rs.next()){
				rpt.claimType[idx] ="All Service Claims";
				rpt.claimTotal[idx]= rs.getLong("total");
				rpt.claimAmtAvg[idx]= (Double) rs.getDouble("avg_amt");
    		}
    		
    		rpt.claimTotalPerc[0] =(float)rpt.claimTotal[0] / rpt.claimTotal[3] *100;
    		rpt.claimTotalPerc[1] =(float)rpt.claimTotal[1] / rpt.claimTotal[3] *100;
    		rpt.claimTotalPerc[2] =(float)rpt.claimTotal[2] / rpt.claimTotal[3] *100;
    		rpt.claimTotalPerc[3] =0;
    		rpt.claimTotalPerc[4] =0;

    		log.info("rptHtml="+getStatsAsHTML());
    		
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

	@SuppressWarnings("rawtypes")
	private void generateReportHibern() {
		List<Class<?>> cList = new ArrayList<Class<?>>();
        cList.add(PlanMember.class);
        cList.add(Enrollment.class);
        cList.add(Provider.class);
        cList.add(ClaimRx.class);
        cList.add(ClaimsCombinedTbl.class);
		

        HibernateHelper h = new HibernateHelper(cList, env, schemaName);
        SessionFactory factory = h.getFactory(env, schemaName);
        
        StatelessSession session = factory.openStatelessSession();
        
        Query query=null;
	    List resultList = null;
	    String cntQry ="";
		Object[] row = null;
		String clmType ="";
		Iterator it=null;
	    
        try{
        	
       		if(debugMode){
       			System.out.println("Env="+env+ " DbDial="+h.getCurrentConfig().getConnParms().getDialect()+" schema="+schemaName+" db="+h.getCurrentConfig().getConnParms().getDatabase());
       		}
        	
			cntQry ="select count(*) from ClaimsCombinedTbl c where c.claim_line_type_code is not null and c.claim_line_type_code <>'' and c.claim_line_type_code not in ('RX') ";
			query = session.createQuery(cntQry);
		    if(query.uniqueResult() != null){
		    	claimRecCnt= (Long)query.uniqueResult();
		    }
		    
			cntQry ="select count(*) from PlanMember";
			query = session.createQuery(cntQry);
			if(query.uniqueResult() != null){
				memRecCnt =(Long)query.uniqueResult();
			}

			cntQry ="select count(*) from Enrollment";
			query = session.createQuery(cntQry);
			if(query.uniqueResult() != null){
				enRecCnt =(Long)query.uniqueResult();
			}
			if(debugMode){
				System.out.println("clmCnt="+claimRecCnt+" memCnt="+memRecCnt+" enrollCnt="+enRecCnt);
			}
			
			cntQry ="select count(*) from Provider";
			query = session.createQuery(cntQry);
			if(query.uniqueResult() != null){
				provRecCnt =(Long)query.uniqueResult();
			}
		    
			cntQry ="select count(*) from ClaimsCombinedTbl c where c.claim_line_type_code='RX'";
		    query = session.createQuery(cntQry);
			if(query.uniqueResult() != null){
				rxRecCnt= (Long)query.uniqueResult();
			}
			if(debugMode){
				System.out.println("prvdCnt="+provRecCnt+" rxRecCnt="+rxRecCnt);
			} 

    		it = session.createQuery("select min(begin_date),max(begin_date) from ClaimsCombinedTbl c where c.claim_line_type_code is not null and c.claim_line_type_code <>'' ").list().iterator();
    		while ( it.hasNext() ) {
    		    row = (Object[]) it.next();
    		    minDate=sdf.format((java.util.Date)row[0]);
    		    maxDate=sdf.format((java.util.Date)row[1]);
    		}
			if(debugMode){
    			System.out.println("mindDt="+ minDate+" maxDt="+maxDate);
			} 
			
			cntQry="select count(*) from PlanMember m,  ClaimsCombinedTbl c where c.claim_line_type_code is not null and c.claim_line_type_code <>'' and m.member_id=c.member_id group by m.member_id order by m.member_id";
		    query = session.createQuery(cntQry);
		    resultList = query.list();
			if(resultList  != null){
				memClaimRecCnt =resultList.size();
			}
			if(debugMode){
				System.out.println(" memClaimRecCnt="+resultList.size());
			}
			
		    idx = 0;
		    cntQry="select c.claim_line_type_code,count(*), avg(allowed_amt) from ClaimsCombinedTbl c where c.claim_line_type_code is not null and c.claim_line_type_code <>'' group by  c.claim_line_type_code order by c.claim_line_type_code";
    		it = session.createQuery(cntQry).list().iterator();
    		while ( it.hasNext() ) {
    		    row = (Object[]) it.next();
    			clmType =(String)row[0];
    			if("IP".equalsIgnoreCase(clmType)){
    				rpt.claimType[idx] ="All Inpatient Claims";
    				rpt.claimTotal[idx]= (long)row[1];
    				rpt.claimAmtAvg[idx]= (double)row[2];
    			    rpt.claimTotal[3]= rpt.claimTotal[idx];
    			    idx++;
    			}else if("OP".equalsIgnoreCase(clmType)){
    				rpt.claimType[idx] ="All Outpatient Claims";
    				rpt.claimTotal[idx]= (long)row[1];
    				rpt.claimAmtAvg[idx]= (double)row[2];
    			    idx++;
    			}else if("PB".equalsIgnoreCase(clmType)){
    				rpt.claimType[idx] ="All Professional Claims";
    				rpt.claimTotal[idx]= (long)row[1];
    				rpt.claimAmtAvg[idx]= (double)row[2];
    			    idx=idx+2;
    			}else if("RX".equalsIgnoreCase(clmType)){
    				rpt.claimType[idx] ="All Pharmacy Claims";
    				rpt.claimTotal[idx]= (long)row[1];
    				rpt.claimAmtAvg[idx]= (double)row[2];
    			}

    		}

			if(debugMode){
				System.out.println(" done calc all clmtypes");
			}
    		
    		//all services(IP,OP,PB)
		    cntQry="select count(*), avg(allowed_amt) from ClaimsCombinedTbl c where c.claim_line_type_code is not null and c.claim_line_type_code <>'' and c.claim_line_type_code<>'RX' ";
    		it = session.createQuery(cntQry).list().iterator();
    		idx = 3;
    		while ( it.hasNext() ) {
    		    row = (Object[]) it.next();
				rpt.claimType[idx] ="All Service Claims";
				rpt.claimTotal[idx]= (long)row[0];
				rpt.claimAmtAvg[idx]= (double)row[1];
    		}
    		
    		rpt.claimTotalPerc[0] =(float)rpt.claimTotal[0] / rpt.claimTotal[3] *100;
    		rpt.claimTotalPerc[1] =(float)rpt.claimTotal[1] / rpt.claimTotal[3] *100;
    		rpt.claimTotalPerc[2] =(float)rpt.claimTotal[2] / rpt.claimTotal[3] *100;
    		rpt.claimTotalPerc[3] =0;
    		rpt.claimTotalPerc[4] =0;
    		
		}catch (HibernateException e) {
			e.printStackTrace(); 
		}finally {
			if(session != null){session.close();}; 
			try {
				h.close(env, schemaName);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	    
		//log.info(getStatsAsHTML());
		
	}
	
	
	
	private void storeReportV(String connUrl, String user, String passWd){
		Properties connProp = new Properties();
        connProp.put("user", user);
        connProp.put("password", passWd);

        String procRptTab ="processReport";
		Connection conn=null;
		
		String insertQ="insert INTO "+schemaName+"."+procRptTab+ " (jobUid,stepName,reportName,report,uid ) VALUES(?,?,?,?,?)";
		String updateQ="update "+schemaName+"."+procRptTab+" set report=? where jobUid =? AND stepname = '" +
				ProcessJobStep.STEP_POSTNORMALIZATION_REPORT  + "' and reportName='"+rpt.reportName+"'";
		
		String nextUidQ=" select ZEROIFNULL (max(uid))+1 uid from  "+schemaName+"."+procRptTab;
		PreparedStatement pstmt=null;
		ResultSet rs=null;
		int uId=0;
		try {
            conn = DriverManager.getConnection(connUrl, connProp);
            conn.setAutoCommit (false); 
    
            rs = conn.createStatement().executeQuery(nextUidQ);
            while(rs.next()){
            	uId=rs.getInt("uid");
            }
            
            pstmt = conn.prepareStatement(updateQ);
            pstmt.setBytes(1,getStatsAsHTML().getBytes());
            pstmt.setString(2,jobUid );
            int rtnCode= pstmt.executeUpdate();
            if(rtnCode ==0){
            	pstmt = conn.prepareStatement(insertQ);
                pstmt.setString(1,jobUid );
                pstmt.setString(2, ProcessJobStep.STEP_POSTNORMALIZATION_REPORT);
                pstmt.setString(3,rpt.reportName);
                pstmt.setBytes(4,getStatsAsHTML().getBytes());
                pstmt.setInt(5,uId );
            }
            pstmt.execute();
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

        Transaction tx = null;
        String connUrl=null;
        String user=null;
        String passWd=null;
        Session session = null;
        
        try{
       		if(debugMode){
       			System.out.println("Db Dialect="+h.getCurrentConfig().getConnParms().getDialect()+env+" schema="+schemaName);
       		}
            	
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
		
            	String hql = "FROM ProcessReport WHERE jobuid = :jobuid AND stepname = '" + ProcessJobStep.STEP_POSTNORMALIZATION_REPORT  + "' and reportName='"+rpt.reportName+"'";
            	Query query = session.createQuery(hql).setLockOptions(new LockOptions(LockMode.PESSIMISTIC_WRITE));
            	query.setParameter("jobuid", jobUid);

            	@SuppressWarnings("unchecked")
            	List<ProcessReport> results = query.list();
            	ProcessReport r;
            	if (results.isEmpty()) {
            		r = new ProcessReport();
            		r.setJobUid(Long.parseLong(jobUid));
            		r.setStepName(ProcessJobStep.STEP_POSTNORMALIZATION_REPORT);
            		r.setReportName(rpt.reportName);
            	} else
            		r = results.get(0);
            	r.setReport(new SerialBlob(getStatsAsHTML().getBytes()));
			
            	session.save(r);
			
            	tx.commit();
            }
			
		}catch (HibernateException e) {
			if (tx!=null) tx.rollback();
			e.printStackTrace(); 
		/*} catch (SerialException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();*/
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
		
		
		StringBuffer sb = new StringBuffer();
		
		sb.append("<table border=\"1\" cellspacing=\"1\" cellpadding=\"5\" width=\"550px\" >");
		sb.append("<tr bgcolor=\"#99CCFF\">");
		sb.append("<th width=\"400px\">Input Record Counts</th>");
		sb.append("<th width=\"150px\">Record Total</th>");
		sb.append("</tr>");

        sb.append("<tr>");
        sb.append("<div align=\"middle\"><td bgcolor=\"#99CCFF\"><b>Claim Input Records</b></td></div>"); 	
        sb.append("<div><td align=\"left\">" + claimRecCnt + "</td></div>"); 	
        sb.append("</tr>");
		
        sb.append("<tr>");
        sb.append("<div align=\"middle\"><td bgcolor=\"#99CCFF\"><b>Enrollment Input Records</b></td></div>"); 	
        sb.append("<div><td align=\"left\">" + enRecCnt + "</td></div>"); 	
        sb.append("</tr>");

        sb.append("<tr>");
        sb.append("<div align=\"middle\"><td bgcolor=\"#99CCFF\"><b>Plan Member Input Records</b></td></div>"); 	
        sb.append("<div><td align=\"left\">" + memRecCnt + "</td></div>"); 	
        sb.append("</tr>");

        sb.append("<tr>");
        sb.append("<div align=\"middle\"><td bgcolor=\"#99CCFF\"><b>Provider Input Records</b></td></div>"); 	
        sb.append("<div><td align=\"left\">" +provRecCnt  + "</td></div>"); 	
        sb.append("</tr>");

        sb.append("<tr>");
        sb.append("<div align=\"middle\"><td bgcolor=\"#99CCFF\"><b>Rx Claims Input Records</b></td></div>"); 	
        sb.append("<div><td align=\"left\">" +rxRecCnt  + "</td></div>"); 	
        sb.append("</tr>");

        sb.append("<tr>");
        sb.append("<div align=\"middle\"><td bgcolor=\"#99CCFF\"><b>Earliest Claim Begin Date</b></td></div>"); 	
        sb.append("<div><td align=\"left\">" + minDate + "</td></div>"); 	
        sb.append("</tr>");

        sb.append("<tr>");
        sb.append("<div align=\"middle\"><td bgcolor=\"#99CCFF\"><b>Latest Claim Begin Date</b></td></div>"); 	
        sb.append("<div><td align=\"left\">" + maxDate + "</td></div>"); 	
        sb.append("</tr>");

        sb.append("<tr>");
        sb.append("<div align=\"middle\"><td bgcolor=\"#99CCFF\"><b>Total Members with Claims</b></td></div>"); 	
        sb.append("<div><td align=\"left\">" + memClaimRecCnt + "</td></div>"); 	
        sb.append("</tr>");

		sb.append("</table>");
		
		sb.append("<br><br>");
		
		sb.append("<table border=\"1\" cellspacing=\"1\" cellpadding=\"5\" width=\"730px\" >");
		sb.append("<tr bgcolor=\"#99CCFF\">");
		sb.append("<th width=\"220px\">Claim Type</th>");
		sb.append("<th width=\"140px\">Total Unique Claims</th>");
		sb.append("<th width=\"150px\">% of All Service Claims</th>");
		sb.append("<th width=\"220px\">Average Claim Amount</th>");
		sb.append("</tr>");
		
		for(int i=0; i<5; i++){
			sb.append("<tr>");
	        sb.append("<div align=\"middle\"><td bgcolor=\"#99CCFF\">"+rpt.claimType[i]+"</td></div>"); 	
	        sb.append("<div><td align=\"left\">" + rpt.claimTotal[i] + "</td></div>");
	        if(i>2){
	        	sb.append("<div><td align=\"left\">&nbsp;</td></div>");
	        }else{
	        	sb.append("<div><td align=\"left\">" + String.format("%.10f",rpt.claimTotalPerc[i]) + "</td></div>");
	        }
	        sb.append("<div><td align=\"left\">" + NumberFormat.getCurrencyInstance().format(rpt.claimAmtAvg[i])  + "</td></div>"); 	
	        sb.append("</tr>");
			
		}

		sb.append("</table>");
		
//    	log.info ("rptHtml="+sb.toString());
		
		
		return sb.toString();
		
	}

		
	
	private void initialize ()  {

        schemaName = parameters.get("jobname")  == null ?  "javatest" : parameters.get("jobname");

		jobUid=parameters.get("jobuid");
		env = parameters.get("env") == null ?  "prd" :  parameters.get("env");
		
		//vertica test 
/*		jobUid="1120";
		schemaName="Warren_Vertica_8_Bench20161129";
		env="prdv";*/

/*		jobUid="1120";
		schemaName="MD_APCD_Enroll_Fix_5_420160711";
		env="prdv2";*/
		
		
		//mysql test
/*		jobUid="1143";
		schemaName="CT_Medicaid_5400220161205";*/
		
		/*jobUid="1138";
		schemaName="Horizon_54002_20160921";*/
		
		
		if(debugMode){
			System.out.println("schemaName="+schemaName+" jobId="+jobUid);
		}
		
	}
	
	class Report {
		
		long medicalClaimsTotalRecords = 0l;
		
		int maxSize = 5;
		
		String claimType[] =new String[maxSize];
		long claimTotal[] =new long[maxSize];
		float claimTotalPerc[] =new float[maxSize];
		double claimAmtAvg[] =new double[maxSize];
		
		String reportName ="DataValidationReport";
		
	}
	
	public static void main(String[] args) {
		
		log.info("Starting DataValidation Report");

		DataValidationReport instance = new DataValidationReport();
		
		// get parameters and make them available to all
		RunParameters rp = new RunParameters();
		instance.parameters = rp.loadParameters(args, parameterDefaults);
				
		String s = instance.parameters.get("runname") + "_" + instance.parameters.get("rundate");
		instance.parameters.put("jobname", s);
		
				
		RunParameters.parameters.put("configfolder", instance.parameters.get("configfolder"));
		
		instance.process();
		
		
		log.info("Complete DataValidation Report 730");
	}
	

	static String [][] parameterDefaults = {
		{"configfolder", "C:/workspace/ECR_Analytics/trunk/EpisodeConstruction/src/"},
		{"rundate", "20150611"},
		{"runname", "CHC"},
		{"jobuid", "1043"}
	};
	
	
	private static org.apache.log4j.Logger log = Logger.getLogger(DataValidationReport.class);

}
