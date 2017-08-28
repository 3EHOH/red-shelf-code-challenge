





public class DataSummaryReport {
	
	
	HashMap<String, String> parameters;
	
	Report rpt = new Report();
	String schemaName=null;
	String jobUid=null;
	String env =null;
	
    
    SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");
    int idx = 0;
    
    HibernateHelper h;
	SessionFactory factory;
	
	boolean debugMode = false;
	
	
	/**
	 * static parameter constructor, just for testing
	 */
	public DataSummaryReport() {	}
	
	/**
	 * constructor using parameters pulled from control database
	 * @param parameters
	 */
	public DataSummaryReport(HashMap<String, String> parameters) {
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
	
	@SuppressWarnings({ "unchecked" })
	private void generateReport() {
		List<Class<?>> cList = new ArrayList<Class<?>>();
        cList.add(ClaimsCombinedTbl.class);
        
        HibernateHelper h = new HibernateHelper(cList, env, schemaName);
        
        String connUrl=null;
	    
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
        	generateReportDetail(connUrl, h.getCurrentConfig().getConnParms().getDbUser(), h.getCurrentConfig().getConnParms().getDbPw());
       		
            
		}catch (Exception e) {
			e.printStackTrace(); 
		}finally {
			try {
				h.close(env, schemaName);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	    
	}
	
	
	
	private void generateReportDetail(String connUrl, String user, String passWd){
		Properties connProp = new Properties();
        connProp.put("user", user);
        connProp.put("password", passWd);

		Connection conn=null;
		
		String rptQry=null;
		
		PreparedStatement pstmt=null;
		ResultSet rs=null;
		int i =0;
		try {
			
    		//data summary
			String claimTypes[]= {"Stay Claims", "Outpatient Facility", "Professional Claims","RX Claims", "OP/IP/RX Claims"};
            conn = DriverManager.getConnection(connUrl, connProp);

            //all clmtypes
            rptQry="select c.claim_line_type_code, min(c.begin_date) min_dt, max(c.begin_date) max_dt, count(*) clm_count,sum(c.allowed_amt) sum_amt, avg(c.allowed_amt) avg_amt from "+
            		schemaName+".claims_combined c where c.claim_line_type_code is not null and c.claim_line_type_code<>'' group by c.claim_line_type_code order by  c.claim_line_type_code";            
       	    rs = conn.createStatement().executeQuery(rptQry);
            while(rs.next()){
	    		rpt.dataSummary[i][0] =claimTypes[i];
        		rpt.dataSummary[i][1]=sdf.format((java.util.Date) rs.getDate("min_dt"))+"-"+sdf.format((java.util.Date) rs.getDate("max_dt"));
	    		rpt.dataSummary[i][3] =String.valueOf(rs.getLong("clm_count"));
        		rpt.dataSummaryAmt[i]=(BigDecimal)rs.getBigDecimal("sum_amt"); 
        		rpt.dataSummaryAvg[i]=(Double) rs.getDouble("avg_amt");
	    		i++;
            }
    		if(debugMode){
    			System.out.println("done all clmtype");
    		}
            	
            //all clmtypes no PB
            rptQry="select case c.claim_line_type_code when 'IP' then 'NOTPB' ELSE 'NOTPB' END, min(c.begin_date) min_dt, max(c.begin_date) max_dt, count(*) clm_count,sum(c.allowed_amt) sum_amt, avg(c.allowed_amt) avg_amt from  "+
            schemaName+".claims_combined  c where c.claim_line_type_code is not null and c.claim_line_type_code<>'' and c.claim_line_type_code not in ('PB') group by case c.claim_line_type_code when 'IP' then 'NOTPB'  ELSE 'NOTPB' END ";
       	    rs = conn.createStatement().executeQuery(rptQry);
            while(rs.next()){
	    		rpt.dataSummary[i][0] =claimTypes[i];
        		rpt.dataSummary[i][1]=sdf.format((java.util.Date) rs.getDate("min_dt"))+"-"+sdf.format((java.util.Date) rs.getDate("max_dt"));
	    		rpt.dataSummary[i][3] =String.valueOf(rs.getLong("clm_count"));
        		rpt.dataSummaryAmt[i]=(BigDecimal)rs.getBigDecimal("sum_amt"); 
        		rpt.dataSummaryAvg[i]=(Double) rs.getDouble("avg_amt");
	    		i++;
            }
    		if(debugMode){
    			System.out.println("done all clmtype no PB");
    		}

            //member count all clmTypes
            rptQry="select t.claim_line_type_code, count(*) mem_count from ( select c.claim_line_type_code,c.member_id, count(*) from "+
            schemaName+".claims_combined c where c.claim_line_type_code is not null and c.claim_line_type_code<>'' group by c.claim_line_type_code, c.member_id) t group by t.claim_line_type_code";
            i=0;
       	    rs = conn.createStatement().executeQuery(rptQry);
            while(rs.next()){
	    		rpt.dataSummary[i][2] =String.valueOf(rs.getLong("mem_count"));
	    		i++;
            }
    		if(debugMode){
    			System.out.println("done all clmtype mem");
    		}

            //member count no PB
            rptQry="select t.clm_type, count(*) mem_count from (select case c.claim_line_type_code when 'IP' then 'NOTPB' ELSE 'NOTPB' END clm_type,c.member_id, count(*) from "+
            schemaName+".claims_combined c where c.claim_line_type_code is not null and c.claim_line_type_code<>'' and c.claim_line_type_code not in ('PB') group by case c.claim_line_type_code when 'IP' then 'NOTPB' ELSE 'NOTPB' END, c.member_id	) t group by t.clm_type";
       	    rs = conn.createStatement().executeQuery(rptQry);
            while(rs.next()){
	    		rpt.dataSummary[i][2] =String.valueOf(rs.getLong("mem_count"));
            }
    		if(debugMode){
    			System.out.println("done all clmtype mem no PB");
    		}

//test            getStatsAsHTML();            
            
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

        String connUrl=null;
        String user=null;
        String passWd=null;
        Transaction tx = null;
        Session session = null;
        try{
   		if(debugMode){
   			System.out.println("Db Dialect="+h.getCurrentConfig().getConnParms().getDialect()+" env="+env+" schema="+schemaName);
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
		        System.out.println("new ProcessReport ");		
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
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();*/
		} catch (Exception e) {
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
		
		//data Summary
		sb.append("<table border=\"1\" cellspacing=\"1\" cellpadding=\"5\" width=\"730px\" >");
		sb.append("<tr bgcolor=\"#99CCFF\">");
		sb.append("<th width=\"130px\">Claim Type</th>");
		sb.append("<th width=\"170px\">Claim Date Range</th>");
		sb.append("<th width=\"100px\">No of Unique Patients</th>");
		sb.append("<th width=\"100px\">No of Claim Lines</th>");
		sb.append("<th width=\"140px\">Total Allowed Amt</th>");
		sb.append("<th width=\"90px\">Avg. Claim Allowed Amt</th>");
		sb.append("</tr>");
		for(int i=0; i<5; i++){
			sb.append("<tr>");
			for(int j=0; j<4; j++){
				sb.append("<div align=\"middle\"><td>"+rpt.dataSummary[i][j]+" </td></div>");
			}
			/*for(int k=0; k<2; k++){
				sb.append("<div align=\"middle\"><td>"+NumberFormat.getCurrencyInstance().format(rpt.dataSummaryAmt[i][k])+" </td></div>");
			}*/
			sb.append("<div align=\"middle\"><td>"+NumberFormat.getCurrencyInstance().format(rpt.dataSummaryAmt[i])+" </td></div>");
			sb.append("<div align=\"middle\"><td>"+NumberFormat.getCurrencyInstance().format(rpt.dataSummaryAvg[i])+" </td></div>");
			
			sb.append("</tr>");
		}

    	sb.append("</table>");
    	
    	//log.info ("rptHtml="+sb.toString());
    	
		return sb.toString();
		
	}

	
	
	
	private void initialize ()  {
        schemaName = parameters.get("jobname")  == null ?  "javatest" : parameters.get("jobname");
		env = parameters.get("env") == null ?  "prd" :  parameters.get("env");
		

		jobUid=parameters.get("jobuid");
		
		//vertica test 
		/*jobUid="1120";
		schemaName="Warren_Vertica_8_Bench20161129";
		env="prdv";

		jobUid="1120";
		schemaName="MD_APCD_Enroll_Fix_5_420160711";
		env="prdv2";*/
		
		
		//mysql test
/*		jobUid="1143";
		schemaName="CT_Medicaid_5400220161205";*/
		
		/*jobUid="1138";
		schemaName="Horizon_54002_20160921";*/
		
		if(debugMode){
			System.out.println("schemaName="+schemaName+" env="+env+" jobId="+jobUid);
		}
        

		
	}
	
	class Report {
		
		
		int maxSize = 5;
		
		String dataSummary[][]= new String[5][4];
		//double dataSummaryAmt[][]= new double[5][2];
		BigDecimal dataSummaryAmt[]= new BigDecimal[5];
		Double dataSummaryAvg[]= new Double[5];
		
		String reportName ="DataSummaryReport";
		
		
	}
	//Arrays.fill(medicalClaimsTotalCosts, BigDecimal.ZERO);
	
	public static void main(String[] args) {
		
		log.info("Starting Data Summary Report");

		DataSummaryReport instance = new DataSummaryReport();
		
		// get parameters and make them available to all
		RunParameters rp = new RunParameters();
		instance.parameters = rp.loadParameters(args, parameterDefaults);
				
		String s = instance.parameters.get("runname") + "_" + instance.parameters.get("rundate");
		instance.parameters.put("jobname", s);
		
				
		RunParameters.parameters.put("configfolder", instance.parameters.get("configfolder"));
		
		instance.process();
		
		
		log.info("Complete Summary Report");
	}
	

	static String [][] parameterDefaults = {
		{"configfolder", "C:/workspace/ECR_Analytics/trunk/EpisodeConstruction/src/"},
		{"rundate", "20150611"},
		{"runname", "CHC"},
		{"jobuid", "1043"}
	};
	
	
	private static org.apache.log4j.Logger log = Logger.getLogger(DataSummaryReport.class);

}
