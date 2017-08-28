




public class PrincipalDXPXReport {
	
	
	HashMap<String, String> parameters;
	
	String schemaName=null;
	String jobUid=null;
	String env = null;
	Report rpt = new Report();
    
    boolean debugMode = false;
    
	long totalRec = 0;
	

	/**
	 * static parameter constructor, just for testing
	 */
	public PrincipalDXPXReport() {	}
	
	/**
	 * constructor using parameters pulled from control database
	 * @param parameters
	 */
	public PrincipalDXPXReport(HashMap<String, String> parameters) {
		this.parameters = parameters;
	}
	
	
	private void process () {

		initialize();
		if(debugMode){
			System.out.println("init done!");
		}
		int rtnCode =0;
    	
		rtnCode = generateReport();
		if(debugMode){
			System.out.println("generateReport done!");
		}
		
		storeReport();
		if(debugMode){
			System.out.println("storeReport done!");
		}
		
	}
		
	
	@SuppressWarnings("unchecked")
	private int generateReport() {
		List<Class<?>> cList = new ArrayList<Class<?>>();
        cList.add(ClaimRx.class);
        cList.add(MedCode.class);
        cList.add(ClaimLineTbl.class);
		
		HibernateHelper h = new HibernateHelper(cList, env, schemaName);
  
        String connUrl=null;
        String rptQry=null;
		Connection conn=null;
		ResultSet rs=null;
		
		int rtnCode = 0;
		
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
        	
        	
         	rptQry ="select count(*) total from "+schemaName+".claims_combined cc where  cc.claim_line_type_code not in ('RX')";
       	    rs = conn.createStatement().executeQuery(rptQry);
            while(rs.next()){
            	totalRec = rs.getLong("total");
            	break;
            }
            
       		if(debugMode){
       			System.out.println("totalRec="+totalRec);
       		}
            
            
        	//principal DX
        	rptQry="select count(*) total from "+schemaName+".claims_combined cc left join "+schemaName+".code cd on cd.u_c_id=cc.id "+
        	"where cc.claim_line_type_code not in ('RX') and cd.nomen='DX' and cd.principal=1 ";
       	    rs = conn.createStatement().executeQuery(rptQry);
            while(rs.next()){
            	rpt.DX2Count=rs.getLong("total");
            	break;
		    } 
       		if(debugMode){
       			System.out.println("DX2Count="+rpt.DX2Count);
       		}

            
        	//invalid principal DX
        	rptQry="select count(*) total from "+schemaName+".claims_combined cc,"+schemaName+".code cd where cc.claim_line_type_code not in ('RX') "+
        	"and cd.nomen='DX' and cd.principal=1 and cc.id =cd.u_c_id and cd.code_value in ('','X') ";
       	    rs = conn.createStatement().executeQuery(rptQry);
            while(rs.next()){
            	rpt.DXInvalidCount=rs.getLong("total");
            	break;
		    }		    
       		if(debugMode){
       			System.out.println("DXInvalidCount="+rpt.DXInvalidCount);
       		}
        	
        	//no principal DX
	        rptQry="select count(*) total from "+schemaName+".claims_combined cc where cc.claim_line_type_code not like 'RX' and cc.id not in (select u_c_id from "+schemaName+".code cd where cd.nomen='DX' and cd.principal=1 )";
       	    rs = conn.createStatement().executeQuery(rptQry);
            while(rs.next()){
            	rpt.DXNoCount=rs.getLong("total");
            	break;
		    }		    
       		if(debugMode){
       			System.out.println("rpt.DXNoCount="+rpt.DXNoCount);
       		}
		    
        	//principal PX
        	rptQry="select count(*) total from "+schemaName+".claims_combined cc left join "+schemaName+".code cd on cd.u_c_id=cc.id "+
        	"where cc.claim_line_type_code not in ('RX') and cd.nomen='PX' and cd.principal=1 ";
       	    rs = conn.createStatement().executeQuery(rptQry);
            while(rs.next()){
            	rpt.PX2Count=rs.getLong("total");
            	break;
		    }
       		if(debugMode){
       			System.out.println("rpt.PX2Count="+rpt.PX2Count);
       		}
            
	        
        	//invalid principal PX
        	rptQry="select count(*) total from "+schemaName+".claims_combined cc,"+schemaName+".code cd where cc.claim_line_type_code not in ('RX') "+
        	"and cd.nomen='PX' and cd.principal=1 and cc.id =cd.u_c_id and cd.code_value in ('','X') ";
       	    rs = conn.createStatement().executeQuery(rptQry);
            while(rs.next()){
            	rpt.PXInvalidCount=rs.getLong("total");
            	break;
		    }		    
       		if(debugMode){
       			System.out.println("rpt.PXInvalidCount="+rpt.PXInvalidCount);
       		}

            
        	//no principal PX
	        rptQry="select count(*) total from "+schemaName+".claims_combined cc where cc.claim_line_type_code not like 'RX' and cc.id not in (select u_c_id from "+schemaName+".code cd where cd.nomen='PX' and cd.principal=1 )";
       	    rs = conn.createStatement().executeQuery(rptQry);
            while(rs.next()){
            	rpt.PXNoCount=rs.getLong("total");
            	break;
		    }		    
       		if(debugMode){
       			System.out.println("rpt.PXNoCount="+rpt.PXNoCount);
       		}

		    
		}catch (HibernateException e) {
			e.printStackTrace(); 
			rtnCode = -1;
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
       	
		log.info("rptHtml="+getStatsAsHTML());
		return rtnCode;
	}


	@SuppressWarnings("unchecked")
	private int generateReportHibern() {
		List<Class<?>> cList = new ArrayList<Class<?>>();
        cList.add(ClaimRx.class);
        cList.add(MedCode.class);
        cList.add(ClaimLineTbl.class);
		
		HibernateHelper h = new HibernateHelper(cList, env, schemaName);
		SessionFactory factory = h.getFactory(env, schemaName );
        Session session = factory.openSession();
        session.setCacheMode(CacheMode.IGNORE);
        session.setFlushMode(FlushMode.COMMIT);
  
		int rtnCode = 0;
		
	    List resultList = null;
        try{
         	String qryTot ="select count(*) from ClaimLineTbl";
        	totalRec =(long)session.createQuery(qryTot).uniqueResult();
        	
        	StringBuffer pxdxQry = new StringBuffer();
        	//principal DX
        	pxdxQry.append("select count(*) from ClaimLineTbl cs, MedCode cd where cs.claim_line_type_code not in ('RX') ");
        	pxdxQry.append("and cd.nomen='DX' and cd.principal=1 and cs.id =cd.u_c_id");
        	Query query = session.createQuery(pxdxQry.toString());
		    resultList = query.list();
		    if (resultList != null && !resultList.isEmpty()) {
            	rpt.DX2Count=(long)query.uniqueResult();
		    }
	        
        	//invalid principal DX
		    pxdxQry = new StringBuffer();
        	pxdxQry.append("select count(*) from ClaimLineTbl cs, MedCode cd where cs.claim_line_type_code not in ('RX') ");
        	pxdxQry.append("and cd.nomen='DX' and cd.principal=1 and cs.id =cd.u_c_id and cd.code_value in ('','X') ");
		    
        	query = session.createQuery(pxdxQry.toString());
		    resultList = query.list();
		    if (resultList != null && !resultList.isEmpty()) {
            	rpt.DXInvalidCount=(long)query.uniqueResult();
		    }		    
        	
        	//no principal DX
	        pxdxQry = new StringBuffer();
	        pxdxQry.append("select count(*) from ClaimLineTbl cs where cs.claim_line_type_code not like 'RX' and cs.id not in (select u_c_id from MedCode cd where cd.nomen='DX' and cd.principal=1 )");
	      	query = session.createQuery(pxdxQry.toString());
		    resultList = query.list();
		    if (resultList != null && !resultList.isEmpty()) {
            	rpt.DXNoCount=(long)query.uniqueResult();
		    }		    
		    
        	//principal PX
		    pxdxQry = new StringBuffer();
        	pxdxQry.append("select count(*) from ClaimLineTbl cs, MedCode cd where cs.claim_line_type_code not in ('RX') ");
        	pxdxQry.append("and cd.nomen='PX' and cd.principal=1 and cs.id =cd.u_c_id");
        	query = session.createQuery(pxdxQry.toString());
		    resultList = query.list();
		    if (resultList != null && !resultList.isEmpty()) {
            	rpt.PX2Count=(long)query.uniqueResult();
		    }
	        
        	//invalid principal PX
		    pxdxQry = new StringBuffer();
        	pxdxQry.append("select count(*) from ClaimLineTbl cs, MedCode cd where cs.claim_line_type_code not in ('RX') ");
        	pxdxQry.append("and cd.nomen='PX' and cd.principal=1 and cs.id =cd.u_c_id and cd.code_value in ('','X') ");
		    
        	query = session.createQuery(pxdxQry.toString());
		    resultList = query.list();
		    if (resultList != null && !resultList.isEmpty()) {
            	rpt.PXInvalidCount=(long)query.uniqueResult();
		    }		    
        	
        	//no principal PX
	        pxdxQry = new StringBuffer();
	        pxdxQry.append("select count(*) from ClaimLineTbl cs where cs.claim_line_type_code not like 'RX' and cs.id not in (select u_c_id from MedCode cd where cd.nomen='PX' and cd.principal=1 )");
	      	query = session.createQuery(pxdxQry.toString());
		    resultList = query.list();
		    if (resultList != null && !resultList.isEmpty()) {
            	rpt.PXNoCount=(long)query.uniqueResult();
		    }		    

		    
		}catch (HibernateException e) {
			e.printStackTrace(); 
			rtnCode = -1;
		}finally {
			if(session !=null)
				session.close();
			try {
				h.close("prd", schemaName);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
       	
		log.info("rptHtml="+getStatsAsHTML());
		return rtnCode;
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
		
		StringBuffer sb = new StringBuffer();
		
		sb.append("<table border=\"1\" cellspacing=\"1\" cellpadding=\"5\" width=\"750px\" >");
		sb.append("<tr bgcolor=\"#99CCFF\">");
		sb.append("<th width=\"200px\">Principal Diagnosis Found</th>");
		sb.append("<th width=\"200px\">Principal Diagnosis Code Missing</th>");
		sb.append("<th width=\"250px\">Principal Diagnosis Code Invalid</th>");
		sb.append("</tr>");

      /*  sb.append("<tr>");
        sb.append("<div><td align=\"left\">" +rpt.DX2Count+"</td></div>"); 	
        sb.append("<div><td align=\"left\">" +rpt.DXNoCount+"</td></div>"); 	
        sb.append("<div><td align=\"left\">" +rpt.DXInvalidCount+"</td></div>"); 	
        sb.append("</tr>");
        */
        float pctDX2Count = (float)rpt.DX2Count / totalRec * 100;
        float pctDXNoCount = (float)rpt.DXNoCount / totalRec * 100;
        float pctDXInvalidCount = (float)rpt.DXInvalidCount / totalRec * 100;
	        
        sb.append("<tr>");
        sb.append("<div><td align=\"left\">" +String.format("%.4f", pctDX2Count)+"%</td></div>"); 	
        sb.append("<div><td align=\"left\">" +String.format("%.4f", pctDXNoCount)+"%</td></div>"); 	
        sb.append("<div><td align=\"left\">" +String.format("%.4f", pctDXInvalidCount)+"%</td></div>"); 	
		sb.append("</tr>");
        
	        
		sb.append("</table><br><br>");

		sb.append("<table border=\"1\" cellspacing=\"1\" cellpadding=\"5\" width=\"750px\" >");
		sb.append("<tr bgcolor=\"#99CCFF\">");
		sb.append("<th width=\"200px\">Principal Procedure Code Found</th>");
		sb.append("<th width=\"200px\">Principal Procedure Code Missing</th>");
		sb.append("<th width=\"250px\">Principal Procedure Code Invalid</th>");
		sb.append("</tr>");

/*        sb.append("<tr>");
        sb.append("<div><td align=\"left\">" +rpt.PX2Count+"</td></div>"); 	
        sb.append("<div><td align=\"left\">" +rpt.PXNoCount+"</td></div>"); 	
        sb.append("<div><td align=\"left\">" +rpt.PXInvalidCount+"</td></div>"); 	
        sb.append("</tr>");
	*/        

        float pctPX2Count = (float)rpt.PX2Count / totalRec * 100;
        float pctPXNoCount = (float)rpt.PXNoCount / totalRec * 100;
        float pctPXInvalidCount = (float)rpt.PXInvalidCount / totalRec * 100;
	        
        sb.append("<tr>");
        sb.append("<div><td align=\"left\">" +String.format("%.4f", pctPX2Count)+"%</td></div>"); 	
        sb.append("<div><td align=\"left\">" +String.format("%.4f", pctPXNoCount)+"%</td></div>"); 	
        sb.append("<div><td align=\"left\">" +String.format("%.4f", pctPXInvalidCount)+"%</td></div>"); 	
        sb.append("</tr>");
        
        
		sb.append("</table>");
		
		
		return sb.toString();
		
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
		schemaName="CT_Medicaid_5400220161205";*/
		
		/*jobUid="1138";
		schemaName="Horizon_54002_20160921";*/		
        		
		if(debugMode){
			System.out.println("schemaName="+schemaName+"  jobId="+jobUid);
		}
		
		rpt.DX2Count = 0;
    	rpt.DXNoCount = 0;
    	rpt.DXInvalidCount = 0;
    	rpt.PX2Count = 0;
    	rpt.PXNoCount = 0;
    	rpt.PXInvalidCount = 0;
	}
	
	class Report {
		
		long medicalClaimsTotalRecords = 0l;
		
		long DX2Count = 0l;
		long DXNoCount = 0l;
		long DXInvalidCount = 0l;

		long PX2Count = 0l;
		long PXNoCount = 0l;
		long PXInvalidCount = 0l;
		
		String reportName ="PrincipalDXPXReport";
		
	}
	//Arrays.fill(medicalClaimsTotalCosts, BigDecimal.ZERO);
	
	public static void main(String[] args) {
		
		log.info("Starting DataValidation Report");

		PrincipalDXPXReport instance = new PrincipalDXPXReport();
		
		// get parameters and make them available to all
		RunParameters rp = new RunParameters();
		instance.parameters = rp.loadParameters(args, parameterDefaults);
				
		String s = instance.parameters.get("runname") + "_" + instance.parameters.get("rundate");
		instance.parameters.put("jobname", s);
		
				
		RunParameters.parameters.put("configfolder", instance.parameters.get("configfolder"));
		
		 instance.process();
		
		
		log.info("Complete PrincipalDX/PX Report");
		
	}
	

	static String [][] parameterDefaults = {
		{"configfolder", "C:/workspace/ECR_Analytics/trunk/EpisodeConstruction/src/"},
		{"rundate", "20150611"},
		{"runname", "CHC"},
		{"jobuid", "1043"}
	};
	
	
	private static org.apache.log4j.Logger log = Logger.getLogger(PrincipalDXPXReport.class);

}
