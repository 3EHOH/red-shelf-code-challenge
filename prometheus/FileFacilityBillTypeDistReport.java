




public class FileFacilityBillTypeDistReport {
	
	
	HashMap<String, String> parameters;

	Report rpt = new Report();
	String schemaName=null;
	String jobUid=null;
	String env=null;
    
    int idx = 0;
    int billTypeCnt = 0;
    int maxSize=20;
    
    HibernateHelper h;
	SessionFactory factory;    

	boolean debugMode = false;
	/**
	 * static parameter constructor, just for testing
	 */
	public FileFacilityBillTypeDistReport() {	}
	
	/**
	 * constructor using parameters pulled from control database
	 * @param parameters
	 */
	public FileFacilityBillTypeDistReport(HashMap<String, String> parameters) {
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
        cList.add(ClaimLineTbl.class);
        
        HibernateHelper h = new HibernateHelper(cList, env, schemaName);
        
        Session session = null;
      
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
			if(session != null && session.isOpen()){
				session.close();
			}
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
            conn = DriverManager.getConnection(connUrl, connProp);
    		//record total by facility Code
			String facTypes[]= {"ST", "LT", "SNF","Null", "Null"};
			String fileTypes[]= {"IP", "IP", "IP","OP", "PB"};
            rptQry="select cb.claim_line_type_code,cb.facility_type_code, count(*) clm_count from "+schemaName+".claim_line cb "+
            		"where cb.facility_type_code in ('LT', 'SNF', 'ST','REH') group by cb.claim_line_type_code,cb.facility_type_code order by cb.claim_line_type_code,cb.facility_type_code";            
       	    rs = conn.createStatement().executeQuery(rptQry);
       	    
            while(rs.next() && i<maxSize){
				rpt.fileFacilityDist[i][0]=rs.getString("claim_line_type_code");
				rpt.fileFacilityDist[i][1]=rs.getString("facility_type_code");
				rpt.fileFacilityDist[i][2]=String.valueOf(rs.getLong("clm_count"));
            	i++;
            }
//            getStatsAsHTML();            
			if(debugMode){
				System.out.println("Done qry, billTypeCnt="+billTypeCnt);
			}
            
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
				
	

	@SuppressWarnings({ "unused", "rawtypes" })
	private void generateReportBK() {
		List<Class<?>> cList = new ArrayList<Class<?>>();
        cList.add(ClaimLineTbl.class);
		
        HibernateHelper h = new HibernateHelper(cList, env, schemaName);
        SessionFactory factory = h.getFactory(env, schemaName);

        Session session = factory.openSession();
        session.setCacheMode(CacheMode.IGNORE);
        session.setFlushMode(FlushMode.COMMIT);
        Transaction tx = null;
        Query query=null;
	    List resultList = null;
        
        try{
        	
       		if(debugMode){
       			System.out.println("Env="+env+ " DbDial="+h.getCurrentConfig().getConnParms().getDialect()+" schema="+schemaName+" db="+h.getCurrentConfig().getConnParms().getDatabase());
       		}
        	
			tx = session.beginTransaction();
		
    		//record total by facility Code
			String facTypes[]= {"ST", "LT", "SNF","Null", "Null"};
			String fileTypes[]= {"IP", "IP", "IP","OP", "PB"};
			for(int i=0; i<fileTypes.length; i++){
				rpt.fileFacilityDist[i][0]=fileTypes[i];
				rpt.fileFacilityDist[i][1]=facTypes[i];
				if(i>2){
					query = session.createQuery("select count(*) from ClaimLineTbl cb where cb.claim_line_type_code=:claimType");
					query.setString("claimType", rpt.fileFacilityDist[i][0]);
				}else{
					query = session.createQuery("select count(*) from ClaimLineTbl cb where cb.claim_line_type_code=:claimType and cb.facility_type_code=:facCode");
					query.setString("claimType", rpt.fileFacilityDist[i][0]);
					query.setString("facCode", rpt.fileFacilityDist[i][1]);
				}
				rpt.fileFacilityDist[i][2]=String.valueOf(query.uniqueResult());
				if(debugMode){
					System.out.println("file="+fileTypes[i]+" fac="+facTypes[i]+" cnt="+rpt.fileFacilityDist[i][2]);
				}
			}
    		

    		Object[] row = null;
    		Iterator it = null;

    		idx = 0;
    		boolean found = false;
	/*rmv 2017Jan		for(int i=0; i<fileTypes.length; i++){
				if(i>2){
					query = session.createQuery("select cs.type_of_bill,count(*) from ClaimLineTbl cs where cs.claim_line_type_code=:claimType group by cs.type_of_bill");
		    		query.setString("claimType", fileTypes[i]);
				}else{
					query = session.createQuery("select cs.type_of_bill,count(*) from ClaimLineTbl cs where cs.claim_line_type_code=:claimType and cs.facility_type_code=:facCode group by cs.type_of_bill");
		    		query.setString("claimType", fileTypes[i]);
		    		query.setString("facCode", facTypes[i]);
				}
				found = false;
	    		for(it=query.iterate();it.hasNext();)   
	    		  {   
	    		   found = true;
	    		   row = (Object[]) it.next();   
	    		   rpt.billTypeDist[idx][0]=fileTypes[i];
	    		   rpt.billTypeDist[idx][1]=facTypes[i];
	    		   rpt.billTypeDist[idx][2]=(String)row[0];
	    		   rpt.billTypeDist[idx][3]=String.valueOf(row[1]);
	    			if(debugMode){
	    				System.out.println("file="+fileTypes[i]+" fac="+facTypes[i]+" billType="+rpt.billTypeDist[idx][2]+" cnt="+row[1]);
	    			}
	    		   idx++;
	    		  }
	    		if(!found){
		    		   rpt.billTypeDist[idx][0]=fileTypes[i];
		    		   rpt.billTypeDist[idx][1]=facTypes[i];
		    		   rpt.billTypeDist[idx][2]=(String)row[0];
		    		   rpt.billTypeDist[idx][3]=String.valueOf(0);
		    		   idx++;
	    		}
			}
    		
    		
            billTypeCnt = idx;*/
    		
         getStatsAsHTML();            
    		
			if(debugMode){
				System.out.println("Done qry, billTypeCnt="+billTypeCnt);
			}
            
    		
		}catch (HibernateException e) {
			if (tx!=null) tx.rollback();
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
		
	    
		log.info(getStatsAsHTML());
		
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
//			query.setParameter("jobuid", parameters.get("jobuid"));
			query.setParameter("jobuid", jobUid);

			@SuppressWarnings("unchecked")
			List<ProcessReport> results = query.list();
			ProcessReport r;
			if (results.isEmpty()) {
				r = new ProcessReport();
//				r.setJobUid(Long.parseLong(parameters.get("jobuid")));
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
		
		//file, facility cnts
		sb.append("<br><br><br><table border=\"1\" cellspacing=\"1\" cellpadding=\"5\" width=\"540px\"  align=\"center\">");
		sb.append("<tr bgcolor=\"#99CCFF\">");
		sb.append("<th width=\"180px\">File Type</th>");
		sb.append("<th width=\"180px\">Facility Type</th>");
		sb.append("<th width=\"180px\">Number of Records</th>");
		sb.append("</tr>");

		for(int i=0; i<maxSize; i++){
			if(rpt.fileFacilityDist[i][0] != null && rpt.fileFacilityDist[i][0]!=""){
			sb.append("<tr>");
			for(int j=0; j<3; j++){
				sb.append("<div align=\"middle\"><td>"+rpt.fileFacilityDist[i][j]+"</td></div>");
			}
			sb.append("</tr>");
			}
		}
        
		sb.append("</table>");
		
		//file, facility, billtype cnts
/*rmv 2017Jan		sb.append("<table border=\"1\" cellspacing=\"1\" cellpadding=\"5\" width=\"750px\" >");
		sb.append("<tr bgcolor=\"#99CCFF\">");
		sb.append("<th width=\"180px\">File Type</th>");
		sb.append("<th width=\"180px\">Facility Type</th>");
		sb.append("<th width=\"180px\">Bill Type</th>");
		sb.append("<th width=\"210px\">Number of Records</th>");
		sb.append("</tr>");
		
		for(int i=0; i<billTypeCnt; i++){
			sb.append("<tr>");
			for(int j=0; j<4; j++){
				sb.append("<div align=\"middle\"><td>"+rpt.billTypeDist[i][j]+"</td></div>");
			}
			sb.append("</tr>");
		}
		

    	sb.append("</table>");
    	*/
		
//    	log.info ("rptHtml="+sb.toString());
		
		return sb.toString();
		
	}

	
	
	
	private void initialize ()  {
		
        schemaName = parameters.get("jobname")  == null ?  "javatest" : parameters.get("jobname");

		jobUid=parameters.get("jobuid");
		env = parameters.get("env") == null ?  "prd" :  parameters.get("env");
		
		//vertica test 
		/*jobUid="1120";
		schemaName="Warren_Vertica_8_Bench20161129";
		env="prdv";
*/
/*		jobUid="1120";
		schemaName="MD_APCD_Enroll_Fix_5_420160711";
		env="prdv2";*/
		
		
		//mysql test
		/*jobUid="1143";
		schemaName="CT_Medicaid_5400220161205";*/
		
		/*jobUid="1138";
		schemaName="Horizon_54002_20160921";*/
		
		
		if(debugMode){
			System.out.println("schemaName="+schemaName+" jobId="+jobUid);
		}

		
	}
	
	class Report {
		String fileFacilityDist[][] = new String[20][3];
		String billTypeDist[][] = new String[150][4];
		
		String reportName ="FileFacilityBillTypeDistReport";
		
	}
	//Arrays.fill(medicalClaimsTotalCosts, BigDecimal.ZERO);
	
	public static void main(String[] args) {
		
		log.info("Starting Post Mapping Control Report");

		FileFacilityBillTypeDistReport instance = new FileFacilityBillTypeDistReport();
		
		// get parameters and make them available to all
		RunParameters rp = new RunParameters();
		instance.parameters = rp.loadParameters(args, parameterDefaults);
				
		String s = instance.parameters.get("runname") + "_" + instance.parameters.get("rundate");
		instance.parameters.put("jobname", s);
		
				
		RunParameters.parameters.put("configfolder", instance.parameters.get("configfolder"));
		
		instance.process();
		
		
		log.info("Complete Report");
	}
	

	static String [][] parameterDefaults = {
		{"configfolder", "C:/workspace/ECR_Analytics/trunk/EpisodeConstruction/src/"},
		{"rundate", "20150611"},
		{"runname", "CHC"},
		{"jobuid", "1043"}
	};
	
	
	private static org.apache.log4j.Logger log = Logger.getLogger(FileFacilityBillTypeDistReport.class);

}
