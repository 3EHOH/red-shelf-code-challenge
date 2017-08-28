
//import java.util.Map;


//import org.hibernate.transform.AliasToEntityMapResultTransformer;



public class PostConstRpt {
	
	
	HashMap<String, String> parameters;
	
	Report rpt = new Report();
	String schemaName=null;
	String jobUid=null;
	
    
    SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");
    int idx = 0;
    
    HibernateHelper h;
	SessionFactory factory;
	
	boolean debugMode = false;
	
	
	/**
	 * static parameter constructor, just for testing
	 */
	public PostConstRpt() {	}
	
	/**
	 * constructor using parameters pulled from control database
	 * @param parameters
	 */
	public PostConstRpt(HashMap<String, String> parameters) {
		this.parameters = parameters;
	}
	
	
	private void process () {
		
		initialize();
		
		generateReport();
		
		storeReport();

		
	}
	
	
	//@SuppressWarnings({ "unchecked" })
	private void generateReport() {
		
        Session session = null;
        Query query=null;
        StringBuilder _sb = new StringBuilder();
        BigInteger _bi;
        //List<Map<String,Object>> aliasToValueMapList;
        
	    
        try{
        	
        	session = factory.openSession();
        	int _tidx = 0;
        	
        	for (String _table : tableNames) {
        		
        		TableCheck _tc = new TableCheck();
        		rpt._tblLines.add(_tc);
        		_tc.tableName = _table;
			
        		// get total number of rows generated
        		query = session.createSQLQuery("SELECT COUNT(id) FROM `" + _table + "`");
        		_bi = (BigInteger)query.uniqueResult();
        		_tc.totalRows = _bi.longValue();
        	
        		// get total number of distinct rows generated
        		/*
        		query = session.createSQLQuery("SELECT * FROM `" + _table + "`");
        		query.setMaxResults(1);
        		query.setResultTransformer(AliasToEntityMapResultTransformer.INSTANCE);
        		aliasToValueMapList=query.list();
        		*/
        		_sb.setLength(0);
        		//if (aliasToValueMapList == null || aliasToValueMapList.isEmpty()) {
        		if(tableKeys[_tidx] == null) {
        			_tc.distinctRows = 0l;
        		}
        		else {
        			//for (Map.Entry<String, Object> entry : aliasToValueMapList.get(0).entrySet()) {
        			for (String s : tableKeys[_tidx]) {	
        				//if ( ((String)entry.getKey()).equalsIgnoreCase("id") )
        				//	continue;
        				if (_sb.length() > 1)
        					_sb.append(", ");
        				//_sb.append('`').append(entry.getKey()).append('`');
        				_sb.append('`').append(s).append('`');
        			}
        			query = session.createSQLQuery("SELECT COUNT(*) FROM  (SELECT DISTINCT " + _sb.toString() + " FROM `" + _table +"`)  as CC");
            		_bi = (BigInteger)query.uniqueResult();
            		_tc.distinctRows = _bi.longValue();
        		}
        		
        		_tidx++;
        	
        	}
        	
        	session.close(); 
            
			

			
		}catch (HibernateException e) {
			e.printStackTrace(); 
		}finally {
			if(session.isOpen()){
				session.close();
			}
			try {
				h.close("prd", schemaName);
			} catch (Exception e) {
				log.error("Hibernate session close error: " + e);
				e.printStackTrace();
			}
			
		}
		
	    
		log.info(getStatsAsHTML());
		
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
			
			String hql = "FROM ProcessReport WHERE jobuid = :jobuid AND stepname = '" + ProcessJobStep.STEP_CONSTRUCTION_REPORT  + "' and reportName='"+Report.reportName+"'";
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
				r.setStepName(ProcessJobStep.STEP_CONSTRUCTION_REPORT);
				r.setReportName(Report.reportName);
			} else
				r = results.get(0);
			r.setReport(new SerialBlob(getStatsAsHTML().getBytes()));
			
			session.save(r);
			
			tx.commit();
			
		}catch (HibernateException e) {
			if (tx!=null) tx.rollback();
			e.printStackTrace(); 
		} catch (SerialException e) {
			log.error("Serialization error: " + e);
			e.printStackTrace();
		} catch (SQLException e) {
			log.error("SQL error: " + e);
			e.printStackTrace();
		}finally {
			session.close(); 

			try {
				h.close("prd", schemaName);
			} catch (Exception e) {
				log.error("Hibernate session close error: " + e);
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
		boolean _beq = false;
		
		// Title
		sb.append("<table border=\"1\" cellspacing=\"1\" cellpadding=\"5\" width=\"730px\" >");
		sb.append("<tr bgcolor=\"#99CCFF\">");
		sb.append("<th width=\"200px\">Table Name</th>");
		sb.append("<th width=\"120px\">Total Rows</th>");
		sb.append("<th width=\"120px\">Total Distinct Rows</th>");
		sb.append("<th width=\"100px\">Note</th>");
		sb.append("</tr>");
		
		
		for(TableCheck _tc : rpt._tblLines) {
			
			_beq = _tc.distinctRows.compareTo(_tc.totalRows) == 0;
			
			sb.append("<tr>");
			
			sb.append("<td style=\"text-align: left;\">" + _tc.tableName + " </td>");
			sb.append("<td style=\"text-align: center;\">" + NumberFormat.getNumberInstance().format(_tc.totalRows) + " </td>");
			sb.append("<td style=\"text-align: center;\">" + NumberFormat.getNumberInstance().format(_tc.distinctRows) + " </td>");
			
			sb.append("<td style=\"text-align: center; " + (_beq ? "color:blue;" : "color:red;")+ "\">" + (_beq ? "OK" : "Warning") + " </td>");
			
			sb.append("</tr>");
		}
	

    	sb.append("</table>");
    	
		return sb.toString();
		
	}

	
	
	
	private void initialize ()  {
		
        schemaName = parameters.get("jobname")  == null ?  "javatest" : parameters.get("jobname");
		jobUid = parameters.get("jobuid");
		
		List<Class<?>> cList = new ArrayList<Class<?>>();
        cList.add(PlanMember.class);
        cList.add(Enrollment.class);
        cList.add(ClaimRx.class);
        cList.add(ClaimServLine.class);
        cList.add(MedCode.class);
        cList.add(AssignmentTbl.class);
        cList.add(AssociationTbl.class);
        cList.add(EpisodeTbl.class);
        cList.add(MedCode.class);
        cList.add(TriggerTbl.class);
		
		
		String env = parameters.get("env") == null ?  "prd" :  parameters.get("env");
        
        h = new HibernateHelper(cList, env, schemaName);
        factory = h.getFactory(env, schemaName);
		
	}
	
	class Report {
		
		public static final String reportName = "Construction Report";
		
		ArrayList<TableCheck> _tblLines = new ArrayList<TableCheck>(); 
			
		
	}
	
	class TableCheck {
		Long totalRows = -1L;
		Long distinctRows = -1L;
		String tableName = "";
	}
	
	// Don't change one of these without changing the other!
	String [] tableNames = {
			"episode", "assignment", "association", "triggers", "claims_combined", "claim_line", "claim_line_rx", "code", "enrollment", "member"
	};
	String [][] tableKeys = {
			{"master_episode_id", "master_claim_id"},		// episode
			{"master_episode_id", "master_claim_id"},		// assignment
			{"parent_master_episode_id", "child_master_episode_id", "association_type", "association_level"}, 	// association
			{"master_episode_id", "master_claim_id"},		// triggers
			{"master_claim_id"},							// claims_combined
			{"master_claim_id"},							// claim_line
			{"master_claim_id"},							// claim_line_rx
			{"u_c_id", "function_code", "code_value", "nomen", "principal"},	// code
			{"member_id", "begin_date", "end_date", "insurance_product", "coverage_type"}, 		// enrollment
			{"member_id", "insurance_type", "insurance_carrier"} 		// member
	};
	//Arrays.fill(medicalClaimsTotalCosts, BigDecimal.ZERO);
	
	public static void main(String[] args) {
		
		log.info("Start Construction Report");
		
		// get parameters and make them available to all
		RunParameters rp = new RunParameters();
		HashMap<String, String> parameters = RunParameters.parameters;
		parameters = rp.loadParameters(args, parameterDefaults);
		
				
		BigKahuna bigKahuna = new BigKahuna();
		String _cf = parameters.get("configfolder");
		
		bigKahuna.parameters = parameters;
		
		bigKahuna.reLoadParameters();
		    	
		// fake stuff for local testing
		parameters.put("configfolder", _cf);		

		PostConstRpt instance = new PostConstRpt(parameters);
		instance.process();
		
		log.info("Complete Construction Report");
		
	}
	

	static String [][] parameterDefaults = {
		{"configfolder", "C:/workspace/ECR_Analytics/trunk/EpisodeConstruction/src/"},
		{"jobuid", "1120"}
	};
	
	
	private static org.apache.log4j.Logger log = Logger.getLogger(PostConstRpt.class);


}
