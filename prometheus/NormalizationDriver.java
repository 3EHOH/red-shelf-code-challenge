







public class NormalizationDriver extends AbstractDriver {
	
	
	private final int sleepSeconds = 60;
	private int stopAfter = -1;
	
	
	
	NormalizationDriver (HashMap<String, String> parameters) {
		
		stepName = ProcessJobStep.STEP_NORMALIZATION;
		this.parameters = parameters;
		
		if(parameters.containsKey("chunksize")) 
			chunkSize = Integer.parseInt(parameters.get("chunksize"));
		if(parameters.containsKey("stopafter"))
			stopAfter = Integer.parseInt(parameters.get("stopafter"));
		
		initialize();

		checkForStaleActive(session);
		
		doCompletionReports();
		
		process();
		
	}
	
	/**
	 * main process loop, wait for work, or do work
	 */
	private void process () {
		
		
		// reload active normalization jobs
		triggerJob(ProcessJobStep.STATUS_ACTIVE);
		
		int iLoops = 0;
		

        while(true) {
        	
        	// if there is nothing to do, wait a minute to see if something shows up
            if (!triggerWork())  {

            	try {
            		log.info("Taking a nap");
            		TimeUnit.SECONDS.sleep(sleepSeconds);
            		log.info("Waking up");
            	} catch (InterruptedException e) {
            		e.printStackTrace();
            	}
            
            }
            // otherwise, get to it
            else {
            	if (jobList == null  ||  jobList.isEmpty()) {
            		iLoops++;
            		continue;
            	}
            	
            	//unLoadParameters();
            	parameters.put("jobuid", Long.toString(jobList.get(0).getJobUid()));
            	bigKahuna.parameters = parameters;
            	String _cfg = parameters.get("configfolder");
            	reLoadParameters();
            	if (parameters.containsKey("localdebug"))
            		bigKahuna.parameters.put("configfolder", _cfg);
            	bigKahuna.parameters.put("jobuid", Long.toString(jobList.get(0).getJobUid()));
            	bigKahuna.parameters.put("jobstep", ProcessJobStep.STEP_NORMALIZATION);

            	/*
        		for(Entry<String, String> entry : bigKahuna.parameters.entrySet()){
        		    log.info("Parameter Key and Value: " +  entry.getKey() + " " + entry.getValue());
        		}
        		*/
        		

        		
        		// initialize data connections
        		
        		String env = parameters.get("env") == null ?  "prd" :  parameters.get("env");
        		String schemaName = parameters.get("jobname")  == null ?  "javatest" : parameters.get("jobname");
        		
        		// add all annotated classes to the class list
        		cListGlobal = new ArrayList<Class<?>>();
        		cListGlobal.add(ClaimServLine.class);
        		cListGlobal.add(ClaimRx.class);
        		cListGlobal.add(MedCode.class);
        		cListGlobal.add(PlanMember.class);
        		cListGlobal.add(Enrollment.class);
        		cListGlobal.add(Provider.class);
        		cListGlobal.add(ProviderSpecialty.class);
        		cListGlobal.add(PerMemberReport.class);
                
                hGlobal = new HibernateHelper(cListGlobal, env, schemaName);
        		factoryGlobal = h.getFactory(env, schemaName);
        		
        		log.info("Process Id: " + InfoUtil.getProcessId("n/a") + " is starting the normalization controller for " + parameters.get("jobuid"));
        		
        		try {
        			new NormalizationController(parameters);
        		} catch (IllegalStateException e) {
        			doFailureReport(e.toString());
        			throw new IllegalStateException ("a severe error caused the process to halt");
        		}
            	
            	try {
					hGlobal.close(env, schemaName);
				} catch (Exception e) {
					log.error("factory close error " +iLoops + " ==> " + new Date());
					e.printStackTrace();
				}
        		
            	log.info("Process Id: " + InfoUtil.getProcessId("n/a") + " has completed a normalization controller cycle for " + parameters.get("jobuid"));
        		
            }
            
            iLoops++;
            if(iLoops % 10000 == 0) { 
				log.info("processing loop " +iLoops + " ==> " + new Date());		
			}
            if (stopAfter > 0  && iLoops > stopAfter)
            	break;
            
            //checkForJobStepUpdates();
            
            /*
            if(iLoops % 10000 == 0) { 
            	JobQueueHelper.checkForStaleActive(session, jobList, ProcessJobStep.STEP_NORMALIZATION);		
			}
			*/
            
            
        }
        
             
	}
	

	
	private void initialize () {
		
		flutter();
		
		// initialize Hibernate
		cList = new ArrayList<Class<?>>();
		cList.add(ProcessJobStep.class);
		cList.add(JobStepQueue.class);
		cList.add(ProcessJob.class);
		cList.add(ProcessJobParameter.class);
		cList.add(ProcessMessage.class);
		cList.add(MapEntry.class);
				
		h = new HibernateHelper(cList, "ecr", "ecr");	
		factory = h.getFactory("ecr", "ecr");
		session = factory.openSession();	
					
	}
	
	
		

	
	
	/**
	 * check for prior normalization
	 * @return
	 */
	@Override
	boolean checkForPriorActivity(String member_id) {
		
		boolean b = false;
	
		// initialize data connections
		
		String env = parameters.get("env") == null ?  "prd" :  parameters.get("env");
		String schemaName = parameters.get("jobname")  == null ?  "javatest" : parameters.get("jobname");
		
		// use member table to verify if output happened
		List<Class<?>> cListLocal = new ArrayList<Class<?>>();
		cListLocal.add(PlanMember.class);
        
		HibernateHelper hLocal = new HibernateHelper(cListLocal, env, schemaName);
		SessionFactory factoryLocal = hLocal.getFactory(env, schemaName);
		Session sessionLocal = factoryLocal.openSession();
		
		Transaction tx = null;
		
		try{

			tx = sessionLocal.beginTransaction();
			
			// look for stale ACTIVE members
			String hql = "FROM PlanMember WHERE member_id = :member_id";
			
			Query query = sessionLocal.createQuery(hql);
			if (hLocal.getCurrentConfig().getConnParms().getDialect().equalsIgnoreCase("vertica")) 
			{} 
			else
				query.setMaxResults(1);
			query.setParameter("member_id", member_id);

			if( query.list().size() > 0) 
				b = true;

 			tx.commit();
 						
			
		}catch (HibernateException e) {
			
			if (tx!=null) tx.rollback();
			e.printStackTrace();
			
		}finally {
			
			sessionLocal.close(); 

			/*
			try {
				h.close("prd", "ecr");
			} catch (Exception e) {
				e.printStackTrace();
			}
			*/
			
		}
			
		
		
		return b;
		
	}
	
	
	private boolean doCompletionReports() {
		
		boolean b = false;
		
		List<ProcessJobStep> URjobList = getUnreported();
		
		if (URjobList.isEmpty())
			return b;
	
		for (ProcessJobStep _PJS : URjobList) {
		
			Transaction tx = null;
		
			try{

				tx = session.beginTransaction();
 				
 				//String hqlJ = "FROM ProcessJobStep WHERE jobuid = :jobuid AND  stepName = '" + stepName + "'";
 				//Query queryJ = session.createQuery(hqlJ);
 				//queryJ.setParameter("jobuid", jobList.get(0).getJobUid());
 				//ProcessJobStep _PJS = (ProcessJobStep) queryJ.list().get(0);
 			 			
 				//removeJob(_PJS);
				parameters.put("jobuid", Long.toString(_PJS.getJobUid()));
				bigKahuna.parameters = parameters;
				String cfg = parameters.get("configfolder");
            	reLoadParameters();
            	parameters.put("configfolder", cfg);
            	Blob blob = new SerialBlob(getStatsAsHTML().getBytes()); 
 				_PJS.setReport(blob);
 				//_PJS.setUpdated(new Date());
 				session.update(_PJS); 
 				
 			

 				tx.commit();
 						
			
			}catch (HibernateException e) {
			
				if (tx!=null) tx.rollback();
				e.printStackTrace();
			
			} catch (SerialException e) {
				log.error("Failed to serialize report for storage in Blob");
				e.printStackTrace();
			} catch (SQLException e) {
				log.error("SQL Exception: " + e);
				e.printStackTrace();
			}finally {
			
			
			}
			
		
		}
			
		
		return b;
		
	}
	
	
	
	/**
	 * gets the statistics for the raw input file and formats them into a html page
	 * @return
	 */
	private String getStatsAsHTML ()  {
		
		// initialize Mongo
		MongoInputCollectionSet	micky = new MongoInputCollectionSet(parameters);
		DB db = micky.getDb();
		
		// Initialize SQL
		String env = parameters.get("env") == null ?  "prd" :  parameters.get("env");
		String schemaName = parameters.get("jobname")  == null ?  "javatest" : parameters.get("jobname");
		
		// put all classes in
		List<Class<?>> cListLocal = new ArrayList<Class<?>>();
		cListLocal.add(ClaimServLine.class);
		cListLocal.add(ClaimRx.class);
		cListLocal.add(PlanMember.class);
		cListLocal.add(Enrollment.class);
		cListLocal.add(Provider.class);
		cListLocal.add(ProviderSpecialty.class);
        
		HibernateHelper hLocal = new HibernateHelper(cListLocal, env, schemaName);
		SessionFactory factoryLocal = hLocal.getFactory(env, schemaName);
		Session sessionLocal = factoryLocal.openSession();
				
		StringBuffer sb = new StringBuffer();
		/*
		sb.append("<!DOCTYPE html>");
		sb.append("<html lang=\"en\">");
		sb.append("<head><title>Input File Statistics</title></head>"); 
		
		sb.append("<body>");
		*/
		
		sb.append("<table style='width:1000px' border=\"1\" cellspacing=\"1\" cellpadding=\"5\">");
		
		sb.append("<tr>");
		sb.append("<th>Collection Name</th>");
		sb.append("<th>Document Count</th>");
		sb.append("<th>Table Name</th>");
		sb.append("<th>Row Count</th>");
		sb.append("</tr>");
		
		sb.append("<tr>");
		sb.append(genInputCounts("Medical Claims", "ClaimServLine", micky, db));
		sb.append("</tr>").append("<tr>");
		sb.append(genOutputCounts("ClaimServLine", sessionLocal));
		sb.append("</tr>");

		sb.append("<tr>");
		sb.append(genInputCounts("Pharmacy Claims", "ClaimRx", micky, db));
		sb.append("</tr>").append("<tr>");
		sb.append(genOutputCounts("ClaimRx", sessionLocal));
		sb.append("</tr>");

		sb.append("<tr>");
		sb.append(genInputCounts("Plan Members", "PlanMember", micky, db));
		sb.append("</tr>").append("<tr>");
		sb.append(genOutputCounts("PlanMember", sessionLocal));
		sb.append("</tr>");

		sb.append("<tr>");
		sb.append(genInputCounts("Enrollments", "Enrollment", micky, db));
		sb.append("</tr>").append("<tr>");
		sb.append(genOutputCounts("Enrollment", sessionLocal));
		sb.append("</tr>");

		sb.append("<tr>");
		sb.append(genInputCounts("Providers", "Provider", micky, db));
		sb.append("</tr>").append("<tr>");
		sb.append(genOutputCounts("Provider", sessionLocal));
		sb.append("</tr>");

		
		
		sb.append("</table>");
		
		/*
		sb.append("</body>");
		sb.append("</html>");
		*/
		
		sessionLocal.close();
		
		
		return sb.toString();
		
	}
	
	
	private StringBuffer genInputCounts(String objTitle, String objName, MongoInputCollectionSet micky, DB db) {
		
		
		StringBuffer sb = new StringBuffer();
		DBCollection coll = null;
		

		switch (objName) {
		case "ClaimServLine":
			coll = db.getCollection(micky.getsClaimColl());
			break;
		case "ClaimRx":
			coll = db.getCollection(micky.getsRxColl());
			break;
		case "PlanMember":
			coll = db.getCollection(micky.getsMemberColl());
			break;
		case "Enrollment":
			coll = db.getCollection(micky.getsEnrollColl());
			break;
		case "Provider":
			coll = db.getCollection(micky.getsProviderColl());
			break;
		}
		
		
		sb.append("<td>" + coll.getFullName() + "</td>");
		sb.append("<td align=\"right\">" + coll.getCount() + "</td>");
		sb.append("<td>" + "&nbsp" + "</td>");
		sb.append("<td>" + "&nbsp" + "</td>");
		

		return sb;
		
	}
	
	
	private StringBuffer genOutputCounts (String objName, Session sessionLocal)  {
		
		StringBuffer sb = new StringBuffer();
		int iCount = 0;
		
		Transaction tx = null;

		try { 

			tx = sessionLocal.beginTransaction(); 
			
			StringBuffer sQ = new StringBuffer("select count(id) from ").append(objName);
			if (objName.equals("ClaimServLine"))
				sQ.append(" WHERE claim_line_type_code != 'RX'" );
			else if (objName.equals("ClaimRx"))
				sQ.append(" WHERE claim_line_type_code = 'RX'" );
			iCount = ((Long) sessionLocal.createQuery(sQ.toString()).uniqueResult()).intValue();
			
			sessionLocal.getTransaction().commit(); 
			
		} 
		catch (HibernateException e) { 
			e.printStackTrace(); 
			if (tx != null) tx.rollback(); 
		} 
		finally {
			
		}
		

		sb.append("<td>" + "&nbsp" + "</td>");
		sb.append("<td>" + "&nbsp" + "</td>");
		sb.append("<td>" + parameters.get("jobname") + "." + objName + "</td>");
		sb.append("<td align=\"right\">" + iCount + "</td>");
		
		
		
		return sb;

	}
	
	
	
	
	
	public static void main(String[] args) throws InterruptedException {
		
		RunParameters rp = new RunParameters();
		rp.loadParameters(args, parameterDefaults);
		
		/*MappingDriver instance = */ new NormalizationDriver(RunParameters.parameters);
		
	        
	}
	
	static String [][] parameterDefaults = {
		{"configfolder", "C:\\workspace\\ECR_Analytics\\trunk\\EpisodeConstruction\\src\\"}
	};
	
	
	private static org.apache.log4j.Logger log = Logger.getLogger(NormalizationDriver.class);



}
