





public class ConstructionDriver extends AbstractDriver{
	
	
	private final int sleepSeconds = 60;
	private int stopAfter = -1;
	
	
	
	
	ConstructionDriver (HashMap<String, String> parameters) {
		
		stepName = ProcessJobStep.STEP_CONSTRUCT;
		this.parameters = parameters;
		
		if(parameters.containsKey("chunksize")) 
			chunkSize = Integer.parseInt(parameters.get("chunksize"));
		if(parameters.containsKey("stopafter"))
			stopAfter = Integer.parseInt(parameters.get("stopafter"));
		
		initialize();
		
		checkForStaleActive(session);
		
		process();
		
	}
	
	/**
	 * main process loop, wait for work, or do work
	 */
	private void process () {
		
		
		// reload active construction jobs
		triggerJob(ProcessJobStep.STATUS_ACTIVE);
		
		int iLoops = 0;
		

        while(true) {
        	
        	/*
        	if (session.isOpen()) {
        		log.info( "Found open session while looking for work.  Iteration # " + iLoops + " Process Id: " + InfoUtil.getProcessId("n/a") );
        	} else {
        		log.error( "Found closed session while looking for work.  Iteration # " + iLoops  + " Process Id: " + InfoUtil.getProcessId("n/a") );
        	}
        	*/
        	
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
            	reLoadParameters();
            	bigKahuna.parameters.put("jobuid", Long.toString(jobList.get(0).getJobUid()));
            	bigKahuna.parameters.put("jobstep", ProcessJobStep.STEP_CONSTRUCT);

        		for(Entry<String, String> entry : bigKahuna.parameters.entrySet()){
        		    log.info("Parameter Key and Value: " +  entry.getKey() + " " + entry.getValue());
        		}
        		

        		
        		// initialize data connections
        		
        		String env = parameters.get("env") == null ?  "tjt" :  parameters.get("env");
        		String schemaName = parameters.get("jobname")  == null ?  "javatest" : parameters.get("jobname");
        		
        		// add all annotated classes to the class list
        		cListGlobal = new ArrayList<Class<?>>();
        		cListGlobal.add(ClaimServLine.class);
        		cListGlobal.add(ClaimRx.class);
        		cListGlobal.add(MedCode.class);
        		cListGlobal.add(PlanMember.class);
        		cListGlobal.add(Enrollment.class);
        		cListGlobal.add(Provider.class);
        		cListGlobal.add(PerMemberReport.class);
        		cListGlobal.add(MedCode.class);
        		cListGlobal.add(AssignmentTbl.class);
        		cListGlobal.add(AssociationTbl.class);
        		cListGlobal.add(EpisodeShell.class);
        		cListGlobal.add(EpisodeTbl.class);
        		cListGlobal.add(MedCode.class);
        		cListGlobal.add(TriggerTbl.class);
                
                hGlobal = new HibernateHelper(cListGlobal, env, schemaName);
        		factoryGlobal = h.getFactory(env, schemaName);
        		
        		log.info("Process Id: " + InfoUtil.getProcessId("n/a") + " is starting the construction controller for " + parameters.get("jobuid"));
        		
        		/*
        		if (session.isOpen()) {
            		log.info( "Found open session while invoking controller.  Iteration # " + iLoops + " Process Id: " + InfoUtil.getProcessId("n/a") );
            	} else {
            		log.error( "Found closed session while invoking controller.  Iteration # " + iLoops  + " Process Id: " + InfoUtil.getProcessId("n/a") );
            	}
            	*/
        		
        		try {
        			new ConstructionController(parameters);
        		} catch (IllegalStateException e) {
        			doFailureReport(e.toString());
        			throw new IllegalStateException ("a severe error caused the process to halt");
        		}
        		
        		/*
        		if (session.isOpen()) {
            		log.info( "Found open session while returning from controller.  Iteration # " + iLoops + " Process Id: " + InfoUtil.getProcessId("n/a") );
            	} else {
            		log.error( "Found closed session while returning from controller.  Iteration # " + iLoops  + " Process Id: " + InfoUtil.getProcessId("n/a") );
            	}
            	*/

            	try {
					hGlobal.close(env, schemaName);
				} catch (Exception e) {
					log.error("factory close error " +iLoops + " ==> " + new Date());
					e.printStackTrace();
				}
            	
            	/*
            	if (session.isOpen()) {
            		log.info( "Found open session at cycle completion.  Iteration # " + iLoops + " Process Id: " + InfoUtil.getProcessId("n/a") );
            	} else {
            		log.error( "Found closed session at cycle completion.  Iteration # " + iLoops  + " Process Id: " + InfoUtil.getProcessId("n/a") );
            	}
            	*/

            	log.info("Process Id: " + InfoUtil.getProcessId("n/a") + " has completed a construction controller cycle for " + parameters.get("jobuid"));
        		
            }
            
            iLoops++;
            if(iLoops % 10000 == 0) { 
				log.info("processing loop " +iLoops + " ==> " + new Date());		
			}
            if (stopAfter > 0  && iLoops > stopAfter)
            	break;
            
            //checkForJobStepUpdates();
            
            
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
		cList.add(MetaDataStore.class);
		
				
		h = new HibernateHelper(cList, "ecr", "ecr");	
		factory = h.getFactory("ecr", "ecr");
		session = factory.openSession();	
					
	}
	
	
	
	
	
	
	
	
	
	/**
	 * check for prior construction
	 * @return
	 */
	@Override
	boolean checkForPriorActivity(String member_id) {
		
		boolean b = false;
	
		// initialize data connections
		
		String env = parameters.get("env") == null ?  "prd" :  parameters.get("env");
		String schemaName = parameters.get("jobname")  == null ?  "javatest" : parameters.get("jobname");
		
		// use episode table to verify if output happened
		List<Class<?>> cListLocal = new ArrayList<Class<?>>();
		cListLocal.add(EpisodeTbl.class);
        
		HibernateHelper hLocal = new HibernateHelper(cListLocal, env, schemaName);
		SessionFactory factoryLocal = hLocal.getFactory(env, schemaName);
		Session sessionLocal = factoryLocal.openSession();
		
		Transaction tx = null;
		
		try{

			tx = sessionLocal.beginTransaction();
			
			// look for stale ACTIVE members
			String hql = "FROM EpisodeTbl WHERE member_id = :member_id";
			
			Query query = sessionLocal.createQuery(hql);
			if (h.getCurrentConfig().getConnParms().getDialect().equalsIgnoreCase("vertica")) 
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
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			*/
			
		}
			
		
		
		return b;
		
	}
	
	
	
	
	
	public static void main(String[] args) throws InterruptedException {
		
		RunParameters rp = new RunParameters();
		rp.loadParameters(args, parameterDefaults);
		
		new ConstructionDriver(RunParameters.parameters);
		
	        
	}
	
	static String [][] parameterDefaults = {
		{"configfolder", "C:\\workspace\\ECR_Analytics\\trunk\\EpisodeConstruction\\src\\"}
};
	
	
	private static org.apache.log4j.Logger log = Logger.getLogger(ConstructionDriver.class);



}
