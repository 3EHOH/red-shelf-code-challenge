







public class BigKahuna {
	
	
	
	public HashMap<String, String> parameters;
	ProcessJob job;
	
	static List<Class<?>> cList;
	static HibernateHelper h;
	static SessionFactory factory;
	static Session session;


	public static void main(String[] args) throws IOException {
		

		log.info(">>>Starting Big Kahuna");
		
		// construct 
		BigKahuna instance = new BigKahuna();
		
		// get parameters and make them available to all
		RunParameters rp = new RunParameters();
		instance.parameters = rp.loadParameters(args, parameterDefaults);
		
		if (!instance.parameters.containsKey("auto")) {
			instance.parameterSet(instance);
		}
		
		// initialize Hibernate
		cList = new ArrayList<Class<?>>();
		cList.add(ProcessJobStep.class);
		cList.add(JobStepQueue.class);
		cList.add(ProcessJob.class);
		cList.add(ProcessJobParameter.class);
		
		h = new HibernateHelper(cList, "ecr", "ecr");	
		factory = h.getFactory("ecr", "ecr");
		
		
		// process
		if (instance.parameters.containsKey("auto")) {
			instance.invokeAuto(instance.parameters.get("auto"));
		}
		else
			instance.process();
		
		
		
		/* instantiate an error manager for the run
		ErrorManager errMgr = new ErrorManager(instance.di);
		instance.di.setErrorManager(errMgr);
		*/
		
		try {
			h.close("prd", "ecr");
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
	}
	
	
	
	private final int sleepSeconds = 60;
	
	
	
	
	void invokeAuto (String autoParm)  {
		
		if (autoParm.equalsIgnoreCase("yes")  ||  autoParm.equalsIgnoreCase("once"))
			
			engine("once");
		
		else  if (autoParm.equalsIgnoreCase("forever")){
		 
			while(true) {
			
				engine("forever");
			
			}
		
		}
		else 
			log.error("Found unknown auto value: " + autoParm);
	}
	
	
	
	
	void engine (String autoParm)  {
		
	
		log.info("Last job step uid: " + lastUid);
			
		ProcessJobStep _PJS = null;
			
		HibernateHelper hC = new HibernateHelper(cList, "ecr", "ecr");
		SessionFactory factoryC = hC.getFactory("ecr", "ecr");
			
		Session sessionC = factoryC.openSession();
		Transaction tx = null;
			
		try{

			tx = sessionC.beginTransaction();
			
			String hql = "FROM ProcessJobStep WHERE status IN ('" + ProcessJobStep.STATUS_READY + "', '" + ProcessJobStep.STATUS_RETRY + "')";
			Query query = sessionC.createQuery(hql); //.setLockOptions(new LockOptions(LockMode.PESSIMISTIC_WRITE));

			@SuppressWarnings("unchecked")
			List<ProcessJobStep> results = query.list();
	 			
			for (ProcessJobStep _J : results) {
	 				
				if (_J.getStatus().equals(ProcessJobStep.STATUS_READY)) {
					if (_J.getStepName().equals(ProcessJobStep.STEP_SETUP)  ||				// handled by panels
							_J.getStepName().equals(ProcessJobStep.STEP_NORMALIZATION) ||		// handled by Normalization Driver
	 						_J.getStepName().equals(ProcessJobStep.STEP_CONSTRUCT)) {			// handled by Construction Driver
						continue;
					}
				}
	 				
				_PJS = _J;
				break;
	 				
			}

			tx.commit();
	 						
				
		}catch (HibernateException e) {
				
			if (tx!=null) tx.rollback();
			e.printStackTrace();
				
		}finally {
				
			sessionC.close(); 
				
		}
			
		// nothing to do
		if (_PJS == null) {
			if(autoParm.equalsIgnoreCase("forever")) {
				try {
					log.info("Taking a nap");
					TimeUnit.SECONDS.sleep(sleepSeconds);
					log.info("Waking up");
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			else 
				log.info("No ready steps found this execution");
		}
			
		// if a step shows up twice, it must have failed the first time
		else if (_PJS.getUid() == lastUid) {
				
			JobStepHelper.failStep(factoryC, _PJS);
			lastUid = -1l;  // allows reset after a minute
				
		}
		
		else {
				
				
				
			try{
				lastUid = _PJS.getUid();
				unLoadParameters();
				parameters.put("jobuid", Long.toString(_PJS.getJobUid()));
				reLoadParameters();
				parameters.put("jobstep", _PJS.getStepName().toLowerCase());
				//parameterSet(this);
				if (_PJS.getStatus().equals(ProcessJobStep.STATUS_RETRY)) {
					
					JobStepManager jsm = new JobStepManager(job.getUid());
					jsm.updateStatus(_PJS.getStepName(), ProcessJobStep.STATUS_CLEANING);
					jsm.updateReport(_PJS.getStepName(), null);
					
					Cleaner cleaner = new Cleaner(parameters);
					String [] steps = new String[]{_PJS.getStepName()};
					cleaner.cleanUp(steps);
				}
				if (_PJS.getStepName().equals(ProcessJobStep.STEP_SETUP)  ||				// handled by panels
						_PJS.getStepName().equals(ProcessJobStep.STEP_NORMALIZATION) ||		// handled by Normalization Driver
						_PJS.getStepName().equals(ProcessJobStep.STEP_CONSTRUCT))  			// handled by Construction Driver
						
				{
					JobStepManager jsm = new JobStepManager(job.getUid());
					jsm.updateStatus(_PJS.getStepName(), ProcessJobStep.STATUS_READY);
					jsm.updateReport(_PJS.getStepName(), null);
					lastUid = -1l;
				}
				else {
					process();
					//log.info("Blob check " + _PJS.getReport());
					Blob ablob = (Blob) _PJS.getReport();
					String str = "";
					if (ablob != null)
						str = new String(ablob.getBytes(1l, (int) ablob.length()));
					UserNotification.sendEmail(_PJS.getStepName() + " has completed for job " + _PJS.getJobUid() +
							System.lineSeparator() + str );
				}
				
			} catch (Throwable e) {
					
				e.printStackTrace();
				log.error(e);
				String sMsg = e.getMessage() == null  ?  "See log for errors" : e.getMessage();
				JobStepManager jsm = new JobStepManager(job.getUid());
				jsm.updateReport(_PJS.getStepName(), sMsg);
				jsm.updateStatus(_PJS.getStepName(), ProcessJobStep.STATUS_FAILED);
					
				UserNotification.sendEmail(_PJS.getStepName() + " failed for job " + _PJS.getJobUid() +
						(_PJS.getReport() != null ? System.lineSeparator() + sMsg : "") );
				
			}
			
		}
	        
	}
	
	private long lastUid = 0l;
	
	
	
	void process () {
		
		
		log.info("Process Id: " + InfoUtil.getProcessId("n/a") + " is starting " + parameters.get("jobstep") + " for " + parameters.get("jobuid"));
		
		switch (parameters.get("jobstep")) {
		
		case ProcessJobStep.STEP_SETUP:
			jobSetUp();
			break;
		
        case ProcessJobStep.STEP_ANALYZE:  
        	// analyze input
    		analyze();
            break;
            
        case ProcessJobStep.STEP_INPUTSTORE:  
        	// analyze input
        	inputstore();
            break;
      
        case ProcessJobStep.STEP_SELECTMAP:
        	// select the proper mapping for the input
    		selectMapSet();
    		break;
    		
        case ProcessJobStep.STEP_SCHEMACREATE:
        	// create EC output SQL schema from template
        	createOutputSchema();
        	break;
        	
        case ProcessJobStep.STEP_MAP:
        	// input mapping
    		mapping();
    		break;
    		
        case ProcessJobStep.STEP_POSTMAP:
        	postMap();
        	break;
        	
        case ProcessJobStep.STEP_POSTMAP_REPORT:
        	postMapReport();
        	break;
        	
        case ProcessJobStep.STEP_NORMALIZATION:
        	normalization();
        	break;
            
        case ProcessJobStep.STEP_POSTNORMALIZATION:
        	postnormalization();
        	break;
          
        case ProcessJobStep.STEP_POSTNORMALIZATION_REPORT:
        	postnormalizationReport();
        	break;
            
        case ProcessJobStep.STEP_CONSTRUCT:
        	// episode construction
    		construction();
    		break;
    		
        case ProcessJobStep.STEP_CONSTRUCTION_REPORT:
        	postConstructionReport();
        	break;
    		
        case ProcessJobStep.STEP_EPISODE_DEDUPE:
        case ProcessJobStep.STEP_PROVIDER_ATTRIBUTION:
        case ProcessJobStep.STEP_COST_ROLLUPS:
        case ProcessJobStep.STEP_FILTERED_ROLLUPS:
        case ProcessJobStep.STEP_MASTER_UNFILTERED_RA_SA:
        case ProcessJobStep.STEP_RA_SA_MODEL:
        case ProcessJobStep.STEP_RED:
        case ProcessJobStep.STEP_RES:
        case ProcessJobStep.STEP_SAVINGS_SUMMARY:
        case ProcessJobStep.STEP_CORE_SERVICES:
        case ProcessJobStep.STEP_PAC_ANALYSIS:
        case ProcessJobStep.STEP_PAS_ANALYSIS:
        case ProcessJobStep.STEP_MATERNITY:
        	
        	postEC_SQL(parameters.get("jobstep"));
			break;
			
        case ProcessJobStep.STEP_ARCHIVE:
        	// archive
    		archive();
    		break;
    		
        case ProcessJobStep.STEP_CLEANOUT:
        	// remove job artifacts
    		purge();
    		break;
    			
        default: 
        	log.error("You asked for a job step I don't recognize " + parameters.get("jobstep"));
        	break;
        	
		}

		
		log.info("Process Id: " + InfoUtil.getProcessId("n/a") + " has completed " + parameters.get("jobstep") + " for " + parameters.get("jobuid"));
		
		
	}
	
	
	public void reLoadParameters() {
		
		log.info("Starting Job Parameter Retrieval");
		JobRetrieval _JR = new JobRetrieval(parameters);
		job = _JR.get_PJ();
		log.info("Completed Job Parameter Retrieval");
		
	}
	
	
	void unLoadParameters() {
		
		log.info("Unloading old Job Parameters");

		Iterator<Entry<String, String>> it = parameters.entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry<String, String> pair = (Map.Entry<String, String>)it.next();
	        System.out.println(pair.getKey() + " = " + pair.getValue());
	        if (pair.getKey().equalsIgnoreCase("configfolder"))
	        	continue;
	        else if (pair.getKey().equalsIgnoreCase("auto"))
	        	continue;
	        else
	        	it.remove(); // avoids a ConcurrentModificationException
	    }
	    
	    
	    it = parameters.entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry<String, String> pair = (Map.Entry<String, String>)it.next();
	        System.out.println(pair.getKey() + " = " + pair.getValue());
	    }

		log.info("Completed Job Parameter Unloading");
		
	}
	
	
	
	/**
	 * get a job going
	 * 
	 */
	void jobSetUp () {
		
		JobSetUp _Job = new JobSetUp(parameters);
		job = _Job.get_PJ();
		
		JobStepManager jsm = new JobStepManager(job.getUid());
		jsm.updateStatus(ProcessJobStep.STEP_SETUP, ProcessJobStep.STATUS_COMPLETE);
		
	}
	
	
	
	
	/** 
	 * go through raw input looking for delimiters, column names, and existing maps
	 */
	void analyze () {
		
		JobStepManager jsm = new JobStepManager(job.getUid());
		jsm.updateStatus(ProcessJobStep.STEP_ANALYZE, ProcessJobStep.STATUS_ACTIVE);
		jsm.updateStepStart(ProcessJobStep.STEP_ANALYZE);
		
		log.info("Starting Input Analysis");
		
		InputAnalyzer _IA = new InputAnalyzer (parameters);
		
		new JobParameter(job.getUid()).saveAllParameters(parameters);
		
		log.info("Output: " + _IA.getStatsAsHTML());
		
		jsm = new JobStepManager(job.getUid());
		jsm.updateReport(ProcessJobStep.STEP_ANALYZE, _IA.getStatsAsHTML());
		jsm.updateStatus(ProcessJobStep.STEP_ANALYZE, ProcessJobStep.STATUS_COMPLETE);
		jsm.updateStepEnd(ProcessJobStep.STEP_ANALYZE);
		
	}
	
	
	
	/** 
	 * go through raw input looking for delimiters, column names, and existing maps
	 */
	void inputstore () {
		
		JobStepManager jsm = new JobStepManager(job.getUid());
		jsm.updateStatus(ProcessJobStep.STEP_INPUTSTORE, ProcessJobStep.STATUS_ACTIVE);
		jsm.updateStepStart(ProcessJobStep.STEP_INPUTSTORE);
		
		log.info("Starting Input Store to Mongodb");
		
		/* FlatFileToMongo _MM = */ new FlatFileToMongo (parameters);
		
		jsm = new JobStepManager(job.getUid());
		jsm.updateStatus(ProcessJobStep.STEP_INPUTSTORE, ProcessJobStep.STATUS_COMPLETE);
		jsm.updateStepEnd(ProcessJobStep.STEP_INPUTSTORE);
		
	}
	
	
	String mapSet = "";
	
	void selectMapSet () {
		//  temporary !!
		if (parameters.get("mapname") == null)
			mapSet = "legacy";
		else
			mapSet = parameters.get("mapname");
		new JobParameter(Long.parseLong(parameters.get("jobuid"))).saveOneParameter("mapname", mapSet);
	}
	
	
	
	// vars for old-style map interfaces
	//ProviderInterface pi;
	//PlanMemberInterface mi;
	//EnrollmentInterface ei;
	//ClaimRxInterface ri;
	//ClaimInterface ci;
	
	
	
	void mapping () {
		
		
		JobStepManager jsm = new JobStepManager(job.getUid());
		jsm.updateStatus(ProcessJobStep.STEP_MAP, ProcessJobStep.STATUS_ACTIVE);
		jsm.updateStepStart(ProcessJobStep.STEP_MAP);
		
		log.info("Starting mapping process" + new Date());
		
		// Mapping
		MappingController _MC = new MappingController(parameters);
		
		
		jsm = new JobStepManager(job.getUid());
		jsm.updateReport(ProcessJobStep.STEP_MAP, _MC.getStatsAsHTML());
		jsm.updateStatus(ProcessJobStep.STEP_MAP, ProcessJobStep.STATUS_COMPLETE);
		jsm.updateStepEnd(ProcessJobStep.STEP_MAP);
		
		/*  old-style mapping using individual interfaces
		log.info("Starting provider mapping " + new Date());
		pi = new ProviderDataFromHCI3_H();
		pi.addSourceFile(parameters.get("provider_file"));
		pi.prepareAllProviders();
		log.info("Provider mapping complete " + new Date());
		
		log.info("Starting member mapping " + new Date());
		mi = new PlanMemberDataFromHCI3_H();
		mi.addSourceFile(parameters.get("member_file"));
		mi.prepareAllPlanMembers();
		log.info("Member mapping complete " + new Date());
		
		log.info("Starting enrollment mapping " + new Date());
		ei = new EnrollmentDataFromHCI3_H();
		ei.addSourceFile(parameters.get("enroll_file"));
		ei.prepareAllEnrollments();
		log.info("Enrollment mapping complete " + new Date());
		
		log.info("Starting Rx mapping " + new Date());
		ri = new ClaimRxDataFromHCI3_H();
		ri.addSourceFile(parameters.get("claim_rx_file"));
		ri.prepareAllRxClaims();
		log.info("Rx mapping complete " + new Date());
		
		log.info("Starting claim mapping " + new Date());
		ci = new ClaimDataFromHCI3_H();
		ci.addSourceFile(parameters.get("claim_file"));
		ci.prepareAllClaims();
		log.info("Claim mapping complete " + new Date());
		*/
		
		
	}
	
	/**
	 * things that need to happen after mapping, but before normalization, validation, and construction
	 */
	void postMap () {
		
		JobStepManager jsm = new JobStepManager(job.getUid());
		jsm.updateStatus(ProcessJobStep.STEP_POSTMAP, ProcessJobStep.STATUS_ACTIVE);
		jsm.updateStepStart(ProcessJobStep.STEP_POSTMAP);
		
		/* PostMappingController _MC = */ new PostMappingController(parameters);
		
		jsm = new JobStepManager(job.getUid());
		jsm.updateStatus(ProcessJobStep.STEP_POSTMAP, ProcessJobStep.STATUS_COMPLETE);
		jsm.updateStepEnd(ProcessJobStep.STEP_POSTMAP);
		
	}
	

	/**
	 * reports that follow mapping
	 */
	void postMapReport () {
		
		JobStepManager jsm = new JobStepManager(job.getUid());
		jsm.updateStatus(ProcessJobStep.STEP_POSTMAP_REPORT, ProcessJobStep.STATUS_ACTIVE);
		jsm.updateStepStart(ProcessJobStep.STEP_POSTMAP_REPORT);
		
		parameters.put("stepname", ProcessJobStep.STEP_POSTMAP_REPORT);
		new ReportDriver(parameters);
		
		jsm = new JobStepManager(job.getUid());
		jsm.updateStatus(ProcessJobStep.STEP_POSTMAP_REPORT, ProcessJobStep.STATUS_COMPLETE);
		jsm.updateStepEnd(ProcessJobStep.STEP_POSTMAP_REPORT);
		
	}
	
	
	/**
	 * prepare HCI3 run units from mapped input (Mongo source, SQL target)
	 */
	void normalization () {
		
		new NormalizationController(parameters);
		
	}
	
	
	/**
	 * execute post-normalization (q for construction)
	 */
	void postnormalization () {
		
		log.info("Starting Post-normalization");
		
		JobStepManager jsm = new JobStepManager(job.getUid());
		jsm.updateStatus(ProcessJobStep.STEP_POSTNORMALIZATION, ProcessJobStep.STATUS_ACTIVE);
		jsm.updateStepStart(ProcessJobStep.STEP_POSTNORMALIZATION);
		
		new PostNormalizationController(parameters);
		
		jsm = new JobStepManager(job.getUid());
		jsm.updateStatus(ProcessJobStep.STEP_POSTNORMALIZATION, ProcessJobStep.STATUS_COMPLETE);
		jsm.updateStepEnd(ProcessJobStep.STEP_POSTNORMALIZATION);
		
		log.info("Completed Post-normalization");
		
	}
	

	/**
	 * reports that follow normalization
	 */
	void postnormalizationReport () {
		
		JobStepManager jsm = new JobStepManager(job.getUid());
		jsm.updateStatus(ProcessJobStep.STEP_POSTNORMALIZATION_REPORT, ProcessJobStep.STATUS_ACTIVE);
		jsm.updateStepStart(ProcessJobStep.STEP_POSTNORMALIZATION_REPORT);
		
		parameters.put("stepname", ProcessJobStep.STEP_POSTNORMALIZATION_REPORT);
		new ReportDriver(parameters);
		
		jsm = new JobStepManager(job.getUid());
		jsm.updateStatus(ProcessJobStep.STEP_POSTNORMALIZATION_REPORT, ProcessJobStep.STATUS_COMPLETE);
		jsm.updateStepEnd(ProcessJobStep.STEP_POSTNORMALIZATION_REPORT);
		
	}
	
	

	/**
	 * reports that follow construction
	 */
	void postConstructionReport () {
		
		JobStepManager jsm = new JobStepManager(job.getUid());
		jsm.updateStatus(ProcessJobStep.STEP_CONSTRUCTION_REPORT, ProcessJobStep.STATUS_ACTIVE);
		jsm.updateStepStart(ProcessJobStep.STEP_CONSTRUCTION_REPORT);
		
		parameters.put("stepname", ProcessJobStep.STEP_CONSTRUCTION_REPORT);
		new ReportDriver(parameters);
		
		jsm = new JobStepManager(job.getUid());
		jsm.updateStatus(ProcessJobStep.STEP_CONSTRUCTION_REPORT, ProcessJobStep.STATUS_COMPLETE);
		jsm.updateStepEnd(ProcessJobStep.STEP_CONSTRUCTION_REPORT);
		
	}
	
	
	
	/**
	 * create the schema that will hold the outputs of mapping and Episode Construction
	 */
	void createOutputSchema ()  {
		
		log.info("Starting DB Template Copy");
		
		JobStepManager jsm = new JobStepManager(job.getUid());
		jsm.updateStatus(ProcessJobStep.STEP_SCHEMACREATE, ProcessJobStep.STATUS_ACTIVE);
		jsm.updateStepStart(ProcessJobStep.STEP_SCHEMACREATE);

		DBTemplateCopy instance = new DBTemplateCopy("template", "prd", parameters);
		
		instance.process();
		
		jsm = new JobStepManager(job.getUid());
		jsm.updateStatus(ProcessJobStep.STEP_SCHEMACREATE, ProcessJobStep.STATUS_COMPLETE);
		jsm.updateStepEnd(ProcessJobStep.STEP_SCHEMACREATE);
		
		log.info("Completed DB Template Copy");
	}
	
	
	void construction ()  {
		
		new ConstructionController(parameters);
		
	}
	
	
	
	void postEC_SQL (String stepName) {
		
		log.info("Starting " + stepName);
		
		JobStepManager jsm = new JobStepManager(job.getUid());
		jsm.updateStatus(stepName, ProcessJobStep.STATUS_ACTIVE);
		jsm.updateStepStart(stepName);
		
		parameters.put("stepname", stepName);
		PostConstruction _pc = new PostConstruction(parameters);
		boolean result = _pc.process();
		
		jsm = new JobStepManager(job.getUid());
		jsm.updateStatus(stepName, result ? ProcessJobStep.STATUS_COMPLETE : ProcessJobStep.STATUS_FAILED);
		jsm.updateStepEnd(stepName);
		
		log.info("Completed " + stepName);
	
	}
	
	
	/** 
	 * create final backup of data schema
	 */
	void archive () {
		
		JobStepManager jsm = new JobStepManager(job.getUid());
		jsm.updateStatus(ProcessJobStep.STEP_ARCHIVE, ProcessJobStep.STATUS_ACTIVE);
		jsm.updateStepStart(ProcessJobStep.STEP_ARCHIVE);
		
		log.info("Starting Archival of Data Schema");
		
		ArchiveController _AC = new ArchiveController (parameters);
		_AC.Backupdbtosql();
		
		jsm = new JobStepManager(job.getUid());
		jsm.updateStatus(ProcessJobStep.STEP_ARCHIVE, ProcessJobStep.STATUS_COMPLETE);
		jsm.updateStepEnd(ProcessJobStep.STEP_ARCHIVE);
		
	}
	
	

	/** 
	 * clean out all sorts of job artificats, but leave the job and its steps in place
	 */
	void purge () {
		
		JobStepManager jsm = new JobStepManager(job.getUid());
		jsm.updateStatus(ProcessJobStep.STEP_CLEANOUT, ProcessJobStep.STATUS_ACTIVE);
		jsm.updateStepStart(ProcessJobStep.STEP_CLEANOUT);
		
		log.info("Starting Archival of Data Schema");
		
		/*PurgeController _AC = */ new PurgeController (parameters);
		
		jsm = new JobStepManager(job.getUid());
		jsm.updateStatus(ProcessJobStep.STEP_CLEANOUT, ProcessJobStep.STATUS_COMPLETE);
		jsm.updateStepEnd(ProcessJobStep.STEP_CLEANOUT);
		
	}
	
	
	void parameterSet (BigKahuna instance) {
		
		String s = instance.parameters.get("runname") + instance.parameters.get("rundate");
		instance.parameters.put("jobname", s);
		RunParameters.parameters.put("jobname", s);
		RunParameters.parameters.put("configfolder", instance.parameters.get("configfolder"));
	
	
	
		if( ! instance.parameters.get("jobstep").equals("setup")) {
			String _sJ = instance.parameters.get("jobstep");
			instance.reLoadParameters();
			instance.parameters.put("jobstep", _sJ);
		}
	

		for(Entry<String, String> entry : instance.parameters.entrySet()){
			log.info("Parameter Key and Value: " +  entry.getKey() + " " + entry.getValue());
		}
		
	}
	
	//HashMap<String, String> parameters = new HashMap<String, String>();
	static String [][] parameterDefaults = {
			{"configfolder", "C:\\workspace\\ECR_Analytics\\trunk\\EpisodeConstruction\\src\\"},
			{"studybegin", "20120101"},
			{"studyend", "21121231"},
			{"typeoutput", "csv"},
			{"runname", "Test"}
	};
	
	
	
	
	DateFormat df1 = new SimpleDateFormat("yyyyMMdd");
	DateFormat df2 = new SimpleDateFormat("yyyy-MM-dd");
	
	
	
	private static org.apache.log4j.Logger log = Logger.getLogger(BigKahuna.class);


	

}
