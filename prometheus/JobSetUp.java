



public class JobSetUp {
	
	
	
	private HashMap<String, String> parameters;
	private ProcessJob _PJ; 
	
	

	JobSetUp (HashMap<String, String> parameters) {
		this.parameters = parameters;
		process();
	}

	private void process() {
		
		if (parameters.get("client") == null)
			throw new IllegalStateException ("Parameter 'client' not found - can not proceed");
		if (parameters.get("jobname") == null)
			throw new IllegalStateException ("Parameter 'jobname' not found - can not proceed");
		
		log.info("Job Setup in progress");
		
		List<Class<?>> cList = new ArrayList<Class<?>>();
		cList.add(ProcessJob.class);
		cList.add(ProcessJobStep.class);
		
		HibernateHelper h = new HibernateHelper(cList, "ecr", "ecr");
		SessionFactory factory = h.getFactory("ecr", "ecr");
		
		Session session = factory.openSession();
		Transaction tx = null;
		
		_PJ = new ProcessJob();
		_PJ.setClient_id(parameters.get("client"));
		_PJ.setJobName(parameters.get("jobname"));
		_PJ.setSubmitted_date(new Date());
		_PJ.setSubmitter_name(parameters.get("submitter_name"));
		

		try{
			tx = session.beginTransaction();
			
			long jobUid = (Long) session.save(_PJ);
			
			tx.commit();
			
			new JobParameter(jobUid).saveAllParameters(parameters);
			
			
			// place all job steps in database
			
			genSteps(jobUid);

			tx = session.beginTransaction();
 			for (ProcessJobStep _step : steps) {
 				_step.setStatus(ProcessJobStep.STATUS_ENTERED);
 				_step.setUpdated(new Date());
			    session.save(_step);
			}
 			tx.commit();
			
			
		}catch (HibernateException e) {
			
			if (tx!=null) tx.rollback();
			e.printStackTrace();
			
		}finally {
			
			session.close(); 

			try {
				h.close("prd", "ecr");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
		}
			
		
	}
	
	
	
	ArrayList<ProcessJobStep> steps = new ArrayList<ProcessJobStep>();
	
	/**
	 * the official list of job steps
	 * @param jobUid
	 */
	private void genSteps (long jobUid) {
		
		steps.add(new ProcessJobStep (jobUid, 500, "setup", "Job Setup"));
		steps.add(new ProcessJobStep (jobUid, 1000, "analyze", "Input Analysis"));
		steps.add(new ProcessJobStep (jobUid, 1100, "inputstore", "Store Input in Mongo"));
		steps.add(new ProcessJobStep (jobUid, 1500, "schemacreate", "Output SQL Schema Creation"));
		steps.add(new ProcessJobStep (jobUid, 2000, "map", "Mapping from Input to Mongo"));
		steps.add(new ProcessJobStep (jobUid, 2100, "postmap", "Mapping Post-Process"));
		steps.add(new ProcessJobStep (jobUid, 2500, "postmapreport", "Reports Following Data Mapping"));
		steps.add(new ProcessJobStep (jobUid, 3000, "normalization", "Prepare Run Units and Store to SQL"));
		steps.add(new ProcessJobStep (jobUid, 3100, "postnormalization", "Normalization Post-Process"));
		steps.add(new ProcessJobStep (jobUid, 3500, "postnormalizationreport", "Reports Following Data Normalization"));
		steps.add(new ProcessJobStep (jobUid, 4000, "construct", "Episode Construction"));
		steps.add(new ProcessJobStep (jobUid, 4100, "postconstructionreport", "Reports Following Episode Construction"));
		steps.add(new ProcessJobStep (jobUid, 4200, "epidedupe", "Episode De-Duplication"));
		steps.add(new ProcessJobStep (jobUid, 5000, "providerattribution", "Provider Attribution"));
		steps.add(new ProcessJobStep (jobUid, 5200, "costrollups", "Cost Rollups"));
		steps.add(new ProcessJobStep (jobUid, 5400, "filteredrollups", "Filtered Rollups"));
		steps.add(new ProcessJobStep (jobUid, 5600, "masterunfiltered_ra_sa", "Master RA/SA"));
		steps.add(new ProcessJobStep (jobUid, 5800, "rasamodel", "RA/SA Model"));
		steps.add(new ProcessJobStep (jobUid, 6000, "red", "Report Episode Detail Generation"));
		steps.add(new ProcessJobStep (jobUid, 6200, "res", "Report Episode Summary Generation"));
		steps.add(new ProcessJobStep (jobUid, 6400, "savingsummary", "Savings Summary"));
		steps.add(new ProcessJobStep (jobUid, 6600, "coreservices", "Core Services Analysis"));
		steps.add(new ProcessJobStep (jobUid, 6800, "pacanalysis", "PAC Analysis"));
		steps.add(new ProcessJobStep (jobUid, 7000, "pasanalysis", "PAS Analysis"));
		steps.add(new ProcessJobStep (jobUid, 8000, "maternity", "Maternity/Newborn"));
		
	}
	
	

	public ProcessJob get_PJ() {
		return _PJ;
	}
	
	
	
	private static org.apache.log4j.Logger log = Logger.getLogger(JobSetUp.class);

}
