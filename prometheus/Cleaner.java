




public class Cleaner {
	
	
	private HashMap<String, String> parameters;
	private List<String> stepsToClean = new ArrayList<String>();
	
	
	private HibernateHelper h;
	private SessionFactory factory;
	private Session session;
	
	String dbUrl;
	String schema;
	String dbUser;
	String dbPw;
	
	Mongo mongoClient;
	DB db;
	DBCollection coll;
	
	MongoInputCollectionSet micky;
	

	public Cleaner (HashMap<String, String> parameters) {
		this.parameters = parameters;
	}
	
	
	/**
	 * Pass a single entry String array with 'all' if you want to wipe the whole job
	 * Otherwise, pass an array of strings with step name(s)
	 * @param steps
	 */
	public void cleanUp (String [] steps) {
		
		if (steps[0].equalsIgnoreCase("all")) {
			getAllSteps();
		}
		else {
			for (String s : steps) {
				stepsToClean.add(s);
			}
		}
			
		for (String s : stepsToClean) {
			
			switch (s) {
			
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
	      
	        /*    
	        case ProcessJobStep.STEP_SELECTMAP:
	        	// select the proper mapping for the input
	    		selectMapSet();
	    		break;
	    	*/
	            
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
	            
	        /*
	        case ProcessJobStep.STEP_VALIDATE:  
	        	log.warn("Validation not implemented");
	        	break;
	        	*/
	        
	        case ProcessJobStep.STEP_CONSTRUCT:
	        	// episode construction
	    		construction();
	    		break;
	    		
	        case ProcessJobStep.STEP_COST_ROLLUPS:
	        case ProcessJobStep.STEP_FILTERED_ROLLUPS:
	        case ProcessJobStep.STEP_MASTER_UNFILTERED_RA_SA:
	        case ProcessJobStep.STEP_RA_SA_MODEL:
	        case ProcessJobStep.STEP_RED:

				break;
	    		
	        default: 
	        	log.error("You asked me to clean a job step I don't recognize " + parameters.get("jobstep"));
	        	break;
	        	
			}

		}
		
	}
	
	
	private void construction() {

		ConnectParameters cpTarget = new ConnectParameters("prd");
		cpTarget.setSchema(parameters.get("jobname"));
		Connection trgConn = ConnectionHelper.getConnection(cpTarget);
		Statement truncate;
		
		try {
		
			truncate = trgConn.createStatement();
			truncate.execute("TRUNCATE TABLE  " + parameters.get("jobname") + ".assignment");
			truncate.execute("TRUNCATE TABLE  " + parameters.get("jobname") + ".association");
			truncate.execute("TRUNCATE TABLE  " + parameters.get("jobname") + ".episode");
			truncate.execute("TRUNCATE TABLE  " + parameters.get("jobname") + ".triggers");
		
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		
	}


	private void postnormalizationReport() {
		
		ConnectParameters cpTarget = new ConnectParameters("prd");
		cpTarget.setSchema(parameters.get("jobname"));
		Connection trgConn = ConnectionHelper.getConnection(cpTarget);
		Statement delete;
		
		try {
		
			delete = trgConn.createStatement();
			delete.execute("DELETE FROM " + parameters.get("jobname") + ".processReport WHERE jobUid = '" + parameters.get("jobuid") + "' AND stepName = '" + ProcessJobStep.STEP_POSTNORMALIZATION_REPORT + "'");
		
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
	}


	/**
	 * delete entries from construction queue
	 */
	private void postnormalization() {
		
		if (h == null) {
			initializeControlDB();
		}
		
		session = factory.openSession();
		Transaction tx = null;
		
		int iCount = 0;

		
		try { 

			tx = session.beginTransaction(); 
			
			StringBuffer sQ = new StringBuffer("select count(id) from JobStepQueue WHERE jobuid = :jobuid AND stepName = '").append(ProcessJobStep.STEP_CONSTRUCT).append("'");

			iCount = ((Long) session.createQuery(sQ.toString()).setString("jobuid", parameters.get("jobuid")).uniqueResult()).intValue();
			
			session.getTransaction().commit(); 
			
		} 
		catch (HibernateException e) { 
			e.printStackTrace(); 
			if (tx != null) tx.rollback(); 
		} 
		finally {
			
		}
		
		
		if (iCount > 0) {
			
			int delResult = 0;
			
			for (int x=0; x < 100; x++) {
			
				try {
					
					tx = session.beginTransaction();
			
					// remove all normalization entries from the job step queue
					String hql = "delete from JobStepQueue where jobuid = :jobuid AND stepName = '" + ProcessJobStep.STEP_CONSTRUCT + "'";
					delResult = session.createQuery(hql).setString("jobuid", parameters.get("jobuid")).executeUpdate();
			
					tx.commit();
			
				} catch (HibernateException e) {
					if (tx!=null) tx.rollback();
					e.printStackTrace(); 
				} finally {
					session.close(); 
				}
				
				
				if (delResult == 0) {
					
					try {
						TimeUnit.SECONDS.sleep(5);
					} catch (InterruptedException e) {
						break;
					}
				} else {
					break;
				}
				
			}
			
			
			
		}
		
		
	}


	/**
	 * truncate all tables representing input
	 * erase the summary report
	 */
	private void normalization() {
		
		ConnectParameters cpTarget = new ConnectParameters("prd");
		cpTarget.setSchema(parameters.get("jobname"));
		Connection trgConn = ConnectionHelper.getConnection(cpTarget);
		Statement truncate;
		
		try {
		
			truncate = trgConn.createStatement();
			truncate.execute("TRUNCATE TABLE " + parameters.get("jobname") + ".claim_line");
			truncate.execute("TRUNCATE TABLE " + parameters.get("jobname") + ".claim_line_rx");
			truncate.execute("TRUNCATE TABLE " + parameters.get("jobname") + ".claims_combined");
			truncate.execute("TRUNCATE TABLE " + parameters.get("jobname") + ".code");
			truncate.execute("TRUNCATE TABLE " + parameters.get("jobname") + ".enrollment");
			//truncate.execute("TRUNCATE TABLE " + parameters.get("jobname") + ".enrollment_gap");
			truncate.execute("TRUNCATE TABLE " + parameters.get("jobname") + ".member");
		
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		if (h == null) {
			initializeControlDB();
		}
		
		session = factory.openSession();
		Transaction tx = null;
		try {
			
			tx = session.beginTransaction();
			
			// remove all normalization entries from the job step queue
			String hql = "UPDATE ProcessJobStep SET report = NULL where jobuid = :jobuid AND stepName = '" + ProcessJobStep.STEP_NORMALIZATION + "'";
			session.createQuery(hql).setString("jobuid", parameters.get("jobuid")).executeUpdate();
			
			tx.commit();
			
		} catch (HibernateException e) {
			if (tx!=null) tx.rollback();
			e.printStackTrace(); 
		} finally {
			session.close(); 
		}
		
	}


	/**
	 * delete postmap reports from data db
	 */
	private void postMapReport() {
		
		ConnectParameters cpTarget = new ConnectParameters("prd");
		cpTarget.setSchema(parameters.get("jobname"));
		Connection trgConn = ConnectionHelper.getConnection(cpTarget);
		Statement delete;
		
		try {
		
			delete = trgConn.createStatement();
			delete.execute("DELETE FROM " + parameters.get("jobname") + ".processReport WHERE jobUid = '" + parameters.get("jobuid") + "' AND stepName = '" + ProcessJobStep.STEP_POSTMAP_REPORT + "'");
		
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
	}


	/**
	 * remove entries from normalization queue
	 * remove providers from data db
	 * Don't worry about Mongo indexes of mapped data.  They don't need to be cleared to be re-created, and will be destroyed with their collections
	 */
	private void postMap() {
		
		if (h == null) {
			initializeControlDB();
		}
		
		int iCount = 0;
		
		session = factory.openSession();
		Transaction tx = null;
		
		try { 

			tx = session.beginTransaction(); 
			
			StringBuffer sQ = new StringBuffer("select count(id) from JobStepQueue WHERE jobuid = :jobuid AND stepName = '").append(ProcessJobStep.STEP_NORMALIZATION).append("'");

			iCount = ((Long) session.createQuery(sQ.toString()).setString("jobuid", parameters.get("jobuid")).uniqueResult()).intValue();
			
			session.getTransaction().commit(); 
			
		} 
		catch (HibernateException e) { 
			e.printStackTrace(); 
			if (tx != null) tx.rollback(); 
		} 
		finally {
			
		}
		
		
		if (iCount > 0) {
			
			int delResult = 0;
			
			for (int x=0; x < 100; x++) {
			
				try {
					
					tx = session.beginTransaction();
			
					// remove all normalization entries from the job step queue
					String hql = "delete from JobStepQueue where jobuid = :jobuid AND stepName = '" + ProcessJobStep.STEP_NORMALIZATION + "'";
					delResult = session.createQuery(hql).setString("jobuid", parameters.get("jobuid")).executeUpdate();
			
					tx.commit();
			
				} catch (HibernateException e) {
					if (tx!=null) tx.rollback();
					e.printStackTrace(); 
				} finally {
					session.close(); 
				}
				
				
				if (delResult == 0) {
					
					try {
						TimeUnit.SECONDS.sleep(5);
					} catch (InterruptedException e) {
						break;
					}
				} else {
					break;
				}
				
			}
			
		}
		
		
		ConnectParameters cpTarget = new ConnectParameters("prd");
		cpTarget.setSchema(parameters.get("jobname"));
		Connection trgConn = ConnectionHelper.getConnection(cpTarget);
		Statement truncate;
		
		try {
		
			truncate = trgConn.createStatement();
			truncate.execute("TRUNCATE TABLE " + parameters.get("jobname") + ".provider");
			truncate.execute("TRUNCATE TABLE " + parameters.get("jobname") + ".provider_specialty");
		
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		
	}


	/**
	 * get rid of the mapped Mongo collections
	 */
	private void mapping() {
		
		micky = new MongoInputCollectionSet(parameters);
		db = micky.getDb();
		
		db.getCollection(micky.getsClaimColl()).drop();
		db.getCollection(micky.getsEnrollColl()).drop();
		db.getCollection(micky.getsMemberColl()).drop();
		db.getCollection(micky.getsProviderColl()).drop();
		db.getCollection(micky.getsRxColl()).drop();
		
	}


	/**
	 * drop the data schema
	 */
	private void createOutputSchema() {

		ConnectParameters cpTarget = new ConnectParameters("prd");
		cpTarget.setSchema(parameters.get("jobname"));
		Connection trgConn = ConnectionHelper.getConnection(cpTarget);
		if (trgConn == null)	// means output db was never created in the first place
			return;
		Statement drop;
		
		try {
		
			drop = trgConn.createStatement();
			if(cpTarget.getDialect().equalsIgnoreCase("vertica"))
				drop.execute("DROP SCHEMA " + parameters.get("jobname") + " CASCADE");
			else
				drop.execute("DROP SCHEMA " + parameters.get("jobname"));
		
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
	}


	/**
	 * get rid of all the Mongo representations of the input
	 */
	private void inputstore() {
		
		Iterator<Entry<String, String>> it = parameters.entrySet().iterator();
		String key, value;
		String sRunDate = parameters.get("rundate");
		
		String env = parameters.get("mongo") == null ?  "md1" :  parameters.get("mongo");

		ConnectParameters connParms = new ConnectParameters(env);
		dbUrl = connParms.getDbUrl();
		schema = connParms.getSchema();
		dbUser = connParms.getDbUser();
		dbPw = connParms.getDbPw();

		log.info(">>>  Connection information - U: " + dbUrl + "; S: " + schema + "; I: " + dbUser + " P:" + dbPw); 
	    

		try {
			mongoClient = new MongoClient(dbUrl);
		} catch (UnknownHostException e) {
			e.printStackTrace();
			throw new IllegalStateException(e);
		}
		db = mongoClient.getDB(schema);
		
		// iterate through all parameters looking for those containing 'file'
	    while (it.hasNext()) {
	    	
	    	Entry<String, String> e = it.next();
	        key = e.getKey();
	        value = e.getValue();
	        if (key.toLowerCase().contains("file")) {
	        	
	        	// get the filename to start establishing the collection name
	    		String sFN = value.contains("/") ? value.substring(value.lastIndexOf("/") + 1) : value;
	    		String sCollectionName = parameters.get("runname").replace(' ', '_') + sFN + sRunDate;

	        	if (db.collectionExists(sCollectionName)) {
	        	    DBCollection myCollection = db.getCollection(sCollectionName);
	        	    myCollection.drop();
	        	}
	        }
	        
	    }

	}


	private void analyze() {
		
		if (h == null) {
			initializeControlDB();
		}
		
		session = factory.openSession();
		Transaction tx = null;
		try {
			
			tx = session.beginTransaction();
			
			// get rid of all input monitor rows
			String hql = "delete from InputMonitor where jobuid = :jobuid";
			session.createQuery(hql).setString("jobuid", parameters.get("jobuid")).executeUpdate();
			// technically I should get rid of the summary report, but it is likely to be recreated or destroyed with the next process anyway
			
			tx.commit();
			
		} catch (HibernateException e) {
			if (tx!=null) tx.rollback();
			e.printStackTrace(); 
		} finally {
			session.close(); 
		}
		
	}


	/**
	 * delete parameters, job steps, and job entry - all in ecr table
	 */
	private void jobSetUp() {
		
		if (h == null) {
			initializeControlDB();
		}
		
		session = factory.openSession();
		Transaction tx = null;
		try {
			
			tx = session.beginTransaction();
			
			// get rid of all parameters
			String hql = "delete from ProcessJobParameter where jobuid = :jobuid";
			session.createQuery(hql).setString("jobuid", parameters.get("jobuid")).executeUpdate();
			// get rid of all job steps
			hql = "delete from ProcessJobStep where jobuid = :jobuid";
			session.createQuery(hql).setString("jobuid", parameters.get("jobuid")).executeUpdate();
			// get rid of job entry
			hql = "delete from ProcessJob where uid = :jobuid";
			session.createQuery(hql).setString("jobuid", parameters.get("jobuid")).executeUpdate();
			
			tx.commit();
			
		} catch (HibernateException e) {
			if (tx!=null) tx.rollback();
			e.printStackTrace(); 
		} finally {
			session.close(); 
		}
		
	}


	/**
	 * add all steps in reverse order so earliest steps can still use parameters, etc.
	 */
	private void getAllSteps () {
		
		stepsToClean.add(ProcessJobStep.STEP_CONSTRUCT);
		stepsToClean.add(ProcessJobStep.STEP_POSTNORMALIZATION_REPORT);
		stepsToClean.add(ProcessJobStep.STEP_NORMALIZATION);
		stepsToClean.add(ProcessJobStep.STEP_POSTMAP_REPORT);
		stepsToClean.add(ProcessJobStep.STEP_POSTMAP);
		stepsToClean.add(ProcessJobStep.STEP_MAP);
		stepsToClean.add(ProcessJobStep.STEP_SCHEMACREATE);
		stepsToClean.add(ProcessJobStep.STEP_INPUTSTORE);
		stepsToClean.add(ProcessJobStep.STEP_ANALYZE);
		stepsToClean.add(ProcessJobStep.STEP_SETUP);
		

	}
	
	
	/**
	 * initialize Hibernate to enable ecr control table access
	 */
	private void initializeControlDB ()  {
		
		List<Class<?>> cList = new ArrayList<Class<?>>();
		cList.add(ProcessJob.class);
		cList.add(ProcessJobStep.class);
		cList.add(ProcessJobParameter.class);
		cList.add(JobStepQueue.class);
		cList.add(InputMonitor.class);
		
		h = new HibernateHelper(cList, "ecr", "ecr");
		factory = h.getFactory("ecr", "ecr");
		
		//session = factory.openSession();
	
	}
	
	
	
	public static void main(String[] args) {
		
		HashMap<String, String> parameters = RunParameters.parameters;
		parameters.put("clientID", "MD_APCD");
		parameters.put("runname", "Sample_Jan_2014");
		parameters.put("rundate", "20160608");
		parameters.put("jobuid", "1117");
		parameters.put("configfolder", "C:\\workspace\\ECR_Analytics\\trunk\\EpisodeConstruction\\src\\");
		parameters.put("env", "prd");
		parameters.put("studybegin", "20120101");
		parameters.put("studyend", "21120101");
		parameters.put("jobname", "MD_APCD_Sample_Jan_201420160608");
		parameters.put("stepname", ProcessJobStep.STEP_MAP);
		parameters.put("mapname", "md_apcd");
		parameters.put("claim_file2", "C:/input/Horizon/stay.sample.csv");
		parameters.put("claim_file1", "C:/input/Horizon/prof.sample.csv");
		parameters.put("claim_rx_file1", "C:/input/Horizon/rx.sample.csv");
		parameters.put("enroll_file1", "C:/input/Horizon/enroll.sample.csv");
		parameters.put("member_file1", "C:/input/Horizon/member.sample.csv");
		parameters.put("provider_file1", "C:/input/Horizon/providers.sample.csv");

		
		Cleaner instance = new Cleaner(parameters);
		String [] steps = {ProcessJobStep.STEP_MAP};
		instance.cleanUp(steps);
		//instance.getAllSteps();
		
	}
	
	
	private static org.apache.log4j.Logger log = Logger.getLogger(Cleaner.class);
	

}
