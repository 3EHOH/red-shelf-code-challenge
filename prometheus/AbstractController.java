



public abstract class AbstractController {
	

	HashMap<String, String> parameters;
	
	
	Session sessionC = null;
	Transaction txGlobal = null;
	
	
	PlanMember m;
	ArrayList<JobStepQueue> memberList = new ArrayList<JobStepQueue>();
	ArrayList<PerMemberReport> rptList = new ArrayList<PerMemberReport>();
	
	int chunkSize = 1;
	
	private final int MAX_TRANSACTION_RETRIES = 15;
	
	
	
	void queueStartAndLock (String stepname) {
		
		// connect to control and get some work
		List<Class<?>> cList = new ArrayList<Class<?>>();
		cList.add(JobStepQueue.class);
		
		int iAttempts = 0;
		while (iAttempts < MAX_TRANSACTION_RETRIES) {
			
			iAttempts++;
		
			HibernateHelper hC = new HibernateHelper(cList, "ecr", "ecr");
			SessionFactory factoryC = hC.getFactory("ecr", "ecr");
		
			sessionC = factoryC.openSession();
			txGlobal = null;
		
			try{

				txGlobal = sessionC.beginTransaction();
			
				String hql = "FROM JobStepQueue WHERE jobuid = :jobuid AND stepname = '" + stepname + "'  AND status = '" + JobStepQueue.STATUS_READY + "'";
				Query query = sessionC.createQuery(hql).setLockOptions(new LockOptions(LockMode.PESSIMISTIC_WRITE));
				if (hC.getCurrentConfig().getConnParms().getDialect().equalsIgnoreCase("vertica")) 
				{} 
				else
					query.setMaxResults(chunkSize);
				query.setParameter("jobuid", parameters.get("jobuid"));
				@SuppressWarnings("unchecked")
				List<JobStepQueue> results = query.list();
 			
				for (JobStepQueue _JSQ : results) {
					memberList.add(_JSQ);
					_JSQ.setStatus(JobStepQueue.STATUS_ACTIVE);
					_JSQ.setUpdated(new Date());
					sessionC.update(_JSQ); 
				}

				txGlobal.commit();
				
				// successful - preempt retries
				iAttempts = MAX_TRANSACTION_RETRIES;
 		
 			
			} catch (LockAcquisitionException e) {
				
				if ( iAttempts < MAX_TRANSACTION_RETRIES )  {
					log.info("JobStepQueue is locked - will retry");
				}
				else {
					log.error("JobStepQueue is locked - retry limit reached");
					throw new IllegalStateException("JobStepQueue is locked - retry limit reached");
				}
					
			
			} catch (HibernateException e) {
			
				if (txGlobal!=null) txGlobal.rollback();
				e.printStackTrace();
			
			} finally {
				
				// sessionC.close(); 
	
			}
			
			if (iAttempts < MAX_TRANSACTION_RETRIES) {
				int r = random.nextInt(59 - 1 + 1) + 1;
				try {
					Thread.sleep( (1 *   			// minutes to sleep
					         60 * 					// seconds to a minute
					         1000)					// milliseconds to a second
					         + (r * 1000));			// flutter seconds * milliseconds to a second
				} catch (InterruptedException e) {
					// no action
				} 
			}
			
			
		}
		
		
		
	}
	
	Random random = new Random();
	
	
	private static org.apache.log4j.Logger log = Logger.getLogger(AbstractController.class);
	
	

}
