




public class JobQueueHelper {
	
	

	/**
	 * update the JobStepQueue per status set during process
	 */
	static synchronized public void updateMemberQueue (List<JobStepQueue> memberList) {
		
		// initialize Hibernate and get someone to process
		List<Class<?>> cList = new ArrayList<Class<?>>();
		cList.add(JobStepQueue.class);
				
		HibernateHelper h = new HibernateHelper(cList, "ecr", "ecr");
		SessionFactory factory = h.getFactory("ecr", "ecr");
				
				Session session = factory.openSession();
				session.setCacheMode(CacheMode.IGNORE);
		        session.setFlushMode(FlushMode.COMMIT);
				Transaction tx = null;
				
				try{

					tx = session.beginTransaction();
		            
					JobStepQueue jsq;
		            for (JobStepQueue _JSQ:memberList) {
		            	
		            	//log.info("Trying to complete " + _JSQ.getUid());
		            	
		            	Query q = session.createQuery("FROM JobStepQueue WHERE uid = :uid ");
		            	q.setParameter("uid", _JSQ.getUid());
		            	jsq = (JobStepQueue)q.list().get(0);
		            	jsq.setStatus(_JSQ.getStatus());
		            	session.merge(jsq);
		            }
		            
		            tx.commit();
		 						
					
				}catch (HibernateException e) {
					
					if (tx!=null) tx.rollback();
					e.printStackTrace();
					
				}finally {
					
					//session.close(); 

				}
					
		
	}
	
	
	

	/**
	 * update the JobStepQueue per status set during process
	 */
	static synchronized public long getQueueCount (long jobUid, String stepName) {
		
		Long count = -1L;
		
		// initialize Hibernate and get someone to process
		List<Class<?>> cList = new ArrayList<Class<?>>();
		cList.add(JobStepQueue.class);
				
		HibernateHelper h = new HibernateHelper(cList, "ecr", "ecr");
		SessionFactory factory = h.getFactory("ecr", "ecr");
				
				Session session = factory.openSession();
				session.setCacheMode(CacheMode.IGNORE);
		        session.setFlushMode(FlushMode.COMMIT);
				Transaction tx = null;
				
				try{

					tx = session.beginTransaction();
					
					Query query = session.createQuery("SELECT COUNT(*) FROM JobStepQueue WHERE jobUid = :jobUid AND stepName = :stepName");
					query.setLong("jobUid", jobUid);
					query.setString("stepName", stepName);
					count = (Long)query.uniqueResult();
		            
		            tx.commit();
		 						
					
				}catch (HibernateException e) {
					
					if (tx!=null) tx.rollback();
					e.printStackTrace();
					
				}finally {
					
					session.close(); 

				}
					
		return count;
		
	}
	

	
	

}
