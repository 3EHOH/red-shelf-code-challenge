




public class PostNormalizationController {
	
	
	HashMap<String, String> parameters;
	
	ArrayList<String> memberList = new ArrayList<String>();

	
	// create member work queue
	PostNormalizationController (HashMap<String, String> parameters) {
		this.parameters = parameters;
		buildMemberWorkQueue();
		//buildClaimsCombined();
	}
	
	
	
	/**
	 * get all unique member id's from medical claims to set up a Q of member run units
	 */
	private void buildMemberWorkQueue ()  {
		
		if (parameters.get("jobuid") == null)
			throw new IllegalStateException("Post-normalization call did not set jobUid - must stop");
		
		if (parameters.get("env") == null)
			throw new IllegalStateException("Post-normalization call did not set env - must stop");
		
		List<Class<?>> cList = new ArrayList<Class<?>>();
		cList.add(JobStepQueue.class);
		
		HibernateHelper h = new HibernateHelper(cList, "ecr", "ecr");
		SessionFactory factory = h.getFactory("ecr", "ecr");
		
		Session session = factory.openSession();
		
		try{
			  
			String hql = "FROM JobStepQueue WHERE jobuid = :jobuid AND stepname = '" + ProcessJobStep.STEP_NORMALIZATION + "'  AND status = '" + JobStepQueue.STATUS_COMPLETE + "'";
			Query query = session.createQuery(hql);
			query.setParameter("jobuid", Long.parseLong(parameters.get("jobuid")));
			@SuppressWarnings("unchecked")
			List<JobStepQueue> results = query.list();
			
			for (JobStepQueue _JSQ : results)
				memberList.add(_JSQ.getMember_id());	
		    
		}catch (HibernateException e) {
			e.printStackTrace();
			throw new IllegalStateException("Post-normalization member query failed - must stop");
		}/*finally{
			session.close();
		}*/

		
		
		Transaction tx = null;
		session.setCacheMode(CacheMode.IGNORE);
        session.setFlushMode(FlushMode.COMMIT);
		
		try{
			
			JobStepQueue _JSQ = null;

			int iB = 0;
			tx = session.beginTransaction();
			
 			for (String _member : memberList) {
 				
 				_JSQ = new JobStepQueue(_member);
 				_JSQ.setJobUid(Long.parseLong(parameters.get("jobuid")));
 				_JSQ.setStepName(ProcessJobStep.STEP_CONSTRUCT);
 				_JSQ.setStatus(JobStepQueue.STATUS_READY);
 				_JSQ.setUpdated(new Date());
			    session.save(_JSQ);
			    
			    if( iB % HibernateHelper.BATCH_INSERT_SIZE == 0 ) {
             		session.flush();
             		session.clear();
                }
             	if(iB % 1000000 == 0) { 
       				log.info("executing member queue insert " + iB + " ==> " + new Date());
             	}
             	
             	iB++;
			    
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
	

	
	
	/**
	 * build claims_combined
	 */
	/*
	private void buildClaimsCombined () {
		
		List<Class<?>> cList = new ArrayList<Class<?>>();
		cList.add(ClaimsCombinedTbl.class);
		
		HibernateHelper h = new HibernateHelper(cList, parameters.get("env"), parameters.get("jobname"));
		SessionFactory factory = h.getFactory(parameters.get("env"), parameters.get("jobname"));
		Session session = factory.openSession();
		
		try{
			  
			session.createSQLQuery("TRUNCATE TABLE " + parameters.get("jobname") + ".claims_combined").executeUpdate();
			log.info("Truncate OK");
			StringBuilder _sb = new StringBuilder();
			_sb.append("INSERT INTO ");
			_sb.append(parameters.get("jobname")).append('.');
			_sb.append("claims_combined ");
			_sb.append("(SELECT id, master_claim_id, member_id, allowed_amt, assigned_count, claim_line_type_code, begin_date, end_date FROM ");
			_sb.append(parameters.get("jobname")).append('.');
			_sb.append("claim_line) ");
			_sb.append("UNION ");
			_sb.append("(SELECT id, master_claim_id, member_id, allowed_amt, assigned_count, 'RX' as claim_line_type_code, rx_fill_date as begin_date, ");
			if (h.getCurrentConfig().getConnParms().getDialect().equalsIgnoreCase("vertica"))
				_sb.append("CAST (NULL as Date) as end_date");
			else
				_sb.append("NULL as end_date");
			_sb.append(" FROM ");
			//_sb.append("(SELECT id, master_claim_id, member_id, allowed_amt, assigned_count, 'RX' as claim_line_type_code, rx_fill_date as begin_date, NULL as end_date FROM ");
			_sb.append(parameters.get("jobname")).append('.');
			_sb.append("claim_line_rx)");
			session.createSQLQuery(_sb.toString()).executeUpdate();
			log.info("Insert OK");
		    
		}catch (HibernateException e) {
			e.printStackTrace();
			log.error("Failure trying to truncate or build claims_combined in " + parameters.get("env") + " " + parameters.get("jobname"));
			throw new IllegalStateException("Generation of claims_combined failed - please see log for more details - " + e);
		}finally{
			session.close();
		}

	}
	
	/*
	private final String buildClaimsCombinedSQL = 
			"INSERT INTO claims_combined "
			+ "(SELECT id, master_claim_id, member_id, allowed_amt, assigned_count, claim_line_type_code, begin_date, end_date FROM claim_line) "
			+ "UNION "
			+ "(SELECT id, master_claim_id, member_id, allowed_amt, assigned_count, 'RX' as claim_line_type_code, rx_fill_date as begin_date, NULL as end_date FROM claim_line_rx)";
	*/

	
	
	private static org.apache.log4j.Logger log = Logger.getLogger(PostNormalizationController.class);

}
