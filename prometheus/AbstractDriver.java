




public abstract class AbstractDriver {
	
	
	int chunkSize = 1;
	
	String stepName;
	
	
	List<Class<?>> cList;
	HibernateHelper h;
	SessionFactory factory;
	Session session;

	HibernateHelper hGlobal;
	SessionFactory factoryGlobal;
	List<Class<?>> cListGlobal;
	
	
	List<ProcessJobStep> jobList = new ArrayList<ProcessJobStep>();
	
	BigKahuna bigKahuna = new BigKahuna();
	
	HashMap<String, String> parameters = new HashMap<String, String>();
	
	Random random = new Random();
	
	
	
	void reLoadParameters ()  {
		
		boolean b = false;
		
		for (HashMap<String, String> pset : parmsets) {
			if ( parameters.get("jobuid").equals(pset.get("jobuid")) ) {
				bigKahuna.parameters = pset;
				b = true;
				break;
			}
		}
		
		if (!b) {
			HashMap<String, String> pset = new HashMap<String, String>();
			for(Entry<String, String> entry : parameters.entrySet()){
			    pset.put(entry.getKey(), entry.getValue());
			}
			bigKahuna.reLoadParameters();
			for(Entry<String, String> entry : bigKahuna.parameters.entrySet()){
				pset.put(entry.getKey(), entry.getValue());
			}
			parmsets.add(pset);
		}
		
	}
	
	List<HashMap<String, String>> parmsets = new ArrayList<HashMap<String, String>>(); 
	

	
	
	/**
	 * kick off any pending work
	 * check active jobs first,
	 * then start any ready jobs
	 * @return
	 */
	boolean triggerWork()  {
		
		boolean b = false;
		
		if (triggerActive())
			b = true;
		else
			b = triggerJob(ProcessJobStep.STATUS_READY);
		return b;
		
	}
	

	
	/**
	 * keep active steps running until they are completed,
	 * then mark them complete when they are done
	 * @return
	 */
	private boolean triggerActive() {
		
		boolean b = false;
		
		jobList = getActive();
		
		if (jobList.isEmpty())
			return b;
	
		//session = factory.openSession();
		Transaction tx = null;
		
		try{

			tx = session.beginTransaction();
			
			// see if this active job has any more members to work on
			String hql = "FROM ProcessJobStep WHERE jobuid = :jobuid AND stepname = '" + stepName + "'";
			Query query = session.createQuery(hql);
			if (h.getCurrentConfig().getConnParms().getDialect().equalsIgnoreCase("vertica")) 
			{} 
			else
				query.setMaxResults(1);
			query.setParameter("jobuid", jobList.get(0).getJobUid());

			ProcessJobStep _PJS = (ProcessJobStep) query.list().get(0);
			
			tx.commit();
 			
			// if no more members
 			if (_PJS.getTargetCount() == null  ||  _PJS.getTargetCount() < 1 )  {
 				
 				b = checkCompleteWhenNoTarget();
 				
 			}
 			else {
 			
 				b = checkCompleteByTarget(_PJS.getTargetCount());
 			
 			}
 			
 			
 						
			
		}catch (HibernateException e) {
			
			if (tx!=null) tx.rollback();
			e.printStackTrace();
			
		}finally {

			
		}
			
		
		return b;
		
	}
	
	
	private boolean checkCompleteByTarget (Long target)  {
		
		boolean b = false;
		
		Transaction tx = null;
				
		try{

			tx = session.beginTransaction();
					
			// see if this active job has any more members to work on
			String hql = "SELECT COUNT(uid) FROM JobStepQueue WHERE jobuid = :jobuid AND stepname = '" + stepName + "'  AND status IN "
					+ "('" + JobStepQueue.STATUS_COMPLETE + "', "
					+ "'" + JobStepQueue.STATUS_FAILED + "')";
			Query query = session.createQuery(hql);
			if (h.getCurrentConfig().getConnParms().getDialect().equalsIgnoreCase("vertica")) 
			{} 
			else
				query.setMaxResults(1);
			query.setParameter("jobuid", jobList.get(0).getJobUid());
			Long count = (Long)query.uniqueResult();
		 			
			// if no more members
			if (count == target.longValue())  {
				
				String hqlJ = "FROM ProcessJobStep WHERE jobuid = :jobuid AND  stepName = '" + stepName + "'";
				Query queryJ = session.createQuery(hqlJ);
				queryJ.setParameter("jobuid", jobList.get(0).getJobUid());
				ProcessJobStep _PJS = (ProcessJobStep) queryJ.list().get(0);
		 			 			
				removeJob(_PJS);
				_PJS.setStatus(ProcessJobStep.STATUS_COMPLETE);
				_PJS.setReport(null);
				Date d = new Date();
				_PJS.setUpdated(d);
				_PJS.setStepEnd(d);
				session.update(_PJS); 
				
				Blob ablob = (Blob) _PJS.getReport();
				String str = "";
				if (ablob != null)
					str = new String(ablob.getBytes(1l, (int) ablob.length()));
				UserNotification.sendEmail(_PJS.getStepName() + " has completed for job " + _PJS.getJobUid() +
						System.lineSeparator() + str );
		 				
			}
			else
				b = true;
			
			tx.commit();
		 						
					
		}catch (HibernateException | SQLException e) {
					
			if (tx!=null) tx.rollback();
			e.printStackTrace();
					
		}finally {
					
		}
					
				
		return b;
				
		
	}
	
	
	
	/**
	 * soon-to-be legacy step end check
	 * suffered from setting steps to complete when there were no more ready members
	 * usually processes completed OK, and all was happy, but sometimes...
	 * @return
	 */
	private boolean checkCompleteWhenNoTarget ()  {
		
		boolean b = false;
		
		Transaction tx = null;
				
		try{

			tx = session.beginTransaction();
					
			// see if this active job has any more members to work on
			String hql = "FROM JobStepQueue WHERE jobuid = :jobuid AND stepname = '" + stepName + "'  AND status = '" + JobStepQueue.STATUS_READY + "'";
			Query query = session.createQuery(hql);
			if (h.getCurrentConfig().getConnParms().getDialect().equalsIgnoreCase("vertica")) 
			{} 
			else
				query.setMaxResults(1);
			query.setParameter("jobuid", jobList.get(0).getJobUid());
			@SuppressWarnings("unchecked")
			List<JobStepQueue> results = query.list();
		 			
			// if no more members
			if (results.isEmpty())  {
				
				String hqlJ = "FROM ProcessJobStep WHERE jobuid = :jobuid AND  stepName = '" + stepName + "'";
				Query queryJ = session.createQuery(hqlJ);
				queryJ.setParameter("jobuid", jobList.get(0).getJobUid());
				ProcessJobStep _PJS = (ProcessJobStep) queryJ.list().get(0);
		 			 			
				removeJob(_PJS);
				_PJS.setStatus(ProcessJobStep.STATUS_COMPLETE);
				Date d = new Date();
				_PJS.setUpdated(d);
				_PJS.setStepEnd(d);
				session.update(_PJS); 
				
				Blob ablob = (Blob) _PJS.getReport();
				String str = "";
				if (ablob != null)
					str = new String(ablob.getBytes(1l, (int) ablob.length()));
				UserNotification.sendEmail(_PJS.getStepName() + " has completed for job " + _PJS.getJobUid() +
						System.lineSeparator() + str );
		 				
			}
			else
				b = true;
			
			tx.commit();
		 						
					
		}catch (HibernateException | SQLException e) {
					
			if (tx!=null) tx.rollback();
			e.printStackTrace();
					
		}finally {
					
		}
					
				
		return b;
				
		
	}
	
	
	
	/**
	 * trigger any steps that are in ready status and update to active
	 * @return
	 */
	boolean triggerJob(String status) {
		
		if (parameters.containsKey("jobuid")) {
			ProcessJobStep _p = new ProcessJobStep();
			_p.setJobUid(Long.parseLong(parameters.get("jobuid")));
			_p.setStepName(stepName);
			jobList.add(_p);
			return true;
		}
		
		boolean b = false;
	
		//session = factory.openSession();
		Transaction tx = null;
		
		try{

			tx = session.beginTransaction();
					
			String hql = "FROM ProcessJobStep WHERE stepName = '" + stepName + "'  AND status = '" + status + "'";
			Query query = session.createQuery(hql);
			@SuppressWarnings("unchecked")
			List<ProcessJobStep> results = query.list();
		 			
			for (ProcessJobStep _PJS : results) {
				b = true;
				addJob(_PJS);
				if (status.equals(ProcessJobStep.STATUS_READY)) {
					_PJS.setStatus(ProcessJobStep.STATUS_ACTIVE);
					Date d = new Date();
					_PJS.setUpdated(d);
					_PJS.setStepStart(d);
					_PJS.setTargetCount(JobQueueHelper.getQueueCount(_PJS.getJobUid(), stepName));
					session.update(_PJS); 
				}
			}
			
			tx.commit();
			
			
		}catch (HibernateException e) {
					
			if (tx!=null) tx.rollback();
			e.printStackTrace();
					
		}finally {
			

		}
		
		return b;
		
	}
	
	
	private List<ProcessJobStep> getActive () {
		
		List<ProcessJobStep> _jobList = new ArrayList<ProcessJobStep>();
		
		Transaction tx = null;
		
		try{

			tx = session.beginTransaction();
					
			String hql = "FROM ProcessJobStep WHERE stepName = '" + stepName + "'  AND status = '" + JobStepQueue.STATUS_ACTIVE + "'";
			Query query = session.createQuery(hql);
			@SuppressWarnings("unchecked")
			List<ProcessJobStep> results = query.list();
		 			
			for (ProcessJobStep _PJS : results) {
				_jobList.add(_PJS);
			}
			
			tx.commit();
			
			
		}catch (HibernateException e) {
					
			if (tx!=null) tx.rollback();
			e.printStackTrace();
			throw new IllegalStateException(e);
					
		}finally {
			
			
		}
		
		return _jobList;
	}
	
	
	
	static private int activeTimeOut = 90;  		// minutes until an ACTIVE status is considered stale
	static private int tooOldToCareAbout = 15; 		// days past which we don't care
	Date staleDate;
	Date oldDate;

	protected void checkForStaleActive (Session session) {
		
		// calculate stale date
		staleDate = new Date(System.currentTimeMillis() - (activeTimeOut * 60 * 1000) );
		oldDate = new Date(System.currentTimeMillis() - (tooOldToCareAbout * 24 *60 * 60 * 1000) );
		String cfg = null;
		if (parameters.containsKey("configfolder"))
			cfg = parameters.get("configfolder");
		
		StringBuilder jobList = getJobsToCheck();
		
		if (jobList.length() < 2) 
			return;
		
		Transaction tx = null;
		
		try{

			tx = session.beginTransaction();
			
			// look for stale ACTIVE members
			String hql = "FROM JobStepQueue WHERE "
					+ " jobuid IN " + jobList.toString()
					+ " AND stepname = '" + stepName + "'"
					+ " AND status = '" + JobStepQueue.STATUS_ACTIVE + "'" 
					+ " AND updated < :staleDate "
					+ " ORDER By jobUid";
			
			Query query = session.createQuery(hql);     //  don't think this ==> is necessary anynmore since writing getJobsToCheck .setLockOptions(new LockOptions(LockMode.PESSIMISTIC_WRITE));
			query.setParameter("staleDate", staleDate);
			//query.setParameter("dontCareDate", oldDate);
			@SuppressWarnings("unchecked")
			List<JobStepQueue> results = query.list();
			
			log.info("Stale active query resulted in " + results.size() + " entries to check");
			
			int iCmp = 0;  int iRdy = 0;
			
			Long _jobUid = 0L;
			boolean bDone = false;
			for (JobStepQueue _JSQ : results) {
				if (_jobUid < _JSQ.getJobUid()) {
					_jobUid = _JSQ.getJobUid();
					parameters.put("jobuid", Long.toString(_jobUid));
					bigKahuna.parameters = parameters;
	            	reLoadParameters();
	            	if (cfg != null) 
	            		parameters.put("configfolder", cfg);
				}
				
				// set stale entries to complete or ready depending upon whether output was found
				bDone = checkForPriorActivity(_JSQ.getMember_id());
				if(bDone) {
					_JSQ.setStatus(JobStepQueue.STATUS_COMPLETE);
					iCmp++;
				}
				else {
					_JSQ.setStatus(JobStepQueue.STATUS_READY);
					iRdy++;
				}
				
				if (parameters.containsKey("debug")  &&  parameters.get("debug").equalsIgnoreCase("process")) {
					ProcessMessage message = new ProcessMessage();
					message.setJobUid(_jobUid);
					message.setStepName(stepName);
					message.setMember_id(_JSQ.getMember_id());
					message.setStatus(_JSQ.getStatus());
					InetAddress IP = null;
					try {
						IP = InetAddress.getLocalHost();
					} catch (UnknownHostException e) { }
					message.setDescription("IP " + IP.getHostAddress() + " reset status from Active to " + _JSQ.getStatus());
					ProcessMessageHelper.writeMessage(factory, message);
				}
				
			}

 			tx.commit();
 			session.flush();
 			session.clear();
 			
 			log.info("Stale active check set Ready: " + iRdy + " | Complete: " + iCmp);
 						
			
		}catch (HibernateException e) {
			
			if (tx!=null) tx.rollback();
			e.printStackTrace();
			
		}finally {
			
			releaseJobs(jobList.toString());
			
		}
			
		
		
	}
	
	
	/**
	 * get a StringBuilder representing all jobs that might have stale active job queue memebers for the invoking step
	 * set commStatus to Active so that only one thread is checking 
	 * @return
	 */
	StringBuilder getJobsToCheck ()  {

		StringBuilder sb = new StringBuilder();
		
		Transaction tx = null;
		
		try{

			tx = session.beginTransaction();
			
			// look for stale ACTIVE members
			String hql = "FROM ProcessJobStep WHERE "
					+ " stepname = '" + stepName + "'"
					+ " AND status IN ('" + JobStepQueue.STATUS_ACTIVE + "', '" + JobStepQueue.STATUS_COMPLETE + "')" 
					+ " AND updated > :dontCareDate"
					+ " AND (commStatus IS NULL OR commStatus != '" + JobStepQueue.STATUS_ACTIVE + "')";
			
			Query query = session.createQuery(hql).setLockOptions(new LockOptions(LockMode.PESSIMISTIC_WRITE));
			//query.setParameter("staleDate", staleDate);
			query.setParameter("dontCareDate", oldDate);
			@SuppressWarnings("unchecked")
			List<ProcessJobStep> results = query.list();
			
			
			sb.append('(');
			
			for (ProcessJobStep _PJS : results) {
				_PJS.setCommStatus(JobStepQueue.STATUS_ACTIVE);
				session.update(_PJS);
				sb.append("'").append(_PJS.getJobUid()).append("',");
			}
			
			sb.setLength(sb.length()-1);
			sb.append(')');
	
 			tx.commit();
			
		}catch (HibernateException e) {
			
			if (tx!=null) tx.rollback();
			e.printStackTrace();
			
		}finally {

			
		}
		
		return sb;
			
	}
	
	
	/**
	 * when stale active checks are done, change the commStatus back to other-than-active
	 * @param joblist
	 */
	void releaseJobs (String joblist) {

		Transaction tx = null;
		
		try{

			tx = session.beginTransaction();
			
			// look for stale ACTIVE members
			String hql = "FROM ProcessJobStep WHERE "
					+ " stepname = '" + stepName + "'"
					+ " AND jobUid IN " + joblist;
			
			Query query = session.createQuery(hql);
			@SuppressWarnings("unchecked")
			List<ProcessJobStep> results = query.list();
			
			for (ProcessJobStep _PJS : results) {
				_PJS.setCommStatus(JobStepQueue.STATUS_COMPLETE);
				session.update(_PJS);
			}
	
 			tx.commit();
			
		}catch (HibernateException e) {
			
			if (tx!=null) tx.rollback();
			e.printStackTrace();
			
		}finally {

			
		}
	}
	
	
	
	boolean checkForPriorActivity(String member_id) {
		throw new IllegalStateException ("Must override checkForPriorActivity in AbstractDriver");
	}



	void addJob(ProcessJobStep _PJS) {
		boolean bFound = false;
		for (ProcessJobStep _p : jobList) {
			if(_p.equals(_PJS)) {
				bFound = true;
				break;
			}
		}
		if (!bFound)
			jobList.add(_PJS);
	}
	

	void removeJob(ProcessJobStep _PJS) {
		boolean bFound = false;
		for (ProcessJobStep _p : jobList) {
			if(_p.equals(_PJS)) {
				bFound = true;
				break;
			}
		}
		if (!bFound)
			jobList.remove(_PJS);
	}
	
	
	void unLoadParameters() {
		
		log.info("Unloading old Job Parameters");

		Iterator<Entry<String, String>> it = parameters.entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry<String, String> pair = (Map.Entry<String, String>)it.next();
	        log.info(pair.getKey() + " = " + pair.getValue());
	        if (pair.getKey().equalsIgnoreCase("configfolder"))
	        	continue;
	        else if (pair.getKey().equalsIgnoreCase("chunksize"))
	        	continue;
	        else if (pair.getKey().equalsIgnoreCase("stopafter"))
	        	continue;
	        else if (pair.getKey().equalsIgnoreCase("env"))
	        	continue;
	        else if (pair.getKey().equalsIgnoreCase("stepname"))
	        	continue;
	        else if (pair.getKey().equalsIgnoreCase("jobname"))
	        	continue;
	        else
	        	it.remove(); // avoids a ConcurrentModificationException
	    }
	    
	    log.info("Unloading <<=== before / after ===>>");
	    
	    
	    it = parameters.entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry<String, String> pair = (Map.Entry<String, String>)it.next();
	        log.info(pair.getKey() + " = " + pair.getValue());
	    }

		log.info("Completed Job Parameter Unloading");
		
	}
	
	
	List<ProcessJobStep> getUnreported () {
		
		List<ProcessJobStep> _jobList = new ArrayList<ProcessJobStep>();
		
		Transaction tx = null;
		
		try{

			tx = session.beginTransaction();
					
			String hql = "FROM ProcessJobStep WHERE stepName = '" + stepName + "'  AND status = '" + JobStepQueue.STATUS_COMPLETE + "'  AND report IS NULL";
			Query query = session.createQuery(hql).setLockOptions(new LockOptions(LockMode.PESSIMISTIC_WRITE));
			@SuppressWarnings("unchecked")
			List<ProcessJobStep> results = query.list();
		 			
			for (ProcessJobStep _PJS : results) {
				_jobList.add(_PJS);
				Blob blob = new SerialBlob(mX.getBytes());
				_PJS.setReport(blob);
				session.update(_PJS);
			}
			
			tx.commit();
			
			
		}catch (HibernateException e) {
					
			if (tx!=null) tx.rollback();
			e.printStackTrace();
					
		} catch (SerialException e) {
			log.error("Invalid serialization for report placeholder");
			e.printStackTrace();
		} catch (SQLException e) {
			log.error("Invalid string for report placeholder");
			e.printStackTrace();
		}finally {
			
		}
		
		return _jobList;
	}
	
	
	private final static String mX = "Report Generation In Progress";
	
	
	boolean doFailureReport(String sError) {
		
		boolean b = false;
		
		Transaction tx = null;
		
		try{

			tx = session.beginTransaction();
					
			String hql = "FROM ProcessJobStep WHERE stepName = '" + stepName + "'  AND jobUid = '" + parameters.get("jobuid") + "'";
			Query query = session.createQuery(hql);
			@SuppressWarnings("unchecked")
			List<ProcessJobStep> results = query.list();
		 			
			for (ProcessJobStep _PJS : results) {

				_PJS.setStatus(ProcessJobStep.STATUS_FAILED);
				_PJS.setUpdated(new Date());
				Blob blob = new SerialBlob(sError.getBytes()); 
 				_PJS.setReport(blob);
 				_PJS.setUpdated(new Date());
 				session.update(_PJS);
 				
 				UserNotification.sendEmail(_PJS.getStepName() + " failed for job " + _PJS.getJobUid() +
 						(_PJS.getReport() != null ? System.lineSeparator() + sError : "") );
				
			}
			
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
	
		
			
		
		return b;
		
	}
	
	
	void flutter () {
		// trying to get daemon driven machines to stop bunching up a bit
		int r = random.nextInt(59 - 1 + 1) + 1;
		try {
			Thread.sleep(r * 1000);			// flutter seconds * milliseconds to a second
		} catch (InterruptedException e) {
			// no action
		} 
	}
	
	
	

	private static org.apache.log4j.Logger log = Logger.getLogger(AbstractDriver.class);

}
