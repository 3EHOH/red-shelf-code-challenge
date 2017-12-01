



public class JobRetrieval {
	
	private HibernateHelper h;
	private SessionFactory factory;
	private HashMap<String, String> parameters;
	
	private ProcessJob _PJ;


	public JobRetrieval (HashMap<String, String> parameters)  {
		
		this.parameters = parameters;
		
		List<Class<?>> cList = new ArrayList<Class<?>>();
		cList.add(ProcessJob.class);
		cList.add(ProcessJobParameter.class);
		
		
		h = new HibernateHelper(cList, "ecr", "ecr");
		factory = h.getFactory("ecr", "ecr");
		
		if (parameters.get("jobuid") != null) 
			getJobByUid();
		else
			getJobByClientAndJobname();
		
		new JobParameter(Long.parseLong(parameters.get("jobuid"))).getAllParameters(parameters);
		
		/*
		try {
			h.close("prd", "ecr");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		*/
		
	
	}
	
	
	/**
	 * precise get of job when uid is known
	 */
	private void getJobByUid () {
		Session session = factory.openSession();
		Transaction tx = null;
		try {
			tx = session.beginTransaction();
			_PJ = (ProcessJob)session.get(ProcessJob.class, Long.parseLong(parameters.get("jobuid")));
			tx.commit();
		} catch (HibernateException e) {
			if (tx!=null) tx.rollback();
			e.printStackTrace(); 
		} finally {
			session.close(); 
			
			/*
			try {
				h.close("prd", "ecr");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			*/
		}
	}
	
	
	/**
	 * when uid is not known, grab the latest uid for this client and jobname
	 */
	private void getJobByClientAndJobname ()  {
		
		Session session = factory.openSession();
		Transaction tx = null;
		try {
			
			tx = session.beginTransaction();
			
			Query query = session.createQuery("from ProcessJob where client_id = :client_id and jobname = :jobname ORDER BY uid DESC"); 
			query.setParameter("client_id", parameters.get("client")); 
			query.setParameter("jobname", parameters.get("jobname"));

			List<?> list = query.list();
			_PJ = (ProcessJob)list.get(0);
			
			parameters.put("jobuid", Long.toString(_PJ.getUid()) );
			
			tx.commit();
			
		} catch (HibernateException e) {
			if (tx!=null) tx.rollback();
			e.printStackTrace(); 
		} finally {
			session.close(); 
			try {
				h.close("prd", "ecr");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
	
	
	
	public ProcessJob get_PJ() {
		return _PJ;
	}

	
	
	//private static org.apache.log4j.Logger log = Logger.getLogger(JobRetrieval.class);
	
	

}
