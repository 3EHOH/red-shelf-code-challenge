



public class JobParameter {
	
		
	
	private long jobUid;
	private List<Class<?>> cList = new ArrayList<Class<?>>();
	HibernateHelper h;
	SessionFactory factory;
	

	public JobParameter(long jobUid) {
		
		this.jobUid = jobUid;
		
		cList.add(ProcessJobParameter.class);
		
		h = new HibernateHelper(cList, "ecr", "ecr");
		factory = h.getFactory("ecr", "ecr");
		
	}

	
	public void saveAllParameters (HashMap<String, String> parameters)  {
		
		
		Session session = factory.openSession();
		Transaction tx = null;
		
		try{
			
			tx = session.beginTransaction();
			
			Query query = session.createQuery("delete ProcessJobParameter where jobUid = :jobUid");
			query.setParameter("jobUid", jobUid);
			 
			int result = query.executeUpdate();
			if (result > 0) {
			    log.info("Job parameters reset for job" + jobUid);
			}

			ProcessJobParameter _Parm;
 			for (Entry<String, String> entry : parameters.entrySet()) {
 				_Parm= new ProcessJobParameter();
			    _Parm.setP_key(entry.getKey());
			    _Parm.setP_value(entry.getValue());
			    _Parm.setJobUid(jobUid);
			    session.save(_Parm);
			}
			
			tx.commit();
			
		}catch (HibernateException e) {
			if (tx!=null) tx.rollback();
			e.printStackTrace(); 
		}finally {
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
	
	

	public void saveOneParameter (String key, String value)  {
		
		
		Session session = factory.openSession();
		Transaction tx = null;
		
		try{
			
			tx = session.beginTransaction();
			
			ProcessJobParameter _Parm = new ProcessJobParameter();;
			_Parm.setP_key(key);
			_Parm.setP_value(value);
			_Parm.setJobUid(jobUid);
			    session.save(_Parm);

			
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
	
	
	
	public void getAllParameters(HashMap<String, String> parameters) {
		
		log.info("Using jobUid: " + parameters.get("jobuid"));
		
		Session session = factory.openSession();
		Transaction tx = null;
		try {
			
			tx = session.beginTransaction();
			
			Query query = session.createQuery("from ProcessJobParameter where jobuid = :uid "); 
			long _uid = Long.parseLong(parameters.get("jobuid"));
			query.setParameter("uid", _uid); 

			@SuppressWarnings("unchecked")
			List<ProcessJobParameter> list = query.list();
			
			for (ProcessJobParameter parm : list) {
				parameters.put(parm.getP_key(), parm.getP_value());
			}
			
			tx.commit();
			
		} catch (HibernateException e) {
			if (tx!=null) tx.rollback();
			e.printStackTrace(); 
		} finally {
			session.close(); 
		}
		
		
		/*
		try {
			h.close("prd", "ecr");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		*/
		
		
	}
	

	
	private static org.apache.log4j.Logger log = Logger.getLogger(JobParameter.class);
	

}
