

//import org.apache.log4j.Logger;




public class JobStepManager {
	
	
	
	private long jobUid;
	private List<Class<?>> cList = new ArrayList<Class<?>>();
	HibernateHelper h;
	SessionFactory factory;
	

	public JobStepManager(long jobUid) {
		
		this.jobUid = jobUid;
		
		cList.add(ProcessJobStep.class);

	}

	
	public void updateStatus (String stepName, String status)  {
		
		h = new HibernateHelper(cList, "ecr", "ecr");
		factory = h.getFactory("ecr", "ecr");
		
		Session session = factory.openSession();
		Transaction tx = null;
		
		try{
			
			tx = session.beginTransaction();
			
			Query query = session.createSQLQuery("update processJobStep set status = :status,"
					+ " updated = :updated"
					+ " where jobuid = :jobuid and stepname = :stepname");
			query.setParameter("status", status);
			query.setParameter("jobuid", jobUid);
			query.setParameter("stepname", stepName);
			query.setParameter("updated", new Date());
			query.executeUpdate();

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
	
	

	public void updateReport (String stepName, String report)  {
		
		h = new HibernateHelper(cList, "ecr", "ecr");
		factory = h.getFactory("ecr", "ecr");
		
		Session session = factory.openSession();
		Transaction tx = null;
		
		try{
			
			tx = session.beginTransaction();
			
			Query query = session.createSQLQuery("update processJobStep set report = :report, "
					+ " updated = :updated "
					+ " where jobuid = :jobuid and stepname = :stepname");
			query.setParameter("report", report);
			query.setParameter("jobuid", jobUid);
			query.setParameter("stepname", stepName);
			query.setParameter("updated", new Date());
			query.executeUpdate();

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
	
	
	public void updateStepStart (String stepName)  {
		
		h = new HibernateHelper(cList, "ecr", "ecr");
		factory = h.getFactory("ecr", "ecr");
		
		Session session = factory.openSession();
		Transaction tx = null;
		
		try{
			
			tx = session.beginTransaction();
			
			Query query = session.createSQLQuery("update processJobStep set stepStart = :now,"
					+ " updated = :updated"
					+ " where jobuid = :jobuid and stepname = :stepname");
			Date now = new Date();
			query.setParameter("now", now);
			query.setParameter("jobuid", jobUid);
			query.setParameter("stepname", stepName);
			query.setParameter("updated", now);
			query.executeUpdate();

			tx.commit();
			
		}catch (HibernateException e) {
			if (tx!=null) tx.rollback();
			e.printStackTrace(); 
		}finally {
			session.close();
		}
		
		
	}
	
	

	public void updateStepEnd (String stepName)  {
		
		h = new HibernateHelper(cList, "ecr", "ecr");
		factory = h.getFactory("ecr", "ecr");
		
		Session session = factory.openSession();
		Transaction tx = null;
		
		try{
			
			tx = session.beginTransaction();
			
			Query query = session.createSQLQuery("update processJobStep set stepEnd = :now,"
					+ " updated = :updated"
					+ " where jobuid = :jobuid and stepname = :stepname");
			Date now = new Date();
			query.setParameter("now", now);
			query.setParameter("jobuid", jobUid);
			query.setParameter("stepname", stepName);
			query.setParameter("updated", now);
			query.executeUpdate();

			tx.commit();
			
		}catch (HibernateException e) {
			if (tx!=null) tx.rollback();
			e.printStackTrace(); 
		}finally {
			session.close();
		}
		
		
	}
	
	


	
	//private static org.apache.log4j.Logger log = Logger.getLogger(JobStepManager.class);
	

}
