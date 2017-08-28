




public class DataRunTableHelper {
	
	
	
	/**
	 * pass values from parameters to the data 'run' table
	 */
	static synchronized public void updateRunTable (HashMap<String, String> parameters) {
		
		// initialize data connections
		
		String env = parameters.get("env") == null ?  "prd" :  parameters.get("env");
		String schemaName = parameters.get("jobname")  == null ?  "javatest" : parameters.get("jobname");
		
		// initialize Hibernate and get someone to process
		List<Class<?>> cList = new ArrayList<Class<?>>();
		cList.add(DataRun.class);
				
		HibernateHelper h = new HibernateHelper(cList, env, schemaName);
		SessionFactory factory = h.getFactory(env, schemaName);
 		
				
		Session session = factory.openSession();
		session.setCacheMode(CacheMode.IGNORE);
		session.setFlushMode(FlushMode.COMMIT);
		Transaction tx = null;
				
		try{

			tx = session.beginTransaction();
		            
			DataRun dr;
			
	        Query q = session.createQuery("DELETE FROM DataRun");
	        q.executeUpdate();
	        
	        dr = new DataRun();
	        dr.setJobUid(Long.parseLong(parameters.get("jobuid")));
	        dr.setData_start_date(format.parse(parameters.get("studybegin")));
	        dr.setData_end_date(format.parse(parameters.get("studyend")));
	        dr.setData_latest_begin_date(format.parse(parameters.get("studyend")));
	        dr.setMetadata_version(parameters.get("metadata"));
	        dr.setRun_date(new Date());
	        dr.setRun_name(parameters.get("jobname"));
	        dr.setSubmitted_date(format.parse(parameters.get("rundate")));
	        
	        session.save(dr);
		            
	        tx.commit();
		 						
					
		}catch (HibernateException | ParseException e) {
					
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
	
	
	private static final DateFormat format = new SimpleDateFormat("yyyyMMdd");
	
	


}
