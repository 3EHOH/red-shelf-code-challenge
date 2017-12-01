



public class ProcessMessageHelper {
	
	
	
	
	
	static synchronized public void writeMessage (SessionFactory factory, ProcessMessage message) {
		
		Session session = factory.openSession();
		session.setCacheMode(CacheMode.IGNORE);
        session.setFlushMode(FlushMode.COMMIT);
		Transaction tx = null;
		
		try{

			tx = session.beginTransaction();
	        
			message.setUpdated(new Date());
	        session.save(message);
            
            tx.commit();
 						
			
		}catch (HibernateException e) {
			
			if (tx!=null) tx.rollback();
			e.printStackTrace();
			
		}finally {
			
			// session.close(); 
			
		}
		
	}
	

}
