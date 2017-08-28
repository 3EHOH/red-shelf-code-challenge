



public class JobStepHelper {
	
	
	public static void failStep (SessionFactory factory, ProcessJobStep _PJS) {
		
		Session sessionX = factory.openSession();
		Transaction txx = null;
		try{
			txx = sessionX.beginTransaction();
			_PJS.setStatus(ProcessJobStep.STATUS_FAILED);
			_PJS.setUpdated(new Date());
			sessionX.update(_PJS);
			//sessionX.flush();
			txx.commit();
		} catch (HibernateException eH) {
			
			if (txx!=null) txx.rollback();
			eH.printStackTrace();
			
		}finally {
			
			sessionX.close(); 
		
		}
		
	}

}
