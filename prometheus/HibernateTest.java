



public class HibernateTest {
	
	
	
	private static SessionFactory factory; 
	
	public static void main(String[] args) {
		   
		Configuration configuration = new Configuration().configure();
		configuration.addAnnotatedClass(EpisodeShell.class);
		StandardServiceRegistryBuilder builder = new StandardServiceRegistryBuilder().
				applySettings(configuration.getProperties());
		factory = configuration.buildSessionFactory(builder.build());
		//- See more at: http://www.javabeat.net/session-factory-hibernate-4/#sthash.sjPDO9VI.dpuf
		//factory.getClassMetadata(EpisodeShell.class);
		
		
		
		EpisodeShell e = new EpisodeShell();
		e.setClaim_id("Claim001");
		e.setEpisode_id("Yahoo");
		e.setMember_id("Me");
		e.setMaster_episode_id("Mast001");
		e.setEpisode_type("T");
		e.setEpisode_begin_date(new Date());
		e.setEpisode_end_date(new Date());
		e.setTrig_begin_date(new Date());
		e.setTrig_end_date(new Date());
		
		addEpisode(e);
		
	}
	
	/* Method to CREATE an employee in the database */
	static Integer addEpisode(EpisodeShell episode){
		Session session = factory.openSession();
		Transaction tx = null;
		Integer employeeID = null;
		try{
			tx = session.beginTransaction();
			session.save(episode); 
			tx.commit();
		}catch (HibernateException e) {
			if (tx!=null) tx.rollback();
			e.printStackTrace(); 
		}finally {
			session.close(); 
		}
		return employeeID;
	}


}
