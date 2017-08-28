


public class AssociationOutputSQL {
	
	private static SessionFactory factory; 
	
	public void doFillAssociationOutput(ArrayList<EpisodeShell> allEpisodes) {
		
		Configuration configuration = new Configuration().configure();
		configuration.addAnnotatedClass(AssociationTbl.class);
		StandardServiceRegistryBuilder builder = new StandardServiceRegistryBuilder().
				applySettings(configuration.getProperties());
		factory = configuration.buildSessionFactory(builder.build());
		//- See more at: http://www.javabeat.net/session-factory-hibernate-4/#sthash.sjPDO9VI.dpuf
		//factory.getClassMetadata(EpisodeShell.class);
		
		//ArrayList<AssociationTbl> socTableInserts = new ArrayList<AssociationTbl>();
		
		for (EpisodeShell es : allEpisodes) {
			
			for (int i=2; i<=5; i++) {
				String si = "" + i;
				if (es.getAssociated_episodes()!=null && es.getAssociated_episodes().get(si)!= null && 
						!es.getAssociated_episodes().get(si).isEmpty()) {
					for (AssociatedEpisode aes : es.getAssociated_episodes().get(si)) {
						
						//row = new String[16];
						AssociationTbl row = new AssociationTbl();
						
						row.setParent_master_episode_id(es.getEpisode_id()+"_"+es.getMember_id()+"_"+es.getClaim().getClaim_id()+"_"+es.getClaim().getClaim_line_id());
						row.setChild_master_episode_id(aes.getAssEpisode().getEpisode_id()+"_"+es.getMember_id()+"_"+aes.getAssEpisode().getClaim().getClaim_id()+"_"+aes.getAssEpisode().getClaim().getClaim_line_id());
						row.setAssociation_type(aes.getAssMetaData().getAss_type());
						row.setAssociation_level(aes.getAssMetaData().getAss_level());
						row.setAssociation_count(aes.getAssEpisode().getAssociationCount());
						row.setAssociation_start_day(aes.getAssMetaData().getAss_start_day());
						row.setAssociation_end_day(aes.getAssMetaData().getAss_end_day());
						
						/*
						row[0] = es.getMember_id();
						row[1] = es.getEpisode_id();
						row[2] = es.getClaim_id();
						row[3] = es.getClaim().getClaim_line_id();
						row[4] = aes.getAssEpisode().getEpisode_id();
						row[5] = aes.getAssEpisode().getClaim().getClaim_id();
						row[6] = aes.getAssEpisode().getClaim().getClaim_line_id();
						row[7] = aes.getAssMetaData().getAss_type();
						row[8] = "" + aes.getAssMetaData().getAss_level();
						row[9] = aes.getAssMetaData().getAss_start_day();
						row[10] = aes.getAssMetaData().getAss_end_day();
						row[11] = "" + aes.getAssEpisode().getAssociationCount();
						row[12] = es.getClaim().getMaster_claim_id();
						row[13] = es.getMaster_episode_id();
						row[14] = aes.getAssEpisode().getClaim().getMaster_claim_id();
						row[15] = aes.getAssEpisode().getMaster_episode_id();
						*/
						addRow(row);
						
					}
				}
			}
			
		}
		
	}
	
	/* Method to CREATE an association in the database */
	static void addRow(Object obj){
		Session session = factory.openSession();
		Transaction tx = null;
		//Integer employeeID = null;
		try{
			tx = session.beginTransaction();
			session.save(obj); 
			tx.commit();
		}catch (HibernateException e) {
			if (tx!=null) tx.rollback();
			e.printStackTrace(); 
		}finally {
			session.close(); 
		}
		//return employeeID;
	}


}
