


//import construction.model.PlanMemberInterface;
//import construction.model.generic.PlanMemberGeneric;
//import construction.model.vertica.PlanMemberVertica;



public class AutoIncrementTest {
	
	List<Class<?>> cList;
	HibernateHelper h;
	SessionFactory factory;
	Session session;
	
	//Collection<PlanMemberInterface> playList = new ArrayList<PlanMemberInterface>();
	Collection<PlanMember> playList = new ArrayList<PlanMember>();
	
	

	private String env = "ecr";
	private String runtype = "write";
	
	
	
	public AutoIncrementTest(HashMap<String, String> parameters) {
		initialize();
		if (runtype.equals("write")) {
			testPopulation();
			writeMembers(playList);
		} else
			readMembers();
	}
	
	
	public void readMembers( ){
		
		String pms;
		/*
		if (env.equals("prd"))
			pms = "PlanMemberVertica";
		else
			pms = "PlanMemberGeneric";
		*/
		pms = "PlanMember";
		
		Session session = factory.openSession();
		Transaction tx = null;
		try{
			tx = session.beginTransaction();
			@SuppressWarnings("unchecked")
			/*
			List<PlanMemberInterface> mbrs = session.createQuery("FROM " + pms).list(); 
			for (Iterator<PlanMemberInterface> iterator =  mbrs.iterator(); 
					iterator.hasNext();
					)
			{
				PlanMemberInterface p = (PlanMemberInterface) iterator.next(); 
				log.info(p + " : " + p.getId());
			}
			*/
			List<PlanMember> mbrs = session.createQuery("FROM " + pms).list(); 
			for (Iterator<PlanMember> iterator =  mbrs.iterator(); 
					iterator.hasNext();
					)
			{
				PlanMember p = (PlanMember) iterator.next(); 
				log.info(p + " : " + p.getId());
			}
			tx.commit();
		}catch (HibernateException e) {
			if (tx!=null) tx.rollback();
			e.printStackTrace(); 
		}finally {
			session.close(); 
		}
	}

	

	//private boolean writeMembers(Collection<PlanMemberInterface> itmb) {
	private boolean writeMembers(Collection<PlanMember> itmb) {

		//log.info("Member Data Output Starting");
		
		Session session = factory.openSession();
        session.setCacheMode(CacheMode.IGNORE);
        session.setFlushMode(FlushMode.COMMIT);
        
        Transaction tx = null;

        try{
               tx = session.beginTransaction();
               /* LOOP HERE, EXAMPLE BELOW
               for (InputMonitor m : monitors) {
                      m.setJobUid(Long.parseLong(parameters.get("jobuid")));
                      session.save(m); 
               }
               */
               
               int iB = 1;
               /*
               for (PlanMemberInterface member : itmb) { 
            	   
            	   log.info(">>>>>>>Save start");
            	   session.save(member);
            	   log.info(">>>>>>>Save done");
            	   
            	   if( iB % HibernateHelper.BATCH_INSERT_SIZE == 0 ) {
            		   session.flush();
 	                   session.clear();
            	   }
            	   iB++;
               }
               */
               for (PlanMember member : itmb) { 
            	   
            	   log.info(">>>>>>>Save start");
            	   session.save(member);
            	   log.info(">>>>>>>Save done");
            	   
            	   if( iB % HibernateHelper.BATCH_INSERT_SIZE == 0 ) {
            		   session.flush();
 	                   session.clear();
            	   }
            	   iB++;
               }
               
               tx.commit();
               
        }catch (HibernateException e) {
               if (tx!=null) tx.rollback();
               e.printStackTrace(); 
               throw new IllegalStateException("SQL save error: " + e.getMessage());
        }finally {
               session.close(); 
        }
				
		
		//log.info("Member Data Output Completed");
		
		return true;
	}
	
	

	private void testPopulation() {
		
		for (int i = 1001; i < 1010; i++) {
			
			//PlanMemberInterface p;
			PlanMember p = new PlanMember();
			/*
			PlanMember p;
			if (env.equals("prd"))
				p = new PlanMemberVertica();
			else
				p = new PlanMemberGeneric();
				*/
			//p.setId(i);
			p.setMember_id(Integer.toString(i));
			
			p.setBirth_year(1000 + i);
			p.setGender_code( i % 2 == 0 ? "M" : "F");
			
			playList.add(p);
			
		}
		
	}



	private void initialize ()  {
		
		// initialize Hibernate
		cList = new ArrayList<Class<?>>();
		
		/*
		if (env.equals("prd"))
			cList.add(PlanMemberVertica.class);
		else
			cList.add(PlanMemberGeneric.class);
			*/
		
		cList.add(PlanMember.class);
						
		//h = new HibernateHelper(cList, env, "Warren_VerticaBench20161114");	
		//factory = h.getFactory(env, "Warren_VerticaBench20161114");
		h = new HibernateHelper(cList, env, "Warren_MySQLBench20161102");	
		factory = h.getFactory(env, "Warren_MySQLBench20161102");
		session = factory.openSession();
		

		//session = factory.withOptions()
        //        .interceptor(new work.util.MyInterceptor())
        //        .openSession(); 
		
		log.info("Session open");
	}
	
	
	public static void main(String[] args) throws InterruptedException {
		
		RunParameters rp = new RunParameters();
		rp.loadParameters(args, parameterDefaults);
		
		/*MappingDriver instance = */ new AutoIncrementTest(RunParameters.parameters);
		
	        
	}
	
	static String [][] parameterDefaults = {
		{"configfolder", "C:\\workspace\\ECR_Analytics\\trunk\\EpisodeConstruction\\src\\"}
	};

	private static org.apache.log4j.Logger log = Logger.getLogger(AutoIncrementTest.class);
	
}
