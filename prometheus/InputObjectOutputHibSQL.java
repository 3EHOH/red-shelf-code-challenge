




public class InputObjectOutputHibSQL implements InputObjectOutputInterface {
	
	//private GenericOutputInterface oi;
	
	
	HashMap<String, String> parameters;
	
	
	List<Class<?>> cList = new ArrayList<Class<?>>();
	HibernateHelper h;
	SessionFactory factory;
	
    
	public InputObjectOutputHibSQL () {
		this (RunParameters.parameters);
	}
	
	public InputObjectOutputHibSQL (HashMap<String, String> parameters) {
		this.parameters = parameters;
		initialize();
	}
	
	
	
	@Override
	public boolean writeMedicalClaims(Collection<ClaimServLine> itmc) {
		
		//log.info("Medical Claim Data Output Starting");
		if (itmc == null) {
			//log.info("No items to process");
			return true;
		}
		//log.info(itmc.size() + " items to process");
		
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
               for (ClaimServLine svcLine : itmc) { 
					
	       			session.save(svcLine);
	       			
	             	if( iB % HibernateHelper.BATCH_INSERT_SIZE == 0 ) {
	             		tx.commit();
	             		session.flush();
	             		session.clear();
	             		tx = session.beginTransaction();
	                }
	             	/*
	             	if(iB % 100000 == 0) { 
	       				log.info("executing svc line insert " + iB + " ==> " + new Date());
	             	}
	             	*/
	             	
	             	iB++;
       			
       			}
       			
               
               tx.commit();
               
        }catch (HibernateException e) {
        	log.error("claim write failure");
        	log.error(e.getCause());
        	log.error(e.getMessage());
        	log.error(e.getLocalizedMessage());
        	log.error("  ====   ");
        	if (tx!=null) tx.rollback();
        	e.printStackTrace(); 
        	throw new IllegalStateException("SQL save error: " + e.getMessage());
        }finally {
        	session.close();
        }
        
        
        session = factory.openSession();
        tx = null;

        try{
        	
        	tx = session.beginTransaction();
           
        	int iL = 1;
        	for (ClaimServLine svcLine : itmc) { 
				
        		iL++;
        		if( iL % HibernateHelper.BATCH_INSERT_SIZE == 0 ) { 
        			session.flush();
        			session.clear();
        		}
        		/*
        		if( iL % 100000 == 0  &&  iL > 0 ) { 
        			log.info("executing code insert for line " + iL + " ==> " + new Date());
        		}
        		*/
        		
        		if (svcLine.getMed_codes() != null
        			   && (!svcLine.getMed_codes().isEmpty()) ) {
        			
        			//tx = session.beginTransaction();
       				
        			int iC = 1;
        			for (MedCode mc : svcLine.getMed_codes()) {
        				
        				if (mc.getCode_value() == null) 
        					log.error("Found null code value: " + mc.getId());
        				else if (mc.getCode_value().length() > 12) 
        					log.error("Found invalid code value: " + svcLine.getClaim_id() + "|" + mc.getCode_value());
       					
        				mc.setU_c_id(svcLine.getId());
        				session.save(mc);
       					
        				if( iC % HibernateHelper.BATCH_INSERT_SIZE == 0 ) {
        					session.flush();
       						session.clear();
        				}
        				/*
       					if( iC % 100 == 0  &&  iC > 0 ) { 
       						log.info("executing code insert " + iC + " ==> " + new Date());
       					}
       					*/
	                	   
       					iC++;
       					
       				}
        			
        			//tx.commit();
       				
       			}
   			
   			}
        	
        	tx.commit();
				
        	
           
        }catch (HibernateException e) {
        	log.error("code table write failure");
        	log.error(e.getCause());
        	log.error(e.getMessage());
        	log.error(e.getLocalizedMessage());
        	log.error("  ====   ");
        	if (tx!=null) tx.rollback();
        	e.printStackTrace(); 
        	throw new IllegalStateException("SQL save error: " + e.getMessage());
        }finally {
        	session.close();
        }
        
		
		
		//log.info("Medical Claim Data Output Completed");
		return true;
		
	}
	
	

	@Override
	public boolean writeRxClaims(Collection<ClaimRx> itrx) {
		
		//log.info("Rx Claim Data Output Starting");
		
		
		/* first submit codes and get the id... */
		
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
               for (ClaimRx rxLine : itrx) { 
            	   
            	   session.save(rxLine);
            	   

            	   if( iB % HibernateHelper.BATCH_INSERT_SIZE == 0 ) {
            		   session.flush();
 	                   session.clear();
            	   }
            	   /*
            	   if(iB % 100000 == 0) { 
            		   log.info("executing Rx insert " + iB + " ==> " + new Date());
            	   }
            	   */
 	             	
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
		
		/* then submit the codes now that we have the ids... */
        session = factory.openSession();
        tx = null;

        try{
        	
        	tx = session.beginTransaction();
               
        	int iL = 1;
        	for (ClaimRx rxLine : itrx) { 
            	   
        		iL++;
        		if( iL % HibernateHelper.BATCH_INSERT_SIZE == 0 ) { 
        			session.flush();
           			session.clear();
           		}
        		/*
           		if( iL % 100000 == 0  &&  iL > 0 ) { 
           			log.info("executing code insert for line " + iL + " ==> " + new Date());
           		}
           		*/

           		if (rxLine.getMed_codes() != null 
           				&&  ( ! rxLine.getMed_codes().isEmpty() ) ) {
           			
           			//tx = session.beginTransaction();
	
           			int iC = 1;
           			for (MedCode mc : rxLine.getMed_codes()) {
           				mc.setU_c_id(rxLine.getId());
           				session.save(mc);
	       					
           				if( iC % HibernateHelper.BATCH_INSERT_SIZE == 0 ) {
           					session.flush();
           					session.clear();
           				}
           				/*
           				if( iC % 1000 == 0  &&  iC > 0 ) { 
           					log.info("executing code insert " + iC + " ==> " + new Date());
           				}
           				*/
	                	   
           				iC++;
	                	   
           			}
           			
           			//tx.commit();
	       				
           		}
	       			
        	}
               
        	tx.commit();    
        	
        }catch (HibernateException e) {
        	log.error("code table (for rx) write failure");
        	log.error(e.getCause());
        	log.error(e.getMessage());
        	log.error(e.getLocalizedMessage());
        	log.error("  ====   ");
        	if (tx!=null) tx.rollback();
        	e.printStackTrace();
        	throw new IllegalStateException("SQL save error: " + e.getMessage());
        }finally {
        	session.close();
        }
		
		
		//log.info("Rx Claim Data Output Completed");
		
		
		return true;
	}

	@Override
	public boolean writeMembers(Collection<PlanMember> itmb) {

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
               for (PlanMember member : itmb) { 
            	   
            	   session.save(member);
            	   
            	   if( iB % HibernateHelper.BATCH_INSERT_SIZE == 0 ) {
            		   session.flush();
 	                   session.clear();
            	   }
            	   /*
            	   if(iB % 100000 == 0) { 
            		   log.info("executing member insert " + iB + " ==> " + new Date());
            	   }
            	   */
 	             	
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

	@Override
	public boolean writeEnrollments(Collection<List<Enrollment>> itme) {

		//log.info("Enrollment Data Output Starting");
		
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
               for (List<Enrollment> enrollments : itme) { 
            	   for (Enrollment enrollment : enrollments) {
       					
            		   session.save(enrollment);
            		   
            		   if( iB % HibernateHelper.BATCH_INSERT_SIZE == 0 ) {
            			   session.flush();
     	                   session.clear();
            		   }
            		   /*
            		   if(iB % 100000 == 0) { 
            			   log.info("executing enrollment insert " + iB + " ==> " + new Date());
            		   }
            		   */
            		   
            		   iB++;
            		   
            	   }	
            	   
               }
               
               tx.commit();
               
        }catch (HibernateException e) {
               if (tx!=null) tx.rollback();
               e.printStackTrace(); 
               throw new IllegalStateException("SQL save error: " + e.getMessage());
        }finally {
        	session.close();
        }

		
		//log.info("Enrollment Data Output Completed");

		return true;
	}
	

	@Override
	public boolean writeProviders(Collection<Provider> itpv) {

		//log.info("Provider Data Output Starting");
		
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
               for (Provider provider : itpv) { 

            	   session.save(provider); 

             	   if( iB % HibernateHelper.BATCH_INSERT_SIZE == 0 ) {
             		   session.flush();
  	                   session.clear();
             	   }
             	   /*
             	   if(iB % 100000 == 0) { 
             		   log.info("executing provider insert " + iB + " ==> " + new Date());
             	   }
             	   */
  	             	
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
		
        session = factory.openSession();
        tx = null;

        try{
        	
        	tx = session.beginTransaction();
        	
        	ProviderSpecialty ps = null;
        	int iL = 1;
        	for (Provider provider : itpv) { 
				
        		iL++;
        		if( iL % HibernateHelper.BATCH_INSERT_SIZE == 0 ) { 
        			session.flush();
        			session.clear();
        		}
        		
        		if (provider.getSpecialty() != null
        			   && (!provider.getSpecialty().isEmpty()) ) {
        			
        			//tx = session.beginTransaction();
       				
        			int iC = 1;
        			for (String _c : provider.getSpecialty()) {
        				
        				if (_c  == null)  {
        					log.error("Found null specialty code value in provider: " + provider.getProvider_id());
        					continue;
        				}
        				else if (_c.length() > 20) {
        					log.error("Found invalid specialty code value: " + provider.getProvider_id() + "|" + _c);
        					continue;
        				}
        				if (_c.trim().isEmpty())
        					continue;
        				
       					ps = new ProviderSpecialty();
        				ps.setProvider_id(provider.getId());		// backlink to provider table
        				ps.setSpecialty_id(_c);
        				ps.setCode_source("cms");
        				session.save(ps);
       					
        				if( iC % HibernateHelper.BATCH_INSERT_SIZE == 0 ) {
        					session.flush();
       						session.clear();
        				}
       					
       				}
        			
        			for (String _c : provider.getProvider_taxonomy_codes()) {
        				
        				if (_c  == null)  {
        					log.error("Found null taxonomy code value in provider: " + provider.getProvider_id());
        					continue;
        				}
        				else if (_c.length() > 20) {
        					log.error("Found invalid taxonomy code value: " + provider.getProvider_id() + "|" + _c);
        					continue;
        				}
        				if (_c.trim().isEmpty())
        					continue;
        				
       					ps = new ProviderSpecialty();
        				ps.setProvider_id(provider.getId());		// backlink to provider table
        				ps.setSpecialty_id(_c);
        				ps.setCode_source("nucc");
        				session.save(ps);
       					
        				if( iC % HibernateHelper.BATCH_INSERT_SIZE == 0 ) {
        					session.flush();
       						session.clear();
        				}
       					
       				}
        			
        			//tx.commit();
       				
       			}
   			
   			}
        	
        	tx.commit();
           
        }catch (HibernateException e) {
        	log.error("provider specialty table write failure");
        	log.error(e.getCause());
        	log.error(e.getMessage());
        	log.error(e.getLocalizedMessage());
        	log.error("  ====   ");
        	if (tx!=null) tx.rollback();
        	e.printStackTrace(); 
        	throw new IllegalStateException("SQL save error: " + e.getMessage());
        }finally {
        	session.close();
        }
        

		//log.info("Provider Data Output Completed");

		return true;
	}
	
	
	
	private void initialize () {

		env = parameters.get("env") == null ?  "prd" :  parameters.get("env");
		schemaName = parameters.get("jobname")  == null ?  "javatest" : parameters.get("jobname");
		
		// add all annotated classes to the class list
		cList.add(ClaimServLine.class);
		cList.add(ClaimRx.class);
		cList.add(MedCode.class);
		cList.add(PlanMember.class);
        cList.add(Enrollment.class);
        cList.add(Provider.class);
        cList.add(ProviderSpecialty.class);

        h = new HibernateHelper(cList, env, schemaName);
        factory = h.getFactory(env, schemaName);
        

	}
	
	
	
	String env, schemaName;
	
	private static org.apache.log4j.Logger log = Logger.getLogger(InputObjectOutputHibSQL.class);

}
