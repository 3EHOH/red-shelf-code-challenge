



public class AssignmentOutputSQL {
	
//private static SessionFactory factory; 
	
	//private GenericOutputInterface oi;
	
	public void doFillAssignmentOutput(ArrayList<EpisodeShell> allEpisodes) {
		
		//oi = new GenericOutputHibernate();
		
		
		
		List<Class<?>> cList = new ArrayList<Class<?>>();
        cList.add(AssignmentTbl.class);

        
        String env = RunParameters.parameters.get("env") == null ?  "prd" :  RunParameters.parameters.get("env");
        String schemaName = RunParameters.parameters.get("jobname")  == null ?  "javatest" : RunParameters.parameters.get("jobname");
        HibernateHelper h = new HibernateHelper(cList, env, schemaName);
        SessionFactory factory = h.getFactory(env, schemaName);

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
               
               
               for(EpisodeShell e : allEpisodes) {
       			
	       			if (e.getAssigned_claims().size() > 0) {
	       				int iB = 0;
	       				for (AssignedClaim as : e.getAssigned_claims()) {
	       					//addRow(buildRow(e, as));
	       					//oi.write(buildRow(e, as));
	       					session.save(buildRow(e, as));
	       					if( iB % HibernateHelper.BATCH_INSERT_SIZE == 0 ) {
	                  		   session.flush();
	       	                   session.clear();
	       					}
	       					iB++;
	       				}
	       			}
	       		}
               
               tx.commit();
        }catch (HibernateException e) {
               if (tx!=null) tx.rollback();
               e.printStackTrace(); 
        }finally {
               session.close(); 
               /*
               try {
                      h.close(env, schemaName);
               } catch (Exception e) {
                      e.printStackTrace();
               }
               */
        }
		
		
		
		
		
		
		
		//oi.close();
		
	}
	
	
	private AssignmentTbl buildRow (EpisodeShell e, AssignedClaim as) {
		
		
		AssignmentTbl row = new AssignmentTbl();
		
		row.setMember_id(e.getMember_id());
		//master claim id below...
		row.setMaster_episode_id(e.getEpisode_id() + "_" + e.getMember_id() + "_" + e.getClaim().getClaim_id() + "_" + e.getClaim().getClaim_line_id());
		row.setClaim_source(as.getType());
		row.setAssigned_type(as.getCategory());
		//assigned count below...
		row.setRule(as.getRule());
			
		if (as.getType().equals("RX")) {
			//row[1] = as.getRxClaim().getClaim_id();
			//row[2] = "1";
			//row[4] = "" + as.getRxClaim().getAssignedCount();
			//row[8] = "" + as.getRxClaim().getAllowed_amt();
			//row[11] = as.getRxClaim().getClaim_id();
			
			row.setMaster_claim_id(e.getMember_id() + "_" + as.getRxClaim().getClaim_id() + "_1");
			row.setAssigned_count(as.getRxClaim().getAssignedCount());
			row.setClaim_source("RX");
				
		} else {
			//row[1] = as.getClaim().getClaim_id();
			//row[2] = as.getClaim().getClaim_line_id();
			//row[4] = "" + as.getClaim().getAssignedCount();
			//row[8] = "" + as.getClaim().getAllowed_amt();
			//row[11] = as.getClaim().getMaster_claim_id();
			
			row.setMaster_claim_id(e.getMember_id() + "_" + as.getClaim().getClaim_id() + "_" + as.getClaim().getClaim_line_id());
			row.setAssigned_count(as.getClaim().getAssignedCount());
			row.setClaim_source(as.getClaim().getClaim_line_type_code());
			
		}
		/*	
		row[3] = "true";
			
		row[5] = e.getEpisode_id();
		row[6] = e.getClaim_id();
		row[7] = e.getClaim().getClaim_line_id();
			
		row[9] = as.getRule();
		row[10] = as.getCategory();
		
		row[12] = e.getClaim().getMaster_claim_id();
		row[13] = e.getMaster_episode_id();
		
		*/
		
		return row;
		
	}

}
