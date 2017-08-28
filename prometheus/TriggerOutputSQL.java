



public class TriggerOutputSQL {
	
	//private static SessionFactory factory; 
	
	public void doFillTriggerOutput(ArrayList<EpisodeShell> allEpisodes) {
		
		
		
		
		
		
		List<Class<?>> cList = new ArrayList<Class<?>>();
        cList.add(TriggerTbl.class);
        cList.add(EpisodeTbl.class);

        env = RunParameters.parameters.get("env") == null ?  "prd" :  RunParameters.parameters.get("env");
        schemaName = RunParameters.parameters.get("jobname")  == null ?  "javatest" : RunParameters.parameters.get("jobname");
        HibernateHelper h = new HibernateHelper(cList, env, schemaName);
        SessionFactory factory = h.getFactory(env, schemaName);

        Session session = factory.openSession();
        session.setCacheMode(CacheMode.IGNORE);
        session.setFlushMode(FlushMode.COMMIT);
        Transaction tx = null;

        try{
               tx = session.beginTransaction();
               
               int iB = 0;
               
               for (EpisodeShell es : allEpisodes) {
       			
       			String mClaimID = es.getMember_id() + "_" + es.getClaim_id() + "_" + es.getClaim().getClaim_line_id();
       			String mEpID = es.getEpisode_id() + "_" + mClaimID;
       			int recConf = 0;
       			if (es.isReq_conf_claim()) recConf = 1;
       			int dropped = 0;
       			if (es.isDropped()) dropped = 1;
       			int truncated = 0;
       			if (es.isTruncated()) truncated = 1;
       			
       			
       			//String conf_claim_id="";
       			String conf_claim_line_id = "";
       			//if (es.getConf_claim()!=null) {
       			//	conf_claim_id=es.getConf_claim().getClaim_id();
       			//	conf_claim_id=es.getConf_claim().getClaim_line_id();
       			//}
       			
       			
       			//-- TRIGGER TABLE ROWS --//
       			TriggerTbl t = new TriggerTbl();
       			
       			
       			t.setMember_id(es.getMember_id());
       			t.setClaim_id(es.getClaim_id());
       			t.setClaim_line_id(es.getClaim().getClaim_line_id());
       			t.setMaster_episode_id(mEpID);
       			t.setMaster_claim_id(mClaimID);
       			t.setEpisode_id(es.getEpisode_id());
       			t.setEpisode_type(es.getEpisode_type());
       			t.setTrig_begin_date(es.getTrig_begin_date());
       			t.setTrig_end_date(es.getTrig_end_date());
       			t.setEpisode_begin_date(es.getEpisode_begin_date());
       			t.setEpisode_end_date(es.getEpisode_end_date());
       			t.setOrig_episode_begin_date(es.getOrig_episode_begin_date());
       			t.setOrig_episode_end_date(es.getEpisode_end_date());
       			t.setLook_back("" + es.getLook_back());
       			t.setLook_ahead(es.getLook_ahead());
       			t.setReq_conf_claim(recConf);
       			t.setConf_claim_id(es.getConf_claim_id());
       			t.setConf_claim_line_id(conf_claim_line_id);
       			t.setMin_code_separation(es.getMin_code_separation());
       			t.setMax_code_separation(es.getMax_code_separation());
       			t.setTrig_by_episode_id(es.getTrig_by_episode_id());
       			t.setTrig_by_master_episode_id(es.getTrig_by_master_episode_id()); // !!! INCORRECT !!! //
       			t.setDx_pass_code(es.getDxPassCode());
       			t.setPx_pass_code(es.getPxPassCode());
       			t.setEm_pass_code(es.getEmPassCode());
       			t.setConf_dx_pass_code(es.getConf_dxPassCode());
       			t.setConf_px_pass_code(es.getConf_pxPassCode());
       			t.setConf_em_pass_code(es.getConf_emPassCode());
       			t.setDropped(dropped);
       			t.setTruncated(truncated);
       			t.setWin_claim_id(es.getWin_claim_id());
       			t.setWin_master_claim_id(es.getWin_master_episode_id());  // !!! INCORRECT !!! //
       			
       			session.save(t); 
       			if( iB % HibernateHelper.BATCH_INSERT_SIZE == 0 ) {
            		   session.flush();
 	                   session.clear();
       			}
       			iB++;

       			
       			//-- EPISODE TABLE ROWS --//
       			if (dropped==0) {
       				
       				EpisodeTbl e = new EpisodeTbl();
       				
       				Calendar start = Calendar.getInstance();
       				Calendar end = Calendar.getInstance();
       				start.setTime(es.getEpisode_begin_date());
       				end.setTime(es.getEpisode_end_date());
       				Date startDate = start.getTime();
       				Date endDate = end.getTime();
       				long startTime = startDate.getTime();
       				long endTime = endDate.getTime();
       				long diffTime = endTime - startTime;
       				int diffDays = (int) Math.round(diffTime / (1000 * 60 * 60 * 24));
       				//DateFormat dateFormat = DateFormat.getDateInstance();
       				//System.out.println("The difference between "+
       				//  dateFormat.format(startDate)+" and "+
       				//  dateFormat.format(endDate)+" is "+
       				//  diffDays+" days.");
       				
       				int associated=0;
       				if (es.isAssociated()) associated=1; 
       				
       				int associatedCount=0;
       				if (es.getAssociationCount()>0) associatedCount=es.getAssociationCount();
       				
       				int associatedLevel=0;
       				if (es.getAssociatedLevel()>0) associatedLevel=es.getAssociatedLevel();
       				
       				e.setMember_id(es.getMember_id());
       				e.setClaim_id(es.getClaim_id());
       				e.setClaim_line_id(es.getClaim().getClaim_line_id());
       				e.setMaster_episode_id(mEpID);
       				e.setMaster_claim_id(mClaimID);
       				e.setEpisode_id(es.getEpisode_id());
       				e.setEpisode_type(es.getEpisode_type());
       				e.setEpisode_begin_date(es.getEpisode_begin_date());
       				e.setEpisode_end_date(es.getEpisode_end_date());
       				e.setEpisode_length_days(diffDays);
       				e.setTrig_begin_date(es.getTrig_begin_date());
       				e.setTrig_end_date(es.getTrig_end_date());
       				e.setAssociated(associated);
       				e.setAssociation_count(associatedCount);
       				e.setAssociation_level(associatedLevel);
       				e.setTruncated(truncated);
       				/*
       				e.setAttrib_cost_physician(es.getAttr_cost_physician_id());
       				e.setAttrib_cost_facility(es.getAttr_cost_facility_id());
       				e.setAttrib_visit_physician(es.getAttr_visit_physician_id());
       				e.setAttrib_visit_facility(es.getAttr_visit_facility_id());
       				
       				if (es.getAttr_cost_physician_id()==null || es.getAttr_cost_physician_id().equals("")) {
       					if (es.getAttr_visit_physician_id()!=null) e.setAttrib_default_physician(es.getAttr_visit_physician_id());
       				} else {
       					e.setAttrib_default_physician(es.getAttr_cost_physician_id());
       				}
       				
       				if (es.getAttr_cost_facility_id()==null || es.getAttr_cost_facility_id().equals("")) {
       					if (es.getAttr_visit_facility_id()!=null) e.setAttrib_default_facility(es.getAttr_visit_facility_id());
       				} else {
       					e.setAttrib_default_facility(es.getAttr_cost_facility_id());
       				}
       				*/
       				

       				session.save(e); 
       				
       				
       				
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
		
		
		//session.close();
		
	}
	
	
	String env;
    String schemaName;
	
	
	/* Method to CREATE an employee in the database */
	static void addRow(Object obj, Session session){
		//Session session = factory.openSession();
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
			//session.close(); 
		}
		//return employeeID;
	}

}
