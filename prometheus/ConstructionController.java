



public class ConstructionController extends AbstractController {
	

	HibernateHelper h;
	SessionFactory factory;
	Session session;
	
	
	HibernateHelper hC;
	SessionFactory factoryC;
	List<Class<?>> cListC;
	
	

	ConstructionController (HashMap<String, String> parameters) {
		
		this.parameters = parameters;
		initialize();
		queueStartAndLock(ProcessJobStep.STEP_CONSTRUCT);
		process();
		
	}
	
	
	private void process ()  {
		
		EpisodeConstructionMain ec = new EpisodeConstructionMain();
		ec.setParameters(parameters);
		
		
		int mmIdx =0;
		for (JobStepQueue _JSQ:memberList) {
			
			try {
			
				String memberId = _JSQ.getMember_id();
			
				PlanMember m = loadMemberFromDB(memberId);
				
				
				//Collections.sort(m.getServiceLines(), new slCompare());
				
				if (m == null) {
					_JSQ.setStatus(JobStepQueue.STATUS_FAILED);
				}
				else {
					
					ec.process(m);
			
					//doMemberReport(m);
				
					_JSQ.setStatus(JobStepQueue.STATUS_COMPLETE);
					
				}
				
				_JSQ.setUpdated(new Date());
				
				if (parameters.containsKey("debug")  &&  parameters.get("debug").equalsIgnoreCase("process")) {
					ProcessMessage message = new ProcessMessage();
					message.setJobUid(Integer.parseInt(parameters.get("jobuid")));
					message.setStepName("construct");
					message.setMember_id(_JSQ.getMember_id());
					message.setStatus(_JSQ.getStatus());
					InetAddress IP = null;
					try {
						IP = InetAddress.getLocalHost();
					} catch (UnknownHostException e) { }
					message.setDescription("IP " + IP.getHostAddress() + " set status to " + _JSQ.getStatus());
					ProcessMessageHelper.writeMessage(factoryC, message);
				}
			
			}
			
			catch (Throwable e){
				_JSQ.setStatus(JobStepQueue.STATUS_FAILED);
				log.error("Error encountered constructing " + _JSQ.getMember_id() + " in " + _JSQ.getJobUid() + " " + e.getMessage());
				e.printStackTrace();
			}
			
					
			
			mmIdx++;
			if(mmIdx % 10000 == 0) { 
				log.info("Completed Construction for  " + mmIdx + " members. " + new Date());
			}
			
		}
		

		//storeMemberReport();
		
		JobQueueHelper.updateMemberQueue(memberList);

	}
	
	
	
	
	/**
	 * get the member record
	 * @param member_id
	 * @return
	 */
	private PlanMember loadMemberFromDB (String member_id)  {
		
		log.info("Starting member load for  " + member_id);
		
		Transaction tx = session.beginTransaction();
		Query query = session.createQuery("from PlanMember where member_id = :id ").setLockOptions(LockOptions.NONE);
		//Query query = session.createQuery("from " + parameters.get("jobname") + ".PlanMember where member_id = :id ").setLockOptions(LockOptions.NONE);
		query.setParameter("id", member_id);
		

		List<?> list = query.list(); 
		
		tx.commit();
		 
		PlanMember m = null;
		 
		if (list.isEmpty()) {
			log.warn("No PlanMember record for " + member_id + " - must bypass");
		}
		else {

			m = (PlanMember)list.get(0); 
		 
			getEnrollments(m);
			getRxClaims(m);
			getMedClaims(m);
		}
		
		log.info("Completed member load for  " + member_id);
		 
		return m;
		
	}
	
	
	/**
	 * get all pharmacy claims for this member
	 */
	private void getRxClaims (PlanMember m) {
		
		log.info("Starting Rx load for  " + m.getMember_id());
		
		Transaction tx = session.beginTransaction();
		Query query = session.createQuery("from ClaimRx where member_id = :id").setLockOptions(LockOptions.NONE); 
		//Query query = session.createQuery("from ClaimRx where member_id = :id   AND claim_line_type_code = 'RX'").setLockOptions(LockOptions.NONE);
		//Query query = session.createQuery("from " + parameters.get("jobname") + ".ClaimRx where member_id = :id   AND claim_line_type_code = 'RX'").setLockOptions(LockOptions.NONE);
		query.setParameter("id", m.getMember_id()); 
		
		
		@SuppressWarnings("unchecked")
		List<ClaimRx> results = query.list();
		
		tx.commit();
		
		log.info("Starting Rx Code load for  " + m.getMember_id());
		
		List<Long> cL = new ArrayList<Long>();
		for (ClaimRx c : results)  {
			cL.add(c.getId());
		}
		
		List<MedCode> mCodes = getMedCodes(cL);
		
		for (ClaimRx c : results) {

			for (MedCode mc : mCodes) {

				if (mc.getU_c_id() == c.getId()) {
					c.addMed_codes(mc.getCode_value(), mc.getNomen(), mc.getPrincipal());
					// reconstruct builder match code - can't get back to original, but don't need it (I hope)
					//c.setBuilder_match_code(mc.getCode_value());
				}
			
			}
			m.addRxClaim(c);

		}
		
		log.info("Completed Rx load for  " + m.getMember_id());
		
	}
	
	
	/**
	 * get all medical claims for this member
	 */
	private void getMedClaims (PlanMember m) {
		
		log.info("Starting med claim load for  " + m.getMember_id());
		
		Transaction tx = session.beginTransaction();
		Query query = session.createQuery("from ClaimServLine where member_id = :id").setLockOptions(LockOptions.NONE);
		//Query query = session.createQuery("from ClaimServLine where member_id = :id  AND claim_line_type_code != 'RX'").setLockOptions(LockOptions.NONE);
		//Query query = session.createQuery("from " + parameters.get("jobname") + ".ClaimServLine where member_id = :id  AND claim_line_type_code != 'RX'").setLockOptions(LockOptions.NONE);
		query.setParameter("id", m.getMember_id()); 
		
		
		@SuppressWarnings("unchecked")
		List<ClaimServLine> results = query.list();
		
		tx.commit();
		
		log.info("Starting non-Rx Code load for  " + m.getMember_id());
		
		List<Long> cL = new ArrayList<Long>();
		for (ClaimServLine c : results)  {
			cL.add(c.getId());
		}
		
		List<MedCode> mCodes = getMedCodes(cL);
		
		for (ClaimServLine c : results)  {
				
			for (MedCode mc : mCodes) {

				if ( mc.getU_c_id() == c.getId() )
					//c.addMed_codes(mc.getFunction_code(), mc.getCode_value(), "PX", mc.getPrincipal());
					c.addMed_codes(mc.getFunction_code(), mc.getCode_value(), mc.getNomen(), mc.getPrincipal());
					
			}
			
			m.addClaimServiceLine(c);	

		}
		
		log.info("Completed med claim load for  " + m.getMember_id());
		
	}
	
	
	/**
	 * get med codes for a list of u_c_id values
	 */
	@SuppressWarnings("unchecked")
	private List<MedCode> getMedCodes (List<Long> uids) {
	
		log.info("Starting code load");
		
		List<MedCode> mcResults = new ArrayList<MedCode>();
		
		if ( ! uids.isEmpty()) {
		
			Transaction tx = session.beginTransaction();
 
			Query query = session.createQuery("from MedCode where u_c_id IN (:uids) ").setLockOptions(LockOptions.NONE); 
			//Query query = session.createQuery("from " + parameters.get("jobname") + ".MedCode where u_c_id IN (:uids) ").setLockOptions(LockOptions.NONE);
			query.setParameterList("uids", uids);
	
			mcResults = query.list();
		
			tx.commit();
		
		}
			
		log.info("Completing code load - loaded code count: " + mcResults.size());

		return mcResults;
		
	}

	
	
	/**
	 * get all enrollments for this member
	 */
	@SuppressWarnings("unchecked")
	private void getEnrollments (PlanMember m) {
		
		log.info("Starting enrollment load for  " + m.getMember_id());
		
		Transaction tx = session.beginTransaction();
		Query query = session.createQuery("from Enrollment where member_id = :id ").setLockOptions(LockOptions.NONE);  
		//Query query = session.createQuery("from " + parameters.get("jobname") + ".Enrollment where member_id = :id ").setLockOptions(LockOptions.NONE);
		query.setParameter("id", m.getMember_id());
		

		m.setEnrollment(query.list());
		
		tx.commit();
		
		log.info("Completed enrollment load for  " + m.getMember_id());
				
	}
	
	
	private void initialize () {
		
		// connect to control and get some work
		List<Class<?>> cList = new ArrayList<Class<?>>();
		cList.add(JobStepQueue.class);
		

        // allow a chunksize parameter - default is 1
        if (parameters.containsKey("chunksize"))
        	chunkSize = Integer.parseInt(parameters.get("chunksize"));
		
        
		
		
		// connect to the mapped data
		schemaName = parameters.get("jobname")  == null ?  "javatest" : parameters.get("jobname");
        
        List<Class<?>> oList = new ArrayList<Class<?>>();
        oList.add(PlanMember.class);
        oList.add(Enrollment.class);
        oList.add(ClaimRx.class);
        oList.add(ClaimServLine.class);
        oList.add(MedCode.class);
        oList.add(AssignmentTbl.class);
        oList.add(AssociationTbl.class);
        oList.add(EpisodeTbl.class);
        oList.add(MedCode.class);
        oList.add(TriggerTbl.class);
		
		h = new HibernateHelper(oList, parameters.get("env"), schemaName);
        factory = h.getFactory(parameters.get("env"), schemaName);
        session = factory.openSession();
        
        cListC = new ArrayList<Class<?>>();
		cListC.add(ProcessMessage.class);
		
		hC = new HibernateHelper(cListC, "ecr", "ecr");
		factoryC = hC.getFactory("ecr", "ecr");
        
		
	}
	
	 String schemaName;
	
	 

	public static void main(String[] args) {
		
		HashMap<String, String> parameters = RunParameters.parameters;
		parameters.put("clientID", "MD_APCD");
		parameters.put("mapname", "md_apcd");
		parameters.put("runname", "5_4_2013_2014");
		parameters.put("rundate", "20160624");
		parameters.put("jobname", "MD_APCD_5_4_2013_201420160624");
		parameters.put("jobuid", "1118");
		parameters.put("studybegin", "20130101");
		parameters.put("studyend", "20141231");
		parameters.put("configfolder", "C:\\workspace\\ECR_Analytics\\trunk\\EpisodeConstruction\\src\\");
		//parameters.put("configfolder", "/ecrfiles/scripts/");
		parameters.put("env", "prd");
		parameters.put("outputPath", "C:\\output\\Test\\");
		parameters.put("typeoutput", "sql");
		//parameters.put("metadata", "C:\\input\\HCI3-ECR-Definition-Tables-2015-05-28-5.3.001_FULL.xml");
		parameters.put("metadata", "c:/input/5_3_metadata_no_icd10.xml");
		//HCI3-ECR-Definition-Tables-2014-08-18-5.2.006_FULL.xml
		parameters.put("debug", "process");
		
		/*ConstructionController instance =*/ new ConstructionController(parameters);
		
		//System.out.println("Not what I meant to do");

	}
	
	
	
	private static org.apache.log4j.Logger log = Logger.getLogger(ConstructionController.class);
	
	

}
