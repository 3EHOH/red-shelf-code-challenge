



public class EpisodeConstructionMain {
	
	// where and how to find episode builder metadata
	//public static final String EP_FILE = 
	//		"C:\\Users\\Warren\\Dropbox (HCI3 Team)\\HCI3 Shared Folder\\Prometheus Operations\\Builder\\Builder Export Function\\DBExport-2014-05-02-5.2.005-trNodxFix\\"
	//		+ "HCI3-ECR-Definition-Tables-2014-08-18-5.2.006_FULL.xml";
	
	public String EP_FILE; 
	
	
	private AllDataInterface di;
	
	
	private int loopCount = 0;
	
	
	
	
	public void setParameters(HashMap<String, String> p) {
		this.parameters = p;
	}
	
	public boolean process (PlanMember m)  {
		
		log.info("Starting Episode Construction for " + m.getMember_id());
		
		getMetaData();
		
		try {
			studyBegin = df1.parse(parameters.get("studybegin"));
			studyEnd = df1.parse(parameters.get("studyend"));
		}
		catch (Exception ex ) {
			System.out.println(ex);
		}
		
		planMember = m;
		rxClaims = m.getRxClaims();
		
		allEpisodes = new ArrayList<EpisodeShell>();
		
		processEachPlanMember();
		
		doOutput();
		
		log.info("Completed Episode Construction for " + m.getMember_id());

		return true;
		
	}
	
	
	
	/**
	 * process driven by Main within this class
	 */
	private void process () {
		
		try {
		
			getMetaData();
		
			getCompleteData();
		
			log.info("Starting Episode Construction");
			log.info("Service lines to process: " + svcLines.size());
		
			//!!!=================================!!!//
		
			processServiceClaims();
		
			log.info("Processed: " + loopCount);
		
			//!!!=================================!!!//
		
			log.info("Completed Episode Construction");
		
			//logOutput(); 
		
			//!!!=================================!!!//
		
			doOutput();
		
		}
		catch (NullPointerException e) {
			log.error("Null pointer occurred" + e);
			e.printStackTrace();
			throw new IllegalStateException(e);
		} 
		catch (Throwable e) {
			log.error("Error occurred" + e);
			e.printStackTrace();
			throw new IllegalStateException(e);
		} 
	}
	
	
	
	/**
	 * common output method
	 */
	private void doOutput () {
		
	
		if (parameters.get("typeoutput").equalsIgnoreCase("csv")) {
			
			ClaimDataToCSV csvWrite = new ClaimDataToCSV();
			try {
				csvWrite.writeClaimLines(allEpisodes);
			} catch (IOException e) {
				log.info(e);
			}
			log.info("claim write done, start episode write" + allEpisodes.size());
			
			EpisodeData epWrite = new EpisodeData();
			try {
				epWrite.writeEpisodeLines(allEpisodes);
			} catch (IOException e) {
				log.info(e);
			}
			
			log.info("episode write should be done");
			
			AssociationLevelData aldWrite = new AssociationLevelData();
			try {
				aldWrite.writeAssociationLevelLines(allEpisodes);
			} catch (IOException e) {
				log.info(e);
			}
			
			log.info("associationLevel write should be done.");
			
			/*
			CostsByLevel cblWrite = new CostsByLevel();
			try {
				cblWrite.writeCostLines(allEpisodes);
			} catch (IOException e) {
				log.info(e);
			}
			
			log.info("cost by level write should be done.");
			*/
		}
		
		else if (parameters.get("typeoutput").equalsIgnoreCase("sql")) {
			
			log.info("Starting database store for " + allEpisodes.size() + " episodes");
			
			AssignmentOutputSQL sigfto = new AssignmentOutputSQL();
			AssociationOutputSQL socfto = new AssociationOutputSQL();
			TriggerOutputSQL dfto = new TriggerOutputSQL();
			
			log.info("Starting database store of associations");
			socfto.doFillAssociationOutput(allEpisodes);
			log.info("Starting database store of assignments");
			sigfto.doFillAssignmentOutput(allEpisodes);
			log.info("Starting database store of triggers");
			dfto.doFillTriggerOutput(allEpisodes);
			
			
			log.info("Finished database store");

			
		}
			
		
	}
	
	
	private void getCompleteData ()  {
		svcLines = di.getAllServiceClaims();
		enrollments = di.getAllEnrollments();
		members = di.getAllPlanMembers();
		//providers = di.getAllProviders();
		rxClaims = di.getAllRxClaims();
	}
	
	
	/**
	 * get all metadata
	 */
	private void getMetaData () {
		
		log.info("Obtaining Episode Metadata");
		EP_FILE = parameters.get("metadata");
		SerializedMetaData _smd = EpisodeMetaDataHelper.getMetaData(EP_FILE);
		epmd = _smd.epList;
		emCodes = _smd.emCodeList;
		mdh = _smd.mdh;
		
		String episode_id;
		log.debug("Episode list is " + epmd == null ? " NULL!" : epmd.size() + " long");
		for (EpisodeMetaData epis : epmd) {
			
			episode_id = epis.getEpisode_id();
			
			/*
			 * ADDING IN THE REMAPPING OF EPMD! NEED THIS IF THE REST IS REMOVED!
			 */
			epmdk.put(episode_id, epis);
			log.debug("EC epmdk - added: |" + episode_id + "|");
				
		}
		
		log.info("Finish loading metadata");
		
	}
	

	/**
	 * pass the ordered service Lines looking for each plan member
	 * only used by this class' Main
	 */
	private void processServiceClaims () {
		
		for (ClaimServLine clm : svcLines) { 
			
			loopCount++;
			if(loopCount % 10000 == 0) { 
				log.info("Executing svcLine " + loopCount + " ==> " + new Date());
			}
			
			// this does all triggering that only requires one claim (and doesn't matter according to member)
			//processEachClaimLine(clm);
			
			// filter out claims not within the study period
			// This should be removed for now...
			/*if (clm.getBegin_date().before(studyBegin)) {
				log.info("Rejecting claim " + clm.getClaim_id() + "|" + clm.getClaim_line_id() + " as before study date");
				continue;
			}*/
			if (clm.getMember_id() == null)
				continue;
			
			
			// build a list of claims for each plan member
			if (clm.getMember_id().equals(planMember.getMember_id()))
			{
				planMember.addClaimServiceLine(clm);
			}
			else
			{
				if (planMember.getMember_id() != null)				// need a null check for first pass
				{
					// do everything for the prior plan member
					processEachPlanMember() ;
					
					//if (planMember.getMember_id().equals("0000316153_A")) {
					//	processEachPlanMember() ;
					//}
				}
				planMember = members.get(clm.getMember_id());
				// a null here indicates that a claim has been found for a member id not in the member list
				// we should probably abort
				if (planMember == null)
				{
					planMember = new PlanMember();
					planMember.setMember_id(clm.getMember_id());
					//log.error("Found member id " + clm.getMember_id() + "in the claim file, but not in the member file");
				}
				planMember.setEnrollment(enrollments.get(clm.getMember_id()));
				planMember.addClaimServiceLine(clm);
			}
		}
		// do everything for the last plan member
		
		processEachPlanMember() ;

	}
	
	
	/**
	 * pass each plan member's service Lines looking for episode triggers
	 */
	private void processEachPlanMember()  {
		
		//if (planMember.getMember_id().equals("2104646")) {
		
//log.info("Processing member " + planMember.getMember_id());
		epsForMember = new ArrayList<EpisodeShell>();
		addlEpsforMember = new ArrayList<EpisodeShell>();
		
		//log.info("!0!0!0! -- " + mdh.getExport_version() + " -- !0!0!0!");
		
		Collections.sort(planMember.getServiceLines(), new slCompare());
		
		for (ClaimServLine c : planMember.getServiceLines()) {
//log.info("Claim in ProcessEachPlanMember: " + c.getClaim_id() + " | " + c.getClaim_line_type_code());		
			/*
			 * The following date check was added to keep us from triggering anything before
			 * the beginning of the study period which would lead to chronics not
			 * re-triggering during the study period and being filtered out
			 */
			
			// first date is zero, counting days FROM there, 
			// so if second date is earlier, the result is negative
			// if later positive...
			long b = getDateDiff(studyBegin,c.getBegin_date(),TimeUnit.DAYS);
			if (b<0) {
				//log.info("claim before study begin (" + studyBegin + "): " + 
				//		c.getMember_id() + "_" + c.getClaim_id() + "_" + c.getClaim_line_id() );
			}
			if (b>=0) {
				processEachClaimLine(c);
			}
		}
		
		//confirm episodes (find confirming claim), trigger episodes by episode, and confirm them
		confirmEpisodes();
		
		Collections.sort(epsForMember, new epCompare() );
		
		//episode resolution for non-episode triggers, non-srf, non-procedural.
		resolveEpisodes();
		
		// remove episodes triggered by dropped episodes...
		resolveDroppedEpisodeTriggers();
		
		// put addl episodes into epsforMember...
		for(EpisodeShell es : addlEpsforMember) {
			epsForMember.add(es);
		}
		
		Collections.sort(epsForMember, new epCompare() );
		
		//resolve again with episode triggered episodes in the mix...
		resolveEpisodes();
		
		// find overlapping procedural episodes and truncate...
		truncateProcedurals();
		
		//assign the IP claims...
		ipAssignment();
		
		//log.info("Start RX");
		// assign Rx claims...
		rxAssignment();
		
		//log.info("Start other assignment");
		// assign other claims (OP/PB)...
		otherAssignment();
		
		//apportion claims (i.e. to how many episodes is a claim assigned)
		claimApportionment();
		
		// now level the things...
		episodeLeveling();
		
		// get dollars for assignment and associations
		//reverse order so we get the chained subsidiary dollars in first...
		//Collections.sort(epsForMember, new epCompareReverse() );
		//rollDollars();
		//put back in correct order
		//Collections.sort(epsForMember, new epCompare() );
		
		//providerAttribution();
		
		
		// add this member's episodes to the full episode list...
		for (EpisodeShell es : epsForMember) {
			if ( episodesByMember.containsKey(es.getMember_id()) ) {
				episodesByMember.get(es.getMember_id()).add(es);
			} else {
				ArrayList<EpisodeShell> hs = new ArrayList<EpisodeShell>();
				hs.add(es);
				episodesByMember.put(es.getMember_id(), hs);
			}
			
			allEpisodes.add(es);
		}
		//for (EpisodeShell es : addlEpsforMember) {
		//	episodesByMember.get(es.getMember_id()).add(es);
		//}
		
		 
		//log.info("This member was born in " + planMember.getBirth_year() + " and has " + planMember.getServiceLines().size() + " claim service lines");
	}//}
	
	
	public void processEachClaimLine (ClaimServLine c) {
		
		
//log.info(">> Claim: " + c.getClaim_id() + " - Claim Line Type Code: " + c.getClaim_line_type_code() + " - " + c.getBegin_date());
		
		//=========================================================
		
		//boolean hasTrig = false;
		
//log.info("Claim: " + c.getClaim_id() + " " + c.getClaim_line_id());
		if (c.getClaim_line_type_code() != null && c.getFacility_type_code() != null) {
			if (c.getClaim_line_type_code().equals("IP") && 
					!c.getFacility_type_code().equals("ST") ) {
				return;
			}
		}
		

		if (parameters.get("debug") != null  && !(parameters.get("debug").equalsIgnoreCase("process")) ) {
			log.info("Claim: " + c);
			if (c.getPrincipal_diag_code_object() == null) {
				log.info("Claim: " + c.getClaim_id() + " No principal diag found");
			} else{
				log.info("Claim: " + c.getClaim_id() + " Princ DX: " + c.getPrincipal_diag_code_object().getCode_value() + " | " + c.getPrincipal_diag_code_object().getNomen());
			}
			for (MedCode pP : c.getPrincipal_proc_code_objects()) {
				log.info(" Princ PX: " + pP.getCode_value() + " | " + pP.getNomen());
			}
			for (MedCode sC : c.getSecondary_diag_code_objects()) {
				log.info("Second DX: " + sC.getCode_value() + " | " + sC.getNomen());
			}
			for (MedCode sC : c.getSecondary_proc_code_objects()) {
				log.info("Second PX: " + sC.getCode_value() + " | " + sC.getNomen());
			}
		}

		MedCode pdco = c.getPrincipal_diag_code_object();
		List<MedCode> ppcos = c.getPrincipal_proc_code_objects();
		List<MedCode> sdcos = c.getSecondary_diag_code_objects();
		List<MedCode> spcos = c.getSecondary_proc_code_objects();
		
		/*
		 * All the following stuff that changes the PX/DX to PXX/DXX if
		 * OCT 1 2015 or later needs to be removed, this is a cruddy workaround
		 * because we haven't properly implemented code nomenclature
		 * 
		 */
		
		Date icd10Day = new Date();
		DateFormat df = new SimpleDateFormat("MM/dd/yyyy");
		try {
			icd10Day = df.parse("09/30/2015");
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		String dk="DX";
		String pk="PX";
		if (c.getBegin_date().after(icd10Day)) {
			dk="DXX";
			pk="PXX";
		}
		String diagKey = c.getDiag_claim_nomen();
		if (diagKey == null) { diagKey = dk; } else {
			if (c.getBegin_date().after(icd10Day)) {
				if (diagKey.equals("DX")) {
					diagKey="DXX";
					c.setDiag_claim_nomen("DXX");
				}
			}
		}
		String procKey = c.getProc_claim_nomen();
		if (procKey == null) { procKey = pk; } else {
			if (c.getBegin_date().after(icd10Day)) {
				if (procKey.equals("PX")) {
					procKey="PXX";
					c.setProc_claim_nomen("PXX");
				}
			}
		}
		String type_code = c.getClaim_line_type_code();
		
		for (MedCode _mc : c.getMed_codes()) {
			if (_mc != null && 
				_mc.getNomen() !=null && 
				_mc.getCode_value() != null && 
				!_mc.getCode_value().isEmpty()
				) {
				if (_mc.getNomen().equals("DX")) {
					_mc.setNomen(dk);
				}
				if (_mc.getNomen().equals("PX")) {
					_mc.setNomen(pk);
				}
			}
		}
		
		List<String> potEps = new ArrayList<String>();
		
		/*
		 * First we're going through and looking at all the codes in the claim and whether or not 
		 * they are trigger codes for any episodes. If so, we're putting that episode id
		 * in the potEps list so we can check whether or this claim passes the single
		 * claim portion of the trigger logic...
		 */
		// once for each episode in the metadata...
		for ( String episode_id : epmdk.keySet()) {
			
			//log.info(epmdk.get("EP0520").getTrigger_code_by_ep());
			
			HashMap<String, HashMap<String, ArrayList<TriggerCodeMetaData>>> tcbe = epmdk.get(episode_id).getTrigger_code_by_ep();
			
			/**
			if (parameters.containsKey("debug")) {
				for (Entry<String, HashMap<String, ArrayList<TriggerCodeMetaData>>> entry : tcbe.entrySet()) {
				    log.info(">>outer key: " + entry.getKey());
				    //HashMap<String, ArrayList<TriggerCodeMetaData>> value = entry.getValue();
				    for (Entry<String, ArrayList<TriggerCodeMetaData>> entryI : entry.getValue().entrySet() ) {
				    	log.info("  inner key: " + entryI.getKey());
				    	for (TriggerCodeMetaData t : entryI.getValue()) {
				    		log.info("    Code: " + t.getCode_id());
				    	}
				    }
				}
			}
			*/
			
			if (parameters.get("debug") != null  && !(parameters.get("debug").equalsIgnoreCase("process")) ) {
				log.info("epid: " + episode_id);
			}
			
			// first, look to see if the primary diag is in the trigger list...
/*			if ( tcbe.containsKey(diagKey) && c.getPrincipal_diag_code() != null ) {
				if (tcbe.get(diagKey).containsKey(c.getPrincipal_diag_code()) ) {
log.info("trigger code match pdx: " + episode_id + " " + diagKey + " " + c.getPrincipal_diag_code());					
					if (!potEps.contains(episode_id)) {
log.info("epid added");
						potEps.add(episode_id); 	
					}
				}
			}
*/
			//new version of the above 1/13/2016 - 
			//using new nomenclature			
			if (pdco != null && 
				pdco.getNomen() !=null && 
				pdco.getCode_value() != null && 
				!pdco.getCode_value().isEmpty()
				) {
				if (tcbe.containsKey(pdco.getNomen())) {
//log.info("trigger code match pdx (new): " + episode_id + " " + pdco.getNomen() + " " + pdco.getCode_value());
					if (tcbe.get(pdco.getNomen()).containsKey(pdco.getCode_value())) {
						if (!potEps.contains(episode_id)) {
//log.info("epid added (new)");
							potEps.add(episode_id); 	
						}
					}
				}
			}
		
			// prin proc codes...
/*			if ( tcbe.containsKey(procKey) ) {
				for (String pc : c.getPrincipal_proc_code()) {
					if (pc!=null && !pc.isEmpty()) {
log.info("Claim: " + c.getClaim_id() + "_" + c.getClaim_line_id() + " Prin Proc: " + pc + " Proc Key: " + procKey);
						if (tcbe.get(procKey).containsKey(pc)) {
log.info("trigger code match ppx: " + episode_id + " " + procKey + " " + pc);
							if (!potEps.contains(episode_id)) {
log.info("epid added");
								potEps.add(episode_id); 
							}
						}
					}
				}
			}
*/
			//new version of the above 1/13/2016 - 
			//using new code object
			
			for (MedCode ppco : ppcos) {
				if (ppco != null && 
					ppco.getNomen() != null &&
					ppco.getCode_value() != null &&
					!ppco.getCode_value().isEmpty()
				) {
//log.info("Claim: " + c.getClaim_id() + "_" + c.getClaim_line_id() + " Prin Proc: " + ppco.getCode_value() + " Proc Key: " + ppco.getNomen());
//log.info("Episode: " + episode_id);
//for (String epval : tcbe.get(ppco.getNomen()).keySet()) {
//	log.info(epval);
//}
					if (tcbe.get(ppco.getNomen()) != null) {	
						if (tcbe.get(ppco.getNomen()).containsKey(ppco.getCode_value())) {
//log.info("trigger code match ppx: " + episode_id);
							if (!potEps.contains(episode_id)) {
//log.info("epid added");
								potEps.add(episode_id); 
							}
						}
					}
// HCPC is default for HCPC and CPT, so if HCPC, also check CPT...
					if (ppco.getNomen().equals("HCPC")) {
						if (tcbe.get("CPT") != null) {
							if (tcbe.get("CPT").containsKey(ppco.getCode_value())) {
								if (!potEps.contains(episode_id)) {
//log.info("HCPC/CPT Match: " + ppco.getCode_value() + " | " + episode_id);
									potEps.add(episode_id); 
								}
							}
						}
					}
					if (ppco.getNomen().equals("CPT")) {
						if (tcbe.get("HCPC") != null) {
							if (tcbe.get("HCPC").containsKey(ppco.getCode_value())) {
								if (!potEps.contains(episode_id)) {
//log.info("HCPC/CPT Match2: " + ppco.getCode_value() + " | " + episode_id);
									potEps.add(episode_id); 
								}
							}
						}
					}
				}
			}
			
			// secondary diag codes...
/*			if ( tcbe.containsKey(diagKey) ) {
				for (String sdc : c.getSecondary_diag_code() ) {
					if (tcbe.get(diagKey).containsKey(sdc)) {
//log.info("trigger code match sdx: " + episode_id + " " + diagKey + " " + sdc);
						if (!potEps.contains(episode_id)) {
//log.info("epid added");
							potEps.add(episode_id); 
						}
					}
				}
			}
*/
			//new version of the above 1/13/2016 - 
			//using new code object
			
			for (MedCode sdco : sdcos) {
				if (sdco != null && 
					sdco.getNomen() != null &&
					sdco.getCode_value() != null &&
					!sdco.getCode_value().isEmpty() &&
					tcbe.get(sdco.getNomen()) != null
				) {
//log.info("Claim: " + c.getClaim_id() + "_" + c.getClaim_line_id() + " Secon Diag: " + sdco.getCode_value() + " Diag Key: " + sdco.getNomen());
					if (tcbe.get(sdco.getNomen()).containsKey(sdco.getCode_value())) {
//log.info("trigger code match sdx: " + episode_id);
						if (!potEps.contains(episode_id)) {
//log.info("epid added");
							potEps.add(episode_id); 
						}
					}
				}
			}
			
			
			
			
			// secondary proc codes...
/*			if (tcbe.containsKey(procKey)) {
				for (String spc : c.getSecondary_proc_code() ) {
log.info("Claim: " + c.getClaim_id() + "_" + c.getClaim_line_id() + " Second Proc: " + spc + " Proc Key: " + procKey);
					if (tcbe.get(procKey).containsKey(spc)) {
log.info("trigger code match spx: " + episode_id + " " + procKey + " " + spc);
						if (!potEps.contains(episode_id)) {
log.info("epid added");
							potEps.add(episode_id); 
						 }
					}
				}
			}
*/
			//new version of the above 1/13/2016 - 
			//using new code object
			
			for (MedCode spco : spcos) {
				if (spco != null && 
					spco.getNomen() != null &&
					spco.getCode_value() != null &&
					!spco.getCode_value().isEmpty() 
				) {
//log.info("Claim: " + c.getClaim_id() + "_" + c.getClaim_line_id() + " Secon Proc: " + spco.getCode_value() + " Proc Key: " + spco.getNomen());
					if (tcbe.get(spco.getNomen()) != null) {
						if (tcbe.get(spco.getNomen()).containsKey(spco.getCode_value())) {
							if (!potEps.contains(episode_id)) {
								potEps.add(episode_id); 
							}
						}
					}
					
					// HCPC is default for HCPC and CPT, so if HCPC, also check CPT...
					if (spco.getNomen().equals("HCPC")) {
						if (tcbe.get("CPT") != null) {
							if (tcbe.get("CPT").containsKey(spco.getCode_value())) {
								if (!potEps.contains(episode_id)) {
									potEps.add(episode_id); 
								}
							}
						}
					}
					
					// HCPC is default for HCPC and CPT, so if HCPC, also check CPT...
					if (spco.getNomen().equals("CPT")) {
						if (tcbe.get("HCPC") != null) {
							
							if (tcbe.get("HCPC").containsKey(spco.getCode_value())) {
								if (!potEps.contains(episode_id)) {
									potEps.add(episode_id); 
								}
							}
						}
					}
				}
			}
		}

		
		
		/* 
		 * Now we have a list of episode id for any episode where the set of trigger codes matches any code in the claim
		 * We want to go through those episodes  check to see if the claim passes all trigger logic for that episode
		 * it can without looking to other claims 
		 */
		String pxcond;
		String emcond;
		String andor;
		String dxcond;
		
		boolean pxpass;
		boolean empass;
		boolean dxpass;
		
		String pxPassCode;
		String dxPassCode;
		String emPassCode;
		
		boolean reqConfClaim;
		
		String minSep = "";
		String maxSep = "";
		
		for (String cleps : potEps) {
			// set claim type trigger meta values
			
			//log.info(potEps);
			
			pxcond="";
			emcond="";
			andor="";
			dxcond="";
			
			pxPassCode="";
			dxPassCode="";
			emPassCode="";
			
			reqConfClaim=false;
			
			minSep = "";
			maxSep = "";
			
			//set holders to know what we've matched
			pxpass=false;
			empass=false;
			dxpass=false;
			
			//set holders for logging in episodeShell object
			
			
			
			//log.info("test- " + epmdk.get(cleps).getTrigger_conditions_by_fac().get("IP").getPx_code_position());
			switch (type_code) {
			case "IP":
				//pxcond = epmdk.get(cleps).getTrigger_conditions_by_fac().get("IP").getPx_code_position();
				pxcond = epmdk.get(cleps).getTrigger_conditions_by_fac().get("IP").getPx_code_position();
				emcond = epmdk.get(cleps).getTrigger_conditions_by_fac().get("IP").getEm_code_position();
				andor = epmdk.get(cleps).getTrigger_conditions_by_fac().get("IP").getAnd_or();
				dxcond = epmdk.get(cleps).getTrigger_conditions_by_fac().get("IP").getDx_code_position();
				minSep = epmdk.get(cleps).getTrigger_conditions_by_fac().get("IP").getMin_code_separation();
				maxSep = epmdk.get(cleps).getTrigger_conditions_by_fac().get("IP").getMax_code_separation();
				break;
			case "OP":
				pxcond = epmdk.get(cleps).getTrigger_conditions_by_fac().get("OP").getPx_code_position();
				emcond = epmdk.get(cleps).getTrigger_conditions_by_fac().get("OP").getEm_code_position();
				andor = epmdk.get(cleps).getTrigger_conditions_by_fac().get("OP").getAnd_or();
				dxcond = epmdk.get(cleps).getTrigger_conditions_by_fac().get("OP").getDx_code_position();
				minSep = epmdk.get(cleps).getTrigger_conditions_by_fac().get("OP").getMin_code_separation();
				maxSep = epmdk.get(cleps).getTrigger_conditions_by_fac().get("OP").getMax_code_separation();
				break;
			case "PB":
				pxcond = epmdk.get(cleps).getTrigger_conditions_by_fac().get("PR").getPx_code_position();
				emcond = epmdk.get(cleps).getTrigger_conditions_by_fac().get("PR").getEm_code_position();
				andor = epmdk.get(cleps).getTrigger_conditions_by_fac().get("PR").getAnd_or();
				dxcond = epmdk.get(cleps).getTrigger_conditions_by_fac().get("PR").getDx_code_position();
				reqConfClaim = epmdk.get(cleps).getTrigger_conditions_by_fac().get("PR").getReq_conf_code();
				minSep = epmdk.get(cleps).getTrigger_conditions_by_fac().get("PR").getMin_code_separation();
				maxSep = epmdk.get(cleps).getTrigger_conditions_by_fac().get("PR").getMax_code_separation();
				break;
			default:
				pxcond = "None";
				emcond = "None";
				andor = "Or";
				dxcond = "None";
			}
			// find the blank parameters...
			boolean nopx = false;
			boolean noem = false;
			boolean nodx = false;
			if (pxcond==null || pxcond.isEmpty() || pxcond.equals("None")) { nopx=true; pxcond=null;}
			if (emcond==null || emcond.isEmpty() || emcond.equals("None")) { noem=true; emcond=null;}
			if (dxcond==null || dxcond.isEmpty() || dxcond.equals("None")) { nodx=true; dxcond=null;}
			if (andor==null) { andor="Or"; }
			
			
			
			// if all px/e+m/dx conditions are null/None, discard this as a potential episode
			if (nopx && noem && nodx) {
				continue;
			}
			
			
			//log.info("CLAIM!: " + c.getClaim_id() + "_" + c.getClaim_line_id());
			//log.info("EPISODE!: " + cleps);
			
			
			// lets start by trying to trigger a record on primary dx...
			//log.info("and or is " + andor);
			if ( dxcond != null) {
				if (pdco != null && 
					pdco.getNomen() !=null && 
					pdco.getCode_value() != null && 
					!pdco.getCode_value().isEmpty()
					) {
					if (epmdk.get(cleps).getTrigger_code_by_ep().containsKey(pdco.getNomen()) ) {
						if (epmdk.get(cleps).getTrigger_code_by_ep().get(pdco.getNomen()).containsKey(pdco.getCode_value())) {
							//log.info("triggered a prin diag episode!!!: " + cleps + " " + c.getPrincipal_diag_code());
							//log.info("diag param passes: " + cleps + " " + c.getPrincipal_diag_code());
							dxpass=true;
							dxPassCode=pdco.getCode_value();
						}
					}
				}
			}
			
			// try to match on secondary dx if dxcond = "any"
/*			if ( dxcond!=null && dxcond.equals("Any") && !dxpass ) {
				if ( epmdk.get(cleps).getTrigger_code_by_ep().containsKey(diagKey) ) {
					for (String sdc : c.getSecondary_diag_code() ) {
						if (sdc!=null && epmdk.get(cleps).getTrigger_code_by_ep().get(diagKey).containsKey(sdc)) {
							dxpass=true;
							dxPassCode=sdc;
							break;
						}
					}
				}
			}
*/
			// try to match on secondary dx if dxcond = "any"
			if ( dxcond!=null && dxcond.equals("Any") && !dxpass ) {
				for (MedCode sdco : sdcos) {
					if (sdco != null && 
					sdco.getNomen() !=null && 
					sdco.getCode_value() != null && 
					!sdco.getCode_value().isEmpty()) {
						
						if (epmdk.get(cleps).getTrigger_code_by_ep().containsKey(sdco.getNomen()) ) {
							if (epmdk.get(cleps).getTrigger_code_by_ep().get(sdco.getNomen()).containsKey(sdco.getCode_value())) {
								dxpass=true;
								dxPassCode=sdco.getCode_value();
								break;
							}
						}
					}
				}
			}
			
			// try to match principal proc on this ep for this claim...
			if ( pxcond != null) {
				for (MedCode ppco : ppcos) {
					if (ppco != null && 
						ppco.getNomen() !=null && 
						ppco.getCode_value() != null && 
						!ppco.getCode_value().isEmpty()) {
						if (epmdk.get(cleps).getTrigger_code_by_ep().containsKey(ppco.getNomen()) ) {
								
							if (epmdk.get(cleps).getTrigger_code_by_ep().get(ppco.getNomen()).containsKey(ppco.getCode_value())) {
								pxpass=true;
								pxPassCode=ppco.getCode_value();
								break;
							}
		
						}
						if (ppco.getNomen().equals("HCPC")) {
							if (epmdk.get(cleps).getTrigger_code_by_ep().containsKey("CPT") ) {
								
								if (epmdk.get(cleps).getTrigger_code_by_ep().get("CPT").containsKey(ppco.getCode_value())) {
									pxpass=true;
									pxPassCode=ppco.getCode_value();
									break;
								}
			
							}
						}
						if (ppco.getNomen().equals("CPT")) {
							if (epmdk.get(cleps).getTrigger_code_by_ep().containsKey("HCPC") ) {
								
								if (epmdk.get(cleps).getTrigger_code_by_ep().get("HCPC").containsKey(ppco.getCode_value())) {
									pxpass=true;
									pxPassCode=ppco.getCode_value();
									break;
								}
			
							}
						}
					}
				}
			}
			
			// try to match on secondary px if pxcond = "any"
			if ( pxcond!=null && pxcond.equals("Any") && !pxpass ) {
				for (MedCode spco : spcos) {
					if (spco != null && 
						spco.getNomen() !=null && 
						spco.getCode_value() != null && 
						!spco.getCode_value().isEmpty()) {
						if ( epmdk.get(cleps).getTrigger_code_by_ep().containsKey(spco.getNomen()) ) {

							if (epmdk.get(cleps).getTrigger_code_by_ep().get(spco.getNomen()).containsKey(spco.getCode_value())) {
								pxpass=true;
								pxPassCode=spco.getCode_value();
								break;
							}

						}
						if (spco.getNomen().equals("HCPC")) {
							if ( epmdk.get(cleps).getTrigger_code_by_ep().containsKey("CPT") ) {
	
								if (epmdk.get(cleps).getTrigger_code_by_ep().get("CPT").containsKey(spco.getCode_value())) {
									pxpass=true;
									pxPassCode=spco.getCode_value();
									break;
								}
	
							}
						}
						if (spco.getNomen().equals("CPT")) {
							if ( epmdk.get(cleps).getTrigger_code_by_ep().containsKey("HCPC") ) {
	
								if (epmdk.get(cleps).getTrigger_code_by_ep().get("HCPC").containsKey(spco.getCode_value())) {
									pxpass=true;
									pxPassCode=spco.getCode_value();
									break;
								}
	
							}
						}
					}
				}
			}
			
			// try to match principal proc to e&m on this ep for this claim...
			// 5.3 rule, em must also be in typical px list...
			if ( emcond != null) {
				for (MedCode ppco : ppcos) {
					if (ppco != null && 
						ppco.getNomen() !=null && 
						ppco.getCode_value() != null && 
						!ppco.getCode_value().isEmpty()) {
						
						if (ppco.getNomen().equals("HCPC") || ppco.getNomen().equals("CPT") ) {
						
							if (emCodes.contains(ppco.getCode_value())) {
								
								// 5.3 also check to see if it is typical px...
								boolean isP = false;
								if (epmdk == null) {
									log.error("Can't have null metadata at this point");
									throw new IllegalStateException("null metadata");
								}
								else if (epmdk.get(cleps) == null) {
									log.error("Need null check in epmdk.get(cleps)");
									throw new IllegalStateException("null metadata typical px key");
								}
								for (PxCodeMetaData px : epmdk.get(cleps).getPx_codes()) {
									if(px.getCode_id() == null) {
										log.error("px code is null " + px);
										throw new IllegalStateException("null px code - episode: " + cleps + " code: " + ppco.getCode_value());
									}
									if (px.getCode_id().equals(ppco.getCode_value())) {
										isP=true;
										break;
									}
								}
								
								if (isP) {
									empass=true;
									emPassCode=ppco.getCode_value();
									break;
								}		
	
							}
						}
					}
				}
			}
			
			
			
			// try to match E&M to secondary px if pxcond = "any"
			// 5.3 update, E&M must be in typical px list...
			if ( emcond!=null && emcond.equals("Any") && !empass ) {
				for (MedCode spco : spcos) {
					if (spco != null && 
						spco.getNomen() !=null && 
						spco.getCode_value() != null && 
						!spco.getCode_value().isEmpty() &&
						(spco.getNomen().equals("HCPC") || spco.getNomen().equals("CPT"))
						) {
						if (emCodes.contains(spco.getCode_value())) {
							
							// 5.3 also check to see if it is typical px...
							boolean isP = false;
							for (PxCodeMetaData px : epmdk.get(cleps).getPx_codes()) {
								if (px.getCode_id().equals(spco.getCode_value())) {
									isP=true;
									break;
								}
							}
							
							if (isP) {
								empass=true;
								emPassCode=spco.getCode_value();
								break;
							}
						}
					}
				}
			}
			
			@SuppressWarnings("unused")
			String logstring = "";
			String andorCond = "";
			String tstConfClaim = "";
			// pEp = new EpisodeShell();
			EpisodeShell pEp = new EpisodeShell();
			boolean epPass = false;
			
			// one of these has to be true or nothing matched...
			if (pxpass || dxpass || empass) {
				
				logstring += c.getClaim_line_type_code() + " ";
				
				if (pxpass) { logstring += "PX passed "; }
				if (dxpass) { logstring += "DX passed "; }
				if (empass) { logstring += "EM passed "; }
				
				if (andor.equals("And")) {
					if ( (pxpass || empass) && dxpass) {
						andorCond = "andor cond PASSED|" + andor + "|";
						epPass = true;
					} else {
						andorCond = " andor cond FAILED|" + andor + "|";
					}
				} else {
					epPass = true;
					andorCond = "andor should be or or not and at least|" + andor + "|";
				}
				
				if (type_code.equals("PB") && reqConfClaim) {
					tstConfClaim = " CONF REQ!";
				}
				
				logstring += epPass + " for episode: " + cleps + andor + andorCond + " claim: " + c.getClaim_id() + tstConfClaim;
	
				//log.info(logstring);
				
				
				//build provisional episode, make sure this episode doesn't already exist for this claim... can it? 
				//Seems logically that it cannot
				if (epPass) {
					//calculate episode begin date and episode end date... 
					Calendar cal = Calendar.getInstance();
					cal.setTime(c.getBegin_date());
					Date bdate;
					int testlb = epmdk.get(cleps).getLook_back();
					cal.add(Calendar.DATE, testlb);
					bdate = cal.getTime();
					Date fdate = cal.getTime(); // BAD WORKAROUND!!!
					
					String testlf = epmdk.get(cleps).getLook_ahead();
					if (testlf.equals("EOS")) {
						
						// !!! MUST ADD IN STUDY PERIOD END DATE FROM PARAMS !!!
						fdate = studyEnd;

					} else {
						cal.setTime(c.getEnd_date() == null ? c.getBegin_date() : c.getEnd_date());
						cal.add(Calendar.DATE, Integer.parseInt(testlf));
						fdate = cal.getTime();
					}
					
					
					
					//log.info("CLAIM: " + c.getClaim_id());
					pEp.setClaim_id(c.getClaim_id());
					pEp.setClaim(c);
					pEp.setEpisode_id(cleps);
					pEp.setMember_id(c.getMember_id());
					pEp.setEpisode_type(ParseEpType(cleps));
					pEp.setTrig_begin_date(c.getBegin_date());
					pEp.setTrig_end_date(c.getEnd_date());
					pEp.setOrig_episode_begin_date(bdate);
					pEp.setOrig_episode_end_date(fdate);
					// date of death truncation:
					if (planMember != null && planMember.getDate_of_death() != null) {
						if (planMember.getDate_of_death().before(fdate)) {
							fdate=planMember.getDate_of_death();
						}
					}
					pEp.setEpisode_begin_date(bdate);
					pEp.setEpisode_end_date(fdate);
					pEp.setLook_ahead(epmdk.get(cleps).getLook_ahead());
					pEp.setLook_back(epmdk.get(cleps).getLook_back());
					pEp.setReq_conf_claim(reqConfClaim);
					pEp.setConf_claim_id("0");
					pEp.setMin_code_separation(minSep);
					pEp.setMax_code_separation(maxSep);
					//pEp.setTrig_by_episode_id("0");
					pEp.setTrig_by_episode(false);
					
					pEp.setDxPassCode(dxPassCode);
					pEp.setPxPassCode(pxPassCode);
					pEp.setEmPassCode(emPassCode);
					
					epsForMember.add(pEp);

				}
			}
		} 
	}
	
	
	
	
	public void confirmEpisodes() {
		
		//provEpisodes is by member with list of episodes (EpisodeShell) triggered...

		//look to see if any episodes require a confirming claim (req_conf false) and don't have a conf claim id (0)
		//if (provEpisodes.get(planMember.getMember_id())!=null) {
		if (epsForMember != null && !epsForMember.isEmpty()) {
			
			for (EpisodeShell pep : epsForMember) {
				
				if (pep.isReq_conf_claim() && pep.getConf_claim_id().equals("0")) {
					
					// we have an episode that requires a confirming claim, see if there is one...
					String epid = pep.getEpisode_id();
					String minSepT = pep.getMin_code_separation();
					String maxSepT = pep.getMax_code_separation();
					Date minSepDate;
					Date maxSepDate;
					Date trigFromDate = pep.getTrig_begin_date();
					
					//calculate min/max sep dates...
					Calendar minD = Calendar.getInstance();
					Calendar maxD = Calendar.getInstance();
					minD.setTime(trigFromDate);
					maxD.setTime(trigFromDate);
					
					int minSep = Integer.parseInt(minSepT);
					minD.add(Calendar.DATE, minSep);
					minSepDate = minD.getTime();
					
					int maxSep;
					if (!maxSepT.equals("EOS")) {
						maxSep = Integer.parseInt(maxSepT);
						maxD.add(Calendar.DATE, maxSep);
						maxSepDate = maxD.getTime();
					} else {
						maxSepDate = studyEnd;
					}
					
					String trigClaim = pep.getClaim_id();
					
					for (ClaimServLine c : planMember.getServiceLines()) {
						
						// first make sure we don't use the triggering claim to confirm
						if (trigClaim==c.getClaim_id()) {
							continue;
						}
						
						if (c.getBegin_date() == null) {
							log.warn("Found claim with null begin date: " + c.getClaim_id());
							continue;
						}
						if (minSepDate == null) {
							log.warn("Found claim with null min sep date: " + c.getClaim_id());
							continue;
						}
						
						if (c.getBegin_date().before(minSepDate)) {
							//log.info("BEFORE");
							continue;
						}
						
						if (maxSepDate == null)
							continue;
						
						if (c.getBegin_date().after(maxSepDate)) {
							//log.info("AFTER");
							continue;
						}
						
						//if we get here we have a claim that is within the min/max separation and
						//can be checked as a potential confirming claim
						//log.info("POT CONF CLAIM " + c.getClaim_id() + " " + epid);
						HashMap<String, String> confCl=confirmClaim(c, epid);
						if (confCl.get("pass").equals("true")) {
							// CONFIRMED!!!
							//log.info("<<<!!!CONFIRMED!!!>>>: " + c.getClaim_id());
							pep.setConf_claim_id(c.getClaim_id());
							pep.setConf_claim(c);
							pep.setConf_dxPassCode(confCl.get("conf_dxPassCode"));
							pep.setConf_pxPassCode(confCl.get("conf_pxPassCode"));
							pep.setConf_emPassCode(confCl.get("conf_emPassCode"));
							break;
						} else {
							// NOT CONFIRMED
							//log.info("NOT confirmed: " + c.getClaim_id());
							continue;
						}
						
						
					}
					
					// the confirming claim id will still be zero at this point if none found, so drop the episode
					if (pep.getConf_claim_id().equals("0")) {
						pep.setDropped(true);
					}
					
				} // end confirming claim if
				
				/*
				 * If this trigger is now fully qualified, it can be checked to see if it can trigger
				 * another episode. Fully qualified means we have a shell (it passed the proc/EM/andor/diag
				 * for its claim type), if it requires a confirming claim, that has been resolved
				 * Shells that are fully qualified (excepting those triggered by other episodes): 
				 * either ( req_conf_claim false OR (req_conf_claim=true && conf_claim_id!=0) )
				 */
				
				//if ( !pep.isReq_conf_claim() || ( pep.isReq_conf_claim() && !pep.getConf_claim_id().equals("0") ) ) {
				if ( !pep.isDropped() ) {
					
					//log.info("Trying to trigger by episode: " + pep.getEpisode_id());
					
					HashMap<Integer, ArrayList<EpisodeShell>> rcEps = new HashMap<Integer, ArrayList<EpisodeShell>>();
					
					int runCount = 0;
					
					//arbitrary while should be replaced with some derivation of how many episodes could be triggered by this episode...
					while (runCount < 10) {
						
						runCount++;
						//log.info("runCount: " + runCount);
						
						//check if first run...
						if (runCount==1) {
							rcEps.put(runCount, triggerByEpisode(pep));
						}
						if (rcEps.get(runCount)==null) {
							break;
						} else {
							ArrayList<EpisodeShell> emptySet = new ArrayList<EpisodeShell>();
							rcEps.put(runCount+1, emptySet);
							if (rcEps.get(runCount)==null) {
								break;
							}
							for (EpisodeShell bPep : rcEps.get(runCount) ) {
								
								if(!bPep.getEpisode_id().isEmpty()) {
									
									// the episode must be confirmed before it can trigger another episode...
									if (bPep.isReq_conf_claim() && bPep.getConf_claim_id().equals("0")) {
										//log.info("mem / episode req conf: " + planMember.getMember_id() + " / " + pep.getEpisode_id());
										
										// we have an episode that requires a confirming claim, see if there is one...
										String epid = bPep.getEpisode_id();
										String minSepT = bPep.getMin_code_separation();
										String maxSepT = bPep.getMax_code_separation();
										Date minSepDate;
										Date maxSepDate;
										Date trigFromDate = bPep.getTrig_begin_date();
										
										//calculate min/max sep dates...
										Calendar minD = Calendar.getInstance();
										Calendar maxD = Calendar.getInstance();
										minD.setTime(trigFromDate);
										maxD.setTime(trigFromDate);
										
										int minSep = Integer.parseInt(minSepT);
										minD.add(Calendar.DATE, minSep);
										minSepDate = minD.getTime();
										
										int maxSep;
										if (!maxSepT.equals("EOS")) {
											maxSep = Integer.parseInt(maxSepT);
											maxD.add(Calendar.DATE, maxSep);
											maxSepDate = maxD.getTime();
										} else {
											maxSepDate = studyEnd;
										}
										
										String trigClaim = bPep.getClaim_id();
										
										for (ClaimServLine c : planMember.getServiceLines()) {
											
											// first make sure we don't use the triggering claim to confirm
											if (trigClaim==c.getClaim_id()) {
												continue;
											}
											
											//log.info("Ep Trig trCl: " + trigClaim + " C date: " + c.getBegin_date() + " minSep: " +  minSepDate + " minSepD: " + minSep);
											//log.info("Ep Trig cl: " + c.getClaim_id() + "orig date: " + trigFromDate);
											
											if (c.getBegin_date().before(minSepDate)) {
												//log.info("BEFORE");
												continue;
											}
											if (c.getBegin_date().after(maxSepDate)) {
												//log.info("AFTER");
												continue;
											}
											//if we get here we have a claim that is within the min/max separation and
											//can be checked as a potential confirming claim
											//log.info("POT CONF CLAIM " + c.getClaim_id() + " " + epid);
											HashMap<String, String> confCl=confirmClaim(c, epid);
											if (confCl.get("pass").equals("true")) {
												// CONFIRMED!!!
												//log.info("Ep Trig <<<!!!CONFIRMED!!!>>>: " + c.getClaim_id());
												bPep.setConf_claim_id(c.getClaim_id());
												bPep.setConf_claim(c);
												bPep.setConf_dxPassCode(confCl.get("conf_dxPassCode"));
												bPep.setConf_pxPassCode(confCl.get("conf_pxPassCode"));
												bPep.setConf_emPassCode(confCl.get("conf_emPassCode"));
												break;
											} else {
												// NOT CONFIRMED
												continue;
											}
											
											
										}
										
										// the confirming claim id will still be zero at this point if none found, so drop the episode
										if (bPep.getConf_claim_id().equals("0")) {
											bPep.setDropped(true);
										}
										
									} // end conf for
								}
								
								//if ( !bPep.isReq_conf_claim() || (bPep.isReq_conf_claim() && !bPep.getConf_claim_id().equals("0")) ) {
								if ( !bPep.isDropped() ) {
									for (EpisodeShell rPep : triggerByEpisode(bPep)) {
										
										if (!rPep.getEpisode_id().isEmpty()) {
											rcEps.get(runCount+1).add(rPep);
										}
									}
								}
							}
						}
						
						
						
					} // end runcount while
					
					// add the episode triggered episodes to the addlProvEps so we can add them to the final list of provEps
					if (!rcEps.isEmpty()) {
						
						for (int rc : rcEps.keySet()) {
							//log.info("AMIHERE?");
							for (EpisodeShell es : rcEps.get(rc)) {
								if (es.getEpisode_id()!=null) {
									if (!es.isDropped()) {
										addlEpsforMember.add(es);
									}
								}
							}
						}
					}
				}
				
			} // end for prov eps loop
			
		}
		
	}
	
	/*
	 * this will create new episodes based on existing triggers AND
	 * it will loop to see if the new episode triggers anything else
	 * MUST find a way to break out of infinite loops... say maybe no more than 5 times?
	 */
	public ArrayList<EpisodeShell> triggerByEpisode(EpisodeShell pep) {

		ArrayList<EpisodeShell> epTeps = new ArrayList<EpisodeShell>();
		String epid=pep.getEpisode_id();
		
		// see if any episodes are triggered by epid...
		//for (String epToTrig : trigEpisodes.keySet()) {
		for (String epToTrig : epmdk.keySet()) {
			
			//if this episode can't be triggered by another episode continue...
			if ( epmdk.get(epToTrig).getTrigger_conditions_ep_trig().isEmpty()) {
				continue;
			}
			
			
			for (String epToFind : epmdk.get(epToTrig).getTrigger_conditions_ep_trig().keySet() ) {
				if (epid.equals(epToFind)) {
					
					//trigger a new episode for epToTrig
					EpisodeShell newPep = new EpisodeShell();
					
					TriggerConditionMetaData npeptcmd = epmdk.get(epToTrig).getTrigger_conditions_ep_trig().get(epToFind);
					
					Calendar cal = Calendar.getInstance();
					cal.setTime(pep.getTrig_begin_date());
					Date bdate;
					int testlb = epmdk.get(epToTrig).getLook_back();
					cal.add(Calendar.DATE, testlb);
					bdate = cal.getTime();
					Date edate;
					
					String testlf = epmdk.get(epToTrig).getLook_ahead();
					
					if (testlf.equals("EOS")) {
						
						edate = studyEnd;
						//cal.setTime(pep.getTrig_end_date());
						//cal.add(Calendar.DATE, 54750); // plus approx 150 years
						//edate = cal.getTime();

					} else {
						cal.setTime(pep.getTrig_end_date());
						cal.add(Calendar.DATE, Integer.parseInt(testlf));
						edate = cal.getTime();
					}
					
					newPep.setClaim_id(pep.getClaim_id());
					newPep.setClaim(pep.getClaim());
					newPep.setEpisode_id(epToTrig);
					newPep.setMember_id(pep.getMember_id());
					newPep.setEpisode_type(ParseEpType(epToTrig));
					newPep.setTrig_begin_date(pep.getTrig_begin_date());
					newPep.setTrig_end_date(pep.getTrig_end_date());
					newPep.setOrig_episode_begin_date(bdate);
					newPep.setOrig_episode_end_date(edate);
					// date of death truncation:
					if (planMember != null && planMember.getDate_of_death() != null) {
						if (planMember.getDate_of_death().before(edate)) {
							edate=planMember.getDate_of_death();
						}
					}
					newPep.setEpisode_begin_date(bdate);
					newPep.setEpisode_end_date(edate);
					newPep.setLook_ahead(epmdk.get(epToTrig).getLook_ahead());
					newPep.setLook_back(epmdk.get(epToTrig).getLook_back());
					newPep.setReq_conf_claim(npeptcmd.getReq_conf_code());
					newPep.setConf_claim_id("0");
					newPep.setMin_code_separation(npeptcmd.getMin_code_separation());
					newPep.setMax_code_separation(npeptcmd.getMax_code_separation());
					newPep.setTrig_by_episode_id(epToFind);
					newPep.setTrig_by_master_episode_id(pep.getMaster_episode_id());
					newPep.setTrig_by_episode(true);
					
					//log.info("EPISODE: " + epToFind + " TRIGGERED: " + epToTrig);
					
					//provEpisodes.get(newPep.getMember_id()).add(newPep);
					 
					epTeps.add(newPep);
					
					
					
				}
			}
		}
		return epTeps;
	}
	
	public HashMap<String, String> confirmClaim(ClaimServLine c, String episode_id) {
		
		String pxcond;
		String emcond;
		String andor;
		String dxcond;
		
		boolean pxpass;
		boolean empass;
		boolean dxpass;
		
		String pxPassCode;
		String dxPassCode;
		String emPassCode;
		
		//boolean reqConfClaim;
		
		//String minSep = "";
		//String maxSep = "";
		
		// set claim type trigger meta values
		pxcond=null;
		emcond=null;
		andor=null;
		dxcond=null;
		
		//reqConfClaim=false;
		
		//minSep = "";
		//maxSep = "";
		
		//set holders to know what we've matched
		pxpass=false;
		empass=false;
		dxpass=false;
		
		pxPassCode=null;
		dxPassCode=null;
		emPassCode=null;
		
		//for returning results...
		HashMap<String, String> returnMap = new HashMap<String, String>();
		
		Date icd10Day = new Date();
		DateFormat df = new SimpleDateFormat("MM/dd/yyyy");
		try {
			icd10Day = df.parse("09/30/2015");
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		String type_code = c.getClaim_line_type_code();
		
		String dk="DX";
		String pk="PX";
		if (c.getBegin_date().after(icd10Day)) {
			dk="DXX";
			pk="PXX";
		}
		String diagKey = c.getDiag_claim_nomen();
		if (diagKey == null) { diagKey = dk; } else {
			if (c.getBegin_date().after(icd10Day)) {
				if (diagKey.equals("DX")) {
					diagKey="DXX";
					c.setDiag_claim_nomen("DXX");
				}
			}
		}
		String procKey = c.getProc_claim_nomen();
		if (procKey == null) { procKey = pk; } else {
			if (c.getBegin_date().after(icd10Day)) {
				if (procKey.equals("PX")) {
					procKey="PXX";
					c.setProc_claim_nomen("PXX");
				}
			}
		}
		//String type_code = c.getClaim_line_type_code();
		
		for (MedCode _mc : c.getMed_codes()) {
			if (_mc != null && 
				_mc.getNomen() !=null && 
				_mc.getCode_value() != null && 
				!_mc.getCode_value().isEmpty()
				) {
				if (_mc.getNomen().equals("DX")) {
					_mc.setNomen(dk);
				}
				if (_mc.getNomen().equals("PX")) {
					_mc.setNomen(pk);
				}
			}
		}
		
		MedCode pdco = c.getPrincipal_diag_code_object();
		List<MedCode> ppcos = c.getPrincipal_proc_code_objects();
		List<MedCode> sdcos = c.getSecondary_diag_code_objects();
		List<MedCode> spcos = c.getSecondary_proc_code_objects();
		
		switch (type_code) {
		case "IP":
			//pxcond = trigParamsByEpisode.get(episode_id).get("IC").getPx_code_position();
			pxcond = epmdk.get(episode_id).getTrigger_conditions_by_fac().get("IC").getPx_code_position();
			emcond =  epmdk.get(episode_id).getTrigger_conditions_by_fac().get("IC").getEm_code_position();
			andor = epmdk.get(episode_id).getTrigger_conditions_by_fac().get("IC").getAnd_or();
			dxcond = epmdk.get(episode_id).getTrigger_conditions_by_fac().get("IC").getDx_code_position();
			//minSep = epmdk.get(episode_id).getTrigger_conditions_by_fac().get("IC").getMin_code_separation();
			//maxSep = epmdk.get(episode_id).getTrigger_conditions_by_fac().get("IC").getMax_code_separation();
			break;
		case "OP":
			pxcond = epmdk.get(episode_id).getTrigger_conditions_by_fac().get("OC").getPx_code_position();
			emcond = epmdk.get(episode_id).getTrigger_conditions_by_fac().get("OC").getEm_code_position();
			andor = epmdk.get(episode_id).getTrigger_conditions_by_fac().get("OC").getAnd_or();
			dxcond = epmdk.get(episode_id).getTrigger_conditions_by_fac().get("OC").getDx_code_position();
			//minSep = epmdk.get(episode_id).getTrigger_conditions_by_fac().get("OC").getMin_code_separation();
			//maxSep = epmdk.get(episode_id).getTrigger_conditions_by_fac().get("OC").getMax_code_separation();
			break;
		case "PB":
			pxcond = epmdk.get(episode_id).getTrigger_conditions_by_fac().get("PC").getPx_code_position();
			emcond = epmdk.get(episode_id).getTrigger_conditions_by_fac().get("PC").getEm_code_position();
			andor = epmdk.get(episode_id).getTrigger_conditions_by_fac().get("PC").getAnd_or();
			dxcond = epmdk.get(episode_id).getTrigger_conditions_by_fac().get("PC").getDx_code_position();
			//reqConfClaim = epmdk.get(episode_id).getTrigger_conditions_by_fac().get("PC").getReq_conf_code();
			//minSep = epmdk.get(episode_id).getTrigger_conditions_by_fac().get("PC").getMin_code_separation();
			//maxSep = epmdk.get(episode_id).getTrigger_conditions_by_fac().get("PC").getMax_code_separation();
			break;
		default:
			pxcond = "None";
			emcond = "None";
			andor = "None";
			dxcond = "None";
		}
		// find the blank parameters...
		boolean nopx = false;
		boolean noem = false;
		boolean nodx = false;
		if (pxcond==null || pxcond.isEmpty() || pxcond.equals("None")) { nopx=true; pxcond=null;}
		if (emcond==null || emcond.isEmpty() || emcond.equals("None")) { noem=true; emcond=null;}
		if (dxcond==null || dxcond.isEmpty() || dxcond.equals("None")) { nodx=true; dxcond=null;}
		if (andor==null) { andor="Or"; }

		
		// if all px/e+m/dx conditions are null/None, discard this as a potential episode
		if (nopx && noem && nodx) {
			returnMap.put("pass", "false");
			return returnMap;
		}
		
/*
		// lets start by trying to trigger a record on primary dx...
		//log.info("and or is " + andor);
		if ( dxcond != null) {
			//if (trigCodesByEpisode.get(episode_id).get(diagKey) != null && c.getPrincipal_diag_code() != null ) {
			if (epmdk.get(episode_id).getTrigger_code_by_ep().containsKey(diagKey) && c.getPrincipal_diag_code() != null ) {
				//if (trigCodesByEpisode.get(episode_id).get(diagKey).contains(c.getPrincipal_diag_code())) {
				if (epmdk.get(episode_id).getTrigger_code_by_ep().get(diagKey).containsKey(c.getPrincipal_diag_code())) {
					//log.info("triggered a prin diag episode!!!: " + episode_id + " " + c.getPrincipal_diag_code());
					//log.info("diag param passes: " + episode_id + " " + c.getPrincipal_diag_code());
					dxpass=true;
					dxPassCode=c.getPrincipal_diag_code();
				}
			}
		}
		
		// try to match on secondary dx if dxcond = "any"
		if ( dxcond!=null && dxcond.equals("Any") && !dxpass ) {
			if ( epmdk.get(episode_id).getTrigger_code_by_ep().containsKey(diagKey) ) {
				for (String sdc : c.getSecondary_diag_code() ) {
					if (sdc!=null && epmdk.get(episode_id).getTrigger_code_by_ep().get(diagKey).containsKey(sdc)) {
						dxpass=true;
						dxPassCode=sdc;
						break;
					}
				}
			}
		}
		
		// try to match principal proc on this ep for this claim...
		if ( pxcond != null) {
			if (epmdk.get(episode_id).getTrigger_code_by_ep().containsKey(procKey) ) {
				for (String pc : c.getPrincipal_proc_code()) {
					if (pc != null && !pc.isEmpty()) {
						
						if (epmdk.get(episode_id).getTrigger_code_by_ep().get(procKey).containsKey(pc)) {
							
							pxpass=true;
							pxPassCode=pc;
							break;
						}
						
					}
				}
			}
		}
		
		// try to match on secondary px if pxcond = "any"
		if ( pxcond!=null && pxcond.equals("Any") && !pxpass ) {
			if ( epmdk.get(episode_id).getTrigger_code_by_ep().containsKey(procKey) ) {
				for (String sdc : c.getSecondary_proc_code() ) {
					if (sdc!=null && epmdk.get(episode_id).getTrigger_code_by_ep().get(procKey).containsKey(sdc)) {
						pxpass=true;
						pxPassCode=sdc;
						break;
					}
				}
			}
		}
		
		// try to match principal proc to e&m on this ep for this claim...
		if ( emcond != null) {
			
			for (String pc : c.getPrincipal_proc_code()) {
				if (pc != null && !pc.isEmpty()) {
					
					if (emCodes.contains(pc)) {
						
						empass=true;
						emPassCode=pc;
						break;
					}
					
				}
			}
			
		}
		
		// try to match secondary proc to e&m if emcond is "Any"
		if ( emcond!=null && emcond.equals("Any") && !empass ) {
			for (String sdc : c.getSecondary_proc_code() ) {
				if (sdc!=null && emCodes.contains(sdc)) {
					empass=true;
					emPassCode=sdc;
					break;
				}
			}
		}
		
		
*/	
		
		

		
		
		// lets start by trying to trigger a record on primary dx...
		//log.info("and or is " + andor);
		if ( dxcond != null) {
			if (pdco != null && 
				pdco.getNomen() !=null && 
				pdco.getCode_value() != null && 
				!pdco.getCode_value().isEmpty()
				) {
				if (epmdk.get(episode_id).getTrigger_code_by_ep().containsKey(pdco.getNomen()) ) {
					if (epmdk.get(episode_id).getTrigger_code_by_ep().get(pdco.getNomen()).containsKey(pdco.getCode_value())) {
						//log.info("triggered a prin diag episode!!!: " + episode_id + " " + c.getPrincipal_diag_code());
						//log.info("diag param passes: " + episode_id + " " + c.getPrincipal_diag_code());
						dxpass=true;
						dxPassCode=pdco.getCode_value();
					}
				}
			}
		}
					
		// try to match on secondary dx if dxcond = "any"
		if ( dxcond!=null && dxcond.equals("Any") && !dxpass ) {
			for (MedCode sdco : sdcos) {
				if (pdco != null && 
				pdco.getNomen() !=null && 
				pdco.getCode_value() != null && 
				!pdco.getCode_value().isEmpty()) {
					
					if (epmdk.get(episode_id).getTrigger_code_by_ep().containsKey(sdco.getNomen()) ) {
						if (epmdk.get(episode_id).getTrigger_code_by_ep().get(sdco.getNomen()).containsKey(sdco.getCode_value())) {
							dxpass=true;
							dxPassCode=sdco.getCode_value();
							break;
						}
					}
				}
			}
		}
		
		// try to match principal proc on this ep for this claim...
		if ( pxcond != null) {
			for (MedCode ppco : ppcos) {
				if (ppco != null && 
					ppco.getNomen() !=null && 
					ppco.getCode_value() != null && 
					!ppco.getCode_value().isEmpty()) {
					if (epmdk.get(episode_id).getTrigger_code_by_ep().containsKey(ppco.getNomen()) ) {
							
						if (epmdk.get(episode_id).getTrigger_code_by_ep().get(ppco.getNomen()).containsKey(ppco.getCode_value())) {
							pxpass=true;
							pxPassCode=ppco.getCode_value();
							break;
						}
	
					}
					if (ppco.getNomen().equals("HCPC")) {
						if (epmdk.get(episode_id).getTrigger_code_by_ep().containsKey("CPT") ) {
							
							if (epmdk.get(episode_id).getTrigger_code_by_ep().get("CPT").containsKey(ppco.getCode_value())) {
								pxpass=true;
								pxPassCode=ppco.getCode_value();
								break;
							}
		
						}
					}
					if (ppco.getNomen().equals("CPT")) {
						if (epmdk.get(episode_id).getTrigger_code_by_ep().containsKey("HCPC") ) {
							
							if (epmdk.get(episode_id).getTrigger_code_by_ep().get("HCPC").containsKey(ppco.getCode_value())) {
								pxpass=true;
								pxPassCode=ppco.getCode_value();
								break;
							}
		
						}
					}
				}
			}
		}
		
		// try to match on secondary px if pxcond = "any"
		if ( pxcond!=null && pxcond.equals("Any") && !pxpass ) {
			for (MedCode spco : spcos) {
				if (spco != null && 
					spco.getNomen() !=null && 
					spco.getCode_value() != null && 
					!spco.getCode_value().isEmpty()) {
					if ( epmdk.get(episode_id).getTrigger_code_by_ep().containsKey(spco.getNomen()) ) {

						if (epmdk.get(episode_id).getTrigger_code_by_ep().get(spco.getNomen()).containsKey(spco.getCode_value())) {
							pxpass=true;
							pxPassCode=spco.getCode_value();
							break;
						}

					}
					if (spco.getNomen().equals("HCPC")) {
						if ( epmdk.get(episode_id).getTrigger_code_by_ep().containsKey("CPT") ) {
	
							if (epmdk.get(episode_id).getTrigger_code_by_ep().get("CPT").containsKey(spco.getCode_value())) {
								pxpass=true;
								pxPassCode=spco.getCode_value();
								break;
							}
	
						}
					}
					if (spco.getNomen().equals("CPT")) {
						if ( epmdk.get(episode_id).getTrigger_code_by_ep().containsKey("HCPC") ) {

							if (epmdk.get(episode_id).getTrigger_code_by_ep().get("HCPC").containsKey(spco.getCode_value())) {
								pxpass=true;
								pxPassCode=spco.getCode_value();
								break;
							}

						}
					}
				}
			}
		}
		
		// try to match principal proc to e&m on this ep for this claim...
		// 5.3 rule, em must also be in typical px list...
		if ( emcond != null) {
			for (MedCode ppco : ppcos) {
				if (ppco != null && 
					ppco.getNomen() !=null && 
					ppco.getCode_value() != null && 
					!ppco.getCode_value().isEmpty()) {
					
					if (ppco.getNomen().equals("HCPC") || ppco.getNomen().equals("CPT") ) {
					
						if (emCodes.contains(ppco.getCode_value())) {
							
							
							// 5.3 also check to see if it is typical px...
							boolean isP = false;
							for (PxCodeMetaData px : epmdk.get(episode_id).getPx_codes()) {
								if (px.getCode_id().equals(ppco.getCode_value())) {
									isP=true;
									break;
								}
							}
							
							if (isP) {
								empass=true;
								emPassCode=ppco.getCode_value();
								break;
							}		

						}
					}
				}
			}
		}
		
		
		
		// try to match E&M to secondary px if pxcond = "any"
		// 5.3 update, E&M must be in typical px list...
		if ( emcond!=null && emcond.equals("Any") && !empass ) {
			for (MedCode spco : spcos) {
				if (spco != null && 
					spco.getNomen() !=null && 
					spco.getCode_value() != null && 
					!spco.getCode_value().isEmpty() &&
					(spco.getNomen().equals("HCPC") || spco.getNomen().equals("CPT"))
					) {
					if (emCodes.contains(spco.getCode_value())) {
						
						// 5.3 also check to see if it is typical px...
						boolean isP = false;
						for (PxCodeMetaData px : epmdk.get(episode_id).getPx_codes()) {
							if (px.getCode_id().equals(spco.getCode_value())) {
								isP=true;
								break;
							}
						}
						
						if (isP) {
							empass=true;
							emPassCode=spco.getCode_value();
							break;
						}
					}
				}
			}
		}


	
		
		@SuppressWarnings("unused")
		String logstring = "";
		@SuppressWarnings("unused")
		String andorCond = "";
		//String tstConfClaim = "";
		// pEp = new EpisodeShell();
		//EpisodeShell pEp = new EpisodeShell();
		boolean epPass = false;
		if (pxpass || dxpass || empass) {
			if (pxpass) { logstring += "PX passed "; }
			if (dxpass) { logstring += "DX passed "; }
			if (empass) { logstring += "EM passed "; }
			if (andor.equals("And")) {
				if ( (pxpass || empass) && dxpass) {
					andorCond = "andor cond PASSED ";
					epPass = true;
					//return true;
				} else {
					andorCond = " andor cond FAILED ";
					//return false;
				}
			} else {
				epPass = true;
				//return true;
			}
			
		} else {
			//return false;
		}
		
		
		if (epPass) {
			returnMap.put("pass", "true");
			returnMap.put("conf_dxPassCode", dxPassCode);
			returnMap.put("conf_pxPassCode", pxPassCode);
			returnMap.put("conf_emPassCode", emPassCode);
		} else {
			returnMap.put("pass", "false");
		}
		return returnMap;
	}
	
	
	
	
	
	
	public void resolveEpisodes() {
		/*
		 * for each episode the member has, look to see if there are other episodes
		 * of the same type. If there are, look to see if they trigger within the episode
		 * window. If so, they are dropped (unless procedural, in which case truncate). 
		 * Ignore SRFs. Ignore episodes triggered by other episodes on the first run? because
		 * those will be dropped if the subsidiary episode is dropped.
		 * 
		 */
		//log.info("Resolve Episodes: " + epsForMember.size());
		for (EpisodeShell es1 : epsForMember) {
			// if already dropped, do nothing...
			if (es1.isDropped()) {
				//log.info("es1 is dropped");
				continue;
			}
			// if this is an SRF, do nothing
			if (es1.getEpisode_type().equals("S")) {
				continue;
			}
			
			String sEpId = es1.getEpisode_id();
			
			// if this is a Procedural, DROP if in TRIGGER window, will truncate later
			if (es1.getEpisode_type().equals("P")) {
				
				for (EpisodeShell es2 : epsForMember) {
					
					// if already dropped, do nothing
					if (es2.isDropped()) {
						continue;  
					}

					boolean es1Win=false;
					boolean es2Win=false;
					
					// only looking at procedural episodes...
					if (!es2.getEpisode_type().equals("P")) {
						continue;
					}
					
					// if the two episodes aren't the same kind, don't do anything
					if (!es2.getEpisode_id().equals(sEpId)) {
						continue;
					}
					
					// if they are the same episode, continue (don't compare the episode to itself)...
					if ( es2.equals(es1) ) {
						continue;
					}
					
					// now should have es1 and es2 same episode id, but not same episode trigger *line*
					
					/*
					 * Several things need to happen for procedural triggers
					 * 1) if the begin dates are the same, use the usual IP/OP/PB preference to choose
					 * 2) if either is later, BUT within the trigger window, it should be dropped
					 * 2) change - 5.3.001 - extend trigger window by 2 days; +2 days to trigger end date
					 * 3) if either is later, BUT within the episode window, truncate (see rules) 
					 * 
					 * For now, just drop the ones in the window
					 */
					
					// compare es1 trig to es2 trig begin... 0 means same date, 
					// negative number es1 is before
					// positive number es1 is after
					int trigBeginCompare = es1.getTrig_begin_date().compareTo(es2.getTrig_begin_date());
					
					/*
					Date newEpeEd;
					Calendar epeEd = Calendar.getInstance();
					epeEd.setTime(epL.getTrig_begin_date());
					epeEd.add(Calendar.DATE, -1);
					newEpeEd = epeEd.getTime();
					*/
					// the two trigger dates are the same...
					if ( trigBeginCompare==0 ) {
						switch (es1.getClaim().getClaim_line_type_code()) {
							case "IP":
								es1Win=true;
								break;
							case "OP":
								if (es2.getClaim().getClaim_line_type_code().equals("IP") ) {
									es2Win=true;
								} else {
									es1Win=true;
								}
								break;
							case "PB": 
								if (es2.getClaim().getClaim_line_type_code().equals("IP")  || es2.getClaim().getClaim_line_type_code().equals("OP") ) {
									es2Win=true;
								} else {
									es1Win=true;
								}
								break;							
						}
					} else
					/*
					 * NOW ordered by date, just skip if episode 1 is later...
					 */
					// es1 is LATER
					if ( trigBeginCompare>0 ) {
						// if es1 trig begin date is equal to or less than in compare (earlier than es2 end TRIGGER), it is in the T window and should be dropped.
						//if (es1.getTrig_begin_date().compareTo(es2.getTrig_end_date())<=0) {
						//	es2Win=true;
						//}
						continue;
					} else
					// es1 is EARLIER
					if ( trigBeginCompare<0 ) {
						// if es2 trig begin date is equal to or less than in compare (earler than es1 end window), it is in the window and should be dropped.
						
						//New 5.3.001 rule, extend the P trigger end date by two days and drop if the next is within that...
						
						// add two days to the trigger end date...
						Date newTrigEnd;
						Calendar tEnd = Calendar.getInstance();
						tEnd.setTime(es1.getTrig_end_date());
						tEnd.add(Calendar.DATE, 2);
						newTrigEnd = tEnd.getTime();
						
						// compare to the new trigger end date so es2 will be dropped if this condition is true...
						if (es2.getTrig_begin_date().compareTo(newTrigEnd)<=0) {
							es1Win=true;
						}
					}
					
					//resolve es1 / es2 win...
					if (es1Win) {
						//log.info("es2 dropped");
						es2.setDropped(true);
						es2.setWin_claim_id(es1.getClaim_id());
						es2.setWin_master_episode_id(es1.getMaster_episode_id());
					}
					// this should never happen because of the continue above in the if
					if (es2Win) {
						//log.info("es2 dropped");
						es1.setDropped(true);
						es1.setWin_claim_id(es2.getClaim_id());
						es1.setWin_master_episode_id(es2.getMaster_episode_id());
					}
					
				} // end es2 loop
				
				
				
				continue;
			} // END "P" processing
			else {
			
				/* going to allow episodes triggered by other episodes because
				 * first pass won't have those
				 */
				// don't bother with episodes triggered by other episodes yet...
				//if ( es1.getTrig_by_episode_id()!=null && !es1.getTrig_by_episode_id().isEmpty() ) {
					//continue;
				//}
				
				
				
				for (EpisodeShell es2 : epsForMember) {
	
					boolean es1Win=false;
					boolean es2Win=false;
					boolean es2Truncate=false;
					// if already dropped, do nothing
					if (es2.isDropped()) {
						continue;  
					}
					// again, ignore SRFs...
					if (es2.getEpisode_type().equals("S")) {
						continue;
					}
					// don't bother with episodes triggered by other episodes yet...
					//if ( es2.getTrig_by_episode_id()!=null && !es2.getTrig_by_episode_id().isEmpty() ) {
					//	continue;
					//}
					// if the two episodes aren't the same kind, don't do anything
					if (!es2.getEpisode_id().equals(sEpId)) {
						continue;
					}
					
					// if they are the same episode, continue (don't compare the episode to itself)...
					if ( es2.equals(es1) ) {
						continue;
					}
					
					
					// now should have es1 and es2 same episode id, but not same episode trigger *line*
					
					// check trigger begin dates of the two episodes, whichever is latest get dropped
					// IF the latest is within the episode window
					// if they are the same, IP/OP/PB preference
					
					// compare es1 trig to es2 trig begin... 0 means same date, 
					// negative number es1 is before
					// positive number es1 is after
					int trigBeginCompare = es1.getTrig_begin_date().compareTo(es2.getTrig_begin_date());
					
					// the two trigger dates are the same...
					if ( trigBeginCompare==0 ) {
						
						switch (es1.getClaim().getClaim_line_type_code()) {
							case "IP":
								es1Win=true;
								break;
							case "OP":
								if (es2.getClaim().getClaim_line_type_code().equals("IP") ) {
									es2Win=true;
								} else {
									es1Win=true;
								}
								break;
							case "PB": 
								if (es2.getClaim().getClaim_line_type_code().equals("IP")  || es2.getClaim().getClaim_line_type_code().equals("OP") ) {
									es2Win=true;
								} else {
									es1Win=true;
								}
								break;							
						}
						
					} else
					//NOW ordered by date, ignore es1 later...
					// es1 is LATER
					if ( trigBeginCompare>0 ) {
						// if es1 trig begin date is equal to or less than in compare (earlier than es2 end window), it is in the window and should be dropped.
						//if (es1.getTrig_begin_date().compareTo(es2.getEpisode_end_date())<=0) {
						//	es2Win=true;
						//}
						continue;
					} else 
					// es1 is EARLIER
					if ( trigBeginCompare<0 ) {
						// if es2 trig begin date is equal to or less than in compare (earler than es1 end window), it is in the window and should be dropped.
						if (es2.getEpisode_begin_date().compareTo(es1.getEpisode_end_date())<=0) {
							// at this point, we know that they overlap. If the trigger of es2 (below
							// doesn't occur before the end of es1, we'll just truncate the lookback...
							es2Truncate=true;
							
							if (!es2.getClaim().getBegin_date().after(es1.getEpisode_end_date()) ) {
								//the trigger of es2 occurs on or before the end of es1, drop it
								es1Win=true;
							}
						}
					}
					
					//resolve es1 / es2 win...
					if (es1Win) {
						//log.info("es2 dropped");
						es2.setDropped(true);
						es2.setWin_claim_id(es1.getClaim_id());
						es2.setWin_master_episode_id(es1.getMaster_episode_id());
					}
						
					if (es2Win) {
						//log.info("es2 dropped");
						es1.setDropped(true);
						es1.setWin_claim_id(es2.getClaim_id());
						es1.setWin_master_episode_id(es2.getMaster_episode_id());
					}
					
					if (es2Truncate && !es1Win) {
						// truncate the lookback of es2 to the day after es1
						
						Date newEplBd;
						Calendar eplBd = Calendar.getInstance();
						eplBd.setTime(es1.getEpisode_end_date());
						eplBd.add(Calendar.DATE, 1);
						newEplBd = eplBd.getTime();

						es2.setTruncated(true);
						es2.setEpisode_begin_date(newEplBd);
						
					}
					
				} // end es2 loop
			
			} // end "P" if
		}
	}
	
	public void resolveDroppedEpisodeTriggers() {
		// find the episode triggered episodes and drop them if triggered by a dropped episode
		for(EpisodeShell esa : addlEpsforMember) {
			for (EpisodeShell eso : epsForMember) {
				/*
				 * just in case there's eventually a situation where an episode triggered by
				 * an episode triggers an episode, the middle episode might be used to compare
				 * and we only want to use the lowest level trigger to check for dropping
				 * the episodes triggered by episodes (we don't know the order, so the ete might
				 * not have been dropped yet)
				 */
				//if ( eso.getTrig_by_episode_id()==null || eso.getTrig_by_episode_id().isEmpty() ) {
				if (!eso.isTrig_by_episode()) {
					if ( esa.getTrig_by_episode_id().equals(esa.getEpisode_id()) && 
							eso.getClaim_id().equals(esa.getClaim_id()) ) {
						// esa should be an episode triggered by eso's claim
						if ( eso.isDropped() ) {
							// drop esa...
							esa.setDropped(true);
							esa.setWin_claim_id(eso.getConf_claim_id());
							esa.setWin_master_episode_id(eso.getMaster_episode_id());
						}
					}
				}
			}
		}
	}
	
	
	
	public void truncateProcedurals() {
		for ( EpisodeShell es1 : epsForMember ) {
			// Don't bother if dropped
			if ( es1.isDropped() ) {
				continue;
			}
			// Don't bother if not procedural
			if ( !es1.getEpisode_type().equals("P") ) {
				continue;
			}
			
			for ( EpisodeShell es2 : epsForMember ) {
				// Don't bother if dropped
				if ( es2.isDropped() ) {
					continue;
				}
				// Don't bother if not procedural
				if ( !es2.getEpisode_type().equals("P") ) {
					continue;
				}
				// If they aren't the same episode type, don't bother
				if ( !es1.getEpisode_id().equals(es2.getEpisode_id()) ) {
					continue;
				}
				// if they are the same episode, continue (don't compare the episode to itself)...
				if ( es2.equals(es1) ) {
					continue;
				}
				
				
				// neither shell should be within each others' trigger window
				// but either might be in the other's episode window
				// follow truncation rules for what situation is found...
				
				// check to see which episode's begin date falls within the other's episode window
				// remember that they could already have been truncated...
				boolean es2Later = false;
				if ( es2.getTrig_begin_date().compareTo(es1.getTrig_begin_date()) > 0 ) {
					es2Later=true;
				}
				if ( es2.getTrig_begin_date().compareTo(es1.getTrig_begin_date()) == 0 ) {
					//es2Later=null;
					//log.info("procedural episodes matched trig begin dates in proc truncation");
				}
				
				EpisodeShell epL;
				EpisodeShell epE;
				
				if (es2Later) {
					epL = es2;
					epE = es1;
				} else {
					epL = es1;
					epE = es2;
				}
				
				// find out if the later trigger start date is within the episode window
				if ( epL.getTrig_begin_date().compareTo(epE.getEpisode_end_date())<=0 ) {
					// truncate according to first rule...
					
					// find day before later episode trigger start date to end first episode
					Date newEpeEd;
					Calendar epeEd = Calendar.getInstance();
					epeEd.setTime(epL.getTrig_begin_date());
					epeEd.add(Calendar.DATE, -1);
					newEpeEd = epeEd.getTime();
					
					//don't set new orig dates if already set
					//if (!epE.isTruncated()) {
					//	epE.setOrig_episode_begin_date(epE.getEpisode_begin_date());
					//	epE.setOrig_episode_end_date(epE.getEpisode_end_date());
					//}
					epE.setTruncated(true);
					epE.setWin_claim_id(epL.getClaim_id());
					epE.setWin_master_episode_id(epL.getMaster_episode_id());
					epE.setEpisode_end_date(newEpeEd);
					
					
					//Set later episode start date to Trigger Start Date...
					//if (!epL.isTruncated()) {
					//	epL.setOrig_episode_begin_date(epL.getEpisode_begin_date());
					//	epL.setOrig_episode_end_date(epL.getEpisode_end_date());
					//}
					epL.setTruncated(true);
					epL.setWin_claim_id(epE.getClaim_id());
					epL.setWin_master_episode_id(epE.getMaster_episode_id());
					epL.setEpisode_begin_date(epL.getTrig_begin_date());
					
					
				} else {
					// the trigger date isn't in the episode window, is the episode begin date?
					if ( epL.getEpisode_begin_date().compareTo(epE.getEpisode_end_date())<=0 ) {
						// later episode should be truncated to end the day after the first eps window ends
						Date newEplBd;
						Calendar eplBd = Calendar.getInstance();
						eplBd.setTime(epE.getEpisode_end_date());
						eplBd.add(Calendar.DATE, 1);
						newEplBd = eplBd.getTime();
						
						//if (!epL.isTruncated()) {
						//	epL.setOrig_episode_begin_date(epL.getEpisode_begin_date());
						//	epL.setOrig_episode_end_date(epL.getEpisode_end_date());
						//}
						epL.setTruncated(true);
						epL.setWin_claim_id(epE.getClaim_id());
						epL.setWin_master_episode_id(epE.getMaster_episode_id());
						epL.setEpisode_begin_date(newEplBd);
						
						
					}
					
				} // end trigger in window compare
				
				
				
				
				
			}// end es2 loop
			
		}
	}
	

	
	public void ipAssignment() { // includes SRF resolution!
		
		//epsForMember  is the list of episodes we need to review...
		//planMember.getServiceLines() is the list of claims...
		
		//start IP claim assignment
		
		
		
		for (ClaimServLine c : planMember.getServiceLines()) {
			
			//don't bother with non-IP claims right now
			if (!c.getClaim_line_type_code().equals("IP")) {
				continue;
			}
			
			// only short term facility should trigger with IP...
			//if (!c.getFacility_type_code().equals("ST")) {
			//	continue; 
			//}
			
			// is the claim a trigger? if so, see if it is a trigger for an A or P...
			
			
			//1.1.0 - any P eps for which this claim is a trigger? 
			
			for (EpisodeShell es : epsForMember) {
				
				if (c.getFacility_type_code() == null ||
						c.getFacility_type_code().equals("") ||
						c.getFacility_type_code().equals("ST") ) 
				{
					// is OK to be used for this rule...
				} else {
					continue; 
				}
				
				if (es.isDropped()) {
					continue;
				}
				
				if (!es.getClaim().equals(c) ) {
					continue;
				}
				
				
				if (es.getEpisode_type().equals("P") ) {
					// we have matched the 110 condition at this point, mark it as so
					// mark the c as assigned so it won't be matched later.
					c.setAssigned(true);
					
					AssignedClaim as = new AssignedClaim();
					
					as.setCategory("T");
					as.setClaim(c);
					as.setRule("1.1.2");
					
					if (isComplication(c, es.getEpisode_id(), "s")) {
						as.setCategory("TC");
						as.setRule("1.1.1");
					}
					
					// put the assigned claim in the episode...
					es.getAssigned_claims().add(as);
					
				}
				
			} // end rule 1.1.0
			
			// if this claim is assigned to anything, don't look to match further rules, go to next claim. 
			if (c.isAssigned()) {
				continue;
			}
			 
			//begin rule 1.2.0
			
			//1.2.0 - any A eps for which this claim is a trigger?
			
			for (EpisodeShell es : epsForMember) {
				
				if (c.getFacility_type_code() == null ||
						c.getFacility_type_code().equals("") ||
						c.getFacility_type_code().equals("ST") ) 
				{
					// is OK to be used for this rule...
				} else {
					continue; 
				}
				
				if (es.isDropped()) {
					continue;
				}
				
				if (!es.getClaim().equals(c) ) {
					continue;
				}
				
				
				if (es.getEpisode_type().equals("A") ) {
					// we have matched the 120 condition at this point, mark it as so
					// mark the c as assigned so it won't be matched later.
					c.setAssigned(true);
					
					
					//look to see if there are any complications...
					AssignedClaim as = new AssignedClaim();
					
					as.setCategory("T");
					as.setClaim(c);
					as.setRule("1.2.2");
					
					if ( isComplication(c, es.getEpisode_id(), "s") ) {
						as.setCategory("TC");
						as.setRule("1.2.1");
					}
					
					
					// put the assigned claim in the episode...
					es.getAssigned_claims().add(as);
					
				}
				
			} // end rule 1.2.0
			
			// if this claim is assigned to anything, don't look to match further rules, go to next claim. 
			if (c.isAssigned()) {
				continue;
			}
			
			// now we should have an IP claim that isn't a trigger for a P or A episode
			// 1.3.0 IP claims where the P or A episodes' prof trigger start date is between the IP claim begin and end dates (inclusive)
			
			for (EpisodeShell es : epsForMember) {
				
				if (c.getFacility_type_code() == null ||
						c.getFacility_type_code().equals("") ||
						c.getFacility_type_code().equals("ST") ) 
				{
					// is OK to be used for this rule...
				} else {
					continue; 
				}
				
				if (es.isDropped()) {
					continue;
				}
				
				//don't check a claim against itself...
				if (es.getClaim().equals(c)) {
					continue;
				}
				if (es.getClaim().getPrincipal_diag_code() == null) {
					//log.error("Claim " + es.getClaim().getClaim_id() + " for member " + es.getClaim().getMember_id() + " has no principle diagnosis code");
					continue;
				}
				if (es.getEpisode_type().equals("A") || es.getEpisode_type().equals("P")) {
					if (es.getClaim().getClaim_line_type_code().equals("PB") ) {
						
//log.info("1.3 Dates: Claim to assign: " + c.getClaim_id() + "_" + c.getClaim_line_id() + 
//		"  begin: " + c.getBegin_date() + " end: " + c.getEnd_date());
//log.info("1.3 Dates: Episode: " + es.getEpisode_id() + "_" + es.getClaim().getMember_id() + "_" +
//		es.getClaim_id() + "_" + es.getClaim().getClaim_line_id() + "  trigbegin: " + 
//		es.getClaim().getBegin_date() + " end: " + es.getClaim().getEnd_date());
						
						// do they overlap?
						// if the trigger begin date is between (inclusive) the claim begin / end...
						
						/*
						 * Per Sarah, making a change to add one day to the end of the triggering claim 
						 * for the window of overlap to grab any IPs that start the day AFTER the PB claim
						 */
						//original:
						/*if (c.getBegin_date().compareTo(es.getClaim().getBegin_date())<=0 
								&& c.getEnd_date().compareTo(es.getClaim().getBegin_date())>=0) {
							// they overlap... is the p diag typical?
						 //NEW:
						 */
						// Add one day to the episode trigger window end to catch IPs that start the day after...
						Date newEpEndDate;
						Calendar cal = Calendar.getInstance();
						cal.setTime(es.getClaim().getEnd_date());
						cal.add(Calendar.DATE, 1);
						newEpEndDate = cal.getTime();
						
						boolean isGood = false;
						
						// true overlap, so first, if the c end date is in, good, if the c begin date is in, good
						// if the c begin date is before AND the c end date is after... good
						if (c.getBegin_date().compareTo(newEpEndDate)<=0 && c.getBegin_date().compareTo(es.getClaim().getBegin_date())>=0) {
							isGood=true;
						}
						if (c.getEnd_date().compareTo(newEpEndDate)<=0 && c.getEnd_date().compareTo(es.getClaim().getBegin_date())>=0) {
							isGood=true;
						}
						if (c.getBegin_date().before(es.getClaim().getBegin_date()) && c.getEnd_date().after(newEpEndDate)) {
							isGood=true;
						}
						

						if (isGood) {

//log.info(" 1.3 made it through date check");

							boolean isD = false;
							boolean isC = false;
							boolean isSC = false;
							
/*							for (DxCodeMetaData dx : epmdk.get(es.getEpisode_id()).getDx_codes()) {
								if (c.getPrincipal_diag_code().equals(dx.getCode_id())) {
									isD = true;
									break;
								}
							}
*/
							if (isPDiagnosis(c, es.getEpisode_id())) {
								isD = true;
								//break;
							}
							
/*							for (ComplicationCodeMetaData cm : epmdk.get(es.getEpisode_id()).getComplication_codes() ) {
								if (c.getPrincipal_diag_code().equals(cm.getCode_id())) {
									isC = true;
									break;
								}
								for (String cid : c.getSecondary_diag_code()) {
									if (cid.equals(cm.getCode_id())) {
										isSC = true;
										break;
									}
								}
							}
*/
							
							if (isComplication(c, es.getEpisode_id(), "p")) {
								isC = true;
							}
							if (isComplication(c, es.getEpisode_id(), "s")) {
								isSC = true;
							}
							
//log.info("isC: " + isC + " isSC: " + isSC + " isD: " + isD);
							
							if (!isD && !isC && !isSC) {
								continue;
							}
							
							/*String assAs = "";
							if (isD || isC) {
								//1.3.1
								if(isSC) {
									assAs="TC";
								} else {
									assAs="T";
								}
							} else if (isC) {
								assAs="TC";
							}*/
							
							if (isD || isC) {
								// met criteria to be assigned as either T or TC 
								AssignedClaim as = new AssignedClaim();
								//as.setCategory(assAs);
								if (isSC) {
									as.setRule("1.3.1");
									as.setCategory("TC");
								} else {
									as.setRule("1.3.2");
									as.setCategory("T");
								}
								as.setClaim(c);
								c.setAssigned(true);
								es.getAssigned_claims().add(as);
							}
						}
					}
				}
			} // end 1.3.0 rule
			
			if (c.isAssigned()) {
				continue; 
			}
			
			// begin 1.4.0 Subsequent IP...
			
			// 1.4.1 Same Day Transfer: IP begin = 0 or 1 days after prior IP end AND facility type CAH or PSY 
			// check if the claim is an IP at an acute facility first...
			if (c.getFacility_type_code() != null && c.getFacility_type_code().equals("ST") ) {
				
				// the primary claim is an acute facility, so we can look through the episodes for others
				
				for (EpisodeShell es : epsForMember) {
					
					if (es.isDropped()) {
						continue;
					}
					
					//Must be Proc or Acute...
					if (es.getEpisode_type().equals("P") || es.getEpisode_type().equals("A")) {
						//fine
					} else {
						continue;
					}
					
					//don't check a claim against itself...
					if (es.getClaim().equals(c)) {
						continue;
					}
					
					ArrayList<AssignedClaim> tAC141 = new ArrayList<AssignedClaim>();
					
					for (AssignedClaim ac : es.getAssigned_claims()) {
						
						//don't check a claim against itself
						if (ac.getClaim().equals(c)) {
							continue;
						}
						
						if ( ac.getClaim().getClaim_line_type_code().equals("IP") ) {
							if (ac.getClaim().getFacility_type_code() != null && ac.getClaim().getFacility_type_code().equals("ST") ) {
								
								//if it is the same facility (providers are same) it doesn't count...
								if (c.getProvider_id() != null && ac.getClaim().getProvider_id()!=null &&
										(c.getProvider_id().equals(ac.getClaim().getProvider_id())) ) {
									continue;
								}
								
								
						
								//int dd = c.getBegin_date().compareTo(ac.getClaim().getEnd_date());
								
								
								// first date is zero, counting days FROM there, 
								// so if second date is earlier, the result is negative
								// if later positive...
								long dd = getDateDiff(ac.getClaim().getEnd_date(),c.getBegin_date(),TimeUnit.DAYS);
								
								
								//log.info("!_!_!_: " + dd + " | " + c.getClaim_id() + " | " + c.getBegin_date() + " | "
								//+ ac.getClaim().getEnd_date() + " | " + es.getEpisode_id() + " | "
								//+ es.getClaim_id());
								
								if (dd == 1 || dd == 0) {
									// then the primary claim is 0 or 1 days after the episode assigned claim
									// and both are from acute facilities
									
									//log.info("yup");
									
									//log.info(ac.getClaim().getClaim_line_type_code() + " | " + ac.getClaim().getClaim_id() + " | " + ac.getRule() + " | "  + c.getClaim_line_type_code() + " | " + es.getEpisode_id() + "_" + es.getClaim_id());
									
									AssignedClaim nac = new AssignedClaim();
									
									nac.setCategory(ac.getCategory());
									nac.setClaim(c);
									nac.setRule("1.4.1");
									
									c.setAssigned(true);
									
									// this rule can allow IP claims in that are outside the episode window because 
									// the original IP window might extend beyond. In these cases, we're extending
									// the episode window to the beginning of the claim

									if (nac.getClaim().getBegin_date().compareTo(es.getEpisode_end_date()) > 0 ) {
										//es.setOrig_episode_end_date(es.getEpisode_end_date());
										es.setEpisode_end_date(nac.getClaim().getBegin_date());
									}
									
									//es.getAssigned_claims().add(nac); concurrent mod!
									
									tAC141.add(nac); // hold here to prevent concurrent mod error
									
									break;
									
								}
							}
						}
					}
					es.getAssigned_claims().addAll(tAC141); // add all newly identified assigned claims...
				}
				
			} // end 1.4.1
			
			// 1.4.2 / 1.4.3 / 1.4.4 - Post acute
			// don't check for assigned yet, we're not done w/ 1.4.0
			

			
			if ( 
					( //lt or snf
							c.getFacility_type_code() != null && 
							( c.getFacility_type_code().equals("LT") || c.getFacility_type_code().equals("SNF") )
					) || ( // 34 for hospice
							c.getPlace_of_svc_code() != null && c.getPlace_of_svc_code().equals("34")
						)
					
				) { 
			
				
				for (EpisodeShell es : epsForMember) {
					
					if (es.isDropped()) {
						continue;
					}
					
					//Must be Proc or Acute...
					if (es.getEpisode_type().equals("P") || es.getEpisode_type().equals("A")) {
						//fine
					} else {
						continue;
					}
					
					//don't check a claim against itself...
					if (es.getClaim().equals(c)) {
						continue;
					}
					
					if (es.getAssigned_claims() == null) {
						continue;
					}
					
					// check to make sure c is not already assigned to this episode...
					boolean contIf = false;
					for (AssignedClaim ac : es.getAssigned_claims()) {
						if (ac.getClaim().equals(c)) {
							contIf = true;
							break;
						}
					}
					if (contIf) {continue;}
					
					if ( es.getClaim().getClaim_line_type_code().equals("IP") ) {
						if ( es.getClaim().getFacility_type_code() != null && es.getClaim().getFacility_type_code().equals("ST") ) {
						
								//int b = c.getBegin_date().compareTo(es.getClaim().getEnd_date());
								//int e = c.getBegin_date().compareTo(es.getEpisode_end_date());
								
								//if (b>=0 && e<=0) {
									
								if (!c.getBegin_date().before(es.getClaim().getEnd_date()) && 
										!c.getBegin_date().after(es.getEpisode_end_date()) ) {
									
									boolean isD = false;
									boolean isC = false;
									boolean isSC = false;
									
/*									for (DxCodeMetaData dx : epmdk.get(es.getEpisode_id()).getDx_codes()) {
										if (c.getPrincipal_diag_code().equals(dx.getCode_id())) {
											isD = true;
											break;
										}
									}
*/
									
									if (isPDiagnosis(c, es.getEpisode_id())) {
										isD = true;
									}
									
/*									for (ComplicationCodeMetaData cm : epmdk.get(es.getEpisode_id()).getComplication_codes() ) {
										if (c.getPrincipal_diag_code().equals(cm.getCode_id())) {
											isC = true;
											break;
										}
										for (String cid : c.getSecondary_diag_code()) {
											if (cid.equals(cm.getCode_id())) {
												isSC = true;
												break;
											}
										}
									}
*/
									
									if (isComplication(c, es.getEpisode_id(), "p")) {
										isC = true;
									}
									if (isComplication(c, es.getEpisode_id(), "s")) {
										isSC = true;
									}
									
									
									if (!isD && !isC) {
										continue;
									}
									
									String assAs = "";
									String rule = "";
									if (isD) {
										if(isSC) {
											//1.4.3
											assAs="TC";
											rule = "1.4.3";
										} else {
											assAs="T";
											rule="1.4.4";
										}
									} else if (isC) {
										assAs="C";
										rule="1.4.2";
									}
									
									AssignedClaim as = new AssignedClaim();
									
									as.setCategory(assAs);
									as.setClaim(c);
									as.setRule(rule);
									
									c.setAssigned(true);
									
									es.getAssigned_claims().add(as);
									

									
								}
								
						}
					}
				}
				
			} // end 1.4.2 / 1.4.3 / 1.4.4
			
			// don't check for assigned yet, not done with 1.4.0
			
			
			
			// 1.4.5 go...
			// Readmissions - IP acute facility (fac ST) prin diag is typ, then gets a little complex...
			
			if (c.getFacility_type_code() != null && c.getFacility_type_code().equals("ST")
					&& c.getProvider_id() != null ) {
				
				// the primary claim is an acute facility, so we can look through the episodes for others
				
				for (EpisodeShell es : epsForMember) {
					
					if (es.isDropped()) {
						continue;
					}
					
					//Must be Proc or Acute...
					if (es.getEpisode_type().equals("P") || es.getEpisode_type().equals("A")) {
						//fine
					} else {
						continue;
					}
					
					//don't check a claim against itself...
					if (es.getClaim().equals(c)) {
						continue;
					}
					
					if (es.getAssigned_claims() == null) {
						continue;
					}
					
					// check to make sure c is not already assigned to this episode...
					boolean contIf = false;
					for (AssignedClaim ac : es.getAssigned_claims()) {
						if (ac.getClaim().equals(c)) {
							contIf = true;
							break;
						}
					}
					if (contIf) {continue;}
					
					boolean isD = false;
					boolean isC = false;
					
/*					for (DxCodeMetaData dx : epmdk.get(es.getEpisode_id()).getDx_codes()) {
						if (c.getPrincipal_diag_code().equals(dx.getCode_id())) {
							isD = true;
							break;
						}
					}
*/
					if (isPDiagnosis(c, es.getEpisode_id())) {
						isD = true;
					}
					
/*					for (ComplicationCodeMetaData cm : epmdk.get(es.getEpisode_id()).getComplication_codes() ) {
						if (c.getPrincipal_diag_code().equals(cm.getCode_id())) {
							isC = true;
							break;
						}
					}
*/
					
					if (isComplication(c, es.getEpisode_id(), "p")) {
						isC = true;
					}
					
					if (!isD && !isC) {
						continue;
					} 
					
					
					
					ArrayList<AssignedClaim> tAC = new ArrayList<AssignedClaim>();
					
					for (AssignedClaim ac : es.getAssigned_claims()) {
						
						// don't check the same claim...
						
						if (ac.getClaim().equals(c)) {
							continue;
						}
						
						if (!ac.getClaim().getClaim_line_type_code().equals("IP")) {
							continue;
						}
						
						//don't bother if poc is null
						if (ac.getClaim().getProvider_id()==null) {
							continue;
						}
						
						//don't bother if ac isn't an acute facility...
						if (ac.getClaim().getFacility_type_code() == null)
							continue;
						if (!ac.getClaim().getFacility_type_code().equals("ST")) {
							continue;
						}
						
						
						//int b = c.getBegin_date().compareTo(es.getClaim().getEnd_date());
						//int e = c.getBegin_date().compareTo(es.getEpisode_end_date());
						
						
						// first date is zero, counting days FROM there, 
						// so if second date is earlier, the result is negative
						// if later positive...
						long b = getDateDiff(es.getClaim().getEnd_date(),c.getBegin_date(),TimeUnit.DAYS);
						long e = getDateDiff(es.getEpisode_end_date(),c.getBegin_date(),TimeUnit.DAYS);
						
						boolean shouldAssign = false;
						
						
						
						if (ac.getClaim().getProvider_id().equals(c.getProvider_id())) {
							if (b>=0 && e<=0) {
								shouldAssign = true;
							}
						} else {
							if (b>1 && e<=0) {
								shouldAssign = true;
							}
							
						}
						
						if (shouldAssign) {
							AssignedClaim as = new AssignedClaim();
							
							as.setCategory("C");
							as.setClaim(c);
							as.setRule("1.4.5");
							
							c.setAssigned(true);
							
							tAC.add(as); // hold here to prevent concurrent mod error
							
							break;

						}

					}
					
					es.getAssigned_claims().addAll(tAC);	// add any newly assigned claims
					
				}
				
			} // end 1.4.5
			
			// still not done w 1.4.0 rules...
			
			// 1.4.6
			if (c.getFacility_type_code() != null && c.getFacility_type_code().equals("ST")) {
				
				// the primary claim is an acute facility, so we can look through the episodes for others
				
				for (EpisodeShell es : epsForMember) {
					
					if (es.isDropped()) {
						continue;
					}
					
					//Must be Proc or Acute...
					if (es.getEpisode_type().equals("P") || es.getEpisode_type().equals("A")) {
						//fine
					} else {
						continue;
					}
					
					//don't check a claim against itself...
					if (es.getClaim().equals(c)) {
						continue;
					}
					
					// check to make sure c is not already assigned to this episode...
					boolean contIf = false;
					for (AssignedClaim ac : es.getAssigned_claims()) {
						if (ac.getClaim().equals(c)) {
							contIf = true;
							break;
						}
					}
					if (contIf) {continue;}
							
					//int b = c.getBegin_date().compareTo(es.getClaim().getEnd_date());
					//int e = c.getBegin_date().compareTo(es.getEpisode_end_date());
					
					
					// first date is zero, counting days FROM there, 
					// so if second date is earlier, the result is negative
					// if later positive...
					long b = getDateDiff(es.getClaim().getEnd_date(),c.getBegin_date(),TimeUnit.DAYS);
					long e = getDateDiff(es.getEpisode_end_date(),c.getBegin_date(),TimeUnit.DAYS);
					
					if (b>1 && e<=0) {
						
						boolean isD = false;
						boolean isC = false;
						
/*						for (DxCodeMetaData dx : epmdk.get(es.getEpisode_id()).getDx_codes()) {
							if (c.getPrincipal_diag_code().equals(dx.getCode_id())) {
								isD = true;
								break;
							}
						}
*/
						
						if (isPDiagnosis(c, es.getEpisode_id())) {
							isD = true;
						}
						
/*						for (ComplicationCodeMetaData cm : epmdk.get(es.getEpisode_id()).getComplication_codes() ) {
							if (c.getPrincipal_diag_code().equals(cm.getCode_id())) {
								isC = true;
								break;
							}
						}
*/ 
						
						if (isComplication(c, es.getEpisode_id(), "p")) {
							isC = true;
						}
						
						if (isD || isC) {
							AssignedClaim as = new AssignedClaim();
							
							as.setCategory("C");
							as.setClaim(c);
							as.setRule("1.4.6");
							
							c.setAssigned(true);
							
							es.getAssigned_claims().add(as);
						}
						
					}
				}
			} // end 1.4.6
			
			/* 
			 * !!!!!!!!!!!!!!!!!!!!!!!!!!!
			 * !!! DONE WITH 1.4 RULES !!!
			 * !!!!!!!!!!!!!!!!!!!!!!!!!!!
			 */
			
			if (c.isAssigned()) {
				continue;
			}
			
			// 1.5.0
			for (EpisodeShell es : epsForMember) {
				
				if (es.isDropped()) {
					continue;
				}
				
				if (!es.getEpisode_type().equals("C")) {
					continue;
				}
				
				//int b = c.getBegin_date().compareTo(es.getClaim().getBegin_date());
				
				// first date is zero, counting days FROM there, 
				// so if second date is earlier, the result is negative
				// if later positive...
				long b = getDateDiff(es.getClaim().getBegin_date(),c.getBegin_date(),TimeUnit.DAYS);
				long e = getDateDiff(es.getEpisode_end_date(),c.getBegin_date(),TimeUnit.DAYS);
				
				if (b<0 || e>0) {
					continue;
				}
				
				boolean isD = false;
				boolean isC = false;
				
/*				for (DxCodeMetaData dx : epmdk.get(es.getEpisode_id()).getDx_codes()) {
					if (c.getPrincipal_diag_code().equals(dx.getCode_id())) {
						isD = true;
						break;
					}
				}
*/
			
				if (isPDiagnosis(c, es.getEpisode_id())) {
					isD = true;
				}
				
/*				for (ComplicationCodeMetaData cm : epmdk.get(es.getEpisode_id()).getComplication_codes() ) {
					if (c.getPrincipal_diag_code().equals(cm.getCode_id())) {
						isC = true;
						break;
					}
				}
*/ 
			
				if (isComplication(c, es.getEpisode_id(), "p")) {
					isC = true;
				}
				
				if (!isC && !isD) {
					continue;
				}
				
				AssignedClaim as = new AssignedClaim();
				
				as.setCategory("C");
				as.setClaim(c);
				as.setRule("1.5.0");
				
				c.setAssigned(true);
				
				es.getAssigned_claims().add(as);
				
			} // end 1.5.0
			
			if (c.isAssigned()) {
				continue;
			}
			
			//1.6 section begin
			// for all 1.6 sections, IP claim start date must be >= trigger start date
			// and episode type must be X (other)
			
			for (EpisodeShell es : epsForMember) {
				
				if (es.isDropped()) {
					continue;
				}
				
				if (!es.getEpisode_type().equals("X")) {
					continue;
				}
				
				//int b = c.getBegin_date().compareTo(es.getClaim().getBegin_date());
				
				// first date is zero, counting days FROM there, 
				// so if second date is earlier, the result is negative
				// if later positive...
				long b = getDateDiff(es.getClaim().getBegin_date(),c.getBegin_date(),TimeUnit.DAYS);
				long e = getDateDiff(es.getEpisode_end_date(),c.getBegin_date(),TimeUnit.DAYS);
				
				if (b<0 || e>0) {
					continue;
				}
				
				boolean isD = false;
				boolean isC = false;
				boolean isSC = false;
				
/*				for (DxCodeMetaData dx : epmdk.get(es.getEpisode_id()).getDx_codes()) {
					if (c.getPrincipal_diag_code().equals(dx.getCode_id())) {
						isD = true;
						break;
					}
				}
*/
				
				if (isPDiagnosis(c, es.getEpisode_id())) {
					isD = true;
				}
				
/*				for (ComplicationCodeMetaData cm : epmdk.get(es.getEpisode_id()).getComplication_codes() ) {
					if (c.getPrincipal_diag_code().equals(cm.getCode_id())) {
						isC = true;
						break;
					}
					for (String cid : c.getSecondary_diag_code()) {
						if (cid.equals(cm.getCode_id())) {
							isSC = true;
							break;
						}
					}
				}
*/
				
				if (isComplication(c, es.getEpisode_id(), "p")) {
					isC = true;
				}
				if (isComplication(c, es.getEpisode_id(), "s")) {
					isSC = true;
				}
				
				if (!isD && !isC) {
					continue;
				}
				
				String assAs = "";
				String rule = "";
				if (isD) {
					if(isSC) {
						//1.4.3
						assAs="TC";
						rule = "1.6.1";
					} else {
						assAs="T";
						rule="1.6.2";
					}
				} else if (isC) {
					assAs="C";
					rule="1.6.3";
				}
				
				AssignedClaim as = new AssignedClaim();
				
				as.setCategory(assAs);
				as.setClaim(c);
				as.setRule(rule);
				
				c.setAssigned(true);
				
				es.getAssigned_claims().add(as);
				
				
				
			} // end 1.6 section
			
			if (c.isAssigned()) {
				continue;
			}
			
			// 1.7 Typical Look Back
			
			/*
			 * 
			 * Coded as option set to YES
			 * changed 11/18/14 - NO unless procedural episode
			 * 
			 */
			
			for (EpisodeShell es : epsForMember) {
				
				if (es.isDropped()) {
					continue;
				}
				
				// claim must be on or after the episode start and before the trigger start
				//int b = c.getBegin_date().compareTo(es.getEpisode_begin_date());
				//int e = c.getBegin_date().compareTo(es.getClaim().getBegin_date());
				
				// first date is zero, counting days FROM there, 
				// so if second date is earlier, the result is negative
				// if later positive...
				long b = getDateDiff(es.getEpisode_begin_date(),c.getBegin_date(),TimeUnit.DAYS);
				long e = getDateDiff(es.getClaim().getBegin_date(),c.getBegin_date(),TimeUnit.DAYS);
				
				if (b<0 || e>=0) {
					continue;
				}
				
				//Claim must be relevant (primary DX is typ or comp)
				
				boolean isD = false;
				boolean isC = false;
				
/*				for (DxCodeMetaData dx : epmdk.get(es.getEpisode_id()).getDx_codes()) {
					if (c.getPrincipal_diag_code().equals(dx.getCode_id())) {
						isD = true;
						break;
					}
				}
*/
				
				if (isPDiagnosis(c, es.getEpisode_id())) {
					isD = true;
				}
				
/*				for (ComplicationCodeMetaData cm : epmdk.get(es.getEpisode_id()).getComplication_codes() ) {
					if (c.getPrincipal_diag_code().equals(cm.getCode_id())) {
						isC = true;
						break;
					}
				}
*/
				
				if (isComplication(c, es.getEpisode_id(), "a")) {
					isC = true;
				}
				
				if (isD) {
					
					AssignedClaim as = new AssignedClaim();
					
					as.setClaim(c);
					
					c.setAssigned(true);
					
// LOOKBACK TOGGLE WORKAROUND: REPLACE WITH DEFAULT LIST UNTIL
// ACTUAL LOOKBACK TOGGLES ARE CODED
					
					// typical lookback toggle 1.7.2, 1.7.3...
					if (isC) {
						as.setCategory("C");
						as.setRule("1.7.2");
					} else {
						as.setCategory("T");
						as.setRule("1.7.3");
					}
					
					if (es.getEpisode_type().equals("P")) {
						as.setCategory("T");
						as.setRule("1.7.1");
					}
					
					es.getAssigned_claims().add(as);
					
				}
				
				
				
				
			}
			
			if (c.isAssigned()) {
				continue;
			}
			
			// 1.8 - Newborn IP Claims - New with 5.3.x...
			
			// New Born Episodes

			/**
			 * KPMG 
			 * 
			 * Classify Newborn episode as Class = Other Condition. 
			 * Add a new set of IP service assignment rules between the current 1.7.0 and 1.8.0 for all unassigned IP stays in the episode window.  
			 * If pdx is complication code, assign as C.  If secondary dx is complication code, assign as TC.  
			 * If no complication codes, assign as T.  No relevant dx codes required.  
			 * Hard code this as applicable only to Newborn episode.
			 * 
			 *   Modified by HCI3 Mike, 2015-06-25 to fix logical errors
			 *   - claim must be within episode window, not just look forward
			 *   - typical only occurs if *neither* C or TC, not either.
			 *   - all secondary diagnosis codes must be reviewed for a match, not just one
			 */
			for (EpisodeShell es : epsForMember) {

				if (es.isDropped()) {
					continue;
				}

				// NEWBORN CASE ONLY
				// TODO make EX1502 a constant
				if (es.getEpisode_id().equals("EX1502")) {

					// claim must be on or after the episode start and before the trigger start
					//int b = c.getBegin_date().compareTo(es.getEpisode_begin_date());
					//int e = c.getBegin_date().compareTo(es.getClaim().getBegin_date());

					// first date is zero, counting days FROM there, 
					// so if second date is earlier, the result is negative
					// if later positive...
					long b = getDateDiff(es.getEpisode_begin_date(),c.getBegin_date(),TimeUnit.DAYS);
					long e = getDateDiff(es.getEpisode_end_date(),c.getBegin_date(),TimeUnit.DAYS);

					if (b<0 || e>0) {
						continue;
					}

					boolean isC = false;
					boolean isTC = false;
					
/*					for (ComplicationCodeMetaData cm : epmdk.get(es.getEpisode_id()).getComplication_codes() ) {
						if (c.getPrincipal_diag_code().equals(cm.getCode_id())) {
							isC = true;
							break;
						}
						for (String cid : c.getSecondary_diag_code()) {
							if (cid.equals(cm.getCode_id())) {
								isTC = true;
								break;
							}
						}
					}
*/
					
					if (isComplication(c, es.getEpisode_id(), "p")) {
						isC = true;
					}
					if (isComplication(c, es.getEpisode_id(), "s")) {
						isTC = true;
					}

					// If no complication codes, assign as T				
					if(!isC && !isTC) {
					}

					AssignedClaim as = new AssignedClaim();

					as.setClaim(c);

					c.setAssigned(true);

					if (isC) {
						as.setCategory("C");
						as.setRule("1.8.1");
					} else if (isTC){
						as.setCategory("TC");
						as.setRule("1.8.2");
					}
					else {
						as.setCategory("T");
						as.setRule("1.8.3");
					}

					es.getAssigned_claims().add(as);

				}

			} // End 1.8
			
			
			
			//if (c.isAssigned()) {
			//	continue;
			//}
			
			/* Moved to the end of all assignment...
			//1.9 SRFs   -- formerly 1.8 SRFs - 
			// 5.3 update, this is now 1.9 and excludes sick care episodes ES9901.
			for (EpisodeShell es : epsForMember) {
				
				if (es.isDropped()) {
					continue;
				}	
				// find the SRF the IP triggered if any...
				if (es.getEpisode_type().equals("S") && !es.getEpisode_id().equals("ES9901")) {
					//log.info("claim: " + c.getClaim_id() + " | episode: " + es.getClaim_id());
					// need to make sure the isAssigned is only done *once* or it'll drop the next time through...
					// we're in IP assignment, so only check if the claim is IP
					if ( es.getClaim().getClaim_line_type_code().equals("IP") && es.getClaim().isAssigned()) {
						//log.info("dropped");
						// the trigger claim has been assigned elsewhere, the SRF doesn't survive...
						es.setDropped(true);
						//log.info("DROPPED: " + es.getEpisode_id() + " isdrop " + es.isDropped());
					} else {
						// if the IP claim is within the episode window...
						
						// claim must be on or after the episode start and on or before the episode end
						
						// first date is zero, counting days FROM there, 
						// so if second date is earlier, the result is negative
						// if later positive...
						long b = getDateDiff(es.getEpisode_begin_date(),c.getBegin_date(),TimeUnit.DAYS);
						long e = getDateDiff(es.getEpisode_end_date(),c.getBegin_date(),TimeUnit.DAYS);
						
						if (b<0 || e>0) {
							continue;
						}
						
						// Primary DX must be relevant
						
						boolean isD = false;
						boolean isC = false;
						
						for (DxCodeMetaData dx : epmdk.get(es.getEpisode_id()).getDx_codes()) {
							if (c.getPrincipal_diag_code().equals(dx.getCode_id())) {
								isD = true;
								break;
							}
						}
						
						for (ComplicationCodeMetaData cm : epmdk.get(es.getEpisode_id()).getComplication_codes() ) {
							if (c.getPrincipal_diag_code().equals(cm.getCode_id())) {
								isC = true;
								break;
							}
						}
						
						if (isD || isC) {
						
							AssignedClaim as = new AssignedClaim();
						
							as.setCategory("C");
							as.setClaim(c);
							as.setRule("1.9.0");
							
							c.setAssigned(true);
							
							es.getAssigned_claims().add(as);
						}
					}
				}
			} // end 1.9
			*/
			//if (c.isAssigned()) {
			//	continue;
			//}
			
			/* Moved to the end of all assignment...
			//1.10 - Sick Care SRFs Only
			// 5.3 update
			for (EpisodeShell es : epsForMember) {
				
				if (es.isDropped()) {
					continue;
				}	
				// If this is an SRF Sick Care episode and the trigger is assigned elsewhere, drop it.
				if (es.getEpisode_type().equals("S") && es.getEpisode_id().equals("ES9901")) {
					//log.info("claim: " + c.getClaim_id() + " | episode: " + es.getClaim_id());
					if (es.getClaim().getClaim_line_type_code().equals("IP") && es.getClaim().isAssigned()) {
						//log.info("dropped");
						// the trigger claim has been assigned elsewhere, the SRF doesn't survive...
						es.setDropped(true);
						//log.info("DROPPED: " + es.getEpisode_id() + " isdrop " + es.isDropped());
					} else {
						// if the IP claim is within the episode window...
						
						// claim must be on or after the episode start and on or before the episode end
						
						// first date is zero, counting days FROM there, 
						// so if second date is earlier, the result is negative
						// if later positive...
						long b = getDateDiff(es.getEpisode_begin_date(),c.getBegin_date(),TimeUnit.DAYS);
						long e = getDateDiff(es.getEpisode_end_date(),c.getBegin_date(),TimeUnit.DAYS);
						
						if (b<0 || e>0) {
							continue;
						}
						
						// Primary DX must be relevant
						
						boolean isD = false;
						boolean isC = false;
						
						for (DxCodeMetaData dx : epmdk.get(es.getEpisode_id()).getDx_codes()) {
							if (c.getPrincipal_diag_code().equals(dx.getCode_id())) {
								isD = true;
								break;
							}
						}
						
						for (ComplicationCodeMetaData cm : epmdk.get(es.getEpisode_id()).getComplication_codes() ) {
							if (c.getPrincipal_diag_code().equals(cm.getCode_id())) {
								isC = true;
								break;
							}
						}
						
						if (isD || isC) {
						
							AssignedClaim as = new AssignedClaim();
						
							as.setCategory("C");
							as.setClaim(c);
							as.setRule("1.10.0");
							
							c.setAssigned(true);
							
							es.getAssigned_claims().add(as);
						}
						
						// if at this point, the c claim is the trigger claim, and it has NOT 
						// been assigned, drop the episode (because if it got to this point
						// it would have been assigned if it could have been
						
						// if it is the trigger claim...
						boolean found=false;
						if (c.equals(es.getClaim())) {
							// look through the assigned claims and see if the trigger is assigned...
							for (AssignedClaim ac : es.getAssigned_claims()) {
								// if the assigned claim is the trigger claim...
								if (!ac.getType().equals("RX") && ac.getClaim().equals(es.getClaim())) {
									found = true;
								}
							}
							// the trigger was not found to be assigned, drop the episode...
							if (!found) {
								es.setDropped(true);
							}
						}
					}
				}
			} // end 1.10
			*/
			
			
			
		} // end claim serv line for loop
		
		
		
		
		
		
		
		
	} // end ip assignment
	
	public void rxAssignment() {

	    // 2.1.0 RX Assignment

	    for (ClaimRx rx : rxClaims) {
	      if (!rx.getMember_id().equals(planMember.getMember_id())) {
	        continue;
	      }
	      for (EpisodeShell es : epsForMember) {
	        if (es.isDropped()) {
	          continue;
	        }

	        // If SRF, needs to follow 2.9/2.10 rules...
	        if (es.getEpisode_type().equals("S")) {
	          continue;
	        }

	        // claim must be on or after the episode start and before the trigger start
	        // int b = rx.getRx_fill_date().compareTo(es.getEpisode_begin_date());
	        // int e = rx.getRx_fill_date().compareTo(es.getEpisode_end_date());

	        // first date is zero, counting days FROM there,
	        // so if second date is earlier, the result is negative
	        // if later positive...
	        if (rx.getRx_fill_date() == null)
	          continue;
	        long b = getDateDiff(es.getEpisode_begin_date(), rx.getRx_fill_date(), TimeUnit.DAYS);
	        long e = getDateDiff(es.getEpisode_end_date(), rx.getRx_fill_date(), TimeUnit.DAYS);

	        if (b < 0 || e > 0) {
	          continue;
	        }

	        boolean isRx = false;
	        boolean isC = false;
	        // for (RxCodeMetaData mrx : epmdk.get(es.getEpisode_id()).getRx_codes() ) {
	        // NOTE! getBuilder_match_code() uses the NDC to multum translation we need to get rid of!
	        // we shoudl eventually have the meta-data have the matching NDC format...
	        Map<String, RxCodeMetaData> codeToMrx = epmdk.get(es.getEpisode_id()).getRx_codes();
	        // mdh.getExport_version() -- if this is 5.2.006, then it is old and we use the ndc to
	        // multum...
	        String rxCode;
	        if (mdh.getExport_version().equals("5.2.006")) {
	          rxCode = rx.getBuilder_match_code();
	        } else {
	          rxCode = rx.getDrug_code();
	        }

	        // if (rxCode != null && rxCode.equals(mrx.getCode_id())) {

	        if (rxCode != null && codeToMrx.containsKey(rxCode)) {

	          isRx = true;
	          RxCodeMetaData mrx = codeToMrx.get(rxCode);
	          if (mrx.getRx_assignment() != null &&  mrx.getRx_assignment().equals("Complication")) {
	            isC = true;
	          }
	          // break;
	        }
	        // }

	        if (isRx) {
	          // the trigger claim hasn't been assigned elsewhere, assign it to this episode...
	          AssignedClaim as = new AssignedClaim();


	          if (isC) {
	            as.setCategory("C");
	            as.setRule("2.1.1");
	          } else {
	            as.setCategory("T");
	            as.setRule("2.1.2");
	          }
	          as.setRxClaim(rx);

	          as.setType("RX");

	          rx.setAssigned(true);

	          es.getAssigned_claims().add(as);
	        }

	      }
	    }

	    // 2.8 RX Rules...
	    
	    for (ClaimRx rx : rxClaims) {
		      if (!rx.getMember_id().equals(planMember.getMember_id())) {
		        continue;
		      }
		      if (rx.isAssigned()) {
		          continue;
		      }
		      for (EpisodeShell es : epsForMember) {
	    	    
		        if (es.isDropped()) {
		          continue;
		        }
		        if (!es.getEpisode_id().equals("EX1502")) {
					continue;
				}		        

		        // claim must be on or after the episode start and before the episode end
		        
		        if (rx.getRx_fill_date().before(es.getEpisode_begin_date()) || rx.getRx_fill_date().after(es.getEpisode_end_date())) {
		        	continue;
		        }

		        boolean isRx = false;
		        boolean isC = false;
		        // for (RxCodeMetaData mrx : epmdk.get(es.getEpisode_id()).getRx_codes() ) {
		        // NOTE! getBuilder_match_code() uses the NDC to multum translation we need to get rid of!
		        // we shoudl eventually have the meta-data have the matching NDC format...
		        Map<String, RxCodeMetaData> codeToMrx = epmdk.get(es.getEpisode_id()).getRx_codes();
		        // mdh.getExport_version() -- if this is 5.2.006, then it is old and we use the ndc to
		        // multum...
		        String rxCode;
		        if (mdh.getExport_version().equals("5.2.006")) {
		          rxCode = rx.getBuilder_match_code();
		        } else {
		          rxCode = rx.getDrug_code();
		        }

		        if (rxCode != null && codeToMrx.containsKey(rxCode)) {
		          isRx = true;
		          RxCodeMetaData mrx = codeToMrx.get(rxCode);
		          if (mrx.getRx_assignment().equals("Complication")) {
		            isC = true;
		          }
		        }		        // }

		       // does not need to be relevant
		        //if (isRx) {

		            AssignedClaim as = new AssignedClaim();
		            
		            if (isC) {
		            	as.setRule("2.8.1");
		            	as.setCategory("C");
		            } else {
		            	as.setRule("2.8.2");
		            	as.setCategory("T");
		            }
	
		            as.setRxClaim(rx);
		            as.setType("RX");
	
		            rx.setAssigned(true);
	
		            es.getAssigned_claims().add(as);
		       // }
				
			} // end 2.8.x
	    }

	    // Need a second loop to isolate the 2.9/2.10.x RX assignment
	    // 2.9 - SRFs except Sick Care 
	    for (ClaimRx rx : rxClaims) {
	      if (!rx.getMember_id().equals(planMember.getMember_id())) {
	        continue;
	      }
	      
	      // at this point, this claim will already have been assigned under another rule
	      if (rx.isAssigned()) {
	    	  continue;
	      }
	      
	      for (EpisodeShell es : epsForMember) {
		        if (es.isDropped()) {
		          continue;
		        }
		        
		        // Must be SRF
		        if (!es.getEpisode_type().equals("S")) {
		          continue;
		        }
		        
		        // can't be sick care for 2.9...
		        if (es.getEpisode_id().equals("ES9901")) {
		        	continue;
		        }
		        
		        // first date is zero, counting days FROM there,
		        // so if second date is earlier, the result is negative
		        // if later positive...
		        if (rx.getRx_fill_date() == null)
		          continue;
		        
		        long b = getDateDiff(es.getEpisode_begin_date(), rx.getRx_fill_date(), TimeUnit.DAYS);
		        long e = getDateDiff(es.getEpisode_end_date(), rx.getRx_fill_date(), TimeUnit.DAYS);

		        if (b < 0 || e > 0) {
		          continue;
		        }

		        boolean isRx = false;
		        boolean isC = false;
		        // for (RxCodeMetaData mrx : epmdk.get(es.getEpisode_id()).getRx_codes() ) {
		        // NOTE! getBuilder_match_code() uses the NDC to multum translation we need to get rid of!
		        // we shoudl eventually have the meta-data have the matching NDC format...
		        Map<String, RxCodeMetaData> codeToMrx = epmdk.get(es.getEpisode_id()).getRx_codes();
		        // mdh.getExport_version() -- if this is 5.2.006, then it is old and we use the ndc to
		        // multum...
		        String rxCode;
		        if (mdh.getExport_version().equals("5.2.006")) {
		          rxCode = rx.getBuilder_match_code();
		        } else {
		          rxCode = rx.getDrug_code();
		        }

		        // if (rxCode != null && rxCode.equals(mrx.getCode_id())) {

		        if (rxCode != null && codeToMrx.containsKey(rxCode)) {
		          isRx = true;
		          RxCodeMetaData mrx = codeToMrx.get(rxCode);
		          if (mrx.getRx_assignment().equals("Complication")) {
		            isC = true;
		          }
		          // break;
		        }
		        // }
	      
		        if (isRx) {

		            // the trigger claim hasn't been assigned elsewhere, assign it to this episode...
		            AssignedClaim as = new AssignedClaim();

		            as.setCategory("C");
		            as.setRule("2.9.0");
		            as.setRxClaim(rx);
		            as.setType("RX");

		            rx.setAssigned(true);

		            es.getAssigned_claims().add(as);
		          
		        }
	      
	      
	      }
	      
	    }
	    
	    
	    // Need another loop to isolate the 2.10.x RX assignment
	    // 2.10  Sick Care ONLY
	    for (ClaimRx rx : rxClaims) {
	      if (!rx.getMember_id().equals(planMember.getMember_id())) {
	        continue;
	      }
	      
	      // at this point, this claim will already have been assigned under another rule
	      if (rx.isAssigned()) {
	    	  continue;
	      }
	      
	      for (EpisodeShell es : epsForMember) {
		        if (es.isDropped()) {
		          continue;
		        }
		        
		        // ONLY sick care...
		        if (!es.getEpisode_id().equals("ES9901")) {
		        	continue;
		        }
		        
		        // first date is zero, counting days FROM there,
		        // so if second date is earlier, the result is negative
		        // if later positive...
		        if (rx.getRx_fill_date() == null)
		          continue;
		        
		        long b = getDateDiff(es.getEpisode_begin_date(), rx.getRx_fill_date(), TimeUnit.DAYS);
		        long e = getDateDiff(es.getEpisode_end_date(), rx.getRx_fill_date(), TimeUnit.DAYS);

		        if (b < 0 || e > 0) {
		          continue;
		        }

		        boolean isRx = false;
		        boolean isC = false;
		        // for (RxCodeMetaData mrx : epmdk.get(es.getEpisode_id()).getRx_codes() ) {
		        // NOTE! getBuilder_match_code() uses the NDC to multum translation we need to get rid of!
		        // we shoudl eventually have the meta-data have the matching NDC format...
		        Map<String, RxCodeMetaData> codeToMrx = epmdk.get(es.getEpisode_id()).getRx_codes();
		        // mdh.getExport_version() -- if this is 5.2.006, then it is old and we use the ndc to
		        // multum...
		        String rxCode;
		        if (mdh.getExport_version().equals("5.2.006")) {
		          rxCode = rx.getBuilder_match_code();
		        } else {
		          rxCode = rx.getDrug_code();
		        }

		        // if (rxCode != null && rxCode.equals(mrx.getCode_id())) {

		        if (rxCode != null && codeToMrx.containsKey(rxCode)) {
		          isRx = true;
		          RxCodeMetaData mrx = codeToMrx.get(rxCode);
		          if (mrx.getRx_assignment().equals("Complication")) {
		            isC = true;
		          }
		          // break;
		        }
		        // }
	      
		        if (isRx) {

		        	 // the trigger claim hasn't been assigned elsewhere, assign it to this episode...
		            AssignedClaim as = new AssignedClaim();

		            if (isC) {
		              as.setCategory("C");
		              as.setRule("2.10.1");
		            } else {
		              as.setCategory("T");
		              as.setRule("2.10.2");
		            }

		            as.setRxClaim(rx);
		            as.setType("RX");

		            rx.setAssigned(true);

		            es.getAssigned_claims().add(as);

		          
		        }
	      }
	    }
	}

      
      
      
      
      
 /* old non-splitting code for srf/sick care RX assignment. 
  * Also had a problem with getting 2.10 assigned to the correct episode...     
  */
 /*     
	      // at this point, this claim will already have been assigned under another rule
	      if (rx.isAssigned()) {
	    	  continue;
	      }
	      
	      for (EpisodeShell es : epsForMember) {
	        if (es.isDropped()) {
	          continue;
	        }

	        // If SRF, needs to follow 2.9/2.10 rules...
	        if (!es.getEpisode_type().equals("S")) {
	          continue;
	        }

	        // first date is zero, counting days FROM there,
	        // so if second date is earlier, the result is negative
	        // if later positive...
	        if (rx.getRx_fill_date() == null)
	          continue;
	        long b = getDateDiff(es.getEpisode_begin_date(), rx.getRx_fill_date(), TimeUnit.DAYS);
	        long e = getDateDiff(es.getEpisode_end_date(), rx.getRx_fill_date(), TimeUnit.DAYS);

	        if (b < 0 || e > 0) {
	          continue;
	        }

	        boolean isRx = false;
	        boolean isC = false;
	        // for (RxCodeMetaData mrx : epmdk.get(es.getEpisode_id()).getRx_codes() ) {
	        // NOTE! getBuilder_match_code() uses the NDC to multum translation we need to get rid of!
	        // we shoudl eventually have the meta-data have the matching NDC format...
	        Map<String, RxCodeMetaData> codeToMrx = epmdk.get(es.getEpisode_id()).getRx_codes();
	        // mdh.getExport_version() -- if this is 5.2.006, then it is old and we use the ndc to
	        // multum...
	        String rxCode;
	        if (mdh.getExport_version().equals("5.2.006")) {
	          rxCode = rx.getBuilder_match_code();
	        } else {
	          rxCode = rx.getDrug_code();
	        }

	        // if (rxCode != null && rxCode.equals(mrx.getCode_id())) {

	        if (rxCode != null && codeToMrx.containsKey(rxCode)) {
	          isRx = true;
	          RxCodeMetaData mrx = codeToMrx.get(rxCode);
	          if (mrx.getRx_assignment().equals("Complication")) {
	            isC = true;
	          }
	          // break;
	        }
	        // }

	        if (isRx) {

	          // start with 2.9...
	          if (!es.getEpisode_id().equals("ES9901")) {

	            // the trigger claim hasn't been assigned elsewhere, assign it to this episode...
	            AssignedClaim as = new AssignedClaim();

	            as.setCategory("C");
	            as.setRule("2.9.0");
	            as.setRxClaim(rx);
	            as.setType("RX");

	            rx.setAssigned(true);

	            es.getAssigned_claims().add(as);

	          } else {
	            // it is a sick care episode, so 2.10...
	        	/*
	            if (rx.isAssigned()) {
	              break;
	            }
	            */
/*
	            // the trigger claim hasn't been assigned elsewhere, assign it to this episode...
	            AssignedClaim as = new AssignedClaim();

	            if (isC) {
	              as.setCategory("C");
	              as.setRule("2.10.1");
	            } else {
	              as.setCategory("T");
	              as.setRule("2.10.2");
	            }

	            as.setRxClaim(rx);
	            as.setType("RX");

	            rx.setAssigned(true);

	            es.getAssigned_claims().add(as);

	          }
	        }
	      }
	    }
	  }
*/
	      
	      
	public void otherAssignment() { //rx assignment is above (2.1.0)
		
		// 2.0.0 assignment rules
		
		// start the rest of the OP/PB line assignment...

		for (ClaimServLine c : planMember.getServiceLines()) {
			
			//don't bother with IP claims, they should be done...
			if (c.getClaim_line_type_code().equals("IP")) {
				continue;
			}
			
			if (c.isAssigned()) {
				continue;
			}
//log.info("2.2.0");			
			//2.2.0 inpatient bubble...
			
			//only do 2.2.0 stuff if bubble is on, otherwise, continue...
			//default is YES / ON
			
			//2.2.1/2.2.2...
			
			
			if (c.getClaim_line_type_code().equals("PB")) {								
				
				for (EpisodeShell es : epsForMember) {
					boolean isAss = false;
					if (es.isDropped()) {
						continue;
					}
					if (es.getAssigned_claims()==null || es.getAssigned_claims().isEmpty()) {
						continue;
					}
					List<AssignedClaim> nac = new ArrayList<AssignedClaim>();
					for (AssignedClaim ac : es.getAssigned_claims()) {
						if (isAss) {
							continue;
						}
						if (ac.getType().equals("RX")) {
							continue;
						}

						if (ac.getClaim().getClaim_line_type_code().equals("IP")) {
	
							// claim must during IP stay (between trigger begin/end)
							//int b = c.getBegin_date().compareTo(ac.getClaim().getBegin_date());
							//int e = c.getBegin_date().compareTo(ac.getClaim().getEnd_date());
						
							// first date is zero, counting days FROM there, 
							// so if second date is earlier, the result is negative
							// if later positive...
							//long b = getDateDiff(ac.getClaim().getBegin_date(),c.getBegin_date(),TimeUnit.DAYS);
							//long e = getDateDiff(ac.getClaim().getEnd_date(),c.getBegin_date(),TimeUnit.DAYS);
							
							
							//if (b<0 || e>0) {
							//	continue;
							//}
							
							if (ac.getClaim().getBegin_date().after(c.getBegin_date()) || 
									ac.getClaim().getEnd_date().before(c.getBegin_date()) ) {
								continue;
							}
							
							boolean isD = false;
							
/*							for (DxCodeMetaData dx : epmdk.get(es.getEpisode_id()).getDx_codes()) {
								if (isD) {
									break;
								}
								if (dx.getCode_id().equals(c.getPrincipal_diag_code())) {
									isD=true;
									break;
								} else {
									for (String dc : c.getSecondary_diag_code()) {
										if (dc.equals(dx.getCode_id())) {
											isD=true;
											break;
										}
									}
								}
							}
*/
							
							if (isPDiagnosis(c, es.getEpisode_id())) {
								isD=true;
							}
							if (!isD && isSDiagnosis(c, es.getEpisode_id())) {
								isD=true;
							}
							
							if (isComplication(c, es.getEpisode_id(), "a")) {
								
								AssignedClaim as = new AssignedClaim();
								
								//as.setCategory(ac.getCategory());
								as.setCategory("C");
								as.setClaim(c);
								as.setRule("2.2.1");
								
								c.setAssigned(true);
								
								nac.add(as);
								
								isAss = true;
								
								// this rule can allow IP claims in that are outside the episode window because 
								// the original IP window might extend beyond. In these cases, we're extending
								// the episode window to the beginning of the claim

								if (as.getClaim().getBegin_date().compareTo(es.getEpisode_end_date()) > 0 ) {
									//es.setOrig_episode_end_date(es.getEpisode_end_date());
									es.setEpisode_end_date(as.getClaim().getBegin_date());
								}
								
							} else {
								if (isD) {
									AssignedClaim as = new AssignedClaim();
									
									//as.setCategory(ac.getCategory());
									as.setCategory("T");
									as.setClaim(c);
									as.setRule("2.2.2");
									
									c.setAssigned(true);
									
									nac.add(as);
									
									isAss=true;
									
									// this rule can allow IP claims in that are outside the episode window because 
									// the original IP window might extend beyond. In these cases, we're extending
									// the episode window to the beginning of the claim
									if (as.getClaim().getBegin_date().compareTo(es.getEpisode_end_date()) > 0 ) {
										//es.setOrig_episode_end_date(es.getEpisode_end_date());
										es.setEpisode_end_date(as.getClaim().getBegin_date());
									}
								}
							}
							
							
							
							//break;
							
						}
					}
					
					for (AssignedClaim ac : nac) {
						es.getAssigned_claims().add(ac);
					}
					
				}
				
				
			}
			
			if (c.isAssigned()) {
				continue;
			}
		
			
			//2.3.0 OP/PB P triggers
	//log.info("2.3.0");
			
			// this rule needs to be reorganized to allow potential triggers...
				
					
			for (EpisodeShell es : epsForMember) {
				
				if (es.isDropped()) {
					continue;
				}
				
				if (!es.getEpisode_type().equals("P")) {
					continue;
				}
				
				// We no longer need to assess whether or not the triggering claim is assigned...
				/*
				 * if (es.getClaim().isAssigned()) {
				 *	//continue;
				 * }
				 */
				
				if (es.getClaim().getClaim_line_type_code().equals("IP")) {
					continue;
				}
				
				boolean isP = false;
				
				//ClaimServLine c = es.getClaim();
				
				
				// claim must during IP stay (between trigger begin/end)
				//int b = c.getBegin_date().compareTo(ac.getClaim().getBegin_date());
				//int e = c.getBegin_date().compareTo(ac.getClaim().getEnd_date());
			
				// first date is zero, counting days FROM there, 
				// so if second date is earlier, the result is negative
				// if later positive...
				long b = getDateDiff(es.getClaim().getBegin_date(),c.getBegin_date(),TimeUnit.DAYS);
				long e = getDateDiff(es.getClaim().getEnd_date(),c.getBegin_date(),TimeUnit.DAYS);
				
				
				if (b<0 || e>0) {
					continue;
				}
				
				
				for (MedCode mc : c.getPrincipal_proc_code_objects()) {
					if (epmdk.get(es.getEpisode_id()).getTrigger_code_by_ep().containsKey(mc.getNomen()) && mc.getCode_value() != null) {
						if (epmdk.get(es.getEpisode_id()).getTrigger_code_by_ep().get(mc.getNomen()).containsKey(mc.getCode_value())) {
							isP = true;
							break;
						}
					}
					// if HCPC, it might be a CPT code...
					if (mc.getNomen() != null && mc.getNomen().equals("HCPC")) {
						if (epmdk.get(es.getEpisode_id()).getTrigger_code_by_ep().containsKey("CPT") && mc.getCode_value() != null) {
							if (epmdk.get(es.getEpisode_id()).getTrigger_code_by_ep().get("CPT").containsKey(mc.getCode_value())) {
								isP = true;
								break;
							}
						}
					}
					if (mc.getNomen() != null && mc.getNomen().equals("CPT")) {
						if (epmdk.get(es.getEpisode_id()).getTrigger_code_by_ep().containsKey("HCPC") && mc.getCode_value() != null) {
							if (epmdk.get(es.getEpisode_id()).getTrigger_code_by_ep().get("HCPC").containsKey(mc.getCode_value())) {
								isP = true;
								break;
							}
						}
					}
				}
			
				if (!isP) {
					for (MedCode mc : c.getSecondary_proc_code_objects()) {
						if (mc.getNomen() != null && mc.getCode_value() != null) {
							if (epmdk.get(es.getEpisode_id()).getTrigger_code_by_ep().get(mc.getNomen()).containsKey(mc.getCode_value())) {
								isP = true;
								break;
							}
						}
						if (mc.getNomen() != null && mc.getNomen().equals("HCPC")) {
							if (epmdk.get(es.getEpisode_id()).getTrigger_code_by_ep().containsKey("CPT") && mc.getCode_value() != null) {
								if (epmdk.get(es.getEpisode_id()).getTrigger_code_by_ep().get("CPT").containsKey(mc.getCode_value())) {
									isP = true;
									break;
								}
							}
						}
						if (mc.getNomen() != null && mc.getNomen().equals("CPT")) {
							if (epmdk.get(es.getEpisode_id()).getTrigger_code_by_ep().containsKey("HCPC") && mc.getCode_value() != null) {
								if (epmdk.get(es.getEpisode_id()).getTrigger_code_by_ep().get("HCPC").containsKey(mc.getCode_value())) {
									isP = true;
									break;
								}
							}
						}
					}
				} 
			
				if (!isP) {
					
					continue;
				}
				
				AssignedClaim as = new AssignedClaim();
				
				as.setCategory("T");
				as.setClaim(c);
				as.setRule("2.3.2");
				
				if (isComplication(c, es.getEpisode_id(), "a")) {
					as.setCategory("TC");
					as.setRule("2.3.1");
				}
				
				c.setAssigned(true);
				
				es.getAssigned_claims().add(as);
				
				
			} // end 2.3
		
		
			if (c.isAssigned()) {
				continue;
			}
			
			// 2.4 - 2.5 - all claims must be between trigger start and episode end (inclusive)
//log.info("2.4.0");			
			// 2.4.0 -  non-trigger op/pb claims in the window above that have a dx complication...
			// 10/14/14 EB removed non-trigger requirement for this, not in documentation as of this date.
			
			boolean isTrig = false;
			for (EpisodeShell es : epsForMember) {
				if (es.isDropped()) {
					continue;
				}

				// cannot be an SRF...
				if (es.getEpisode_type().equals("S")) {
					continue;
				}
				
				
				if (c.equals(es.getClaim())) {
					// removed per EB's change in rule...
					//isTrig = true;
				}
			
				if (!isTrig) {
					// claim must during between trigger start and episode end
					//int b = c.getBegin_date().compareTo(es.getClaim().getBegin_date());
					//int e = c.getBegin_date().compareTo(es.getEpisode_end_date());
					
					
					// first date is zero, counting days FROM there, 
					// so if second date is earlier, the result is negative
					// if later positive...
					long b = getDateDiff(es.getClaim().getBegin_date(),c.getBegin_date(),TimeUnit.DAYS);
					long e = getDateDiff(es.getEpisode_end_date(),c.getBegin_date(),TimeUnit.DAYS);
					
					if (b<0 || e>0) {
						continue;
					}
					
					if (isComplication(c, es.getEpisode_id(), "a")) {
						
						AssignedClaim as = new AssignedClaim();
						as.setCategory("C");
						as.setClaim(c);
						as.setRule("2.4.0");
						
						c.setAssigned(true);
						
						es.getAssigned_claims().add(as);
					}
				}
			} // end 2.4.0
			
			if (c.isAssigned()) {
				continue;
			}
			
			// 2.5.0 - Potentially Typical Services (w/in trigger start to episode end inclusive)
			
			// !!! ALL 2.5 SUB RULES ARE TERMINAL - LOOP FOR EACH !!! //
//log.info("2.5.0");	

			// 2.5.1
			for (EpisodeShell es : epsForMember) {
				if (es.isDropped()) {
					continue;
				}
				// cannot be an SRF...
				if (es.getEpisode_type().equals("S")) {
					continue;
				}
				
				if (c.getBegin_date().before(es.getClaim().getBegin_date()) ) {
					continue;
				}
				if (c.getBegin_date().after(es.getEpisode_end_date()) ) {
					continue;
				}
				
				boolean isP = false;
				boolean isD = false;
				boolean isSuff = false;				
				
				String proc = isPProcedural(c, es.getEpisode_id());
				if (proc.equals("p") || proc.equals("s")) {
					isP=true;
					if (proc.equals("s")) {
						isSuff=true;
					}
				}
				if (!isP) {
					String sproc = isSProcedural(c, es.getEpisode_id());
					if (sproc.equals("p") || sproc.equals("s")) {
						isP=true;
						if (sproc.equals("s")) {
							isSuff=true;
						}
					}
				}

				if (!isP) {
					continue;
				}
				
				if (isPDiagnosis(c, es.getEpisode_id())) {
					isD=true;
				}
				if (!isD && isSDiagnosis(c, es.getEpisode_id())) {
					isD=true;
				}
				
				if (!isD) {
					continue;
				}
				
				if (!isSuff) {
					continue;
				}
				
				AssignedClaim as = new AssignedClaim();
				as.setCategory("T");
				as.setClaim(c);
				as.setRule("2.5.1");
				
				c.setAssigned(true);
				
				es.getAssigned_claims().add(as);
				
				
			} // end 2.5.1
			
			if (c.isAssigned()) {
				continue;
			}
			
			//2.5.2
			
			for (EpisodeShell es : epsForMember) {
				if (es.isDropped()) {
					continue;
				}
				// cannot be an SRF...
				if (es.getEpisode_type().equals("S")) {
					continue;
				}

				if (c.getBegin_date().before(es.getClaim().getBegin_date()) ) {
					continue;
				}
				if (c.getBegin_date().after(es.getEpisode_end_date()) ) {
					continue;
				}
				
				boolean isP = false;
				boolean isSuff = false;
				
				String proc = isPProcedural(c, es.getEpisode_id());
				if (proc.equals("p") || proc.equals("s")) {
					isP=true;
					if (proc.equals("s")) {
						isSuff=true;
					}
				}
				if (!isP) {
					String sproc = isSProcedural(c, es.getEpisode_id());
					if (sproc.equals("p") || sproc.equals("s")) {
						isP=true;
						if (sproc.equals("s")) {
							isSuff=true;
						}
					}
				}

				if (!isP) {
					continue;
				}
				
				isTrig=false;
				if (c.getPrincipal_diag_code_object() != null  &&  c.getPrincipal_diag_code_object().getNomen()  !=  null 
						&& epmdk.get(es.getEpisode_id()).getTrigger_code_by_ep().get(c.getPrincipal_diag_code_object().getNomen())  !=  null) {
					if (epmdk.get(es.getEpisode_id()).getTrigger_code_by_ep().get(c.getPrincipal_diag_code_object().getNomen()).containsKey(c.getPrincipal_diag_code())) {
						isTrig = true;
					} else {
						//if (c.getSecondary_diag_code_objects().get(0).getNomen())
						for (MedCode mc : c.getSecondary_diag_code_objects()) {
							if (mc.getNomen()!=null && epmdk.get(es.getEpisode_id()).getTrigger_code_by_ep().get(mc.getNomen())!=null) {
								if (epmdk.get(es.getEpisode_id()).getTrigger_code_by_ep().get(mc.getNomen()).containsKey(mc.getCode_value())) {
									isTrig=true;
									break;
								}
							}
						}
					}
				}
				
				if (!isTrig) {
					continue;
				}
				
				AssignedClaim as = new AssignedClaim();
				as.setCategory("T");
				as.setClaim(c);
				as.setRule("2.5.2");
				
				c.setAssigned(true);
				
				es.getAssigned_claims().add(as);
				
				
			} // end 2.5.2
			
			if (c.isAssigned()) {
				continue;
			}
			
			// 2.5.3
			
			for (EpisodeShell es : epsForMember) {
				if (es.isDropped()) {
					continue;
				}
				// cannot be an SRF...
				if (es.getEpisode_type().equals("S")) {
					continue;
				}
	
				if (c.getBegin_date().before(es.getClaim().getBegin_date()) ) {
					continue;
				}
				if (c.getBegin_date().after(es.getEpisode_end_date()) ) {
					continue;
				}

				boolean isSuff = false;
				
				if (isPProcedural(c, es.getEpisode_id()).equals("s")) {
					isSuff=true;
				}
				if (!isSuff && isSProcedural(c, es.getEpisode_id()).equals("s")) {
					isSuff=true;
				}
				
				if (!isSuff) {
					continue;
				}
				
				AssignedClaim as = new AssignedClaim();
				as.setCategory("T");
				as.setClaim(c);
				as.setRule("2.5.3");
				
				c.setAssigned(true);
				
				es.getAssigned_claims().add(as);
				
			} // end 2.5.3
			
			if (c.isAssigned()) {
				continue;
			}
			
			//2.5.4
			
			for (EpisodeShell es : epsForMember) {
				if (es.isDropped()) {
					continue;
				}
				// cannot be an SRF...
				if (es.getEpisode_type().equals("S")) {
					continue;
				}
//log.info("episode: " + es.getEpisode_id() + "_ mem _" + es.getClaim().getClaim_id() + "_" + es.getClaim().getClaim_line_id());
				if (c.getBegin_date().before(es.getClaim().getBegin_date()) ) {
					continue;
				}
				if (c.getBegin_date().after(es.getEpisode_end_date()) ) {
					continue;
				}
//log.info("claim passed date compare: " + c.getClaim_id() + "_" + c.getClaim_line_id());				
				boolean isP = false;
				boolean isD = false;
				
				String proc = isPProcedural(c, es.getEpisode_id());
				if (proc.equals("p") || proc.equals("s")) {
					isP=true;
				}
				if (!isP) {
					String sproc = isSProcedural(c, es.getEpisode_id());
					if (sproc.equals("p") || sproc.equals("s")) {
						isP=true;
					}
				}
//log.info("isP: " + isP);				
				if (!isP) {
					continue;
				}
				
				if (isPDiagnosis(c, es.getEpisode_id())) {
					isD=true;
				}
				if (!isD && isSDiagnosis(c, es.getEpisode_id())) {
					isD=true;
				}
//log.info("claim: " + c.getClaim_id() + "_" + c.getClaim_line_id() + " 2.5.4 results: isD: " + isD + " | isP: " + isP);				
				if (!isD) {
					continue;
				}
//log.info("2.5.4 Passed");				
				AssignedClaim as = new AssignedClaim();
				as.setCategory("T");
				as.setClaim(c);
				as.setRule("2.5.4");
				
				c.setAssigned(true);
				
				es.getAssigned_claims().add(as);
				
				
			} // end 2.5.4
			
			if (c.isAssigned()) {
				continue;
			}
//log.info("2.6.0");			
			// 2.6 - 2.8 all claims must be in lookback (>= episode begin < trigger begin)
			// All a/b sub rules ARE TERMINAL!
			
			//2.6.1/2 - Coded as Typical Lookback "YES"
			// Needs to be coded as yes only for "P" as default
			
			for (EpisodeShell es : epsForMember) {
				if (es.isDropped()) {
					continue;
				}
				// cannot be an SRF...
				if (es.getEpisode_type().equals("S")) {
					continue;
				}

				// claim must between Episode start and trigger Begin (>=ES <tb)
/*				
log.info("2.6 Dates: Claim to assign: " + c.getClaim_id() + "_" + c.getClaim_line_id() + 
	"  begin: " + c.getBegin_date() + " end: " + c.getEnd_date());
log.info("2.6 Dates: Episode: " + es.getEpisode_id() + "_" + es.getClaim().getMember_id() + "_" +
	es.getClaim_id() + "_" + es.getClaim().getClaim_line_id() + "  trigbegin: " + 
	es.getClaim().getBegin_date() + " epBegin: " + es.getEpisode_begin_date());
*/			
				
				if (es.getEpisode_begin_date().after(c.getBegin_date())) {
					continue;
				}
				if (es.getClaim().getBegin_date().before(c.getBegin_date())) {
					continue;
				}
				
//log.info("2.6 date check pass");				
				if (!isComplication(c, es.getEpisode_id(), "a")) {
					continue;
				}
//log.info("2.6 is complication pass");				

				for (EpisodeShell tes : epsForMember) {
					if (es.isDropped()) {
						continue;
					}
					if (es.getEpisode_type().equals("S")) {
						continue;
					}
					if (es.getClaim().equals(c)) {
						isTrig=true;
						break;
					}
				}
				// must NOT be the trigger of another episode...
				if (isTrig) {
					continue;
				}
//log.info("2.6 isTrig Pass");				
				AssignedClaim as = new AssignedClaim();
				as.setClaim(c);
				c.setAssigned(true);
				
				if (es.getEpisode_type().equals("P")) {
					
					as.setCategory("T");
					as.setRule("2.6.2");
					
				} else {
					
					as.setCategory("C");
					as.setRule("2.6.1");
					
				}
				
				es.getAssigned_claims().add(as);
			
			}  // End 2.6
			
			if (c.isAssigned()) {
				continue;
			}
			
			// 2.7 (Another A/B with non-terminal sub rules...
			// NO! per Sarah 2017/01 must be terminal sub rules...
//log.info("2.7.0");			
			
			/*
			 * !!! --- !!!			
			 */
			
			// 2.7.1
			for (EpisodeShell es : epsForMember) {
				if (es.isDropped()) {
					continue;
				}
				// cannot be an SRF...
				if (es.getEpisode_type().equals("S")) {
					continue;
				}
//log.info("episode: " + es.getEpisode_id() + "_ mem _" + es.getClaim().getClaim_id() + "_" + es.getClaim().getClaim_line_id());				
				// >= episode start < trigger start
				if (c.getBegin_date().before(es.getEpisode_begin_date()) ) {
					continue;
				}
				if (c.getBegin_date().before(es.getClaim().getBegin_date()) ) {
					//good
				} else {
					continue;
				}
//log.info("claim passed date compare: " + c.getClaim_id() + "_" + c.getClaim_line_id());				
				boolean isP = false;
				boolean isD = false;
				boolean isSuff = false;
				isTrig = false;
				
				String proc = isPProcedural(c, es.getEpisode_id());
				if (proc.equals("p") || proc.equals("s")) {
					isP=true;
					if (proc.equals("s")) {
						isSuff=true;
					}
				}
				if (!isP) {
					String sproc = isSProcedural(c, es.getEpisode_id());
					if (sproc.equals("p") || sproc.equals("s")) {
						isP=true;
						if (sproc.equals("s")) {
							isSuff=true;
						}
					}
				}
//log.info("isP: " + isP + " | isSuff: " + isSuff);				
				if (isPDiagnosis(c, es.getEpisode_id())) {
					isD=true;
				}
				if (!isD && isSDiagnosis(c, es.getEpisode_id())) {
					isD=true;
				}
				
//log.info("claim: " + c.getClaim_id() + "_" + c.getClaim_line_id() + " 2.7.1 results: isD: " + isD + " | isP: " + isP + " | isSuff: " + isSuff);
				
				if (isD && isSuff) {
					AssignedClaim as = new AssignedClaim();
					as.setCategory("T");
					as.setClaim(c);
					as.setRule("2.7.1");
					
					c.setAssigned(true);
					
					es.getAssigned_claims().add(as);
//log.info("2.7.1 Assigned");
				}
			}
			
			if (c.isAssigned()) {
				continue;
			}
			
			// 2.7.2
			for (EpisodeShell es : epsForMember) {
				if (es.isDropped()) {
					continue;
				}
				// cannot be an SRF...
				if (es.getEpisode_type().equals("S")) {
					continue;
				}
//log.info("episode: " + es.getEpisode_id() + "_ mem _" + es.getClaim().getClaim_id() + "_" + es.getClaim().getClaim_line_id());				
				// >= episode start < trigger start
				if (c.getBegin_date().before(es.getEpisode_begin_date()) ) {
					continue;
				}
				if (c.getBegin_date().before(es.getClaim().getBegin_date()) ) {
					//good
				} else {
					continue;
				}
//log.info("claim passed date compare: " + c.getClaim_id() + "_" + c.getClaim_line_id());				
				boolean isP = false;
				boolean isD = false;
				boolean isSuff = false;
				isTrig = false;
				
				String proc = isPProcedural(c, es.getEpisode_id());
				if (proc.equals("p") || proc.equals("s")) {
					isP=true;
					if (proc.equals("s")) {
						isSuff=true;
					}
				}
				if (!isP) {
					String sproc = isSProcedural(c, es.getEpisode_id());
					if (sproc.equals("p") || sproc.equals("s")) {
						isP=true;
						if (sproc.equals("s")) {
							isSuff=true;
						}
					}
				}
//log.info("isP: " + isP + " | isSuff: " + isSuff);				
				if (isPDiagnosis(c, es.getEpisode_id())) {
					isD=true;
				}
				if (!isD && isSDiagnosis(c, es.getEpisode_id())) {
					isD=true;
				}

				
				isTrig = false;
				
				if (c.getPrincipal_diag_code_object() != null  &&  c.getPrincipal_diag_code_object().getNomen()  !=  null 
						&& epmdk.get(es.getEpisode_id()).getTrigger_code_by_ep().get(c.getPrincipal_diag_code_object().getNomen())  !=  null) {
					if (epmdk.get(es.getEpisode_id()).getTrigger_code_by_ep().get(c.getPrincipal_diag_code_object().getNomen()).containsKey(c.getPrincipal_diag_code())) {
						isTrig = true;
					} else {
						//if (c.getSecondary_diag_code_objects().get(0).getNomen())
						for (MedCode mc : c.getSecondary_diag_code_objects()) {
							if (mc.getNomen()  !=  null && epmdk.get(es.getEpisode_id()).getTrigger_code_by_ep().get(mc.getNomen())  !=  null) {
								if (epmdk.get(es.getEpisode_id()).getTrigger_code_by_ep().get(mc.getNomen()).containsKey(mc.getCode_value())) {
									isTrig=true;
									break;
								}
							}
						}
					}
				}
				
				if (isD && isP && isTrig) {
//log.info("2.7.2 passed");					
					AssignedClaim as = new AssignedClaim();
					as.setCategory("T");
					as.setClaim(c);
					as.setRule("2.7.2");
					
					c.setAssigned(true);
					
					es.getAssigned_claims().add(as);
				
				}
			
			}
			
			if (c.isAssigned()) {
				continue;
			}
			
			// 2.7.3
			for (EpisodeShell es : epsForMember) {
				if (es.isDropped()) {
					continue;
				}
				// cannot be an SRF...
				if (es.getEpisode_type().equals("S")) {
					continue;
				}
//log.info("episode: " + es.getEpisode_id() + "_ mem _" + es.getClaim().getClaim_id() + "_" + es.getClaim().getClaim_line_id());				
				// >= episode start < trigger start
				if (c.getBegin_date().before(es.getEpisode_begin_date()) ) {
					continue;
				}
				if (c.getBegin_date().before(es.getClaim().getBegin_date()) ) {
					//good
				} else {
					continue;
				}
//log.info("claim passed date compare: " + c.getClaim_id() + "_" + c.getClaim_line_id());				
				boolean isP = false;
				boolean isD = false;
				boolean isSuff = false;
				isTrig = false;
				
				String proc = isPProcedural(c, es.getEpisode_id());
				if (proc.equals("p") || proc.equals("s")) {
					isP=true;
					if (proc.equals("s")) {
						isSuff=true;
					}
				}
				if (!isP) {
					String sproc = isSProcedural(c, es.getEpisode_id());
					if (sproc.equals("p") || sproc.equals("s")) {
						isP=true;
						if (sproc.equals("s")) {
							isSuff=true;
						}
					}
				}
//log.info("isP: " + isP + " | isSuff: " + isSuff);				
				if (isPDiagnosis(c, es.getEpisode_id())) {
					isD=true;
				}
				if (!isD && isSDiagnosis(c, es.getEpisode_id())) {
					isD=true;
				}

				
				isTrig = false;
				
				if (c.getPrincipal_diag_code_object() != null  &&  c.getPrincipal_diag_code_object().getNomen()  !=  null 
						&& epmdk.get(es.getEpisode_id()).getTrigger_code_by_ep().get(c.getPrincipal_diag_code_object().getNomen())  !=  null) {
					if (epmdk.get(es.getEpisode_id()).getTrigger_code_by_ep().get(c.getPrincipal_diag_code_object().getNomen()).containsKey(c.getPrincipal_diag_code())) {
						isTrig = true;
					} else {
						//if (c.getSecondary_diag_code_objects().get(0).getNomen())
						for (MedCode mc : c.getSecondary_diag_code_objects()) {
							if (mc.getNomen()  !=  null && epmdk.get(es.getEpisode_id()).getTrigger_code_by_ep().get(mc.getNomen())  !=  null) {
								if (epmdk.get(es.getEpisode_id()).getTrigger_code_by_ep().get(mc.getNomen()).containsKey(mc.getCode_value())) {
									isTrig=true;
									break;
								}
							}
						}
					}
				}
				
				if ( isSuff ) {
//log.info("2.7.3 passed");
					AssignedClaim as = new AssignedClaim();
					as.setCategory("T");
					as.setClaim(c);
					
					as.setRule("2.7.3");

					
					c.setAssigned(true);
					
					es.getAssigned_claims().add(as);
				}
			
			}
			
			if (c.isAssigned()) {
				continue;
			}
			
			// 2.7.4
			for (EpisodeShell es : epsForMember) {
				if (es.isDropped()) {
					continue;
				}
				// cannot be an SRF...
				if (es.getEpisode_type().equals("S")) {
					continue;
				}
//log.info("episode: " + es.getEpisode_id() + "_ mem _" + es.getClaim().getClaim_id() + "_" + es.getClaim().getClaim_line_id());				
				// >= episode start < trigger start
				if (c.getBegin_date().before(es.getEpisode_begin_date()) ) {
					continue;
				}
				if (c.getBegin_date().before(es.getClaim().getBegin_date()) ) {
					//good
				} else {
					continue;
				}
//log.info("claim passed date compare: " + c.getClaim_id() + "_" + c.getClaim_line_id());				
				boolean isP = false;
				boolean isD = false;
				boolean isSuff = false;
				isTrig = false;
				
				String proc = isPProcedural(c, es.getEpisode_id());
				if (proc.equals("p") || proc.equals("s")) {
					isP=true;
					if (proc.equals("s")) {
						isSuff=true;
					}
				}
				if (!isP) {
					String sproc = isSProcedural(c, es.getEpisode_id());
					if (sproc.equals("p") || sproc.equals("s")) {
						isP=true;
						if (sproc.equals("s")) {
							isSuff=true;
						}
					}
				}
//log.info("isP: " + isP + " | isSuff: " + isSuff);				
				if (isPDiagnosis(c, es.getEpisode_id())) {
					isD=true;
				}
				if (!isD && isSDiagnosis(c, es.getEpisode_id())) {
					isD=true;
				}

				
				isTrig = false;
				
				if (c.getPrincipal_diag_code_object() != null  &&  c.getPrincipal_diag_code_object().getNomen()  !=  null 
						&& epmdk.get(es.getEpisode_id()).getTrigger_code_by_ep().get(c.getPrincipal_diag_code_object().getNomen())  !=  null) {
					if (epmdk.get(es.getEpisode_id()).getTrigger_code_by_ep().get(c.getPrincipal_diag_code_object().getNomen()).containsKey(c.getPrincipal_diag_code())) {
						isTrig = true;
					} else {
						//if (c.getSecondary_diag_code_objects().get(0).getNomen())
						for (MedCode mc : c.getSecondary_diag_code_objects()) {
							if (mc.getNomen()  !=  null && epmdk.get(es.getEpisode_id()).getTrigger_code_by_ep().get(mc.getNomen())  !=  null) {
								if (epmdk.get(es.getEpisode_id()).getTrigger_code_by_ep().get(mc.getNomen()).containsKey(mc.getCode_value())) {
									isTrig=true;
									break;
								}
							}
						}
					}
				}
				
				if ( isD && isP ) {
//log.info("2.7.4 passed");
					AssignedClaim as = new AssignedClaim();
					as.setCategory("T");
					as.setClaim(c);
					
					as.setRule("2.7.4");

					
					c.setAssigned(true);
					
					es.getAssigned_claims().add(as);
				}
			
			}		
			
			
			//////OLD//////
			
/*			
			// 2.7.1
			for (EpisodeShell es : epsForMember) {
				if (es.isDropped()) {
					continue;
				}
				// cannot be an SRF...
				if (es.getEpisode_type().equals("S")) {
					continue;
				}
//log.info("episode: " + es.getEpisode_id() + "_ mem _" + es.getClaim().getClaim_id() + "_" + es.getClaim().getClaim_line_id());				
				// >= episode start < trigger start
				if (c.getBegin_date().before(es.getEpisode_begin_date()) ) {
					continue;
				}
				if (c.getBegin_date().before(es.getClaim().getBegin_date()) ) {
					//good
				} else {
					continue;
				}
//log.info("claim passed date compare: " + c.getClaim_id() + "_" + c.getClaim_line_id());				
				boolean isP = false;
				boolean isD = false;
				boolean isSuff = false;
				isTrig = false;
				
				String proc = isPProcedural(c, es.getEpisode_id());
				if (proc.equals("p") || proc.equals("s")) {
					isP=true;
					if (proc.equals("s")) {
						isSuff=true;
					}
				}
				if (!isP) {
					String sproc = isSProcedural(c, es.getEpisode_id());
					if (sproc.equals("p") || sproc.equals("s")) {
						isP=true;
						if (sproc.equals("s")) {
							isSuff=true;
						}
					}
				}
//log.info("isP: " + isP + " | isSuff: " + isSuff);				
				if (isPDiagnosis(c, es.getEpisode_id())) {
					isD=true;
				}
				if (!isD && isSDiagnosis(c, es.getEpisode_id())) {
					isD=true;
				}
				
//log.info("claim: " + c.getClaim_id() + "_" + c.getClaim_line_id() + " 2.7.1 results: isD: " + isD + " | isP: " + isP + " | isSuff: " + isSuff);
				
				if (isD && isSuff) {
					AssignedClaim as = new AssignedClaim();
					as.setCategory("T");
					as.setClaim(c);
					as.setRule("2.7.1");
					
					c.setAssigned(true);
					
					es.getAssigned_claims().add(as);
//log.info("2.7.1 Assigned");
				} else {
					
					isTrig = false;
					
					if (c.getPrincipal_diag_code_object() != null  &&  c.getPrincipal_diag_code_object().getNomen()  !=  null 
							&& epmdk.get(es.getEpisode_id()).getTrigger_code_by_ep().get(c.getPrincipal_diag_code_object().getNomen())  !=  null) {
						if (epmdk.get(es.getEpisode_id()).getTrigger_code_by_ep().get(c.getPrincipal_diag_code_object().getNomen()).containsKey(c.getPrincipal_diag_code())) {
							isTrig = true;
						} else {
							//if (c.getSecondary_diag_code_objects().get(0).getNomen())
							for (MedCode mc : c.getSecondary_diag_code_objects()) {
								if (mc.getNomen()  !=  null && epmdk.get(es.getEpisode_id()).getTrigger_code_by_ep().get(mc.getNomen())  !=  null) {
									if (epmdk.get(es.getEpisode_id()).getTrigger_code_by_ep().get(mc.getNomen()).containsKey(mc.getCode_value())) {
										isTrig=true;
										break;
									}
								}
							}
						}
					}
					
					if (isD && isP && isTrig) {
//log.info("2.7.2 passed");					
						AssignedClaim as = new AssignedClaim();
						as.setCategory("T");
						as.setClaim(c);
						as.setRule("2.7.2");
						
						c.setAssigned(true);
						
						es.getAssigned_claims().add(as);
					
					} else {
//log.info("claim: " + c.getClaim_id() + "_" + c.getClaim_line_id() + " 2.7.3 results: isD: " + isD + " | isP: " + isP + " | isSuff: " + isSuff);						
						if ( (isD && isP) || isSuff ) {
//log.info("2.7.3 or 4 passed");
							AssignedClaim as = new AssignedClaim();
							as.setCategory("T");
							as.setClaim(c);
							
							if (isSuff) {
								as.setRule("2.7.3");
							} else {
								as.setRule("2.7.4");
							}
							
							c.setAssigned(true);
							
							es.getAssigned_claims().add(as);
						}
						
					}
					
				}
				
				
			} // end 2.7.1-4
*/			
			
			if (c.isAssigned()) {
				continue;
			}
			
			// Adding in 5.3 changes which make the above not accept SRFs and
			// the below are new rules for SRFs and Sick Care and Newborn
			
			// begin 2.8 Newborn Episodes Only: EX1502
			
			// begin 2.8.x
			
			for (EpisodeShell es : epsForMember) {
				if (es.isDropped()) {
					continue;
				}
				// cannot be an SRF...
				if (!es.getEpisode_id().equals("EX1502")) {
					continue;
				}
				// claim must be after or equal to episode start date
		
				// first date is zero, counting days FROM there, 
				// so if second date is earlier, the result is negative
				// if later positive...
				
				long b = getDateDiff(es.getEpisode_begin_date(),c.getBegin_date(),TimeUnit.DAYS);
				long e = getDateDiff(es.getEpisode_end_date(),c.getBegin_date(),TimeUnit.DAYS);
				
				if (b<0 || e>0) {
					continue;
				}
				
				boolean isC = false;
				/*
				for (DxCodeMetaData dx : epmdk.get(es.getEpisode_id()).getDx_codes()) {
					if (isD) {
						break;
					}
					if (dx.getCode_id().equals(c.getPrincipal_diag_code())) {
						isD=true;
						break;
					} else {
						for (String dc : c.getSecondary_diag_code()) {
							if (dc.equals(dx.getCode_id())) {
								isD=true;
								break;
							}
						}
					}
				}
				
				*/
				//if (!isD) {
				//	continue;
				//}
				
				if (isComplication(c, es.getEpisode_id(), "a")) {
					isC = true;
				}
				
				AssignedClaim as = new AssignedClaim();
				as.setCategory("T");
				as.setClaim(c);
				as.setRule("2.8.2");
				
				// if complication...
				if (isC) {
					as.setCategory("C");
					as.setRule("2.8.1");
				}
				
				c.setAssigned(true);
				
				es.getAssigned_claims().add(as);
				
				
			} // end 2.8.x
			
			if (c.isAssigned()) {
				continue;
			}

			
		} // end claim loop
		
// STARTING SRFs and SICK CARE episode assignment HERE...

		/*
		 * before starting a new claim loop for srf/sick care, roll through and drop any whose
		 * trigger claim is already assigned elsewhere...
		 */
		
		for (EpisodeShell es : epsForMember) {
			if (es.isDropped()) {
				continue;
			}
			if (es.getEpisode_type().equals("S")) {
				if (es.getClaim().isAssigned()) {
					es.setDropped(true);
				}
			}
		}
		
		/*
		 * New SRF/SICKCARE process will be to loop BY EPISODE first, then claim
		 */
		
		//1.9 SRFs   -- formerly 1.8 SRFs - 
		// 5.3 update, this is now 1.9 and excludes sick care episodes ES9901.
		
		for (EpisodeShell es : epsForMember) {
			
			if (es.isDropped()) {
				continue;
			}	
			// make sure this is an SRF and not SICK Care...
			if (es.getEpisode_type().equals("S") && !es.getEpisode_id().equals("ES9901")) {

				// Drop this episode and continue if the trigger has already been assigned elsewhere...
				if (es.getClaim().isAssigned()) {
					es.setDropped(true);
					continue;
				}

				
				// the episode's  trigger has not been assigned elsewhere, so lets assign it to this episode
				// and start assigning any other IP's within the time period...
				AssignedClaim as = new AssignedClaim();
				
				as.setCategory("C");
				as.setClaim(es.getClaim());
				as.setRule("1.9.0");
				
				es.getClaim().setAssigned(true);
				
				es.getAssigned_claims().add(as);
				
				// now see what other claims we can assign to this episode...
				for (ClaimServLine c : planMember.getServiceLines()) {
					
					if (es.getClaim().equals(c)) {
						continue;
					}
					
					if (!c.getClaim_line_type_code().equals("IP")) {
						continue;
					}
					
					// in order to allow multi-assignment, we'll see if this claim is assigned to
					// another episode already *under the same rule*, if it is unassigned OR
					// already assigned under the same rule, it can proceed.
					
					//isAssignedTo(ClaimServLine c, String rule, String epType)
					if (c.isAssigned() && !isAssignedTo(c, "1.9.0", "S")) {
						continue;
					}
					
					
					
					// claim must be on or after the episode start and on or before the episode end
					
					if (c.getBegin_date().before(es.getEpisode_begin_date())) {
						continue;
					}
					if (c.getBegin_date().after(es.getEpisode_end_date())) {
						continue;
					}
					
					
						
					// Primary DX must be relevant
					
					boolean isD = false;
					boolean isC = false;
													
					if (isPDiagnosis(c, es.getEpisode_id())) {
						isD = true;
					}
												
					if (isComplication(c, es.getEpisode_id(), "p")) {
						isC = true;
					}
					
					//if (isD || isC) {
					
					AssignedClaim as2 = new AssignedClaim();
				
					as2.setCategory("C");
					as2.setClaim(c);
					as2.setRule("1.9.0");
					
					c.setAssigned(true);
					
					es.getAssigned_claims().add(as2);
					//}
				}
				
			}
		}  // end 1.9
		
		//2.9 SRFs 
		// excludes sick care episodes ES9901. Rx is handled in RX loop by early 2.x rules 
		for (EpisodeShell es : epsForMember) {
			if (es.isDropped()) {
				continue;
			}
			
			// make sure this is an SRF and not SICK Care...
			if (es.getEpisode_type().equals("S") && !es.getEpisode_id().equals("ES9901")) {
				//good
			} else {
				continue;
			}

			// try to assign OP/PB claims
					
			for (ClaimServLine c : planMember.getServiceLines()) {

				
				if (c.getClaim_line_type_code().equals("IP")) {
					continue;
				}
				
				// in order to allow multi-assignment, we'll see if this claim is assigned to
				// another episode already *under the same rule*, if it is unassigned OR
				// already assigned under the same rule, it can proceed.
				
				//isAssignedTo(ClaimServLine c, String rule, String epType)
				if (c.isAssigned() && !isAssignedTo(c, "2.9.0", "S")) {
					continue;
				}
				
				
				
				// claim must be on or after the episode start and on or before the episode end
				
				if (c.getBegin_date().before(es.getEpisode_begin_date())) {
					continue;
				}
				if (c.getBegin_date().after(es.getEpisode_end_date())) {
					continue;
				}
				
					
				AssignedClaim as = new AssignedClaim();
			
				as.setCategory("C");
				as.setClaim(c);
				as.setRule("2.9.0");
				
				c.setAssigned(true);
				
				es.getAssigned_claims().add(as);

			}
		} // end 2.9
		
		
		
		//2.11 / 2.12 Sick Care Only
		// sick care episodes ES9901. 

		for (EpisodeShell es : epsForMember) {
			
			if (es.isDropped()) {
				continue;
			}	
			
			if (es.getEpisode_id().equals("ES9901")) {
				// make sure you only check to see if the type we're evaluating is assigned or we'll drop all SRFs
				
				if (es.getClaim().isAssigned() && !es.isTrig_by_episode()) {
					es.setDropped(true);
					continue;
				}
				
				//try to assign the trigger to this episode, if you can't, drop the episode...
				boolean trigAssigned=false;
				
				boolean isC = false;
				if (isComplication(es.getClaim(), es.getEpisode_id(), "a")) {
					isC = true;
				}
				
				if (isC) {
				
					AssignedClaim as = new AssignedClaim();
				
					as.setCategory("C");
					as.setClaim(es.getClaim());
					as.setRule("2.11.0");
					
					es.getClaim().setAssigned(true);
					
					es.getAssigned_claims().add(as);
					trigAssigned=true;
					
				} else {
				
					// 2.12 ...
					
					boolean isP = false;
					boolean isD = false;
					boolean isSuff = false;
					
					String pp = isPProcedural(es.getClaim(), es.getEpisode_id());
					if (!pp.equals("no")) {
						isP = true;
						if (pp.equals("s")) {
							isSuff = true;
						}
						if (!isP) {
							String sp = isSProcedural(es.getClaim(), es.getEpisode_id());
							if (!sp.equals("no")) {
								isP = true;
								if (sp.equals("s")) {
									isSuff = true;
								}
							}
						}
					}
					
					if (isP && !isSuff) {
					
					
								
						if (isPDiagnosis(es.getClaim(), es.getEpisode_id())) {
							isD = true;
						}
						if (!isD && isSDiagnosis(es.getClaim(), es.getEpisode_id())) {
							isD = true;
						}
						
					}
					
					// needs to either be sufficient or have both typical px and dx...
					if (isSuff || (isD && isP)) {
					
						AssignedClaim as = new AssignedClaim();
					
						as.setCategory("T");
						as.setClaim(es.getClaim());
						as.setRule("2.12.0");
						
						es.getClaim().setAssigned(true);
						
						es.getAssigned_claims().add(as);
						trigAssigned=true;
					
					}

				}
				
				/* 
				 * drop if the triggering episode is dropped 
				 * -- added by Mike 02/22/2017 --
				 * */
				if (es.isTrig_by_episode()) {
					// the triggering episode instance isn't saved in the triggered episode
					// so look for it...
					for (EpisodeShell esC : epsForMember) {
						// if it was triggered from the same claim it might be...
						if (es.getClaim().equals(esC.getClaim())) {
							/* if it was triggered from the same claim and has the triggering episode_id 
							 * it should be the triggering claim. Check and see if it is dropped. 
							 * If so, drop this sick care episode...
							 */
							if (es.getTrig_by_episode_id().equals(esC.getEpisode_id())) {
								if (esC.isDropped()) {
									es.setDropped(true);
								}
							}
						}
					}
				}
				
				if(!trigAssigned && !es.isTrig_by_episode()) {
					es.setDropped(true);
					continue;
				}
				
				
				for (ClaimServLine c : planMember.getServiceLines()) {
					
					if (c.isAssigned()) {
						continue;
					}
					
					if (c.getClaim_line_type_code().equals("IP")) {
						continue;
					}
					
					if (c.getBegin_date().before(es.getEpisode_begin_date()) 
							|| c.getBegin_date().after(es.getEpisode_end_date())) {
//log.info("Date fail 5898");
						continue;
					} else {
					
						isC = false;
						if (isComplication(c, es.getEpisode_id(), "a")) {
							isC = true;
						}
						
						if (isC) {
						
							AssignedClaim as = new AssignedClaim();
						
							as.setCategory("C");
							as.setClaim(c);
							as.setRule("2.11.0");
							
							c.setAssigned(true);
							
							es.getAssigned_claims().add(as);
							
						} else {
						
							// 2.12 ...
							
							boolean isP = false;
							boolean isD = false;
							boolean isSuff = false;
							
							String pp = isPProcedural(c, es.getEpisode_id());
							if (!pp.equals("no")) {
								isP = true;
								if (pp.equals("s")) {
									isSuff = true;
								}
								if (!isP) {
									String sp = isSProcedural(c, es.getEpisode_id());
									if (!sp.equals("no")) {
										isP = true;
										if (sp.equals("s")) {
											isSuff = true;
										}
									}
								}
							}
							
							if (isP && !isSuff) {
										
								if (isPDiagnosis(c, es.getEpisode_id())) {
									isD = true;
								}
								if (!isD && isSDiagnosis(c, es.getEpisode_id())) {
									isD = true;
								}
								
							}
							
							// needs to either be sufficient or have both typical px and dx...
							if (isSuff || (isD && isP)) {
								
								if (c.isAssigned()) {
									continue;
								}
							
								AssignedClaim as = new AssignedClaim();
							
								as.setCategory("T");
								as.setClaim(c);
								as.setRule("2.12.0");
								
								c.setAssigned(true);
								
								es.getAssigned_claims().add(as);
							
							}

						} // end 2.11 else	
					}
				}
				

			}
		}  // end 2.11 / 2.12
		
		
		//1.10 - Sick Care SRFs Only IP
		// This comes AFTER OP/PB assignment because it cannot be triggered by IP
		// we don't need to try and drop any episodes here...
		for (EpisodeShell es : epsForMember) {
			
			if (es.isDropped()) {
				continue;
			}	
			// make sure this is a sick care episode
			if (es.getEpisode_type().equals("S") && es.getEpisode_id().equals("ES9901")) {
				
				// now see what other claims we can assign to this episode...
				for (ClaimServLine c : planMember.getServiceLines()) {
					// there won't be any splitting in sick care...
					if (c.isAssigned()) {
						continue;
					}

					if (!c.getClaim_line_type_code().equals("IP")) {
						continue;
					}
					
					// claim must be on or after the episode start and on or before the episode end
					
					if (c.getBegin_date().before(es.getEpisode_begin_date())) {
						continue;
					}
					if (c.getBegin_date().after(es.getEpisode_end_date())) {
						continue;
					}
						
					// Primary DX must be relevant
					
					boolean isD = false;
					boolean isC = false;
													
					if (isPDiagnosis(c, es.getEpisode_id())) {
						isD = true;
					}
												
					if (isComplication(c, es.getEpisode_id(), "p")) {
						isC = true;
					}
					
					if (isD || isC) {
					
						AssignedClaim as2 = new AssignedClaim();
					
						as2.setCategory("C");
						as2.setClaim(c);
						as2.setRule("1.10.0");
						
						c.setAssigned(true);
						
						es.getAssigned_claims().add(as2);
					}
				}
			}
		} // end 1.10

		
		

	} // end other claim assignment
	
	// full rule number, if epType is "ALL" it'll look at all types of episodes
	public boolean isAssignedTo(ClaimServLine c, String rule, String epType) {
		
		boolean isMatch = false;
		for (EpisodeShell es : epsForMember) {
			if (es.getEpisode_type().equals(epType) || epType.equals("ALL")) {
				if (es.getAssigned_claims() != null && !es.getAssigned_claims().isEmpty()) {
					for (AssignedClaim ac : es.getAssigned_claims()) {
						if (ac.getClaim() != null) {
							if (ac.getClaim().equals(c) && ac.getRule().equals(rule)) {
								isMatch = true;
							}
						}
					}
				}
			}
		}
		
		return isMatch;
	}
	
	public void claimApportionment() {
		/*
		for (ClaimServLine c : planMember.getServiceLines()) {
			if (!c.isAssigned()) {
				continue;
			}
			int assCount = 0;
			for (EpisodeShell es : epsForMember) {
				if (es.isDropped()) {
					continue;
				}
				for (AssignedClaim ac : es.getAssigned_claims()) {
					if (ac.getClaim().equals(c)) {
						assCount++;
						break;
					}
				}

			}
			c.setAssignedCount(assCount);
		}
		*/
		
		for (EpisodeShell es : epsForMember) {
			if (es.isDropped()) {
				continue;
			}
			if (es.getAssigned_claims().isEmpty()) {
				continue;
			}
			for (AssignedClaim ac : es.getAssigned_claims()) {
				if (ac.getType().equals("RX")) {
					int newCount = ac.getRxClaim().getAssignedCount()+1;
					ac.getRxClaim().setAssignedCount(newCount);
				} else {
					int newCount = ac.getClaim().getAssignedCount()+1;
					ac.getClaim().setAssignedCount(newCount);
				}
			}
		}
	}

/*	
	public void episodeLevelingOLD() {
		
		for (EpisodeShell es1 : epsForMember) {
			if (es1.isDropped()) {
				continue;
			}
			for (EpisodeShell es2 : epsForMember) {
				if (es2.isDropped()) {
					continue;
				}
				if (es1.equals(es2)) { 
					continue;
				}
				
				for (AssociationMetaData amd : epmdk.get(es1.getEpisode_id()).getAssociations() ) {
					if (amd.getEpisode_id().equals(es2.getEpisode_id())) {
						
						Date assWindowEndDate;
						Date assWindowBeginDate;
						
						if (amd.getAss_end_day().equals("Default")) {
							
							if (es1.getOrig_episode_end_date()==null || es1.getEpisode_end_date().compareTo(es1.getOrig_episode_end_date())<0) {
								assWindowEndDate = es1.getEpisode_end_date();
							} else {
								assWindowEndDate = es1.getOrig_episode_end_date();
							}
							
							//assWindowEndDate = es1.getTrig_end_date();
						} else {
							Calendar cal = Calendar.getInstance();
							cal.setTime(es1.getTrig_end_date());
							int endDayCount = Integer.parseInt(amd.getAss_end_day());
							cal.add(Calendar.DATE, endDayCount);
							assWindowEndDate = cal.getTime();
						}
						
						if (amd.getAss_start_day().equals("Default")) {
							
							assWindowBeginDate = es1.getTrig_begin_date();
						} else {
							Calendar cal = Calendar.getInstance();
							cal.setTime(es1.getTrig_begin_date());
							int beginDayCount = Integer.parseInt(amd.getAss_start_day());
							cal.add(Calendar.DATE, beginDayCount);
							assWindowBeginDate = cal.getTime();
						}
						
						int beginComp = es2.getClaim().getBegin_date().compareTo(assWindowBeginDate);
						int endComp = es2.getClaim().getEnd_date().compareTo(assWindowEndDate);
						
						if (beginComp>=0 && endComp <=0) {
							// this is a potential associated episode... 
							
							AssociatedEpisode ae = new AssociatedEpisode();
							
							ae.setAssEpisode(es2);
							ae.setAssMetaData(amd);
							ae.setParentEpisode(es1);
							
							es1.getPotentially_associated_episodes().add(ae);
							// below line added by Mike for association rewrite 2/2016
							es2.getPotentially_associated_parent_episodes().add(ae);
							potAssEps.add(ae);
						}
						
					}
				}
			}
		} // end potential association loop
		
		Collections.sort(potAssEps, new assEpCompare() );
		
		// now the order of potAssEps is REVERSED...
		// By start date of the potentially associated episode.
		
		// do this once for each level of assocation...
		for (int i=2; i<6; i++) {
			
			if (i==4) {
				// before running level 4 associations, look for subsidiary to procedural associations to do...
				for (AssociatedEpisode ae : potAssEps) {
					if (ae.isDropped()) {
						continue;
					}
					if (!ae.getParentEpisode().getEpisode_type().equals("P")) {
						continue;
					}
					//if the parent *to which* we're trying to associate is already associated, go to next...
					if (ae.getParentEpisode().isAssociated()) {
						continue;
					}
					if (ae.getAssEpisode().isAssociated()) {
						continue;
					}
					if (ae.getAssMetaData().getSubsidiary_to_procedural().equals("Yes")) {
						//associate this episode as sub to proc...
						
						ae.getAssEpisode().setAssociated(true);
						ae.getAssEpisode().setAssociatedLevel(3);
						ae.getAssEpisode().setAssociationCount(ae.getAssEpisode().getAssociationCount()+1);
						String si = "3";
						if (ae.getParentEpisode().getAssociated_episodes().containsKey(si)) {
							ae.getParentEpisode().getAssociated_episodes().get(si).add(ae);
						} else {
							ArrayList<AssociatedEpisode> alae = new ArrayList<AssociatedEpisode>();
							alae.add(ae);
							ae.getParentEpisode().getAssociated_episodes().put(si, alae);
						}
						// if acute episode, find its potential associations and 
						//look for sub to proc to now associate
						if (ae.getAssEpisode().getEpisode_type().equals("A")) {
							ArrayList<AssociatedEpisode> gchilds =  new ArrayList<AssociatedEpisode>();
							for (AssociatedEpisode aeSub : ae.getAssEpisode().getPotentially_associated_episodes()) {
								gchilds.add(aeSub);
								
							}
							
							// find gchildes and so forth...
							ArrayList<AssociatedEpisode> arc = new ArrayList<AssociatedEpisode>();
							arc = setSubProgeny(gchilds);
							
							// recursively look into generations to find subsidiary to proc episodes to associate... 
							while (arc.size()>0) {
								arc = setSubProgeny(arc);
							}
						}
					}
				}
			}
				
			for (AssociatedEpisode ae : potAssEps) {
				if (ae.isDropped()) {
					continue;
				}
				
				//if the parent *to which* we're trying to associate is already associated, go to next...
				if (ae.getParentEpisode().isAssociated()) {
					continue;
				}
				
				//if (ae.getAssEpisode().isAssociated()) {
					//continue;
				//} 
				
				
				/*
				// need to allow level 5 episodes to multi-associate
				if (i<5 && ae.getAssMetaData().getAss_level()<5) {
					if (ae.getAssEpisode().isAssociated()) {
						continue;
					}
				} else {
					// need to make sure the potential association isn't already done...
					boolean alreadyAssignedToThisEpisode = false;
					if (ae.getParentEpisode().getAssociated_episodes().get('5')!=null 
							&& !ae.getParentEpisode().getAssociated_episodes().get('5').isEmpty()) {
						for (AssociatedEpisode mae : ae.getParentEpisode().getAssociated_episodes().get('5')) {
							if (mae.equals(ae)) {
								alreadyAssignedToThisEpisode=true;
								break;
							}
						}
						if (alreadyAssignedToThisEpisode) {
							continue;
						}
					}
				}
was end commented here				
				if (ae.getAssMetaData().getSubsidiary_to_procedural().equals("Yes")) {
					continue;
				}
				// need to allow level 5 associations to multi-associate
				// NEED TO ALLOW EPISODES ON SAME DAY TO MULTI-ASSOCIATE
				//if (ae.getAssEpisode().isAssociated() && ae.getAssEpisode().getAssociatedLevel()<5) {
				//	continue;
				//}
				
				// if ae contains a potential association AT the correct level...
				if (ae.getAssMetaData().getAss_level()==i) {
					
					String si = "";
					si+=i;
					
					
						
					// if the episode we're trying to associate is already associated to this parent episode, go to next...
					if (ae.getParentEpisode().getAssociated_episodes().get(si)!=null) {
						//log.info("Maybe?");
						
						
						if (ae.getParentEpisode().getAssociated_episodes().get(si).contains(ae.getAssEpisode())) {
							//log.info("Yeah?");
							continue;
						}
						
						boolean SameEp = false;
						for (AssociatedEpisode pae : ae.getParentEpisode().getAssociated_episodes().get(si)) {
							EpisodeShell pes = pae.getAssEpisode();
							EpisodeShell ces = ae.getAssEpisode();
							//log.info(pes);
							//log.info(ces);
							if (pes.equals(ces)) {
								SameEp = true;
								break;
							}
						}
						if (SameEp) {
							continue;
						}
					}
					
					// if the episode we're trying to associate is already associated to any episode at a lower level
					// don't allow it to associate again...
					boolean isAssLower = false;
					if (ae.getAssEpisode().isAssociated()) {
						for (AssociatedEpisode aesubs : potAssEps) {
							if (aesubs.getAssEpisode() == ae.getAssEpisode()) {
								if (aesubs.getAssEpisode().getAssociatedLevel() < i) {
									isAssLower=true;
									continue;
								}
							}
						}
					}
					// isAssLower = true then don't multi-associate...
					if (isAssLower) {
						continue;
					}
					
					
					
					
					// if level 5, we can multi-associate, or if the two episode are the same day we can multi-associate
					// otherwise we need to not let it go through if already associated.
					if (ae.getAssEpisode().isAssociated()) {
						if (i==5 || ae.getAssEpisode().getClaim().getBegin_date().equals(ae.getParentEpisode().getClaim().getBegin_date())) {
							if (i==5 && ae.getAssEpisode().getAssociatedLevel()<5) {
								continue;
							}
							// we can let this association through to multi-associate
						} else {
							// block this association
							continue;
						}
					} 
					
					ae.getAssEpisode().setAssociated(true);
					ae.getAssEpisode().setAssociatedLevel(i);
					ae.getAssEpisode().setAssociationCount(ae.getAssEpisode().getAssociationCount()+1);
					
					if (ae.getParentEpisode().getAssociated_episodes().containsKey(si)) {
						ae.getParentEpisode().getAssociated_episodes().get(si).add(ae);
					} else {
						ArrayList<AssociatedEpisode> alae = new ArrayList<AssociatedEpisode>();
						alae.add(ae);
						ae.getParentEpisode().getAssociated_episodes().put(si, alae);
					}
				}
				
			}
			
		}
		
	}
*/	
	
/* this is a new episodeLeveling method using better understanding of
 * how leveling should work. We need to identify the closest association
 * regardless of level, but then choose the lowest levels first 
 * (i.e. look for potential level 2 associations, make sure that potential association
 * is the closest to its potential parent of any it could make, it only becomes the final
 * association if it is. So, if an episode could associate at level 2 and level 3, whichever one
 * is closer to the parent is the one that will win, but we don't do the 3 association until 
 * the 2's are done 
 * 
 * NOTE: Level 5 should NOT get precedence. Closest association regardless of level EXCEPT level 5
 */
	public void episodeLeveling() {
		
		for (EpisodeShell es1 : epsForMember) {
			if (es1.isDropped()) {
				continue;
			}
			for (EpisodeShell es2 : epsForMember) {
				if (es2.isDropped()) {
					continue;
				}
				if (es1.equals(es2)) { 
					continue;
				}
				
				for (AssociationMetaData amd : epmdk.get(es1.getEpisode_id()).getAssociations() ) {
					if (amd.getEpisode_id().equals(es2.getEpisode_id())) {
						
						Date assWindowEndDate;
						Date assWindowBeginDate;
						
						if (amd.getAss_end_day().equals("Default")) {
							
							if (es1.getOrig_episode_end_date()==null || es1.getEpisode_end_date().before(es1.getOrig_episode_end_date())) {
								assWindowEndDate = es1.getEpisode_end_date();
							} else {
								assWindowEndDate = es1.getOrig_episode_end_date();
							}
							
							//assWindowEndDate = es1.getTrig_end_date();
						} else {
							Calendar cal = Calendar.getInstance();
							cal.setTime(es1.getTrig_end_date());
							int endDayCount = Integer.parseInt(amd.getAss_end_day());
							cal.add(Calendar.DATE, endDayCount);
							assWindowEndDate = cal.getTime();
						}
						
						if (amd.getAss_start_day().equals("Default")) {
							
							assWindowBeginDate = es1.getTrig_begin_date();
						} else {
							Calendar cal = Calendar.getInstance();
							cal.setTime(es1.getTrig_begin_date());
							int beginDayCount = Integer.parseInt(amd.getAss_start_day());
							cal.add(Calendar.DATE, beginDayCount);
							assWindowBeginDate = cal.getTime();
						}
// NOTE: does this need to be changed to overlap rather than within? 
// Yes, but we're just talking trigger window here so not a big difference and only IP. 
// updated March 3, 2017 - Mike
						
						int beginComp = es2.getClaim().getBegin_date().compareTo(assWindowBeginDate);
						int endComp = es2.getClaim().getBegin_date().compareTo(assWindowEndDate);
						
						if (beginComp>=0 && endComp <=0) {
							
							// this is a potential associated episode... 
							
							AssociatedEpisode ae = new AssociatedEpisode();
							
							ae.setAssEpisode(es2);
							ae.setAssMetaData(amd);
							ae.setParentEpisode(es1);
							
							es1.getPotentially_associated_episodes().add(ae);
							// below line added by Mike for association rewrite 2/2016
							es2.getPotentially_associated_parent_episodes().add(ae);
							potAssEps.add(ae);
						}
						
					}
				}
			}
		} // end potential association loop
		
		Collections.sort(potAssEps, new assEpCompare() );
		
		// now the order of potAssEps is REVERSED...
		// By start date of the potentially associated episode.
		
		// do this once for each level of assocation...
		for (int i=2; i<6; i++) {
			
			if (i==4) {
				// before running level 4 associations, look for subsidiary to procedural associations to do...
				for (AssociatedEpisode ae : potAssEps) {
					if (ae.isDropped()) {
						continue;
					}
					if (!ae.getParentEpisode().getEpisode_type().equals("P")) {
						continue;
					}
					//if the parent *to which* we're trying to associate is already associated at a lower level go to next...
					if (ae.getParentEpisode().isAssociated() && ae.getParentEpisode().getAssociatedLevel() < 3) {
						continue;
					}
					if (ae.getAssEpisode().isAssociated() && ae.getAssEpisode().getAssociatedLevel() < 3) {
						continue;
					}
					
					if (ae.getAssMetaData().getSubsidiary_to_procedural().equals("Yes")) {
						//try to associate this episode as sub to proc...
						
						boolean doAssociate = true;
						
						// is this association the closest association this episode could make at any level?
						// is there a potental parent with a later trigger date than the current potential parent?
						// the closer association must be at this or a higher level! (or we might not associate it at all)
						// NEW - Take any POSITIVE association before any negative one.
						
						// make sure the list of other potential association is not empty...
						if (!ae.getAssEpisode().getPotentially_associated_parent_episodes().isEmpty()) {
							
							// cycle through each potential parent...
							for (AssociatedEpisode pape : ae.getAssEpisode().getPotentially_associated_parent_episodes()) {
							
								// if the comparison parent is at the same or higher level...
								if (pape.getAssMetaData().getAss_level()>=3) {
									
									// if both potential associations are same day as child or before...
									if ( 
										!ae.getAssEpisode().getTrig_begin_date().before(pape.getParentEpisode().getTrig_begin_date())
										&& !ae.getAssEpisode().getTrig_begin_date().before(ae.getParentEpisode().getTrig_begin_date())
									) {
										// if the comparison association is later, it is closer, continue...
										if (pape.getParentEpisode().getTrig_begin_date().after(ae.getParentEpisode().getTrig_begin_date())) {
											// do not make the current ae association...

											doAssociate=false;
											break;
											
											
										}
										
									}
									
									// if the comparison association is before or same as child (not after), 
									// but the index association is after the child, do not allow this association to occur...
									if ( 
											!ae.getAssEpisode().getTrig_begin_date().before(pape.getParentEpisode().getTrig_begin_date())
											&& ae.getAssEpisode().getTrig_begin_date().before(ae.getParentEpisode().getTrig_begin_date())
										) {
										// do not make the current ae association...

										doAssociate=false;
										break;
									}
									
									// if the child is before BOTH, and the comparison is earlier, do not allow this association to occur...
									if ( 
											ae.getAssEpisode().getTrig_begin_date().before(pape.getParentEpisode().getTrig_begin_date())
											&& ae.getAssEpisode().getTrig_begin_date().before(ae.getParentEpisode().getTrig_begin_date())
										) {
										if (pape.getParentEpisode().getTrig_begin_date().before(ae.getParentEpisode().getTrig_begin_date())) {
											// do not make the current ae association...

											doAssociate=false;
											break;
											
											
										}
									}
								}
							}
						}
						
						if (doAssociate) {
						
							ae.getAssEpisode().setAssociated(true);
							ae.getAssEpisode().setAssociatedLevel(3);
							ae.getAssEpisode().setAssociationCount(ae.getAssEpisode().getAssociationCount()+1);
							String si = "3";
							if (ae.getParentEpisode().getAssociated_episodes().containsKey(si)) {
								ae.getParentEpisode().getAssociated_episodes().get(si).add(ae);
							} else {
								ArrayList<AssociatedEpisode> alae = new ArrayList<AssociatedEpisode>();
								alae.add(ae);
								ae.getParentEpisode().getAssociated_episodes().put(si, alae);
							}
							// if acute episode, find its potential associations and 
							//look for sub to proc to now associate
							if (ae.getAssEpisode().getEpisode_type().equals("A")) {
								ArrayList<AssociatedEpisode> gchilds =  new ArrayList<AssociatedEpisode>();
								for (AssociatedEpisode aeSub : ae.getAssEpisode().getPotentially_associated_episodes()) {
									gchilds.add(aeSub);
									
								}
								
								// find gchildes and so forth...
								ArrayList<AssociatedEpisode> arc = new ArrayList<AssociatedEpisode>();
								arc = setSubProgeny(gchilds);
								
								// recursively look into generations to find subsidiary to proc episodes to associate... 
								while (arc.size()>0) {
									arc = setSubProgeny(arc);
								}
							}
						}
					}
				}
			}
				
			for (AssociatedEpisode ae : potAssEps) {
				if (ae.isDropped()) {
					continue;
				}
				
				// if this is a sub to proc association, skip, handling separately...
				if (ae.getAssMetaData().getSubsidiary_to_procedural().equals("Yes")) {
					continue;
				}
				
				// if ae contains a potential association AT the correct level...
				if (ae.getAssMetaData().getAss_level()==i) {
//log.info("assignment attempted | " + ae.getAssEpisode().getEpisode_id());					
					// if child or parent is already associated at a lower level, skip...
					if (ae.getAssEpisode().isAssociated() && ae.getAssEpisode().getAssociatedLevel()<i) {
						continue;
					}
					if (ae.getParentEpisode().isAssociated() && ae.getParentEpisode().getAssociatedLevel()<i) {
						continue;
					}
					
					String si = "";
					si+=i;
						
					// if the episode we're trying to associate is already associated to this parent episode, go to next...
					if (ae.getParentEpisode().getAssociated_episodes().get(si)!=null) {
						//log.info("Maybe?");
						
						if (ae.getParentEpisode().getAssociated_episodes().get(si).contains(ae.getAssEpisode())) {
							//log.info("Yeah?");
							continue;
						}
						
						boolean SameEp = false;
						for (AssociatedEpisode pae : ae.getParentEpisode().getAssociated_episodes().get(si)) {
							EpisodeShell pes = pae.getAssEpisode();
							EpisodeShell ces = ae.getAssEpisode();
							//log.info(pes);
							//log.info(ces);
							if (pes.equals(ces)) {
								SameEp = true;
								break;
							}
						}
						if (SameEp) {
							continue;
						}
					}
					
					boolean doAssociate = true;
					
					// !!! DON'T DO THE FOLLOWING ON LEVEL 5, ALL WILL MULTI-ASSOCIATE AT LEVEL 5...
					
					if (i<5) {
						
						
						// is this association the closest association this episode could make at any level?
						// is there a potental parent with a later trigger date than the current potential parent?
						// the closer association must be at this or a higher level! (or we might not associate it at all)
						// NEW - Take any POSITIVE association before any negative one.
						
						// make sure the list of other potential association is not empty...
						if (!ae.getAssEpisode().getPotentially_associated_parent_episodes().isEmpty()) {
							
							// cycle through each potential parent...
							for (AssociatedEpisode pape : ae.getAssEpisode().getPotentially_associated_parent_episodes()) {
							
								// if the comparison parent is at the same or higher, but not level 5...
								if (pape.getAssMetaData().getAss_level()>=i && pape.getAssMetaData().getAss_level() < 5) {
									
									// if both potential associations are same day as child or before...
									if ( 
										!ae.getAssEpisode().getTrig_begin_date().before(pape.getParentEpisode().getTrig_begin_date())
										&& !ae.getAssEpisode().getTrig_begin_date().before(ae.getParentEpisode().getTrig_begin_date())
									) {
										// if the comparison association is later, it is closer, continue...
										if (pape.getParentEpisode().getTrig_begin_date().after(ae.getParentEpisode().getTrig_begin_date())) {
											// do not make the current ae association...

											doAssociate=false;
											break;
											
											
										}
										
									}
									
									// if the comparison association is before or same as child (not after), 
									// but the index association is after the child, do not allow this association to occur...
									if ( 
											!ae.getAssEpisode().getTrig_begin_date().before(pape.getParentEpisode().getTrig_begin_date())
											&& ae.getAssEpisode().getTrig_begin_date().before(ae.getParentEpisode().getTrig_begin_date())
										) {
										// do not make the current ae association...

										doAssociate=false;
										break;
									}
									
									// if the child is before BOTH, and the comparison is earlier, do not allow this association to occur...
									if ( 
											ae.getAssEpisode().getTrig_begin_date().before(pape.getParentEpisode().getTrig_begin_date())
											&& ae.getAssEpisode().getTrig_begin_date().before(ae.getParentEpisode().getTrig_begin_date())
										) {
										if (pape.getParentEpisode().getTrig_begin_date().before(ae.getParentEpisode().getTrig_begin_date())) {
											// do not make the current ae association...

											doAssociate=false;
											break;
											
											
										}
									}
								}
							}
						}	
					}
					
					
					// if level 5, we can multi-associate
					if (ae.getAssEpisode().isAssociated() && ae.getAssEpisode().getAssociatedLevel()<5) {
						doAssociate=false;
					} 
						
					if (doAssociate) {
					
						ae.getAssEpisode().setAssociated(true);
						ae.getAssEpisode().setAssociatedLevel(i);
						ae.getAssEpisode().setAssociationCount(ae.getAssEpisode().getAssociationCount()+1);
						
						if (ae.getParentEpisode().getAssociated_episodes().containsKey(si)) {
							ae.getParentEpisode().getAssociated_episodes().get(si).add(ae);
						} else {
							ArrayList<AssociatedEpisode> alae = new ArrayList<AssociatedEpisode>();
							alae.add(ae);
							ae.getParentEpisode().getAssociated_episodes().put(si, alae);
						}
					}
				}
				
			}
			
		}
		
	}
	
	public ArrayList<AssociatedEpisode> setSubProgeny(ArrayList<AssociatedEpisode> ael) {
		// assumes all incoming episode associations are for acutes
		ArrayList<AssociatedEpisode> AAssoc = new ArrayList<AssociatedEpisode>();
		
		for (AssociatedEpisode ae : ael) {
			for (AssociatedEpisode subAe : ae.getAssEpisode().getPotentially_associated_episodes()) {
				 //grandchildren
				if (subAe.isDropped()) {
					continue;
				}
				
				//if the parent *to which* we're trying to associate is already associated at a lower level go to next...
				if (subAe.getParentEpisode().isAssociated() && subAe.getParentEpisode().getAssociatedLevel() < 3) {
					continue;
				}
				if (subAe.getAssEpisode().isAssociated() && subAe.getAssEpisode().getAssociatedLevel() < 3) {
					continue;
				}
				
				if (!subAe.getAssMetaData().getSubsidiary_to_procedural().equals("Yes")) {
					continue;
				}
				
				/*
				// is this association the closest association this episode could make at this level or higher?
				// is there a potental parent with a later trigger date than the current potential parent?
				// the closer association must be at this or a higher level! (or we might not associate it at all)
				if (!subAe.getAssEpisode().getPotentially_associated_parent_episodes().isEmpty()) {
					for (AssociatedEpisode pape : subAe.getAssEpisode().getPotentially_associated_parent_episodes()) {
						if (pape.getParentEpisode().getTrig_begin_date().after(subAe.getParentEpisode().getTrig_begin_date())
								&& pape.getAssMetaData().getAss_level()>=3) {
							continue;
						}
					}
				}
				
				*/
				
				boolean doAssociate = true;
				
				// is this association the closest association this episode could make at any level?
				// is there a potental parent with a later trigger date than the current potential parent?
				// the closer association must be at this or a higher level! (or we might not associate it at all)
				// NEW - Take any POSITIVE association before any negative one.
				
				// make sure the list of other potential association is not empty...
				if (!subAe.getAssEpisode().getPotentially_associated_parent_episodes().isEmpty()) {
					
					// cycle through each potential parent...
					for (AssociatedEpisode pape : subAe.getAssEpisode().getPotentially_associated_parent_episodes()) {
					
						// if the comparison parent is at the same or higher level...
						if (pape.getAssMetaData().getAss_level()>=3) {
							
							// if both potential associations are same day as child or before...
							if ( 
								!subAe.getAssEpisode().getTrig_begin_date().before(pape.getParentEpisode().getTrig_begin_date())
								&& !subAe.getAssEpisode().getTrig_begin_date().before(subAe.getParentEpisode().getTrig_begin_date())
							) {
								// if the comparison association is later, it is closer, continue...
								if (pape.getParentEpisode().getTrig_begin_date().after(subAe.getParentEpisode().getTrig_begin_date())) {
									// do not make the current ae association...

									doAssociate=false;
									break;
									
									
								}
								
							}
							
							// if the comparison association is before or same as child (not after), 
							// but the index association is after the child, do not allow this association to occur...
							if ( 
									!subAe.getAssEpisode().getTrig_begin_date().before(pape.getParentEpisode().getTrig_begin_date())
									&& subAe.getAssEpisode().getTrig_begin_date().before(subAe.getParentEpisode().getTrig_begin_date())
								) {
								// do not make the current ae association...

								doAssociate=false;
								break;
							}
							
							// if the child is before BOTH, and the comparison is earlier, do not allow this association to occur...
							if ( 
									subAe.getAssEpisode().getTrig_begin_date().before(pape.getParentEpisode().getTrig_begin_date())
									&& subAe.getAssEpisode().getTrig_begin_date().before(subAe.getParentEpisode().getTrig_begin_date())
								) {
								if (pape.getParentEpisode().getTrig_begin_date().before(subAe.getParentEpisode().getTrig_begin_date())) {
									// do not make the current ae association...

									doAssociate=false;
									break;
									
									
								}
							}
						}
					}
				}
				
				if (doAssociate) {
				
				
					// it should be associated as a level 3 sub to proc
					
					subAe.getAssEpisode().setAssociated(true);
					subAe.getAssEpisode().setAssociatedLevel(3);
					subAe.getAssEpisode().setAssociationCount(ae.getAssEpisode().getAssociationCount()+1);
					String si = "3";
					if (ae.getParentEpisode().getAssociated_episodes().containsKey(si)) {
						subAe.getParentEpisode().getAssociated_episodes().get(si).add(subAe);
					} else {
						ArrayList<AssociatedEpisode> alae = new ArrayList<AssociatedEpisode>();
						alae.add(subAe);
						subAe.getParentEpisode().getAssociated_episodes().put(si, alae);
					}
					
					// then if any are A, do this whole thing again...
					if (subAe.getAssEpisode().getEpisode_type().equals("A")) {
						for (AssociatedEpisode subSubAe : subAe.getAssEpisode().getPotentially_associated_episodes()) {
							if (subSubAe.getAssEpisode().isAssociated()) {
								continue;
							}
							if (!subSubAe.getAssMetaData().getSubsidiary_to_procedural().equals("Yes")) {
								continue;
							}
							AAssoc.add(subAe);
						}
					}
				}
			}
		}
		
		return AAssoc;
	}
	
/*	public void rollDollars() {
		
		//note NEED to reverse order to catch subsidiary to procedural chain dollars correctly
		
		
		List<String> clTypeList = new ArrayList<String>();
		clTypeList.add("CL");
		clTypeList.add("IP");
		clTypeList.add("OP");
		clTypeList.add("PB");
		clTypeList.add("RX");
		
		// first get level 1 assignment dollars...
		for (EpisodeShell es : epsForMember) {
			if (es.isDropped()) {
				continue;
			}
			
			//BigDecimal runningTotalSplit = new BigDecimal("0");
			//BigDecimal runningTotalUnsplit = new BigDecimal("0");
			
			if (es.getAssigned_claims()==null || es.getAssigned_claims().isEmpty()) {
				continue;
			}
			
			for (AssignedClaim cs : es.getAssigned_claims()) {
				BigDecimal bdAssCount;
				BigDecimal bdAllowedAmt;
				
				//log.info("1 - " + es.getAssociatedDollarsByLevel().get("1").get("CL").get("associatedDollarsSplit").toString() + " | " + 
				//		es.getAssociatedDollarsByLevel().get("1").get("RX").get("associatedDollarsSplit").toString());
				
				if (cs.getType().equals("RX")) {
					bdAssCount = new BigDecimal(cs.getRxClaim().getAssignedCount());
					bdAssCount.setScale(10, BigDecimal.ROUND_HALF_EVEN);
					bdAllowedAmt = new BigDecimal(cs.getRxClaim().getAllowed_amt());
					bdAllowedAmt.setScale(10, BigDecimal.ROUND_HALF_EVEN);
				} else {
					bdAssCount = new BigDecimal(cs.getClaim().getAssignedCount());
					bdAssCount.setScale(10, BigDecimal.ROUND_HALF_EVEN);
					bdAllowedAmt = new BigDecimal(cs.getClaim().getAllowed_amt());
					bdAllowedAmt.setScale(10, BigDecimal.ROUND_HALF_EVEN);
				}
				
				
				BigDecimal bdAmountSplit = bdAssCount.compareTo(BigDecimal.ZERO) == 0 ? BigDecimal.ZERO : bdAllowedAmt.divide(bdAssCount, 10, RoundingMode.HALF_EVEN);
				
				
				
				/*
				associatedDollarsSplit
				associatedDollarsUnSplit
				associateddollarsSplit_T
				associateddollarsSplit_C
				associatedDollarsUnsplit_T
				associatedDollarsUnsplit_C
				///
				
				//es.getAssociatedDollarsByLevel().get("1").get("CL").get("associatedDollarsSplit").add(bdAmountSplit);
				//es.getAssociatedDollarsByLevel().get("1").get("CL").get("associatedDollarsUnsplit").add(bdAllowedAmt);
				
				String tmpType = "";
				if (cs.getType().equals("RX")) {
					tmpType = "RX";
				} else {
					tmpType = cs.getClaim().getClaim_line_type_code();
				}
				
				es.getAssociatedDollarsByLevel().get("1").get("CL").put("associatedDollarsSplit", 
						es.getAssociatedDollarsByLevel().get("1").get("CL").get("associatedDollarsSplit").add(bdAmountSplit));
				
				es.getAssociatedDollarsByLevel().get("1").get("CL").put("associatedDollarsUnsplit", 
						es.getAssociatedDollarsByLevel().get("1").get("CL").get("associatedDollarsUnsplit").add(bdAllowedAmt));
				
				
				es.getAssociatedDollarsByLevel().get("1").get(tmpType).put("associatedDollarsSplit", 
						es.getAssociatedDollarsByLevel().get("1").get(tmpType).get("associatedDollarsSplit").add(bdAmountSplit));
				
				es.getAssociatedDollarsByLevel().get("1").get(tmpType).put("associatedDollarsUnsplit", 
						es.getAssociatedDollarsByLevel().get("1").get(tmpType).get("associatedDollarsUnsplit").add(bdAllowedAmt));
				
				//log.info("test - " + es.getAssociatedDollarsByLevel().get("1").get("CL").get("associatedDollarsUnsplit").toString() + "|" + bdAllowedAmt + "|" + bdAmountSplit);
				
				//runningTotalSplit = runningTotalSplit.add(bdAmountSplit);
				//runningTotalUnsplit = runningTotalUnsplit.add(bdAllowedAmt);
				
				
				
				switch (cs.getCategory()) {
				case "T":
					
					es.getAssociatedDollarsByLevel().get("1").get("CL").put("associatedDollarsSplit_T", 
							es.getAssociatedDollarsByLevel().get("1").get("CL").get("associatedDollarsSplit_T").add(bdAmountSplit));
					
					es.getAssociatedDollarsByLevel().get("1").get("CL").put("associatedDollarsUnsplit_T",
						es.getAssociatedDollarsByLevel().get("1").get("CL").get("associatedDollarsUnsplit_T").add(bdAllowedAmt));
					
					es.getAssociatedDollarsByLevel().get("1").get(tmpType).put("associatedDollarsSplit_T", 
						es.getAssociatedDollarsByLevel().get("1").get(tmpType).get("associatedDollarsSplit_T").add(bdAmountSplit));
					
					es.getAssociatedDollarsByLevel().get("1").get(tmpType).put("associatedDollarsUnsplit_T", 
						es.getAssociatedDollarsByLevel().get("1").get(tmpType).get("associatedDollarsUnsplit_T").add(bdAllowedAmt));
					
					break;
				case "TC":
					
					es.getAssociatedDollarsByLevel().get("1").get("CL").put("associatedDollarsSplit_TC", 
							es.getAssociatedDollarsByLevel().get("1").get("CL").get("associatedDollarsSplit_TC").add(bdAmountSplit));
					
					es.getAssociatedDollarsByLevel().get("1").get("CL").put("associatedDollarsUnsplit_TC",
						es.getAssociatedDollarsByLevel().get("1").get("CL").get("associatedDollarsUnsplit_TC").add(bdAllowedAmt));
					
					es.getAssociatedDollarsByLevel().get("1").get(tmpType).put("associatedDollarsSplit_TC", 
						es.getAssociatedDollarsByLevel().get("1").get(tmpType).get("associatedDollarsSplit_TC").add(bdAmountSplit));
					
					es.getAssociatedDollarsByLevel().get("1").get(tmpType).put("associatedDollarsUnsplit_TC", 
						es.getAssociatedDollarsByLevel().get("1").get(tmpType).get("associatedDollarsUnsplit_TC").add(bdAllowedAmt));
					
					
					
					break;
					
				case "C":
					
					es.getAssociatedDollarsByLevel().get("1").get("CL").put("associatedDollarsSplit_C", 
							es.getAssociatedDollarsByLevel().get("1").get("CL").get("associatedDollarsSplit_C").add(bdAmountSplit));
					
					es.getAssociatedDollarsByLevel().get("1").get("CL").put("associatedDollarsUnsplit_C",
						es.getAssociatedDollarsByLevel().get("1").get("CL").get("associatedDollarsUnsplit_C").add(bdAllowedAmt));
					
					es.getAssociatedDollarsByLevel().get("1").get(tmpType).put("associatedDollarsSplit_C", 
						es.getAssociatedDollarsByLevel().get("1").get(tmpType).get("associatedDollarsSplit_C").add(bdAmountSplit));
					
					es.getAssociatedDollarsByLevel().get("1").get(tmpType).put("associatedDollarsUnsplit_C", 
						es.getAssociatedDollarsByLevel().get("1").get(tmpType).get("associatedDollarsUnsplit_C").add(bdAllowedAmt));
					
					
					break;
				default:
					
					break;
				}
				
			}
			
			//log.info(es.getAssociatedDollarsByLevel().get("1").get("CL").get("associatedDollarsSplit").toString() + " | " + 
			//		es.getAssociatedDollarsByLevel().get("1").get("RX").get("associatedDollarsSplit").toString());
		}
		
		
		
		// now get level dollars starting with level 2 up through 5
		// level 5 allows splitting episodes, so we need to split costs there...
		
		// do this once for each level of assocation...
		for (int i=2; i<6; i++) {
			String si = "" + i;
			int minus = i;
			minus--;
			String siMinus = "" + minus;
			
			for (EpisodeShell es : epsForMember) {
				
				if (es.isDropped()) {
					continue;
				}
				
				// need to get costs AT the current level and put them in the episode...
				
				// don't bother rolling up costs for the episode at or above the level at which it is associated to another episode. 
				// it no longer exists after that point
				if (!es.isAssociated() || (es.isAssociated() && es.getAssociatedLevel()>i)) {
					
					for (String ty : clTypeList) {
						
						es.getAssociatedDollarsByLevel().get(si).get(ty).put("associatedDollarsSplit", 
							es.getAssociatedDollarsByLevel().get(siMinus).get(ty).get("associatedDollarsSplit"));						
							//.add(es.getAssociatedDollarsByLevel().get(siMinus).get(ty).get("associatedDollarsSplit")));
						es.getAssociatedDollarsByLevel().get(si).get(ty).put("associatedDollarsUnsplit",
							es.getAssociatedDollarsByLevel().get(siMinus).get(ty).get("associatedDollarsUnsplit"));
							//.add(es.getAssociatedDollarsByLevel().get(siMinus).get(ty).get("associatedDollarsUnsplit")));
						es.getAssociatedDollarsByLevel().get(si).get(ty).put("associatedDollarsSplit_T",
							es.getAssociatedDollarsByLevel().get(siMinus).get(ty).get("associatedDollarsSplit_T"));
							//.add(es.getAssociatedDollarsByLevel().get(siMinus).get(ty).get("associatedDollarsSplit_T")));
						es.getAssociatedDollarsByLevel().get(si).get(ty).put("associatedDollarsSplit_C", 
							es.getAssociatedDollarsByLevel().get(siMinus).get(ty).get("associatedDollarsSplit_C"));
							//.add(es.getAssociatedDollarsByLevel().get(siMinus).get(ty).get("associatedDollarsSplit_C")));
						es.getAssociatedDollarsByLevel().get(si).get(ty).put("associatedDollarsUnsplit_T", 
							es.getAssociatedDollarsByLevel().get(siMinus).get(ty).get("associatedDollarsUnsplit_T"));
							//.add(es.getAssociatedDollarsByLevel().get(siMinus).get(ty).get("associatedDollarsUnsplit_T")));
						es.getAssociatedDollarsByLevel().get(si).get(ty).put("associatedDollarsUnsplit_C", 
							es.getAssociatedDollarsByLevel().get(siMinus).get(ty).get("associatedDollarsUnsplit_C"));
							//.add(es.getAssociatedDollarsByLevel().get(siMinus).get(ty).get("associatedDollarsUnsplit_C")));
						
						if (es.getAssociatedLevel() == 2) {
							es.getAssociatedDollarsByLevel().get(si).get(ty).put("associatedDollarsSplit_T", 
								es.getAssociatedDollarsByLevel().get(si).get(ty).get("associatedDollarsSplit_T")
								.add(es.getAssociatedDollarsByLevel().get(siMinus).get(ty).get("associatedDollarsSplit_TC")));
							es.getAssociatedDollarsByLevel().get(si).get(ty).put("associatedDollarsUnsplit_T", 
								es.getAssociatedDollarsByLevel().get(si).get(ty).get("associatedDollarsUnsplit_T")
								.add(es.getAssociatedDollarsByLevel().get(siMinus).get(ty).get("associatedDollarsUnsplit_TC")));
						}
						
					}
					
				}

				//if (es.getAssociatedLevel()==i) {
				
				//log.info("!!! - " + es.getAssociatedDollarsByLevel().get(siMinus).get(ty).get("associatedDollarsSplit").toString());
					
				// now need to find associated episodes and add them in...	
				if (es.getAssociated_episodes().get(si)==null || es.getAssociated_episodes().get(si).isEmpty()) {
					continue;
				}
				for (AssociatedEpisode ae : es.getAssociated_episodes().get(si)) {
					
					for (String ty : clTypeList) {
					
						//if (i!=5) {
						
						// split amounts, if required:
						
						BigDecimal assCount = new BigDecimal("1");
						if (ae.getAssEpisode().getAssociationCount()>0) {
							assCount = new BigDecimal(ae.getAssEpisode().getAssociationCount());
						}
						
						//if (i==5 && ty.equals("CL")) {
						//	log.info("Level 5: " + ty + " " + assCount + " | " + es.getEpisode_id() + "_" + es.getClaim_id() + " | " + ae.getAssEpisode().getEpisode_id()+"_"+ae.getAssEpisode().getClaim_id());
						//}
							
						BigDecimal assDollUnsplit = new BigDecimal("0");
						assDollUnsplit.setScale(10, BigDecimal.ROUND_HALF_EVEN);
						assDollUnsplit = ae.getAssEpisode().getAssociatedDollarsByLevel().get(siMinus).get(ty).get("associatedDollarsUnsplit").divide(assCount, 10, RoundingMode.HALF_EVEN);
						
						//if (i==5 && ty.equals("CL")) {
						//	log.info("pre/post divide " + es.getAssociatedDollarsByLevel().get(siMinus).get(ty).get("associatedDollarsUnsplit") + " | " + assDollUnsplit);
						//}
						
						
						BigDecimal assDollUnsplit_T = new BigDecimal("0");
						assDollUnsplit_T.setScale(10, BigDecimal.ROUND_HALF_EVEN);
						assDollUnsplit_T = ae.getAssEpisode().getAssociatedDollarsByLevel().get(siMinus).get(ty).get("associatedDollarsUnsplit_T").divide(assCount, 10, RoundingMode.HALF_EVEN);
						
						BigDecimal assDollUnsplit_C = new BigDecimal("0");
						assDollUnsplit_C.setScale(10, BigDecimal.ROUND_HALF_EVEN);
						assDollUnsplit_C = ae.getAssEpisode().getAssociatedDollarsByLevel().get(siMinus).get(ty).get("associatedDollarsUnsplit_C").divide(assCount, 10, RoundingMode.HALF_EVEN);
						
						BigDecimal assDollSplit = new BigDecimal("0");
						assDollSplit.setScale(10, BigDecimal.ROUND_HALF_EVEN);
						assDollSplit = ae.getAssEpisode().getAssociatedDollarsByLevel().get(siMinus).get(ty).get("associatedDollarsSplit").divide(assCount, 10, RoundingMode.HALF_EVEN);
						
						BigDecimal assDollSplit_T = new BigDecimal("0");
						assDollSplit_T.setScale(10, BigDecimal.ROUND_HALF_EVEN);
						assDollSplit_T = ae.getAssEpisode().getAssociatedDollarsByLevel().get(siMinus).get(ty).get("associatedDollarsSplit_T").divide(assCount, 10, RoundingMode.HALF_EVEN);
						
						BigDecimal assDollSplit_C = new BigDecimal("0");
						assDollSplit_C.setScale(10, BigDecimal.ROUND_HALF_EVEN);
						assDollSplit_C = ae.getAssEpisode().getAssociatedDollarsByLevel().get(siMinus).get(ty).get("associatedDollarsSplit_C").divide(assCount, 10, RoundingMode.HALF_EVEN);
						
						es.getAssociatedDollarsByLevel().get(si).get(ty).put("associatedDollarsSplit", 
							es.getAssociatedDollarsByLevel().get(si).get(ty).get("associatedDollarsSplit")
							.add(assDollSplit));
						es.getAssociatedDollarsByLevel().get(si).get(ty).put("associatedDollarsUnsplit", 
							es.getAssociatedDollarsByLevel().get(si).get(ty).get("associatedDollarsUnsplit")
							.add(assDollUnsplit));
						es.getAssociatedDollarsByLevel().get(si).get(ty).put("associatedDollarsSplit_C", 
							es.getAssociatedDollarsByLevel().get(si).get(ty).get("associatedDollarsSplit_C")
							.add(assDollSplit_C));
						es.getAssociatedDollarsByLevel().get(si).get(ty).put("associatedDollarsUnsplit_C", 
							es.getAssociatedDollarsByLevel().get(si).get(ty).get("associatedDollarsUnsplit_C")
							.add(assDollUnsplit_C));
					
						if (ae.getAssMetaData().getAss_type().equals("Complication")) {
							
							es.getAssociatedDollarsByLevel().get(si).get(ty).put("associatedDollarsSplit_C", 
								es.getAssociatedDollarsByLevel().get(si).get(ty).get("associatedDollarsSplit_C")
								.add(assDollSplit_T));
							
							es.getAssociatedDollarsByLevel().get(si).get(ty).put("associatedDollarsUnsplit_C", 
								es.getAssociatedDollarsByLevel().get(si).get(ty).get("associatedDollarsUnsplit_C")
								.add(assDollUnsplit_T));
							
						}
						if (ae.getAssMetaData().getAss_type().equals("Typical")) {
							
							es.getAssociatedDollarsByLevel().get(si).get(ty).put("associatedDollarsSplit_T", 
								es.getAssociatedDollarsByLevel().get(si).get(ty).get("associatedDollarsSplit_T")
								.add(assDollSplit_T));
						
							es.getAssociatedDollarsByLevel().get(si).get(ty).put("associatedDollarsUnsplit_T", 
								es.getAssociatedDollarsByLevel().get(si).get(ty).get("associatedDollarsUnsplit_T")
								.add(assDollUnsplit_T));
							
						}
					/*	} else {
							// level 5, split associations...
							BigDecimal assCount = new BigDecimal("1");
							if (es.getAssociationCount()>0) {
								assCount = new BigDecimal(es.getAssociationCount());
							}
								
							BigDecimal assDollSplit = new BigDecimal("0");
							assDollSplit.setScale(10, BigDecimal.ROUND_HALF_EVEN);
							//assDollSplit = es.getAssociatedDollarsSplit().divide(assCount, 0, RoundingMode.HALF_EVEN);
							assDollSplit = es.getAssociatedDollarsByLevel().get(siMinus).get(ty).get("associatedDollarsSplit").divide(assCount, 0, RoundingMode.HALF_EVEN);
							
							BigDecimal assDollSplit_T = new BigDecimal("0");
							assDollSplit_T.setScale(10, BigDecimal.ROUND_HALF_EVEN);
							//assDollSplit_T = es.getAssociatedDollarsSplit_T().divide(assCount, 0, RoundingMode.HALF_EVEN);
							assDollSplit_T = es.getAssociatedDollarsByLevel().get(siMinus).get(ty).get("associatedDollarsSplit_T").divide(assCount, 0, RoundingMode.HALF_EVEN);
							
							BigDecimal assDollSplit_C = new BigDecimal("0");
							assDollSplit_C.setScale(10, BigDecimal.ROUND_HALF_EVEN);
//<<<<<<< .mine
							//assDollSplit_C = es.getAssociatedDollarsSplit_C().divide(assCount, 0, RoundingMode.HALF_EVEN);
							assDollSplit_C = es.getAssociatedDollarsByLevel().get(siMinus).get(ty).get("associatedDollarsSplit_C").divide(assCount, 0, RoundingMode.HALF_EVEN);
//=======
//								assDollSplit_C = assCount.compareTo(BigDecimal.ZERO) == 0  ? BigDecimal.ZERO : es.getAssociatedDollarsSplit_C().divide(assCount, 0, RoundingMode.HALF_EVEN);
//								assDollSplit_C = assCount.compareTo(BigDecimal.ZERO) == 0  ? BigDecimal.ZERO : es.getAssociatedDollarsByLevel().get(siMinus).get(ty).get("associatedDollarsSplit_C").divide(assCount, 0, RoundingMode.HALF_EVEN);
//>>>>>>> .r103
							
							
							es.getAssociatedDollarsByLevel().get(si).get(ty).put("associatedDollarsSplit", 
								es.getAssociatedDollarsByLevel().get(si).get(ty).get("associatedDollarsSplit")
								.add(assDollSplit));
							es.getAssociatedDollarsByLevel().get(si).get(ty).put("associatedDollarsUnsplit", 
								es.getAssociatedDollarsByLevel().get(si).get(ty).get("associatedDollarsUnsplit")
								.add(ae.getAssEpisode().getAssociatedDollarsByLevel().get(siMinus).get(ty).get("associatedDollarsUnsplit")));
							es.getAssociatedDollarsByLevel().get(si).get(ty).put("associatedDollarsSplit_C", 
								es.getAssociatedDollarsByLevel().get(si).get(ty).get("associatedDollarsSplit_C")
								.add(assDollSplit_C));
							es.getAssociatedDollarsByLevel().get(si).get(ty).put("associatedDollarsUnsplit_C", 
								es.getAssociatedDollarsByLevel().get(si).get(ty).get("associatedDollarsUnsplit_C")
								.add(ae.getAssEpisode().getAssociatedDollarsByLevel().get(siMinus).get(ty).get("associatedDollarsUnsplit_C")));
							
							
							//es.setAssociatedDollarsSplit(es.getAssociatedDollarsSplit().add(assDollSplit));
							//es.setAssociatedDollarsUnsplit(es.getAssociatedDollarsUnsplit().add(ae.getAssEpisode().getAssociatedDollarsUnsplit()));
							//es.setAssociatedDollarsSplit_C(es.getAssociatedDollarsSplit_C().add(assDollSplit_C));
							//es.setAssociatedDollarsUnsplit_C(es.getAssociatedDollarsUnsplit_C().add(ae.getAssEpisode().getAssociatedDollarsUnsplit_C()));
							
							if (ae.getAssMetaData().getAss_type().equals("Complication")) {
								
								es.getAssociatedDollarsByLevel().get(si).get(ty).put("associatedDollarsSplit_C", 
									es.getAssociatedDollarsByLevel().get(si).get(ty).get("associatedDollarsSplit_C")
									.add(assDollSplit_T));
								es.getAssociatedDollarsByLevel().get(si).get(ty).put("associatedDollarsUnsplit_C", 
									es.getAssociatedDollarsByLevel().get(si).get(ty).get("associatedDollarsUnsplit_C")
									.add(ae.getAssEpisode().getAssociatedDollarsByLevel().get(siMinus).get(ty).get("associatedDollarsUnsplit_C")));
								
								//es.setAssociatedDollarsSplit_C(es.getAssociatedDollarsSplit_C().add(assDollSplit_T));
								//es.setAssociatedDollarsUnsplit_C(es.getAssociatedDollarsUnsplit_C().add(ae.getAssEpisode().getAssociatedDollarsUnsplit_T()));
							}
							if (ae.getAssMetaData().getAss_type().equals("Typical")) {
								
								es.getAssociatedDollarsByLevel().get(si).get(ty).put("associatedDollarsSplit_T", 
									es.getAssociatedDollarsByLevel().get(si).get(ty).get("associatedDollarsSplit_T")
									.add(assDollSplit_T));
								es.getAssociatedDollarsByLevel().get(si).get(ty).put("associatedDollarsUnsplit_T", 
									es.getAssociatedDollarsByLevel().get(si).get(ty).get("associatedDollarsUnsplit_T")
									.add(ae.getAssEpisode().getAssociatedDollarsByLevel().get(siMinus).get(ty).get("associatedDollarsUnsplit_T")));
								
								//es.setAssociatedDollarsSplit_T(es.getAssociatedDollarsSplit_T().add(assDollSplit_T));
								//es.setAssociatedDollarsUnsplit_T(es.getAssociatedDollarsUnsplit_T().add(ae.getAssEpisode().getAssociatedDollarsUnsplit_T()));
							}
						
						}
					///
					}
				}
				//}
			}
			
		}
		
	}
*/	
	
	public void providerAttribution() {
		
		for (EpisodeShell es : epsForMember) {
			
			
			// START PROCEDURAL EPISODES
			if (es.getEpisode_type().equals("P")) {
				
				// Start Proc A and B
				if (es.getClaim().getClaim_line_type_code().equals("IP") || es.getClaim().getClaim_line_type_code().equals("OP")) {
					//a
					//boolean noFac = false;
					boolean noPhys = false;
					if (es.getClaim().getFacility_id()!=null && !es.getClaim().getFacility_id().isEmpty()) {
						es.setAttr_cost_facility_id(es.getClaim().getFacility_id());
					} else {
						//noFac = true;
					}
					
					if (es.getClaim().getPhysician_id()!=null && !es.getClaim().getPhysician_id().isEmpty()) {
						es.setAttr_cost_physician_id(es.getClaim().getPhysician_id());
					} else {
						noPhys = true;
					}
					
					//b
					if (noPhys) {
						ClaimServLine curLine = new ClaimServLine();
						for (AssignedClaim ac : es.getAssigned_claims()) {
							if (ac.getType().equals("RX")) {
								continue;
							}
							// needs to be a professional code...
							if (!ac.getClaim().getClaim_line_type_code().equals("PB")) {
								continue;
							}
							// if the claim is before the start of the triggering claim, don't use it...
							if (ac.getClaim().getBegin_date().compareTo(es.getClaim().getBegin_date())<0) {
								continue;
							}
							// if the claim is after the end of the triggering claim, don't use it...
							if (ac.getClaim().getBegin_date().compareTo(es.getClaim().getEnd_date())>0) {
								continue;
							}
							if (curLine.getClaim_id() != null && !curLine.getClaim_id().isEmpty()) {
								if (ac.getClaim().getAllowed_amt() > curLine.getAllowed_amt() ) {
									curLine = ac.getClaim();
									es.setAttr_cost_physician_id(curLine.getPhysician_id());
								} 
							} else {
								if (ac.getClaim().getPhysician_id()!=null && !ac.getClaim().getPhysician_id().isEmpty()) {
									curLine = ac.getClaim();
									es.setAttr_cost_physician_id(curLine.getPhysician_id());
								}
							}
						}
					}
					// End b
				}
				// End Proc A and B
				
				// Start Proc C
				if (es.getClaim().getClaim_line_type_code().equals("PB")) {
					//a
					// 5.3 change, automatic attribution to the triggering physician id is no longer happening
					// looking for the line with the highest cost in the window of the claim used in b below.
					
					//es.setAttr_cost_physician_id(es.getClaim().getPhysician_id());
					
					//b
					boolean foundFac = false;
					// this will hold hte claim found in b to use in a
					AssignedClaim attribAC = new AssignedClaim();
					
					for (AssignedClaim ac : es.getAssigned_claims()) {
						if (ac.getType().equals("RX")) {
							continue;
						}
						// needs to be a IP code...
						if (!ac.getClaim().getClaim_line_type_code().equals("IP")) {
							continue;
						}
						// if the ac claim start date is after the es begin date, no overlap
						if (ac.getClaim().getBegin_date().compareTo(es.getClaim().getBegin_date())>0) {
							continue;
						}
						// and the ac claim end date before the es begin date, no overlap
						if (ac.getClaim().getEnd_date().compareTo(es.getClaim().getBegin_date())<0) {
							continue;
						}
						
						if (ac.getClaim().getFacility_id()!=null && !ac.getClaim().getFacility_id().isEmpty()) {
							es.setAttr_cost_facility_id(ac.getClaim().getFacility_id());
							foundFac = true;
							attribAC = ac;
							break;
						}
					}
					if (!foundFac) {
						ClaimServLine curLine = new ClaimServLine();
						for (AssignedClaim ac : es.getAssigned_claims()) {
							if (ac.getType().equals("RX")) {
								continue;
							}
							// needs to be a OP code...
							if (!ac.getClaim().getClaim_line_type_code().equals("OP")) {
								continue;
							}
							// if the ac claim start date is after the es begin date, no overlap
							if (ac.getClaim().getBegin_date().compareTo(es.getClaim().getBegin_date())>0) {
								continue;
							}
							// and the ac claim end date before the es begin date, no overlap
							if (ac.getClaim().getEnd_date().compareTo(es.getClaim().getBegin_date())<0) {
								continue;
							}
							if (curLine.getClaim_id() != null && !curLine.getClaim_id().isEmpty() &&
										ac.getClaim().getFacility_id()!=null && !ac.getClaim().getFacility_id().isEmpty()) {
								if (ac.getClaim().getAllowed_amt() > curLine.getAllowed_amt()) {
									curLine = ac.getClaim();
									es.setAttr_cost_facility_id(curLine.getFacility_id());
									attribAC = ac;
								} 
							} else {
								if (ac.getClaim().getFacility_id()!=null && !ac.getClaim().getFacility_id().isEmpty()) {
									curLine = ac.getClaim();
									es.setAttr_cost_facility_id(curLine.getFacility_id());
									attribAC = ac;
								}
							}
						}
					}
					// End b
					
					// 5.3 a moved here so we can evaluate a as a result of b...
					
					ClaimServLine curLine = new ClaimServLine();
					
					// make sure attribAC from above isn't null
					// if it is, do the default attribution...
					
					if (!foundFac) {
						// only use this if we can't do the alternative
						es.setAttr_cost_physician_id(es.getClaim().getPhysician_id());
						continue;
					}
					
					for (AssignedClaim ac : es.getAssigned_claims()) {
						if (ac.getType().equals("RX")) {
							continue;
						}
						// needs to be a PB code...
						if (!ac.getClaim().getClaim_line_type_code().equals("PB")) {
							continue;
						}
						// if the ac claim start date is after the es (now the IP/OP from above) begin date, no overlap
						if (ac.getClaim().getBegin_date().compareTo(attribAC.getClaim().getBegin_date())>0) {
							continue;
						}
						// and the ac claim end date before the es (now the IP/OP from above) begin date, no overlap
						if (ac.getClaim().getEnd_date().compareTo(attribAC.getClaim().getBegin_date())<0) {
							continue;
						}
						if (curLine.getClaim_id() != null && !curLine.getClaim_id().isEmpty() &&
									ac.getClaim().getPhysician_id()!=null && !ac.getClaim().getPhysician_id().isEmpty()) {
							if (ac.getClaim().getAllowed_amt() > curLine.getAllowed_amt()) {
								curLine = ac.getClaim();
								es.setAttr_cost_physician_id(curLine.getPhysician_id());
							} 
						} else {
							if (ac.getClaim().getPhysician_id()!=null && !ac.getClaim().getPhysician_id().isEmpty()) {
								curLine = ac.getClaim();
								es.setAttr_cost_physician_id(curLine.getPhysician_id());
							}
						}
					}
					
					
				}
				// End Proc C
				
			}
			// END PROCEDURAL EPISODES
			
			// START ACUTE EPISODES
			if (es.getEpisode_type().equals("A")) {
				
				// a - triggers on IP
				// go ahead and do this for all types of claims and resolve the PBs at the end...
				//if (es.getEpisode_type().equals("IP")) {
					
				// facility claim to the triggering facility id
				es.setAttr_cost_facility_id(es.getClaim().getFacility_id());
				
				//provider by the following...
				// ii - 1-3
				
				// for counting the number of claim lines with E&M the provider is on
				HashMap<String, Integer> providerCount = new HashMap<String, Integer>();
				HashMap<String, BigDecimal> providerCost = new HashMap<String, BigDecimal>();
				
				// need to find PB claim lines with E&Ms on them and find the cost
				for (AssignedClaim ac : es.getAssigned_claims()) {
					
					if (ac.getType().equals("RX")) {
						continue;
					}
					if (!ac.getClaim().getClaim_line_type_code().equals("PB")) {
						continue;
					}
					
					if (ac.getClaim().getPhysician_id()==null || ac.getClaim().getPhysician_id().isEmpty()) {
						continue;
					}
					
					boolean isEM = false;
					boolean isP=false;
					boolean foundMatch=false;
					
					for (String pc : ac.getClaim().getPrincipal_proc_code()) {
						if (pc != null && !pc.isEmpty()) {
							
							isEM = false;
							isP=false;
							
							if (emCodes.contains(pc)) {
								isEM=true;
								for (PxCodeMetaData px : epmdk.get(es.getEpisode_id()).getPx_codes()) {
									if (px.getCode_id().equals(pc)) {
										isP=true;
										break;
									}
								}
							}
							if (isEM && isP) {
								foundMatch=true;
								break;
							}
							
						}
					}
					
					if (!foundMatch) {
						
						for (String sdc : ac.getClaim().getSecondary_proc_code() ) {
							
							isEM = false;
							isP=false;
							
							if (sdc!=null && emCodes.contains(sdc)) {
								isEM=true;
								for (PxCodeMetaData px : epmdk.get(es.getEpisode_id()).getPx_codes()) {
									if (px.getCode_id().equals(sdc)) {
										isP=true;
										break;
									}
								}
								if (isEM && isP) {
									foundMatch=true;
									break;
								}
							}
						}
					}
					
					if (foundMatch) {
						
						BigDecimal allowedAmt = new BigDecimal(ac.getClaim().getAllowed_amt());
						allowedAmt.setScale(10, BigDecimal.ROUND_HALF_EVEN);
						
						String curPhys = ac.getClaim().getPhysician_id();
						
						if (providerCount.containsKey(curPhys)) {
							providerCount.put(curPhys, providerCount.get(curPhys)+1);
							providerCost.put(curPhys, providerCost.get(curPhys).add(allowedAmt));
						} else {
							// put new key and values in...
							providerCount.put(curPhys, 1);
							providerCost.put(curPhys, allowedAmt);	
						}
						
					}
					
					
				}
				
				// now we should have all of the claims information...
				if (providerCount.size()>0) {
					//at least one provider was found...
					
					//int numProviders = providerCount.size();
					BigDecimal totalCost = new BigDecimal("0");
					totalCost.setScale(10, BigDecimal.ROUND_HALF_EVEN);
					for (String pid : providerCost.keySet()) {
						totalCost.add(providerCost.get(pid));
					}
					
					// 30% of total cost is minimum cost for attribution...
					BigDecimal minCost = new BigDecimal("0");
					minCost.setScale(10, BigDecimal.ROUND_HALF_EVEN);
					BigDecimal minPercent = new BigDecimal("3");
					minPercent.setScale(10, BigDecimal.ROUND_HALF_EVEN);
					minCost = totalCost.divide(minPercent, 10, BigDecimal.ROUND_HALF_EVEN);
					
					//Option A: by count...
					for (String pid : providerCount.keySet()) {
						
						if (es.getAttr_visit_physician_id()==null || es.getAttr_visit_physician_id().isEmpty()) {
							es.setAttr_visit_physician_id(pid);
						} else {
							if (providerCount.get(pid)>=providerCount.get(es.getAttr_visit_physician_id())) {
								if (providerCount.get(pid)==providerCount.get(es.getAttr_visit_physician_id())) {
									if (providerCost.get(pid).compareTo(providerCost.get(es.getAttr_visit_physician_id()))>0) {
										es.setAttr_visit_physician_id(pid);
									}
								} else {
									es.setAttr_visit_physician_id(pid);
								}
							}
						}
					}
					
					// Option B: by cost
					for (String pid : providerCost.keySet()) {
						
						if ((es.getAttr_cost_physician_id()==null || es.getAttr_cost_physician_id().isEmpty()) && providerCost.get(pid).compareTo(minCost)>0) {
							es.setAttr_cost_physician_id(pid);
						} else {
							if ( providerCost.get(pid).compareTo(providerCost.get(es.getAttr_visit_physician_id()))>=0 ) {
								if (providerCost.get(pid).compareTo(providerCost.get(es.getAttr_visit_physician_id()))==0) {
									if (providerCount.get(pid)>providerCount.get(es.getAttr_visit_physician_id())) {
										es.setAttr_cost_physician_id(pid);
									}
								} else {
									es.setAttr_cost_physician_id(pid);
								}
							}
						}
						
					}
					
					
				}
				
				if (es.getClaim().getClaim_line_type_code().equals("PB")) {
					es.setAttr_cost_facility_id("");
					// i and ii
					
					for (AssignedClaim ac : es.getAssigned_claims()) {
						if (ac.getType().equals("RX")) {
							continue;
						}
						// c - i:
						boolean foundFac = false;
						if (ac.getClaim().getClaim_line_type_code().equals("IP")) {
							if (ac.getClaim().getBegin_date().compareTo(es.getClaim().getBegin_date())>0) {
								continue;
							}
							// and the ac claim end date before the es begin date, no overlap
							if (ac.getClaim().getEnd_date().compareTo(es.getClaim().getBegin_date())<0) {
								continue;
							}
							
							if (ac.getClaim().getFacility_id()!=null && !ac.getClaim().getFacility_id().isEmpty()) {
								es.setAttr_cost_facility_id(ac.getClaim().getFacility_id());
								foundFac = true;
								break;
							}
						}
						
						if (!foundFac) {
							if (ac.getClaim().getClaim_line_type_code().equals("OP")) {
								if (ac.getClaim().getBegin_date().compareTo(es.getClaim().getBegin_date())>0) {
									continue;
								}
								// and the ac claim end date before the es begin date, no overlap
								if (ac.getClaim().getEnd_date().compareTo(es.getClaim().getBegin_date())<0) {
									continue;
								}
								
								if (ac.getClaim().getFacility_id()!=null && !ac.getClaim().getFacility_id().isEmpty()) {
									es.setAttr_cost_facility_id(ac.getClaim().getFacility_id());
									foundFac = true;
									break;
								}
							}
						}
					}
				}
				
			}
			// END ACUTE EPISODES
			
			// START CHRONIC / OTHER EPISODES (C, X)
			
			if (es.getEpisode_type().equals("C") || es.getEpisode_type().equals("X")) {
				
				
				// for counting the number of claim lines with E&M the provider is on
				HashMap<String, Integer> providerCount = new HashMap<String, Integer>();
				HashMap<String, BigDecimal> providerCost = new HashMap<String, BigDecimal>();
				
				// need to find PB claim lines with E&Ms on them and find the cost
				for (AssignedClaim ac : es.getAssigned_claims()) {
					if (ac.getType().equals("RX")) {
						continue;
					}
					
					if (ac.getClaim().getPhysician_id()==null || ac.getClaim().getPhysician_id().isEmpty()) {
						continue;
					}
					
					
					/*
					boolean foundMatch = false;
					
					for (String pc : ac.getClaim().getPrincipal_proc_code()) {
						if (pc != null && !pc.isEmpty()) {
							
							if (emCodes.contains(pc)) {
								foundMatch=true;
								break;
							}
							
						}
					}
					
					if (!foundMatch) {
						for (String sdc : ac.getClaim().getSecondary_proc_code() ) {
							if (sdc!=null && emCodes.contains(sdc)) {
								foundMatch=true;
								break;
							}
						}
					}*/
					
					boolean isEM = false;
					boolean isP=false;
					boolean foundMatch=false;
					
					for (String pc : ac.getClaim().getPrincipal_proc_code()) {
						if (pc != null && !pc.isEmpty()) {
							
							isEM = false;
							isP=false;
							
							if (emCodes.contains(pc)) {
								isEM=true;
								for (PxCodeMetaData px : epmdk.get(es.getEpisode_id()).getPx_codes()) {
									if (px.getCode_id().equals(pc)) {
										isP=true;
										break;
									}
								}
							}
							if (isEM && isP) {
								foundMatch=true;
								break;
							}
							
						}
					}
					
					if (!foundMatch) {
						
						for (String sdc : ac.getClaim().getSecondary_proc_code() ) {
							
							isEM = false;
							isP=false;
							
							if (sdc!=null && emCodes.contains(sdc)) {
								isEM=true;
								for (PxCodeMetaData px : epmdk.get(es.getEpisode_id()).getPx_codes()) {
									if (px.getCode_id().equals(sdc)) {
										isP=true;
										break;
									}
								}
								if (isEM && isP) {
									foundMatch=true;
									break;
								}
							}
						}
					}
					
					if (foundMatch) {
						
						BigDecimal allowedAmt = new BigDecimal(ac.getClaim().getAllowed_amt());
						allowedAmt.setScale(10, BigDecimal.ROUND_HALF_EVEN);
						
						String curPhys = ac.getClaim().getPhysician_id();
						
						if (providerCount.containsKey(curPhys)) {
							providerCount.put(curPhys, providerCount.get(curPhys)+1);
							providerCost.put(curPhys, providerCost.get(curPhys).add(allowedAmt));
						} else {
							// put new key and values in...
							providerCount.put(curPhys, 1);
							providerCost.put(curPhys, allowedAmt);	
						}
						
					}
					
					
				}
				
				// now we should have all of the claims information...
				if (providerCount.size()>0) {
					//at least one provider was found...
					
					//int numProviders = providerCount.size();
					BigDecimal totalCost = new BigDecimal(0);
					totalCost.setScale(10, BigDecimal.ROUND_HALF_EVEN);
					for (String pid : providerCost.keySet()) {
						totalCost = totalCost.add(providerCost.get(pid));
					}
					
					// 30% of total cost is minimum cost for attribution...
					BigDecimal minCost = new BigDecimal("0");
					minCost.setScale(10, BigDecimal.ROUND_HALF_EVEN);
					BigDecimal minPercent = new BigDecimal("3");
					minPercent.setScale(10, BigDecimal.ROUND_HALF_EVEN);
					minCost = totalCost.divide(minPercent, 10, BigDecimal.ROUND_HALF_EVEN);
					
					//Option A: by count...
					for (String pid : providerCount.keySet()) {
					
						if (es.getAttr_visit_physician_id()==null || es.getAttr_visit_physician_id().isEmpty()) {
							es.setAttr_visit_physician_id(pid);
						} else {
							if (providerCount.get(pid)>=providerCount.get(es.getAttr_visit_physician_id())) {
								if (providerCount.get(pid)==providerCount.get(es.getAttr_visit_physician_id())) {
									if (providerCost.get(pid).compareTo(providerCost.get(es.getAttr_visit_physician_id()))>0) {
										es.setAttr_visit_physician_id(pid);
									}
								} else {
									es.setAttr_visit_physician_id(pid);
								}
							}
						}
					}
					
					
					// Option B: by cost
					for (String pid : providerCost.keySet()) {
						if ((es.getAttr_cost_physician_id()==null || es.getAttr_cost_physician_id().isEmpty()) && providerCost.get(pid).compareTo(minCost)>0) {
							es.setAttr_cost_physician_id(pid);
						} else {
							if ( providerCost.get(pid).compareTo(providerCost.get(es.getAttr_visit_physician_id()))>=0 ) {
								if (providerCost.get(pid).compareTo(providerCost.get(es.getAttr_visit_physician_id()))==0) {
									if (providerCount.get(pid)>providerCount.get(es.getAttr_visit_physician_id())) {
										es.setAttr_cost_physician_id(pid);
									}
								} else {
									es.setAttr_cost_physician_id(pid);
								}
							}
						}
					}	
				}
			}
		}
		
	}
	
	public void providerAttributionTrigger(EpisodeShell es) {
		/*
		 * This sets the physician and facility based on trigger
		 * and gets the physician and facility based on triggerable claims
		 * with the highest cost
		 */
		
		// set physician and facility trigger attribution...
		if (es.getClaim().getFacility_id()!=null && !es.getClaim().getFacility_id().isEmpty()) {
			es.setAttr_trigger_facility_id(es.getClaim().getFacility_id());
		} 
		
		if (es.getClaim().getPhysician_id()!=null && !es.getClaim().getPhysician_id().isEmpty()) {
			es.setAttr_trigger_physician_id(es.getClaim().getPhysician_id());
		}
		
		// set physician and facility trigger cost attribution
		// remember to add 2 days to the end of trigger window for procedurals
		
		Date trigBegin;
		Date trigEnd;
		
		trigBegin = es.getTrig_begin_date();
		trigEnd = es.getTrig_end_date();
		
		if (es.getEpisode_type().equals("P")) {
			// add two days to trigEnd...
			Calendar tEnd = Calendar.getInstance();
			tEnd.setTime(trigEnd);
			tEnd.add(Calendar.DATE, 2);
			trigEnd = tEnd.getTime();
			
		}
		
		// provider w/ highest cost...
		ClaimServLine curLine = new ClaimServLine();
		for (AssignedClaim ac : es.getAssigned_claims()) {
			if (ac.getType().equals("RX")) {
				continue;
			}
			// needs to be a professional code...
			if (!ac.getClaim().getClaim_line_type_code().equals("PB")) {
				continue;
			}
			// if the claim is before the start of the triggering claim, don't use it...
			if (ac.getClaim().getBegin_date().compareTo(trigBegin)<0) {
				continue;
			}
			// if the claim is after the end of the triggering claim, don't use it...
			if (ac.getClaim().getBegin_date().compareTo(trigEnd)>0) {
				continue;
			}
			
			boolean isTrigMatch = false;
			
			for (MedCode ppc : ac.getClaim().getPrincipal_proc_code_objects()) {
				if ( ppc != null && ppc.getNomen() != null && epmdk.get(es.getEpisode_id()).getTrigger_code_by_ep().containsKey(ppc.getNomen())) {
					if (epmdk.get(es.getEpisode_id()).getTrigger_code_by_ep().get(ppc.getNomen()).containsKey(ppc.getCode_value())) {
						isTrigMatch = true;
						break;
					}
				}
			}
			if (!isTrigMatch) {
				/*
				for (String pid : ac.getClaim().getSecondary_proc_code()) {
					if (epmdk.get(es.getEpisode_id()).getTrigger_code_by_ep().get("PX").containsKey(pid)) {
						isTrigMatch = true;
						break;
					}
				}
				*/
				for (MedCode spc : ac.getClaim().getSecondary_proc_code_objects()) {
					if ( spc != null && spc.getNomen() != null && epmdk.get(es.getEpisode_id()).getTrigger_code_by_ep().containsKey(spc.getNomen())) {
						if (epmdk.get(es.getEpisode_id()).getTrigger_code_by_ep().get(spc.getNomen()).containsKey(spc.getCode_value())) {
							isTrigMatch = true;
							break;
						}
					}
				}
				
			}
			
			// claim must have proc trigger code match...
			if (!isTrigMatch) { continue; }
			
			if (curLine.getClaim_id() != null && !curLine.getClaim_id().isEmpty()) {
				if (ac.getClaim().getAllowed_amt() > curLine.getAllowed_amt() && 
						(ac.getClaim().getPhysician_id()!=null && !ac.getClaim().getPhysician_id().isEmpty())
					) {
					curLine = ac.getClaim();
					es.setAttr_trig_cost_physician_id(curLine.getPhysician_id());
				} 
			} else {
				if (ac.getClaim().getPhysician_id()!=null && !ac.getClaim().getPhysician_id().isEmpty()) {
					curLine = ac.getClaim();
					es.setAttr_trig_cost_physician_id(curLine.getPhysician_id());
					
				}
			}
		}
		
		//facility with highest cost...
		//b
		boolean foundFac = false;
		new AssignedClaim();
		
		for (AssignedClaim ac : es.getAssigned_claims()) {
			if (ac.getType().equals("RX")) {
				continue;
			}
			// needs to be a IP code...
			if (!ac.getClaim().getClaim_line_type_code().equals("IP")) {
				continue;
			}
			// if the ac claim start date is after the es begin date, no overlap
			if (ac.getClaim().getBegin_date().compareTo(es.getClaim().getBegin_date())>0) {
				continue;
			}
			// and the ac claim end date before the es begin date, no overlap
			if (ac.getClaim().getEnd_date().compareTo(es.getClaim().getBegin_date())<0) {
				continue;
			}
			if (ac.getClaim().getFacility_id()==null || ac.getClaim().getFacility_id().isEmpty()) {
				continue;
			}
			
			/*
			boolean isTrigMatch = false;
			
			for (String ppc : ac.getClaim().getPrincipal_proc_code()) {
				if (epmdk.get(es.getEpisode_id()).getTrigger_code_by_ep().containsKey("PX") && ppc != null ) {
					if (epmdk.get(es.getEpisode_id()).getTrigger_code_by_ep().get("PX").containsKey(ppc)) {
						isTrigMatch = true;
						break;
					}
				}
			}
			if (!isTrigMatch) {
				for (String pid : ac.getClaim().getSecondary_proc_code()) {
					if (epmdk.get(es.getEpisode_id()).getTrigger_code_by_ep().get("PX").containsKey(pid)) {
						isTrigMatch = true;
						break;
					}
				}
			}
			*/
			
			boolean isTrigMatch = false;
			
			for (MedCode ppc : ac.getClaim().getPrincipal_proc_code_objects()) {
				if ( ppc != null && ppc.getNomen() != null && epmdk.get(es.getEpisode_id()).getTrigger_code_by_ep().containsKey(ppc.getNomen())) {
					if (epmdk.get(es.getEpisode_id()).getTrigger_code_by_ep().get(ppc.getNomen()).containsKey(ppc.getCode_value())) {
						isTrigMatch = true;
						break;
					}
				}
			}
			if (!isTrigMatch) {
				for (MedCode spc : ac.getClaim().getSecondary_proc_code_objects()) {
					if ( spc != null && spc.getNomen() != null && epmdk.get(es.getEpisode_id()).getTrigger_code_by_ep().containsKey(spc.getNomen())) {
						if (epmdk.get(es.getEpisode_id()).getTrigger_code_by_ep().get(spc.getNomen()).containsKey(spc.getCode_value())) {
							isTrigMatch = true;
							break;
						}
					}
				}
				
			}
			
			// claim must have proc trigger code match...
			if (!isTrigMatch) { continue; }
			
			//we've passed all conditions, take this one...
			es.setAttr_trig_cost_facility_id(ac.getClaim().getFacility_id());
			foundFac = true;
			break;
			
		}
		if (!foundFac) {
			curLine = new ClaimServLine();
			for (AssignedClaim ac : es.getAssigned_claims()) {
				if (ac.getType().equals("RX")) {
					continue;
				}
				// needs to be a OP code...
				if (!ac.getClaim().getClaim_line_type_code().equals("OP")) {
					continue;
				}
				// if the ac claim start date is after the es begin date, no overlap
				if (ac.getClaim().getBegin_date().compareTo(trigBegin)>0) {
					continue;
				}
				// and the ac claim end date before the es begin date, no overlap
				if (ac.getClaim().getEnd_date().compareTo(trigEnd)<0) {
					continue;
				}
				
				if (ac.getClaim().getFacility_id()==null || ac.getClaim().getFacility_id().isEmpty()) {
					continue;
				}
				
				/*
				boolean isTrigMatch = false;
				
				for (String ppc : ac.getClaim().getPrincipal_proc_code()) {
					if (epmdk.get(es.getEpisode_id()).getTrigger_code_by_ep().containsKey("PX") && ppc != null ) {
						if (epmdk.get(es.getEpisode_id()).getTrigger_code_by_ep().get("PX").containsKey(ppc)) {
							isTrigMatch = true;
							break;
						}
					}
				}
				if (!isTrigMatch) {
					for (String pid : ac.getClaim().getSecondary_proc_code()) {
						if (epmdk.get(es.getEpisode_id()).getTrigger_code_by_ep().get("PX").containsKey(pid)) {
							isTrigMatch = true;
							break;
						}
					}
				}
				*/
				
				boolean isTrigMatch = false;
				
				for (MedCode ppc : ac.getClaim().getPrincipal_proc_code_objects()) {
					if ( ppc != null && ppc.getNomen() != null && epmdk.get(es.getEpisode_id()).getTrigger_code_by_ep().containsKey(ppc.getNomen())) {
						if (epmdk.get(es.getEpisode_id()).getTrigger_code_by_ep().get(ppc.getNomen()).containsKey(ppc.getCode_value())) {
							isTrigMatch = true;
							break;
						}
					}
				}
				if (!isTrigMatch) {
					for (MedCode spc : ac.getClaim().getSecondary_proc_code_objects()) {
						if ( spc != null && spc.getNomen() != null && epmdk.get(es.getEpisode_id()).getTrigger_code_by_ep().containsKey(spc.getNomen())) {
							if (epmdk.get(es.getEpisode_id()).getTrigger_code_by_ep().get(spc.getNomen()).containsKey(spc.getCode_value())) {
								isTrigMatch = true;
								break;
							}
						}
					}
					
				}
				
				// claim must have proc trigger code match...
				if (!isTrigMatch) { continue; }
				
				
				if ( curLine.getClaim_id() != null && !curLine.getClaim_id().isEmpty() ) {
					if (ac.getClaim().getAllowed_amt() > curLine.getAllowed_amt()) {
						curLine = ac.getClaim();
						es.setAttr_trig_cost_facility_id(curLine.getFacility_id());
					} 
				} else {
					if (ac.getClaim().getFacility_id()!=null && !ac.getClaim().getFacility_id().isEmpty()) {
						curLine = ac.getClaim();
						es.setAttr_trig_cost_facility_id(curLine.getFacility_id());
					}
				}
			}
		}
		// End b
		
	
		
		
	}
	
	public void logOutput() {
		
		//String nl = System.getProperty("line.separator");
		
		String headerRow = "";
		
		headerRow += "|Ep_or_assigned_claim"
				+ "|member_id" 		//1
				+ "|episode_id"			//2
				+ "|claim_id"			//3
				+ "|claim_line_id"		//4
				+ "|claim_line_type_code"
				+ "|episode_type"		//5
				+ "|dropped"			//6
				+ "|truncated"
				+ "|winning_claim_id"
				+ "|orig_episode_begin"
				+ "|orig_episode_end"
				+ "|trig_begin_date"
				+ "|trig_end_date"
				+ "|episode_begin_date"
				+ "|episode_end_date"
				+ "|dx_code_matched"
				+ "|px_code_matched"
				+ "|em_code_matched"
				+ "|look_back"
				+ "|look_ahead"
				+ "|req_conf_claim"
				+ "|conf_claim_id"
				+ "|min_sep"
				+ "|max_sep"
				+ "|conf_dx_code_matched"
				+ "|conf_px_code_matched"
				+ "|conf_em_code_matched"
				+ "|triggered_by_episode"
				+ "|trig_by_episode_id"
				+ "|claim_begin_date"
				+ "|claim_end_date"
				+ "|claim_diag_nomen"
				+ "|claim_proc_nomen"
				+ "|claim_facil_type_code"
				+ "|assign_rule"
				+ "|assign_count"
				+ "|assign_category";
		
		log.fatal(headerRow);
		
		String pd = "";
		
		for (String mem : episodesByMember.keySet()) {
			for (EpisodeShell e : episodesByMember.get(mem)) {
				
				if (e.isDropped()) {
					continue;
				}
				
				pd = "|Episode" +
						"|" + e.getClaim().getMember_id() + 
						"|" + e.getEpisode_id() + 
						"|" + e.getClaim_id() + 
						"|" + e.getClaim().getClaim_line_id() +
						"|" + e.getClaim().getClaim_line_type_code() +
						"|" + e.getEpisode_type() + 
						"|" + e.isDropped() + 
						"|" + e.isTruncated() + 
						"|" + e.getWin_claim_id() + 
						"|" + e.getOrig_episode_begin_date() +
						"|" + e.getOrig_episode_end_date() + 
						"|" + e.getTrig_begin_date() + 
						"|" + e.getTrig_end_date() +
						"|" + e.getEpisode_begin_date() + 
						"|" + e.getEpisode_end_date() + 
						"|" + e.getDxPassCode() + 
						"|" + e.getPxPassCode() + 
						"|" + e.getEmPassCode() + 
						"|" + e.getLook_back() + 
						"|" + e.getLook_ahead() + 
						"|" + e.isReq_conf_claim() + 
						"|" + e.getConf_claim_id() + 
						"|" + e.getMin_code_separation() + 
						"|" + e.getMax_code_separation() + 
						"|" + e.getConf_dxPassCode() + 
						"|" + e.getConf_pxPassCode() + 
						"|" + e.getConf_emPassCode() + 
						"|" + e.isTrig_by_episode() + 
						"|" + e.getTrig_by_episode_id() + 
						"|" + e.getClaim().getBegin_date() + 
						"|" + e.getClaim().getEnd_date() + 
						"|" + e.getClaim().getDiag_claim_nomen() + 
						"|" + e.getClaim().getProc_claim_nomen() + 
						"|" + e.getClaim().getFacility_type_code() +
						"|" +
						"|" +
						"|";
				
				log.fatal(pd);
				
				if (e.getAssigned_claims().size()>0) {
					for (AssignedClaim as : e.getAssigned_claims()) {
						pd = "|ASSIGNED" +
								"|" + as.getClaim().getMember_id() +
								"|" + e.getEpisode_id();
							if (as.getType().equals("CL")) {
								pd += "|" + as.getClaim().getClaim_id() + 
								"|" + as.getClaim().getClaim_line_id() +
								"|" + as.getClaim().getClaim_line_type_code();
							} else if (as.getType().equals("RX")) {
								pd += "|" + as.getRxClaim().getDrug_code() + 
									"|" + 
									"|RX"; 
							}
								pd += "|" + 
								"|" + as.getCategory() + 
								"|" + 
								"|" + 
								"|" + 
								"|" + 
								"|" +  
								"|" +
								"|" +  
								"|" + 
								"|" + 
								"|" + 
								"|" + 
								"|" + 
								"|" + 
								"|" + 
								"|" + 
								"|" + 
								"|" +  
								"|" +
								"|" +  
								"|" + 
								"|" + 
								"|" + 
								"|" + 
								"|" + 
								"|" + 
								"|" + 
								"|" +
								"|" + as.getRule() +//ass rule
								"|" + as.getClaim().getAssignedCount() + // ass count
								"|" + as.getCategory();
						
						log.fatal(pd);
					}
				}
				
				
			}
		}
	}
	
	public String isPProcedural(ClaimServLine c, String episode_id) {
		
		String isProc = "no";
		
		for (PxCodeMetaData px : epmdk.get(episode_id).getPx_codes()) {
			for (MedCode pp : c.getPrincipal_proc_code_objects()) {
//log.info("isPProcedural: " + c.getClaim_id() + "_" + c.getClaim_line_id() + " meta type: " + 
//		px.getType_id() + " claim type: " + pp.getNomen() + 
//		" meta code: " + px.getCode_id() + " claim code: " + pp.getCode_value() );
				if (px.getType_id().equals(pp.getNomen()) 
					|| ( pp.getNomen().equals("HCPC") && px.getType_id().equals("CPT") )
					|| ( pp.getNomen().equals("CPT") && px.getType_id().equals("HCPC") )
						) {
					if (px.getCode_id().equals(pp.getCode_value()) ) {
						isProc="p";
						if (px.isSufficient()) {
							isProc="s";
						}
						break;
					}
				}
			}
			if (!isProc.equals("no")) {
				break;
			}
		}
		
		return isProc;
			
	}
	
	public String isSProcedural(ClaimServLine c, String episode_id) {
		
		String isProc = "no";
		
		for (PxCodeMetaData px : epmdk.get(episode_id).getPx_codes()) {
			for (MedCode sp : c.getSecondary_proc_code_objects()) {
				if (sp.getNomen()!=null && px.getType_id().equals(sp.getNomen()) 
						|| ( sp.getNomen().equals("HCPC") && px.getType_id().equals("CPT") )
						|| ( sp.getNomen().equals("CPT") && px.getType_id().equals("HCPC") ) 
				) {
					if (px.getCode_id().equals(sp.getCode_value()) ) {
						isProc="p";
						if (px.isSufficient()) {
							isProc="s";
						}
						break;
					}
				}
			}
			if (!isProc.equals("no")) {
				break;
			}
		}
		
		return isProc;
			
	}
	
	public boolean isPDiagnosis(ClaimServLine c, String episode_id) {
		
		boolean isDiag = false;
		
		for (DxCodeMetaData dx : epmdk.get(episode_id).getDx_codes()) {
//log.info("isPDiagnosis: " + c.getClaim_id() + "_" + c.getClaim_line_id() + " meta type: " + 
//		dx.getType_id() + " claim type: " + c.getPrincipal_diag_code_object().getNomen() + 
//		" meta code: " + dx.getCode_id() + " claim code: " + c.getPrincipal_diag_code_object().getCode_value() );
			if (c.getPrincipal_diag_code_object() == null)
				continue;
			if (dx.getType_id().equals(c.getPrincipal_diag_code_object().getNomen()) ) {
				if (dx.getCode_id().equals(c.getPrincipal_diag_code_object().getCode_value()) ) {
					isDiag=true;
					break;
				}
			}
		}
		
		return isDiag;
			
	}
	
	public boolean isSDiagnosis(ClaimServLine c, String episode_id) {
		
		boolean isDiag = false;
		
		for (DxCodeMetaData dx : epmdk.get(episode_id).getDx_codes()) {
			for (MedCode sd : c.getSecondary_diag_code_objects()) {
				if (dx.getType_id().equals(sd.getNomen()) ) {
					if (dx.getCode_id().equals(sd.getCode_value()) ) {
						isDiag=true;
						break;
					}
				}
			}
			if (isDiag) {
				break;
			}
		}
		
		return isDiag;
			
	}
	
	// modifying this to look for a complication amongst all codes, and just amongst diagnosis codes. 
	// third argument is p for principal, s for secondary, or a for any match
	public boolean isComplication(ClaimServLine c, String episode_id, String pos) {
		
		boolean isComp = false;
		//pos can be p for primary, s for secondary or a for either
		
		
		
		for (ComplicationCodeMetaData cc : epmdk.get(episode_id).getComplication_codes()) {
			
/*			if (!cc.getType_id().equals(code_type)) {
				continue;
			}
			if (c.getPrincipal_diag_code() == null) {
				//log.error("Claim " + c.getClaim_id() + " for member " + c.getMember_id() + " has no principle diagnosis code");
				continue;
			}
*/
			
			for (MedCode mc : c.getMed_codes()) {
				if ( mc.getFunction_code().equals("DX") ) {
					if (    
						( pos.equals("p") && mc.getPrincipal()==1 ) ||
						( pos.equals("s") && mc.getPrincipal()!=1 ) ||
						( pos.equals("a") )
						) {
							
						if ( mc.getNomen().equals(cc.getType_id()) ) {
							if (mc.getCode_value().equals(cc.getCode_id())) {
								isComp = true;
								break;
							}
						}
					}
				}
			}
		}
/* 			if (code_type.equals("DX")) {
				if ( c.getPrincipal_diag_code().equals(cc.getCode_id()) ) {
					isComp = true;
					break;
				}
				for (String sdc : c.getSecondary_diag_code()) {
					if (sdc.equals(cc.getCode_id())) {
						isComp = true;
						break;
					}
				}
			} else if (code_type.equals("PX")) {
				for (String ppc : c.getPrincipal_proc_code()) {
					if ( ppc.equals(cc.getCode_id()) ) {
						isComp = true;
						break;
					}
				}
				if (isComp) {
					break;
				}
				for (String sdc : c.getSecondary_proc_code()) {
					if (sdc.equals(cc.getCode_id())) {
						isComp = true;
						break;
					}
				}
			}	
		}
*/
		
		return isComp;
	}
	
	public String ParseEpType(String episode_id) {
		
		String epType="";
		if (episode_id!=null && !episode_id.isEmpty()) {
		
			episode_id = episode_id.trim();
			epType = episode_id.substring(1, 2);
			
		} else {
			epType=null;
		}
		
		return epType;
	}
	
	
	class epCompare implements Comparator<EpisodeShell> {

	    @Override
	    public int compare(EpisodeShell e1, EpisodeShell e2) {
	    	
	    	/*
	    	 * order by episode, then by date, then by claim, then by line number
	    	 */
	    	
	    	int c = 0;

	    	c = e1.getEpisode_id().compareTo(e2.getEpisode_id());
	    	//log.info("e1 claim " + e1.getClaim());
	    	//log.info("e2 claim " + e2.getClaim());
	    	if (c == 0) {
	    		//log.info("comparing begin date " );
	    		c = e1.getClaim().getBegin_date().compareTo(e2.getClaim().getBegin_date());
	    	}
	    	if (c == 0) {
	    		//log.info("comparing claim id " );
	    		c = e1.getClaim().getClaim_id().compareTo(e2.getClaim().getClaim_id());
	    	}
	    	if (c == 0) {
	    		if (e1.getClaim().getClaim_line_id()==null || e2.getClaim().getClaim_line_id()==null) {
	    			c=1;
	    		} else {
	    			//log.info("comparing claim line id " );
	    			try {
	    				Integer a = Integer.parseInt(e1.getClaim().getClaim_line_id());
	    				Integer b = Integer.parseInt(e2.getClaim().getClaim_line_id());
	    				c = a.compareTo(b);
	    			}
	    			catch (NumberFormatException nfe) {
	    				
	    			}
	    		}
	    	}
	    	
	        return c;
	    }
	}
	
	class epCompareReverse implements Comparator<EpisodeShell> {

	    @Override
	    public int compare(EpisodeShell e1, EpisodeShell e2) {
	    	
	    	/*
	    	 * order by episode, then by date, then by claim, then by line number
	    	 */
	    	
	    	int c;
	    	c = e2.getEpisode_id().compareTo(e1.getEpisode_id());
	    	if (c == 0) {
	    		c = e2.getClaim().getBegin_date().compareTo(e1.getClaim().getBegin_date());
	    	}
	    	if (c == 0) {
	    		c = e2.getClaim().getClaim_id().compareTo(e1.getClaim().getClaim_id());
	    	}
	    	if (c == 0) {
	    		
	    		if (e1.getClaim().getClaim_line_id()==null || e2.getClaim().getClaim_line_id()==null) {
	    			c=-1;
	    		} 
	    		else {
	    			try {
	    			
	    				Integer a = Integer.parseInt(e2.getClaim().getClaim_line_id());
	    				Integer b = Integer.parseInt(e1.getClaim().getClaim_line_id());
	    				c = a.compareTo(b);
	    			}
		    		catch (NumberFormatException nfe) {
					
		    		}
	    		}
	    		
	    	}
	        return c;
	    }
	}
	
	class assEpCompare implements Comparator<AssociatedEpisode> {

	    @Override
	    public int compare(AssociatedEpisode a1, AssociatedEpisode a2) {
	    	
	    	/*
	    	 * order by episode, then by date, then by claim, then by line number
	    	 */
	    	
	    	int c;
	    	c = a2.getAssEpisode().getClaim().getBegin_date().compareTo(
	    			a1.getAssEpisode().getClaim().getBegin_date());
	    	if (c==0) {
	    		c = a2.getParentEpisode().getClaim().getBegin_date().compareTo(a1.getParentEpisode().getClaim().getBegin_date());
	    	}
	    	
	        return c;
	    }
	}
	
	class slCompare implements Comparator<ClaimServLine> {

	    @Override
	    public int compare(ClaimServLine o1, ClaimServLine o2) {
	    	int c;
	    	
	    	
	    	// avoid null pointers during sort
	    	if (o1.getClaim_id() == null)
	    		o1.setClaim_id("");
	    	if (o2.getClaim_id() == null)
	    		o2.setClaim_id("");
	    	if (o1.getClaim_line_type_code() == null)
	    		o1.setClaim_line_type_code("");
	    	if (o2.getClaim_line_type_code() == null)
	    		o2.setClaim_line_type_code("");
	    	if (o1.getClaim_line_id() == null)
	    		o1.setClaim_line_id("");
	    	if (o2.getClaim_line_id() == null)
	    		o2.setClaim_line_id("");
	    	if (o1.getSequence_key() == null)
	    		o1.setSequence_key("");
	    	if (o2.getSequence_key() == null)
	    		o2.setSequence_key("");
	    	
	    	
	    	// claim line type (IP. OP, PB); claim id; claim line id; sequence key (reverse)
	    	
	    	c = o1.getClaim_line_type_code().compareTo(o2.getClaim_line_type_code());
	    	
	    	if ( c == 0) {
	    		
	    		c = o1.getClaim_id().compareTo(o2.getClaim_id());
	    		
	    		if (c == 0) {
		    		
		    		c = o1.getClaim_line_id().compareTo(o2.getClaim_line_id());	
		    			
		    		if (c == 0) {
		    			
		    			// arguments reversed to create descending sort
		    			c = o2.getSequence_key().compareTo(o1.getSequence_key());	
		    			
		    		}
		    		
		    	}
	    		
	    	}
	    	
	    	
	    		
	        return c;
	    }
	}
	

	/**
	 * Get a diff between two dates
	 * @param date1 the oldest date
	 * @param date2 the newest date
	 * @param timeUnit the unit in which you want the diff
	 * @return the diff value, in the provided unit
	 */
	private static long getDateDiff(Date date1, Date date2, TimeUnit timeUnit) {
	    long diffInMillies = date2.getTime() - date1.getTime();
	    return timeUnit.convert(diffInMillies,TimeUnit.MILLISECONDS);
	}
	
	
	List<EpisodeMetaData> epmd;
	List<String> emCodes;
	MetaDataHeader mdh;
	// Mike is reformatting the above list to a Map so episodes can be retrieved by episode_id
	// if all moves to using the second object, the other files should be rewritten to support this
	Map<String, EpisodeMetaData> epmdk = new HashMap<String, EpisodeMetaData>();
	List<ClaimServLine> svcLines;
	List<ClaimRx> rxClaims;
	Map<String, List<Enrollment>> enrollments;
	Map<String, PlanMember> members;
	//Map<String, Provider> providers;
	Date studyBegin, studyEnd;
//	Map<String, HashSet<String>> trigCodes = new HashMap<String, HashSet<String>>();
	// trigCodedsByEpisode to contain episode id lookup to code type id (dx/px etc) lookup to code list
	//Map<String, HashMap<String, HashSet<String>>> trigCodesByEpisode = new HashMap<String, HashMap<String, HashSet<String>>>();
	//Map<String, HashMap<String, TriggerConditionMetaData>> trigParamsByEpisode = new HashMap<String, HashMap<String, TriggerConditionMetaData>>();
	// this will contain a list of episodes that can trigger the key episode with req conf claim and window vars
	//Map<String, HashMap<String, TriggerConditionMetaData>> trigEpisodes = new HashMap<String, HashMap<String, TriggerConditionMetaData>>();
	//hold provisional (not necessarily finalized) episodes (by claim by episode so get claim get episode)...
	//Map<String, HashMap<String, EpisodeShell>> provEpisodesOLD = new HashMap<String, HashMap<String, EpisodeShell>>();
	//this one is just member key with shells...
	//Map<String, HashSet<EpisodeShell>> provEpisodes = new HashMap<String, HashSet<EpisodeShell>>();
	// when episodes trigger episodes, the new ones will go here before getting added into provEpisodes
	List<EpisodeShell> addlProvEps = new ArrayList<EpisodeShell>();
	
	List<EpisodeShell> epsForMember = new ArrayList<EpisodeShell>();
	List<EpisodeShell> addlEpsforMember = new ArrayList<EpisodeShell>();
	// master list of episodes created with each pass of claims by member...
	HashMap<String, ArrayList<EpisodeShell>> episodesByMember = new HashMap<String, ArrayList<EpisodeShell>>();
	ArrayList<EpisodeShell> allEpisodes = new ArrayList<EpisodeShell>();
	ArrayList<AssociatedEpisode> potAssEps = new ArrayList<AssociatedEpisode>();
	
	PlanMember planMember = new PlanMember();
	

	public static void main(String[] args) {
		
		EpisodeConstructionMain instance = new EpisodeConstructionMain();
		
		// get parameters (if any)
		instance.loadParameters(args);
				
		// choose an input interface
		if (instance.parameters.get("typeinput").equalsIgnoreCase("csv"))
			instance.di = new AllDataCSV(instance.parameters);
		else if (instance.parameters.get("typeinput").equalsIgnoreCase("apcd"))
			instance.di = new AllDataAPCD(instance.parameters);
		else if (instance.parameters.get("typeinput").equalsIgnoreCase("xg"))
			instance.di = new AllDataXG(instance.parameters);
		else if (instance.parameters.get("typeinput").equalsIgnoreCase("wellpoint"))
			instance.di = new AllDataWellpoint(instance.parameters);
		else if (instance.parameters.get("typeinput").equalsIgnoreCase("pebtf"))
			instance.di = new AllDataPebtf(instance.parameters);
		else if (instance.parameters.get("typeinput").equalsIgnoreCase("hci3"))
			instance.di = new AllDataHCI3(instance.parameters);
		else
			throw new IllegalStateException ("Invalid input type parameter: " + instance.parameters.get("typeinput"));
		
		// choose an output interface
		if (instance.parameters.get("typeoutput").equalsIgnoreCase("csv")) 
		{}
			//instance.oi = new GenericOutputCSV();
		else if (instance.parameters.get("typeoutput").equalsIgnoreCase("sql"))
		{}
			//instance.oi = new GenericOutputSQL();
		else
			throw new IllegalStateException ("Invalid output type parameter: " + instance.parameters.get("typeoutput"));
		
		// process
		instance.process();

	}
	
	HashMap<String, String> parameters = RunParameters.parameters;
	String [][] parameterDefaults = {
			{"EP_FILE", 	"C:\\ECRAnalyticsTestFiles\\HCI3-ECR-Definition-Tables-2014-08-18-5.2.006_FULL.xml"},
			{"typeoutput", "sql"},
			{"studybegin", "20120101"},
			{"studyend", "21121231"}
	};
	
	/**
	 * load default parameters and 
	 * put any run arguments in the hash map as well
	 * arguments should take the form keyword=value (e.e., studybegin=20140101)
	 * @param args
	 */
	private void loadParameters (String[] args) {
		// load any default parameters from the default parameter array
		for (int i = 0; i < parameterDefaults.length; i++) {
			parameters.put(parameterDefaults[i][0], parameterDefaults[i][1]);
		}
		// overlay or add any incoming parameters
		for (int i = 0; i < args.length; i++) {
			parameters.put(args[i].substring(0, args[i].indexOf('=')), args[i].substring(args[i].indexOf('=')+1)) ;
		}
		// set specific variables
		try {
			
			studyBegin = df1.parse(parameters.get("studybegin"));
			studyEnd = df1.parse(parameters.get("studyend"));
		}
		catch (Exception ex ) {
			System.out.println(ex);
		}
		
	}
	
	DateFormat df1 = new SimpleDateFormat("yyyyMMdd");
	DateFormat df2 = new SimpleDateFormat("yyyy-MM-dd");
	
	private static org.apache.log4j.Logger log = Logger.getLogger(EpisodeConstructionMain.class);
	

	

}
