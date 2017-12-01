






public class NormalizationController extends AbstractController {
	

	DB db;
	//DBCollection coll;
	DBCursor cursor;
	BasicDBObject query;
	
	GenericInputInterface memberGetter;
	GenericInputInterface enrollmentGetter;
	GenericInputInterface RxGetter;
	GenericInputInterface claimGetter;
	
	
	ClaimHelper hClaim;
	
	HibernateHelper hC;
	SessionFactory factoryC;
	List<Class<?>> cListC;
	
	MapDefinitionInterface mdi = new MapDefinitionSQL();
	MapEntry mapEntry;
	private InputMap imap;

	private MongoInputCollectionSet micky;


	
	NormalizationController (HashMap<String, String> parameters) {
		this.parameters = parameters;
		initialize();
		queueStartAndLock(ProcessJobStep.STEP_NORMALIZATION);
		process();
	}
	

	
	/**
	 * process the members in the member list
	 * the list should be chunkSize long
	 */
	private void process ()  {
		
		memberGetter = new GenericInputMongo (new PlanMember(), parameters);
		enrollmentGetter = new GenericInputMongo (new Enrollment(), parameters);
		RxGetter = new GenericInputMongo (new ClaimRx(), parameters);
		claimGetter = new GenericInputMongo (new ClaimServLine(), parameters);

		ArrayList<Object> returnList;
		ArrayList<Enrollment> eList;
		
		InputObjectOutputInterface oi = new InputObjectOutputHibSQL(parameters);
		
		//int mmIdx =0;
		for (JobStepQueue _JSQ:memberList) {
			
			try {
			
				String memberId = _JSQ.getMember_id();
				
				if(memberId == null || memberId.trim().isEmpty()) {
					throw new IllegalStateException("Cannot process member with empty Id");
				}
				
				log.info("Starting Normalization step for " + memberId);
			
				returnList = memberGetter.read("member_id", memberId);
				if (returnList.size() == 0) {
					log.warn ("Member Id " + memberId + " has claims, but no member definition");
					m = new PlanMember();
					m.setMember_id(memberId);
				}
				else {
					log.debug ("Member Id " + memberId + " member definition OK");
					m = (PlanMember) returnList.get(0);
				}
			
			
				returnList = enrollmentGetter.read("member_id", memberId);
				eList = new ArrayList<Enrollment>();
				for (Object o:returnList) {
					eList.add((Enrollment) o);
				}
				m.setEnrollment(eList);
				log.debug("Enrollment read OK");
			
				returnList = RxGetter.read("member_id", memberId);
				for (Object o:returnList) {
					m.addRxClaim((ClaimRx) o);
					//log.info (">Rx " + ((ClaimRx) o).getClaim_id() + " Rx OK");
				}
				log.debug("Rx claim read OK");
			
				returnList = claimGetter.read("member_id", memberId);
				for (Object o:returnList) {
					m.addClaimServiceLine((ClaimServLine) o);
					//log.info (">Med " + ((ClaimServLine) o).getClaim_id() + " Med OK");
				}
				log.debug("Medical claim read OK");
				
				log.debug("Preparing to sort items related to " + memberId);
				
				Collections.sort(m.getServiceLines(), new slCompare());
				Collections.sort(m.getRxClaims(), new rxCompare());
				Collections.sort(m.getEnrollment(), new enCompare());
				
				log.info("== Input member  " + memberId + " - SvcLines: " + m.getServiceLines().size() + " Rx: " + m.getRxClaims().size() + " Enrollments: " + m.getEnrollment().size());
			
				// normalize
				normalize();
				
				log.info("== Output member " + memberId + " - SvcLines: " + m.getServiceLines().size() + " Rx: " + m.getRxClaims().size() + " Enrollments: " + m.getEnrollment().size());
			
				doMemberReport();
			
				// put objects out to database
				// yes, this sucks, but I don't want to break 'the old way'....yet
				ArrayList<PlanMember> mL = new ArrayList<PlanMember>();
				mL.add(m);
				oi.writeMembers(mL);
				// this too
				ArrayList< List<Enrollment>> enrollments = new ArrayList< List<Enrollment>>();
				enrollments.add(m.getEnrollment());
				oi.writeEnrollments(enrollments);
				// these guys are OK
				oi.writeMedicalClaims(m.getServiceLines());
				oi.writeRxClaims(m.getRxClaims());
				
				_JSQ.setStatus(JobStepQueue.STATUS_COMPLETE);
				_JSQ.setUpdated(new Date());
				log.info("Completed Normalization step for " + m.getMember_id());
			
			}
			
			catch (IllegalStateException e) {
				log.info("Caught IllegalStateException: " + e);
				_JSQ.setStatus(JobStepQueue.STATUS_FAILED);
				_JSQ.setUpdated(new Date());
				Blob blob;
				try {
					if (e.getMessage() != null)
						blob = new SerialBlob(e.getMessage().getBytes());
					else
						blob = new SerialBlob(e.toString().getBytes());
					_JSQ.setReport(blob);
				} catch (SQLException e1) {
					log.error("Error encountered saving step report " + _JSQ.getMember_id() + " in " + _JSQ.getJobUid() + " " + e1.getMessage());
				}
				_memberFailCount++;
				if (_memberFailCount > _memberFailTolerance)
					throw new IllegalStateException (_memberFailCount  + " members have failed causing the process to halt: " + e);
			}
			
			catch (Throwable e){
				log.info("Caught an Exception: " + e);
				_JSQ.setStatus(JobStepQueue.STATUS_FAILED);
				_JSQ.setUpdated(new Date());
				Blob blob;
				try {
					if (e.getMessage() != null)
						blob = new SerialBlob(e.getMessage().getBytes());
					else
						blob = new SerialBlob(e.toString().getBytes());
					_JSQ.setReport(blob);
				} catch (SQLException e1) {
					log.error("Error encountered saving step report " + _JSQ.getMember_id() + " in " + _JSQ.getJobUid() + " " + e1.getMessage());
				}
				log.error("Error encountered normalizing " + _JSQ.getMember_id() + " in " + _JSQ.getJobUid() + " " + e.getMessage());
				e.printStackTrace();
			}
				
			if (parameters.containsKey("debug")  &&  parameters.get("debug").equalsIgnoreCase("process")) {
				ProcessMessage message = new ProcessMessage();
				message.setJobUid(Integer.parseInt(parameters.get("jobuid")));
				message.setStepName("normalization");
				message.setMember_id(_JSQ.getMember_id());
				message.setStatus(_JSQ.getStatus());
				InetAddress IP = null;
				try {
					IP = InetAddress.getLocalHost();
				} catch (UnknownHostException e) { }
				message.setDescription("IP " + IP.getHostAddress() + " set status to " + _JSQ.getStatus());
				ProcessMessageHelper.writeMessage(factoryC, message);
			}
			
			//mmIdx++;
			//if(mmIdx % 10000 == 0) { 
			//	log.info("Completed Validation and Normalization for  " + mmIdx + " members. " + new Date());
			//}
			
		}
		

		storeMemberReport();
		
		JobQueueHelper.updateMemberQueue(memberList);
		
		
	}
	
	private int _memberFailTolerance = 25;
	private int _memberFailCount = 0;
	
	
	// ip op pb
	// physician attribution
	// final version
	// roll-up
	//TODO ED visit logic
	// validation reporting
	// put input objects to db
	// make ready for EC itself
	private void normalize () {
		
		ArrayList<ClaimServLine> _MemberSvcLines = new ArrayList<ClaimServLine>();
		ArrayList<ClaimServLine> _EachClaimSvcLines = new ArrayList<ClaimServLine>();
		_lastClaimKey = ""; 
		
		//log.info("Number of service lines incoming: " + m.getServiceLines().size());
		countMerged = 0;
		
		doMedClaimAdjustments();
		
		doRxClaimAdjustments();
		
		doMemberAdjustments();
		
		// Do roll ups
		for (ClaimServLine svcLine : m.getServiceLines()) {
			
		
			
			// ignore any prior versions of the exact same claim line to keep only final version
			if(_lastClaimVersionKey == null  ||  (!(genMedClaimVersionKey(svcLine).equals(_lastClaimVersionKey)))) {
				_lastClaimVersionKey = genMedClaimVersionKey(svcLine);
			}
			else
				continue;
			
			if (_lastClaimKey.isEmpty()  ||  genMedClaimKey(svcLine).equals(_lastClaimKey)) {
				_EachClaimSvcLines.add(svcLine);
				_lastClaimKey = genMedClaimKey(svcLine);
			}
			else {
				_MemberSvcLines.addAll(doMedClaimRollUp(_EachClaimSvcLines));
				_EachClaimSvcLines = new ArrayList<ClaimServLine>();
				_EachClaimSvcLines.add(svcLine);
				_lastClaimKey = genMedClaimKey(svcLine);
			}
			
		}
		_MemberSvcLines.addAll(doMedClaimRollUp(_EachClaimSvcLines));
		
		
		m.setServiceLines(_MemberSvcLines);
		
		//log.info("=====================================================");
		
		
		//log.info("Number of service lines outgoing: " + m.getServiceLines().size());
		//log.info("Number of merged service lines: " + countMerged);
		
		m.setRxClaims(doRxRollUp(m.getRxClaims()));
		
		m.setEnrollment(doEnrollmentRollUp(m.getEnrollment()));
		m.setEnrollment(doGapEnrollmentRollUp(m.getEnrollment()));
		
	
		
	}
	
	
	private StringBuilder builder = new StringBuilder();
	
	private String _lastClaimKey;
	
	// mechanism to determine where breaks between claims go
	private String genMedClaimKey (ClaimServLine svcLine) {
		
		builder.setLength(0);
		builder.append(svcLine.getClaim_line_type_code());
	    builder.append(svcLine.getClaim_id());

	    return builder.toString();
	    
	}
	
	private String _lastClaimVersionKey;
	
	// mechanism to help identify duplicate claim lines
	private String genMedClaimVersionKey (ClaimServLine svcLine) {
		
		builder.setLength(0);
		builder.append(svcLine.getClaim_id());
		builder.append(svcLine.getClaim_line_type_code());
	    builder.append(svcLine.getClaim_line_id());
	    builder.append(svcLine.getSequence_key());
	    return builder.toString();
	}
	
	private String _lastRxVersionKey;
	
	
	// mechanism to help identify duplicate claim lines
	private String genRxClaimVersionKey (ClaimRx rxLine) {
		
		builder.setLength(0);
		builder.append(rxLine.getClaim_id());
		builder.append(rxLine.getLineCounter());
	    builder.append(rxLine.getOrig_adj_rev());
	    builder.append(rxLine.getSequence_key());
	    return builder.toString();
	}
	
	
	
	private boolean doSanityCheck(ClaimServLine svcLine) {
		
		boolean bR = true;
		
		if (svcLine.getBegin_date() == null) {
			bR = false;
			log.error("Begin date is null for " + svcLine.getClaim_id() + "|" + svcLine.getClaim_line_id());
		}
		if (svcLine.getEnd_date() == null) {
			bR = false;
			log.error("Begin date is null for " + svcLine.getClaim_id() + "|" + svcLine.getClaim_line_id());
		}
		return bR;
	}
	
	
	/**
	 * perform any adjustments for missing data in medical claims
	 */
	private void doMedClaimAdjustments ()  {
		
		for (ClaimServLine svcLine : m.getServiceLines()) {
			
			
			/*
			log.info("Claim: " + svcLine.getClaim_id() + " : " + svcLine.getClaim_line_id() + " : " + svcLine.getSequence_key() 
					+ " : " + svcLine.getClaim_line_type_code());
			*/
			
			
			// Bill type check must precede date checks
			
			
			// ip op pb
			// give valid entry from sender preference over bill type algorithm
			if (svcLine.getClaim_line_type_code() != null  &&  (
				svcLine.getClaim_line_type_code().equalsIgnoreCase("OP")  ||
				svcLine.getClaim_line_type_code().equalsIgnoreCase("IP")  ||	
				svcLine.getClaim_line_type_code().equalsIgnoreCase("PB") )
				) {
					svcLine.setClaim_line_type_code(svcLine.getClaim_line_type_code().toUpperCase());
			}

			else {
				// bill type algorithm
				if (svcLine.getType_of_bill() != null && svcLine.getType_of_bill().length() > 1) {
					String sBTprefix = BillTypeManager.cleanBillType(svcLine.getType_of_bill());
					//if (svcLine.getFacility_type_code() == null  ||  svcLine.getFacility_type_code().isEmpty())
					//	svcLine.setFacility_type_code(BillTypeManager.getFacilityCode(sBTprefix));
					svcLine.setClaim_line_type_code(BillTypeManager.getFileType(sBTprefix));
					if (svcLine.getClaim_line_type_code() == null  ||  svcLine.getClaim_line_type_code().isEmpty() )
						svcLine.setClaim_line_type_code("PB");
				}
				else
					svcLine.setClaim_line_type_code("PB");
			}
			
			// set facility type if not received from sender
			if (svcLine.getFacility_type_code() == null  ||  svcLine.getFacility_type_code().isEmpty()) {
				if (svcLine.getType_of_bill() != null && svcLine.getType_of_bill().length() > 1) {
					String sBTprefix = BillTypeManager.cleanBillType(svcLine.getType_of_bill());
					svcLine.setFacility_type_code(BillTypeManager.getFacilityCode(sBTprefix));
				}
			}
			
			
			// make sure required start and end dates are filled in, and don't exceed study date
			doDateChecks(svcLine);		
									
			if ( ! doSanityCheck(svcLine) ) {
				throw new IllegalStateException ("Required data is null - can not normalize");
			}
									
						
			if(svcLine.getBegin_date().before(studyBegin)) {
				if (parameters.containsKey("dateremove")) {
					if(svcLine.getBegin_date().before(studyBegin)) {
						log.info("Rejecting: (M)" + svcLine.getMember_id() + " (C)" + svcLine.getClaim_id() + ":" + svcLine.getClaim_line_id() + 
								" because service line start date is prior to study period " +
								svcLine.getBegin_date() + " vs " + studyBegin);
						continue;
					}
					if(svcLine.getEnd_date() != null  &&  svcLine.getEnd_date().after(studyEnd)) {
						log.info("Rejecting: (M)" + svcLine.getMember_id() + " (C)" + svcLine.getClaim_id() + ":" + svcLine.getClaim_line_id() + 
								" because service line end date is after study period " +
								svcLine.getEnd_date() + " vs " + studyEnd);
						continue;
					}
				}
			}
			
			
			// if allowed amount is missing, try summing paid amt, prepaid amt, co-pay amt , coinsurance amt, and  deductible amt
			if (svcLine.getAllowed_amtD() == null ) {  
				Double tAA = 0.00d;
				if (svcLine.getPaid_amtD() != null )
					tAA = tAA + svcLine.getPaid_amt();
				if (svcLine.getPrepaid_amtD() != null )
					tAA = tAA + svcLine.getPrepaid_amt();
				if (svcLine.getCopay_amtD() != null )
					tAA = tAA + svcLine.getCopay_amt();
				if (svcLine.getCoinsurance_amtD() != null )
					tAA = tAA + svcLine.getCoinsurance_amt();
				if (svcLine.getDeductible_amtD() != null )
					tAA = tAA + svcLine.getDeductible_amt();
				svcLine.setAllowed_amt( tAA );
			}
			// for those (like Verisk) that don't fill allowed amount, but fill charge amount
			if (svcLine.getAllowed_amtD() == null  &&  svcLine.getCharge_amtD() != null)
				svcLine.setAllowed_amt(svcLine.getCharge_amtD());
			
			// physician attribution
			setAttributionProvider(svcLine);
			
		}
		
		//TODO get rid of mapname check soon (2016/07/29)
		if(bAddLineNumber || parameters.get("mapname").toLowerCase().equals("md_apcd"))
			m.setServiceLines( NormalizationHelper.MD_APCD_ClaimLineNumberGenerator_md((ArrayList<ClaimServLine>) m.getServiceLines()));
		if(bAddSequenceNumber)
			m.setServiceLines( NormalizationHelper.claimLineSequenceNumberGenerator((ArrayList<ClaimServLine>) m.getServiceLines()));
		
	}
	
	
	/**
	 * perform any adjustments for missing data in pharmacy claims
	 */
	private void doRxClaimAdjustments ()  {
		
		// Rx adjustments
		for (ClaimRx rxLine : m.getRxClaims()) {
			// if allowed amount is missing, try summing paid amt, prepaid amt, co-pay amt , coinsurance amt, and  deductible amt
			if (rxLine.getAllowed_amtD() == null ) {  
				Double tAA = 0.00d;
				if (rxLine.getPaid_amtD() != null )
					tAA = tAA + rxLine.getPaid_amt();
				if (rxLine.getPrepaid_amtD() != null )
					tAA = tAA + rxLine.getPrepaid_amt();
				if (rxLine.getCopay_amtD() != null )
					tAA = tAA + rxLine.getCopay_amt();
				if (rxLine.getCoinsurance_amtD() != null )
					tAA = tAA + rxLine.getCoinsurance_amt();
				if (rxLine.getDeductible_amtD() != null )
					tAA = tAA + rxLine.getDeductible_amt();
				rxLine.setAllowed_amt( tAA );
			}
		}
				
	}
	
	
	/**
	 * perform any adjustments for missing data in member records
	 */
	private void doMemberAdjustments ()  {

		// if no birth year, try to determine from age upon enrollment
		if(m.getBirth_year() == null) {
			
			for (Enrollment _e : m.getEnrollment()) {
				if (_e.getAge_at_enrollment() != null  &&  _e.getBegin_date() != null) {
					_eBeg.setTime(_e.getBegin_date());
					_eBeg.add(Calendar.YEAR, -(_e.getAge_at_enrollment()));
					m.setBirth_year(_eBeg.get(Calendar.YEAR));
					break;
				}
			}
			
		}
				
	}
	private Calendar _eBeg = Calendar.getInstance();
	private Calendar _eEnd = Calendar.getInstance();


	/**
	 * default roll-up
	 * For IP, combine all lines into one service line.
	 * For OP and PB, combine all line adjustments into one service line: 
	 * get earliest start date and latest end date
	 * add all amounts
	 * concatenate all lists
	 */
	private ArrayList<ClaimServLine> doMedClaimRollUp(ArrayList<ClaimServLine> lines) {
 
		if(lines.isEmpty()  ||  lines.get(0) == null) {
			log.info("No Lines to roll-up for this member");
			return new ArrayList<ClaimServLine>();
		}
		
		//log.info("medclaimrollup: " + parameters.get("mapname"));
		
		switch (parameters.get("mapname").toLowerCase()) {
		//TODO replace with a map directive next time xg_ghc comes around
		case "xg_ghc":
			lines = NormalizationHelper.xg_highSequenceRollUpMd(lines);
			if(lines.isEmpty()  ||  lines.get(0) == null) {
				log.info("No Lines to roll-up for this member after XG roll-up");
				return new ArrayList<ClaimServLine>();
			}
			break;
		/*	
		case "md_apcd":
			lines = NormalizationHelper.MD_APCD_ClaimLineNumberGenerator_md(lines);
			break;
		*/	
		default :
			break;
		}
		
		String line_type = lines.get(0).getClaim_line_type_code();
		
		if(line_type == null) {
			log.info("No Line type for this member");
			return new ArrayList<ClaimServLine>();
		}
		
		return line_type.equals("IP")  ?  doIPRollUp(lines)  :  doNonIPRollUp(lines);
		
	}
	
		
	/**
	 * set line number to zeroes
	 * combine all amounts, lists, etc.
	 * @param lines
	 * @return
	 */
	private ArrayList<ClaimServLine> doIPRollUp(ArrayList<ClaimServLine> lines) {
		
		ArrayList<ClaimServLine> _result = new ArrayList<ClaimServLine>();
		
		ClaimServLine cTarget = null;
		
		for (ClaimServLine svcLine : lines) {
			
			if (cTarget == null) {
				cTarget = lines.get(0);
				cTarget.setClaim_line_id("00");
			}
			else
				combineLines(cTarget, svcLine);
		}
		
		_result.add(cTarget);
		
		return _result;
		
	}
	
	
	/**
	 * roll up by line number
	 * combine all amounts, lists, etc.
	 * @param lines
	 * @return
	 */
	private ArrayList<ClaimServLine> doNonIPRollUp(ArrayList<ClaimServLine> lines) {
		
		ArrayList<ClaimServLine> _result = new ArrayList<ClaimServLine>();
		
		ClaimServLine cTarget = null;
		String _line_no = null;// = lines.get(0).getClaim_line_id();

		for (ClaimServLine svcLine : lines) {
			
			if (cTarget == null) {
				cTarget = lines.get(0);
				_line_no = svcLine.getClaim_line_id();
			}
			else if (svcLine.getClaim_line_id().equals(_line_no)) {
				combineLines(cTarget, svcLine);
			}
			else {
				_result.add(cTarget);
				cTarget = svcLine;
				_line_no = svcLine.getClaim_line_id();
			}
			
		}
		
		_result.add(cTarget);
		
		return _result;
		
	}
	
	
	/**
	 * roll up Rx claims
	 */
	private ArrayList<ClaimRx> doRxRollUp (List<ClaimRx> rxs) {
		
		ArrayList<ClaimRx> _result = new ArrayList<ClaimRx>();
		
		ClaimRx cTarget = null;
		
		for (ClaimRx rxIn : rxs) {
			
			// ignore any prior versions of the exact same claim line to keep only final version
			if(_lastRxVersionKey == null  ||  ( !(_lastRxVersionKey.equals(genRxClaimVersionKey(rxIn)) ) ) ) {
				_lastRxVersionKey = genRxClaimVersionKey(rxIn);
			}
			else 
				continue;

			
			if (cTarget == null) {
				cTarget = rxIn;
				_lastRxKey = genRxClaimKey(rxIn);
			}
			else if (genRxClaimKey(rxIn).equals(_lastRxKey)) {
				combineRxLines(cTarget, rxIn);
			}
			else {
				_result.add(cTarget);
				cTarget = rxIn;
				_lastRxKey = genRxClaimKey(rxIn);
			}
			
		}
		
		if (cTarget != null)
			_result.add(cTarget);
		
		return _result;
		
	}
	
	
	
	private String _lastRxKey;
	
	// mechanism to determine where breaks between rx claims go
	private String genRxClaimKey (ClaimRx rxLine) {
		
		StringBuilder builder = new StringBuilder();
		//builder.append(rxLine.getClaim_line_type_code());
	    builder.append(rxLine.getClaim_id());

	    return builder.toString();
	    
	}
	
	

	private void combineRxLines (ClaimRx cTarget, ClaimRx rxIn) {
		
		cTarget.setAllowed_amt(cTarget.getAllowed_amt() + rxIn.getAllowed_amt());
		cTarget.setProxy_allowed_amt(cTarget.getProxy_allowed_amt() + rxIn.getProxy_allowed_amt());
		cTarget.setReal_allowed_amt(cTarget.getReal_allowed_amt() + rxIn.getReal_allowed_amt());
		cTarget.setCharge_amt(cTarget.getCharge_amt() + rxIn.getCharge_amt());
		cTarget.setCoinsurance_amt(cTarget.getCoinsurance_amt() + rxIn.getCoinsurance_amt());
		cTarget.setCopay_amt(cTarget.getCopay_amt() + rxIn.getCopay_amt());
		cTarget.setDeductible_amt(cTarget.getDeductible_amt() + rxIn.getDeductible_amt());
		cTarget.setPaid_amt(cTarget.getPaid_amt() + rxIn.getPaid_amt());
		cTarget.setPrepaid_amt(cTarget.getPrepaid_amt() + rxIn.getPrepaid_amt());
		
	}
	
	
	
	
	/**
	 * 
	 */
	private void doDateChecks(ClaimServLine svcLine) {
		
		if (svcLine.getClaim_line_type_code().equals("IP")) {
			if (svcLine.getAdmission_date() == null  &&
					svcLine.getBegin_date() != null)
				svcLine.setAdmission_date(svcLine.getBegin_date());
			if (svcLine.getDischarge_date() == null  &&
					svcLine.getEnd_date() != null)
				svcLine.setDischarge_date(svcLine.getEnd_date());
			if (svcLine.getDischarge_date() == null  &&
					svcLine.getAdmission_date() != null)
				svcLine.setDischarge_date(svcLine.getAdmission_date());
		}
		
		if (bAlwaysUseAdmitDischarge) {
			if (svcLine.getAdmission_date() != null /* &&
				svcLine.getBegin_date() == null */)
				svcLine.setBegin_date(svcLine.getAdmission_date());
			if (svcLine.getDischarge_date() != null /* &&
				svcLine.getEnd_date() == null*/)
				svcLine.setEnd_date(svcLine.getDischarge_date());
		}
		
		if (svcLine.getBegin_date() == null)
			log.error("Claim still has null begin date " + svcLine.getClaim_id() + "|" + svcLine.getClaim_line_id() );
		
		if (svcLine.getEnd_date() == null  &&
				svcLine.getBegin_date() != null)
			svcLine.setEnd_date(svcLine.getBegin_date());
			
		if (svcLine.getBegin_date() != null  &&  svcLine.getBegin_date().after(icd10Day)) {
			List<MedCode> x = svcLine.getMed_codes();
			for (MedCode _mc : x) {
				if(_mc.getNomen().equals("DX"))
					_mc.setNomen("DXX");
				else if(_mc.getNomen().equals("PX"))
				_mc.setNomen("PXX");
			}
		}
			

	}	
	
	static final SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
	static Date icd10Day = null;
	
	static {
			
		try {
			icd10Day = sdf.parse("30/09/2015");
		} catch (ParseException e) {
			e.printStackTrace();
		}
			
	}
	

	/**
	 * set attribution provider
	 * @param svcLine
	 */
	private void setAttributionProvider(ClaimServLine svcLine) {
		
		String prov_id;
		
		if (bPlanProviderIdPreferred) {
			prov_id =svcLine.getProvider_id();
		}
		else {
			// old map based defaults - should be replaced by map directives - just leaving temporarily (2016/07/29)
			switch (parameters.get("mapname").toLowerCase()) {
				case "xg_ghc":
					prov_id =svcLine.getProvider_id();
					break;
				case "wellpoint":
					prov_id =svcLine.getProvider_NPI();
					break;
				default :
					prov_id = svcLine.getProvider_NPI() == null  || svcLine.getProvider_NPI().isEmpty()   ?
							svcLine.getProvider_id()  :  svcLine.getProvider_NPI();
			}
		}

				
		if(svcLine.getClaim_line_type_code().equals("PB") )
			svcLine.setPhysician_id(prov_id);
		else
			svcLine.setFacility_id(prov_id);
		
	}
	
	
	
	/**
	 * stock enrollment roll-up
	 * if product and coverage types are the same
	 * if this record's begin date is one day greater than a prior record's enrollment, 
	 * just extend the end date of the existing record
	 * @param enlist
	 */
	private List<Enrollment> doEnrollmentRollUp(List<Enrollment> enlist) {
		
		List<Enrollment> _eList = new ArrayList<Enrollment>();
		Enrollment _e = null;
		
		
		// go through all incoming enrollments looking for records to concatenate 
		// (prior record end date is one day less than current begin date)
		for (Enrollment e : enlist) {
			
			//log.info("roll up enrollment");
			
			if (e.getCoverage_type() == null)
				e.setCoverage_type("");
			if (e.getInsurance_product() == null)
				e.setInsurance_product("");
			
			// on first pass, seed the comparison object variable
			if (_e == null) {
				_e = e;
				_eList.add(e);
				//log.info("Added first enrollment");
				continue;
			}
			
			// if all key fields match, check current instance (e) against held temporary instance
			if (_e.getInsurance_product().equals(e.getInsurance_product())  &&
					_e.getCoverage_type().equals(e.getCoverage_type()))  {
				c.setTime(_e.getEnd_date());
				c.add(Calendar.DAY_OF_MONTH, 1);
				d.setTime(c.getTimeInMillis());
				// if it should be concatenated, set the outgoing end date
				if ( ( ! e.getBegin_date().after(d) )  ) {
					if (e.getEnd_date() == null  ||  e.getEnd_date().after(_e.getEnd_date())) 
				//if (fmt.format(d).equals(fmt.format(e.getBegin_date()))) {
					_e.setEnd_date(e.getEnd_date());
				}
				// if not, add the non-contiguous object to the output list and get ready to resume checking
				else {
					_eList.add(e);
					//log.info("Added enrollment due to break in coverage or product");
					_e = e;
				}
			}
			else {
				_eList.add(e);
				//log.info("Added unique enrollment");
				_e = e;
			}
			
		}
		
		return _eList;
		
	}
	
	Calendar c = Calendar.getInstance();
	Date d = new Date(0);
	SimpleDateFormat fmt = new SimpleDateFormat("yyyyMMdd");
	
	/**
	 * Gap Enrollment Finder...
	 * @param enlist
	 */
	private List<Enrollment> doGapEnrollmentRollUp(List<Enrollment> enlist) {
		
		int miliToDays = (24 * 60 * 60 * 1000);
		SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
		Date studyBegin = null;
		Date studyEnd = null;
		try {
			studyBegin = format.parse(parameters.get("studybegin"));
			studyEnd = format.parse(parameters.get("studyend"));
		}
		catch (Exception ex ) {
			System.out.println(ex);
			log.error("A problem occurred trying to parse the study dates.  Please check them.  Begin date is: " + parameters.get("studybegin") + " and End date is: " + parameters.get("studyend"));
		}
		
		
		Collections.sort(enlist, new enGapCompare());
		
		Enrollment _e = null;
		
		List<Enrollment> gaplist = new ArrayList<Enrollment>();

		
		// go through all incoming enrollments looking for gaps
		// a gap is defined as any lapse in coverage from the study begin date till its end
		// (prior record end date is one day less than current begin date)
		for (Enrollment e : enlist) {
			
			if(e.getBegin_date() == null) {
				log.error("Enrollment gap roll up can not continue due to lack of enrollment begin date.  Please check mapping for Enrollment begin_date.");
				throw new IllegalStateException ("Enrollment gap roll up can not continue due to lack of enrollment begin date.  Please check mapping for Enrollment begin_date.");
			}

			
			// on first pass, check first record against beginning of study
			if (_e == null) {
				
				_e = e;
				
				long diffDays = (e.getBegin_date().getTime() - studyBegin.getTime()) / miliToDays;
				
				if (diffDays>0) {
					//e.setGap(true);
					_eBeg.setTime(e.getBegin_date());
					_eBeg.add(Calendar.DATE, -1);
					gaplist.add(createGapRecord(e, studyBegin, _eBeg.getTime()));
				}

				continue;
			}
			

			//  if coverage or insurance product change, check gap to end of study for prior enrollment record
			if ( _e.getEnd_date() != null  &&
					( ! (e.getCoverage_type().equals(_e.getCoverage_type())) )  || 
					( ! (e.getInsurance_product().equals(_e.getInsurance_product())) ) ) {
				
				long diffDays = (studyEnd.getTime() - _e.getEnd_date().getTime()) / miliToDays;
				
				if (diffDays>0) {
					_eEnd.setTime(_e.getEnd_date());
					_eEnd.add(Calendar.DATE, 1);
					gaplist.add(createGapRecord(_e, _eEnd.getTime(), studyEnd));
				}
				
			}
			
			// if end of e is more than 1 day before begin of _e, we have a gap that needs to be recorded
			long daysDiffGap = (e.getBegin_date().getTime() - _e.getEnd_date().getTime()) / miliToDays;
			
			if (daysDiffGap>1) {
				_eBeg.setTime(_e.getEnd_date());
				_eBeg.add(Calendar.DATE, 1);
				_eEnd.setTime(e.getBegin_date());
				_eEnd.add(Calendar.DATE, -1);
				gaplist.add(createGapRecord(e, _eBeg.getTime(), _eEnd.getTime()));
			}
			
			_e = e;
			
		}
		
		//  check last enrollment record for gap to end of study
		if ( _e != null  &&  _e.getEnd_date() != null ) {
					
			long diffDays = (studyEnd.getTime() - _e.getEnd_date().getTime()) / miliToDays;
					
			if (diffDays>0) {
				_eEnd.setTime(_e.getEnd_date());
				_eEnd.add(Calendar.DATE, 1);
				gaplist.add(createGapRecord(_e, _eEnd.getTime(), studyEnd));
			}
					
		}
		
		if (! gaplist.isEmpty() ) {
			enlist.addAll(gaplist);
			Collections.sort(enlist, new enGapCompare());
		}
		
		return enlist;
		
	}
	
	
	private Enrollment createGapRecord (Enrollment e, Date gap_begin, Date gap_end)  {
		
		Enrollment gapRecd = new Enrollment();
		// gapRecd.setAge_at_enrollment(e.getAge_at_enrollment());  // leave this null
		gapRecd.setBegin_date(gap_begin);
		gapRecd.setCoverage_type(e.getCoverage_type());
		gapRecd.setEnd_date(gap_end);
		gapRecd.setGap(true);
		gapRecd.setInsurance_product(e.getInsurance_product());
		gapRecd.setMember_id(e.getMember_id());
		return gapRecd;
		
	}
	
	
	
	/**
	 * per member report
	 */
	private void doMemberReport () {

		
		PerMemberReport _PMReport = new PerMemberReport();
		_PMReport.setJobUid(Integer.parseInt(parameters.get("jobuid")));
		_PMReport.setMember_id(m.getMember_id());
		_PMReport.setAuthorizer("Auto-authorize");
		_PMReport.setAuthorizeDate(new Date());
		
		// do summarization and bypass checks
		String _lastClaimId = "";
		for (ClaimServLine svcLine : m.getServiceLines()) {
				
			/*
			log.info("Claim: " + svcLine.getClaim_id() + " : " + svcLine.getClaim_line_id() + " : " + svcLine.getSequence_key() 
					+ " : " + svcLine.getClaim_line_type_code());
			*/		
					
			if (!svcLine.getClaim_id().equals(_lastClaimId)) {
				switch (svcLine.getClaim_line_type_code()) {
				case "IP":
					_PMReport.setIPClaimCount(_PMReport.getIPClaimCount() + 1);
					break;
				case "OP":
					_PMReport.setOPClaimCount(_PMReport.getOPClaimCount() + 1);
					break;
				case "PB":
					_PMReport.setPBClaimCount(_PMReport.getPBClaimCount() + 1);
					break;
				default:
					break;
				}
				_lastClaimId = svcLine.getClaim_id();
			}
					
			//TODO bypass claims over $1MM  & claims < $0
					
			switch (svcLine.getClaim_line_type_code()) {
			case "IP":
				_PMReport.setIPClaimTotalAmount(_PMReport.getIPClaimTotalAmount().add(BigDecimal.valueOf(svcLine.getAllowed_amt())));
				break;
			case "OP":
				_PMReport.setOPClaimTotalAmount(_PMReport.getOPClaimTotalAmount().add(BigDecimal.valueOf(svcLine.getAllowed_amt())));
				break;
			case "PB":
				_PMReport.setPBClaimTotalAmount(_PMReport.getPBClaimTotalAmount().add(BigDecimal.valueOf(svcLine.getAllowed_amt())));
				break;
			default:
				break;
			}
					
					
		}
		
		// bypass member if claims are greater than $1MM
		if (ONE_MILLION.compareTo
				(_PMReport.getIPClaimTotalAmount().add(_PMReport.getOPClaimTotalAmount()).add(_PMReport.getPBClaimTotalAmount())) == -1)
		{
			_PMReport.setBypass(true);
		}
		// bypass member if claims are less than $0
		else if (ZERO.compareTo
				(_PMReport.getIPClaimTotalAmount().add(_PMReport.getOPClaimTotalAmount()).add(_PMReport.getPBClaimTotalAmount())) == 1)
		{
			_PMReport.setBypass(true);
		}
		
		for (ClaimRx rxLine : m.getRxClaims()) {
			_PMReport.setRxClaimCount(_PMReport.getRxClaimCount() + 1);
			try {
				if (! (rxLine.getAllowed_amtD().compareTo(0.0d) == 0) )
					_PMReport.setRXClaimTotalAmount(_PMReport.getRXClaimTotalAmount().add(BigDecimal.valueOf(rxLine.getAllowed_amt())));
			} catch (NullPointerException e) {
				log.error("getRXClaimTotalAmount: " + _PMReport.getRXClaimTotalAmount() + " | rxLine: " + rxLine);
			}
			
		}
		
		rptList.add(_PMReport);
				
	}
	
	private static final BigDecimal ONE_MILLION = new BigDecimal("1000000");
	private static final BigDecimal ZERO = new BigDecimal("0");
	
	int countMerged = 0;
	
	
	/**
	 * medical claim service line combination utility
	 * @param cTarget
	 * @param svcLine
	 */
	private void combineLines (ClaimServLine cTarget, ClaimServLine svcLine) {
		
		cTarget.setAllowed_amt(cTarget.getAllowed_amt() + svcLine.getAllowed_amt());
		cTarget.setProxy_allowed_amt(cTarget.getProxy_allowed_amt() + svcLine.getProxy_allowed_amt());
		cTarget.setReal_allowed_amt(cTarget.getReal_allowed_amt() + svcLine.getReal_allowed_amt());
		cTarget.setCharge_amt(cTarget.getCharge_amt() + svcLine.getCharge_amt());
		cTarget.setCoinsurance_amt(cTarget.getCoinsurance_amt() + svcLine.getCoinsurance_amt());
		cTarget.setCopay_amt(cTarget.getCopay_amt() + svcLine.getCopay_amt());
		cTarget.setDeductible_amt(cTarget.getDeductible_amt() + svcLine.getDeductible_amt());
		cTarget.setPaid_amt(cTarget.getPaid_amt() + svcLine.getPaid_amt());
		cTarget.setPrepaid_amt(cTarget.getPrepaid_amt() + svcLine.getPrepaid_amt());
		cTarget.setStandard_payment_amt(cTarget.getStandard_payment_amt() + svcLine.getStandard_payment_amt());
		if (cTarget.getQuantity() < svcLine.getQuantity())
			cTarget.setQuantity(svcLine.getQuantity());
		
		if (cTarget.getBegin_date() == null) {
			cTarget.setBegin_date(svcLine.getBegin_date());
		}
		else {
			if (svcLine.getBegin_date() != null  &&
				svcLine.getBegin_date().before(cTarget.getBegin_date()))
					cTarget.setBegin_date(svcLine.getBegin_date());
		}
		
		if (cTarget.getEnd_date() == null) {
			cTarget.setEnd_date(svcLine.getEnd_date());
		}
		else {
			if (svcLine.getEnd_date() != null  &&
				svcLine.getEnd_date().after(cTarget.getEnd_date()))
					cTarget.setEnd_date(svcLine.getEnd_date());
		}	
		
		if (cTarget.getAdmission_date() == null) {
			cTarget.setAdmission_date(svcLine.getAdmission_date());
		}
		else {
			if (svcLine.getAdmission_date() != null  &&
				svcLine.getAdmission_date().before(cTarget.getAdmission_date()))
					cTarget.setAdmission_date(svcLine.getAdmission_date());
		}	
		
		if (cTarget.getDischarge_date() == null) {
			cTarget.setDischarge_date(svcLine.getDischarge_date());
		}
		else {
			if (svcLine.getDischarge_date() != null  &&
				svcLine.getDischarge_date().after(cTarget.getDischarge_date()))
					cTarget.setDischarge_date(svcLine.getDischarge_date());
		}	
		for(MedCode _mc : svcLine.getPrincipal_proc_code_objects()) {
			String s = _mc.getCode_value().replace(".", "");
			if(!cTarget.getPrincipal_proc_code().contains(s))
				cTarget.addPrinProcCodeAndNomen(_mc.getCode_value(), _mc.getNomen());
				
		}
		for(String s : svcLine.getRev_code()) {
			if(!cTarget.getRev_code().contains(s))
				cTarget.addRevCode(s);
		}
		//if(!cTarget.getSecondary_diag_code().contains(svcLine.getPrincipal_diag_code()))		// multiple varying principal diag codes on roll-up?
		//	cTarget.addDiagCode(svcLine.getPrincipal_diag_code());
		for(String s : svcLine.getSecondary_diag_code()) {
			if(!cTarget.getSecondary_diag_code().contains(s))
				cTarget.addDiagCode(s);
		}
		for(String s : svcLine.getSecondary_proc_code()) {
			if(!cTarget.getSecondary_proc_code().contains(s))
				cTarget.addProcCode(s);
				
		}
		
		if(cTarget.getSequence_key() == null || cTarget.getSequence_key().isEmpty()) {
			// leave as is
		} 
		else
		 	cTarget.setSequence_key("00");
		
		
		countMerged++;
		
	}
	

	
	/**
	 * store the member's validation report
	 */
	private void storeMemberReport ()  {
		
		// initialize Hibernate and get someone to process
		List<Class<?>> cList = new ArrayList<Class<?>>();
		cList.add(PerMemberReport.class);
		
		String schemaName = parameters.get("jobname")  == null ?  "javatest" : parameters.get("jobname");
		
		HibernateHelper h = new HibernateHelper(cList, parameters.get("env"), schemaName);
		SessionFactory factory = h.getFactory(parameters.get("env"), schemaName);
		
		Session session = factory.openSession();
		session.setCacheMode(CacheMode.IGNORE);
        session.setFlushMode(FlushMode.COMMIT);
		Transaction tx = null;
		
		try{

			tx = session.beginTransaction();
			
			int iB = 0;
            for (PerMemberReport rpt : rptList) { 
					
            	session.save(rpt);
	       			
            	if( iB % HibernateHelper.BATCH_INSERT_SIZE == 0 ) {
            		tx.commit();
            		session.flush();
            		session.clear();
            		tx = session.beginTransaction();
            	}
            	if(iB % 100000 == 0) { 
            		log.info("executing rpt insert " + iB + " ==> " + new Date());
            	}
	             	
            	iB++;
    			
            }
            
            tx.commit();

			
		}catch (HibernateException e) {
			
			if (tx!=null  && !tx.wasCommitted()) tx.rollback();
			e.printStackTrace();
			
		}finally {
			
		}
			
		
	}
	
	
	

	private void initialize ()  {
		
		// initialize Mongo
		micky = new MongoInputCollectionSet(parameters);
		db = micky.getDb();
		
		
		// allow a chunksize parameter - default is 1
		if (parameters.containsKey("chunksize"))
			chunkSize = Integer.parseInt(parameters.get("chunksize"));

		
		try {
			studyBegin = df1.parse(parameters.get("studybegin"));
			studyEnd = df1.parse(parameters.get("studyend"));
		}
		catch (Exception ex ) {
			System.out.println("There is a problem with the study period settings for job: " +  parameters.get("jobuid") + " - " + ex);
			throw new IllegalStateException("There is a problem with the study period settings for job: " +  parameters.get("jobuid") + " - " + ex);
		}
		
		cListC = new ArrayList<Class<?>>();
		cListC.add(ProcessMessage.class);
		
		hC = new HibernateHelper(cListC, "ecr", "ecr");
		factoryC = hC.getFactory("ecr", "ecr");
		
		mapEntry = mdi.getMapDefinition(parameters.get("mapname"));
		imap = new InputMap(mapEntry.getMapContents()).getMapping();
		loadDirectives(imap.getDirectivesFromMap());
		
		
	}
	
	private DateFormat df1 = new SimpleDateFormat("yyyyMMdd");
	Date studyBegin, studyEnd;
	
	
	private void loadDirectives (List<String> directives) {
		for (String s : directives) {
			switch (s) {
			case "addSequenceNumber" : {
				bAddSequenceNumber = true;
			    break;
			}
			case "addLineNumber" : {
				bAddLineNumber = true;
				break;
			}
			case "preferPlanProviderId" : {
				bPlanProviderIdPreferred = true;
				break;
			}
			case "alwaysUseAdmitDischarge" : {
				bPlanProviderIdPreferred = true;
				break;
			}
			default: {
			    log.info("Encountered unknown directive '" + s + "' in map " + imap.getMapName());
			}
		    }

		}
	}
	
	private boolean bAddSequenceNumber = false;
	private boolean bAddLineNumber = false;
	private boolean bPlanProviderIdPreferred = false;
	private boolean bAlwaysUseAdmitDischarge = false;
	

	public static void main(String[] args) {
		
		///HashMap<String, String> parameters = RunParameters.parameters;
		//parameters.put("clientID", "Wellpoint");
		//parameters.put("runname", "5_3_");
		//parameters.put("rundate", "20150918");
		//parameters.put("jobuid", "1086");
		//parameters.put("testlimit", "200");
		//parameters.put("configfolder", "C:\\workspace\\ECR_Analytics\\trunk\\EpisodeConstruction\\src\\");
		//parameters.put("env", "prd");
		//parameters.put("studybegin", "20120101");
		//parameters.put("studyend", "21120101");
		//parameters.put("jobname", "Wellpoint_5_3_20150918");
		//parameters.put("mapname", "wellpoint");
		
		HashMap<String, String> parameters = RunParameters.parameters;
		
		parameters.put("configfolder", "C:\\workspace\\ECR_Analytics\\trunk\\EpisodeConstruction\\src\\");
		parameters.put("env", "prd");
		
		parameters.put("jobuid", "1119");
		parameters.put("clientID", "PEBTF");
		parameters.put("runname", "PEBTF_maptest");
		parameters.put("rundate", "20160720");
		parameters.put("jobname", "PEBTF_maptest20160720");
		parameters.put("mapname", "pebtf");
		parameters.put("studybegin", "20120101");
		parameters.put("studyend", "20160430");
		
		parameters.put("chunksize", "10");
		
		
		/*NormalizationController instance = */   new NormalizationController(parameters);
		
		//System.out.println("Not what I meant to do");

	}
	
	
	
	private static org.apache.log4j.Logger log = Logger.getLogger(NormalizationController.class);
	
	
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


	

	class enCompare implements Comparator<Enrollment> {

	    @Override
	    public int compare(Enrollment o1, Enrollment o2) {
	    	int c;
	    	
	    	
	    	// avoid null pointers during sort
	    	if (o1.getInsurance_product() == null)
	    		o1.setInsurance_product("");
	    	if (o2.getInsurance_product() == null)
	    		o2.setInsurance_product("");
	    	if (o1.getCoverage_type() == null)
	    		o1.setCoverage_type("");
	    	if (o2.getCoverage_type() == null)
	    		o2.setCoverage_type("");
	    	if (o1.getBegin_date() == null)
	    		o1.setBegin_date(new Date(0));
	    	if (o2.getBegin_date() == null)
	    		o2.setBegin_date(new Date(0));
	    	if (o1.getEnd_date() == null)
	    		o1.setEnd_date(new Date());
	    	if (o2.getEnd_date() == null)
	    		o2.setEnd_date(new Date());
	    	
	    	c = o1.getInsurance_product().compareTo(o2.getInsurance_product());
	    	
	    	if (c == 0) {
	    		
	    		c = o1.getCoverage_type().compareTo(o2.getCoverage_type());	
	    			
	    		if (c == 0) {
	    			
	    			c = o1.getBegin_date().compareTo(o2.getBegin_date());	
	    		
	    		}
	    		
	    	}
	    		
	        return c;
	    }
	}
	
	
	class rxCompare implements Comparator<ClaimRx> {

	    @Override
	    public int compare(ClaimRx o1, ClaimRx o2) {
	    	
	    	int c;
	    	
	    	// avoid null pointers during sort
	    	if (o1.getClaim_id() == null)
	    		o1.setClaim_id("");
	    	if (o2.getClaim_id() == null)
	    		o2.setClaim_id("");
	    	if (Integer.valueOf(o1.getLineCounter()) == null)
	    		o1.setLineCounter(0);
	    	if (Integer.valueOf(o2.getLineCounter()) == null)
	    		o2.setLineCounter(0);
	    	if (o1.getOrig_adj_rev() == null)
	    		o1.setOrig_adj_rev("");
	    	if (o2.getOrig_adj_rev() == null)
	    		o2.setOrig_adj_rev("");
	    	if (o1.getSequence_key() == null)
	    		o1.setSequence_key("");
	    	if (o2.getSequence_key() == null)
	    		o2.setSequence_key("");
	    	
	    	// claim id; line number; original versus adjustment, etc., sequence (version) in reverse order
	    	
	    	c = o1.getClaim_id().compareTo(o2.getClaim_id());
	    	
	    	if (c == 0) {
	    		
	    		c = Integer.valueOf(o1.getLineCounter()).compareTo(o2.getLineCounter());	
	    			
	    		if (c == 0) {
	    			
	    			c = o1.getOrig_adj_rev().compareTo(o2.getOrig_adj_rev());	
	    			
	    			if (c == 0) {
		    			
		    			// arguments reversed to create descending sort
		    			c = o2.getSequence_key().compareTo(o1.getSequence_key());	
		    			
		    		}
	    		
	    		}
	    		
	    	}
	    		
	        return c;
	    }
	}
	

	class enGapCompare implements Comparator<Enrollment> {

	    @Override
	    public int compare(Enrollment o1, Enrollment o2) {
	    	int c;
	    	
	    	if (o1.getBegin_date() == null)
	    		o1.setBegin_date(new Date(0));
	    	if (o2.getBegin_date() == null)
	    		o2.setBegin_date(new Date(0));
	    	if (o1.getEnd_date() == null)
	    		o1.setEnd_date(new Date());
	    	if (o2.getEnd_date() == null)
	    		o2.setEnd_date(new Date());
	    	
	    	c = o1.getBegin_date().compareTo(o2.getBegin_date());
	    			
    		if (c == 0) {
    			
    			c = o1.getEnd_date().compareTo(o2.getEnd_date());	
    		
    		}
	    		
	    		
	        return c;
	    }
	}

	
	

}
