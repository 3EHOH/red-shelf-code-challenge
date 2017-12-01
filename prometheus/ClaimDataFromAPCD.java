





public class ClaimDataFromAPCD extends ClaimDataAbstract implements ClaimDataInterface {
	
	
	protected AllDataInterface di;
	Map<String, Provider> providers;

	/**
	 * Constructor that will provide error message management functionality
	 * @param errMgr
	 */
	public ClaimDataFromAPCD(AllDataAPCD di, ErrorManager errMgr) {
		this.errMgr = errMgr;
		this.di = di;
		providers = di.getAllProviders();
	}
	
	/**
	 * Basic constructor without error message management
	 */
	public ClaimDataFromAPCD (AllDataAPCD di) {
		this.errMgr = null;
		this.di = di;
		providers = di.getAllProviders();
	}

	
	protected boolean doMap(int col_ix, String col_value) {
		
		boolean bResult = true;
		
		
		
		ColumnFinder col_clue = col_index.get(col_ix);
		if (col_clue == null)
			{}
		else {
			if (col_value != null  && !(col_value.isEmpty())) {
				stats.column_stats.get(col_clue.col_name.toUpperCase()).filled_count++;
			}
			if (col_clue.col_name.toUpperCase().startsWith("OtherDiagnosis".toUpperCase())
					&& !col_value.isEmpty())
	    	{
				svcLine.addDiagCode(col_value);
	    	}
	    	else if (col_clue.col_name.toUpperCase().startsWith("OtherICD9CMProcedureCode".toUpperCase())
	    			&& !col_value.isEmpty())
	    	{
	    		svcLine.addProcCode(col_value);
	    	}
	    	else
			switch (col_clue.col_name) {
			/*
			case "MemberLinkEID":
				//log.info("Member Id is: " + col_value);
				svcLine.setMember_id(col_value);
				break;
			case "MemberLinkMCL":
				if(svcLine.getMember_id() == null ||  svcLine.getMember_id().isEmpty())
					svcLine.setMember_id(col_value);
				break;
				*/
			case "HashCarrierSpecificUniqueMemberI":
				svcLine.setMember_id(col_value);
				break;
			case "PayerClaimControlNumber":
				svcLine.setClaim_id(col_value);
				break;
			case "LineCounter":
				svcLine.setClaim_line_id(col_value);
				break;
			case "AllowedAmountCleaned":
				if (!col_value.isEmpty())
					try {
						svcLine.setAllowed_amt(Double.parseDouble(col_value));
					}
					catch (NumberFormatException e) {
						log.error("Rejecting claim " + svcLine.getClaim_id() + "|" + svcLine.getClaim_line_id() + " for invalid allowed amount");
						doErrorReporting("svc104", col_value, svcLine);
						bResult = false;
					};
				break;
			/*	
			case "STD_AMT":
				if (!col_value.isEmpty())
					try {
						svcLine.setStandard_payment_amt(Double.parseDouble(col_value));
					}
					catch (NumberFormatException e) {
						log.error("Rejecting claim " + svcLine.getClaim_id() + "|" + svcLine.getClaim_line_id() + " for invalid standard amount");
						doErrorReporting("svc105", col_value, svcLine);
						bResult = false;
					};
				break;
			*/
			case "ServiceProviderNumber_Linkage_ID":
				svcLine.setProvider_id(col_value);
				break;
			case "NationalServiceProviderIDCleaned":
				svcLine.setProvider_NPI(col_value);
				setAttributionProvider(col_value);
				break;
			// derive both Facility Code and File Type from TypeOfBill
			case "TypeofBillOnFacilityClaims":
				svcLine.setType_of_bill(col_value);
				if (col_value != null && col_value.length() > 1) {
					svcLine.setFacility_type_code(BillTypeManager.getFacilityCode(col_value.substring(0,2)));
					svcLine.setClaim_line_type_code(BillTypeManager.getFileType(col_value.substring(0,2)));
					if (svcLine.getClaim_line_type_code() == null  ||  svcLine.getClaim_line_type_code().isEmpty() )
						svcLine.setClaim_line_type_code("PB");
				}
				else
					svcLine.setClaim_line_type_code("PB");
				break;
			case "DateofServiceFrom":
				try {
					svcLine.setBegin_date(DateUtil.doParse(col_clue.col_name, col_value));
				} catch (ParseException e) {
					log.error("Rejecting claim " + svcLine.getClaim_id() + "|" + svcLine.getClaim_line_id() + " for invalid begin date: " + col_value);
					doErrorReporting("svc106", col_value, svcLine);
					bResult = false;
				}
				break;
			case "DateofServiceTo":
				try {
					svcLine.setEnd_date(DateUtil.doParse(col_clue.col_name, col_value));
				} catch (ParseException e) {
					log.error("Rejecting claim " + svcLine.getClaim_id() + "|" + svcLine.getClaim_line_id() + " for invalid end date: " + col_value);
					doErrorReporting("svc107", col_value, svcLine);
					bResult = false;
				}
				break;
			case "SiteofServiceOnNSFCMS1500ClaimsCleaned":
				svcLine.setPlace_of_svc_code(col_value);
				break;
			case "PrincipalDiagnosisCleaned":
				svcLine.setPrincipal_diag_code(col_value);
				break;
			case "ProcedureCodeCleaned":
				if (!col_value.isEmpty()) {
					svcLine.addPrinProcCode(col_value);
				}
				break;
			case "ICD9CMProcedureCodeCleaned":
				if (!col_value.isEmpty()) {
					svcLine.addPrinProcCode(col_value);
				}
				break;
			case "AdmissionSource":
				svcLine.setAdmission_src_code(col_value);
				break;
			case "AdmissionDate":
				try {
					svcLine.setAdmission_date(DateUtil.doParse(col_clue.col_name, col_value));
				} catch (ParseException e) {
					log.error("Rejecting claim " + svcLine.getClaim_id() + "|" + svcLine.getClaim_line_id() + " for invalid admission date: " + col_value);
					doErrorReporting("svc113", col_value, svcLine);
					bResult = false;
				}
				break;
			case "DischargeStatus":
				svcLine.setDischarge_status_code(col_value);
				break;
			case "DischargeDate":
				try {
					svcLine.setDischarge_date(DateUtil.doParse(col_clue.col_name, col_value));
				} catch (ParseException e) {
					log.error("Rejecting claim " + svcLine.getClaim_id() + "|" + svcLine.getClaim_line_id() + " for invalid discharge date: " + col_value);
					doErrorReporting("svc114", col_value, svcLine);
					bResult = false;
				}
				break;
			case "AdmissionType":
				if (!col_value.isEmpty())
					try {
						svcLine.setAdmit_type_code(Integer.parseInt(col_value));
					}
					catch (NumberFormatException e) {
						log.error("Rejecting claim " + svcLine.getClaim_id() + "|" + svcLine.getClaim_line_id() + " for invalid admit type code");
						doErrorReporting("svc108", col_value, svcLine);
						bResult = false;
					};
				break;
			case "RevenueCodeCleaned":
				svcLine.addRevCode(col_value);
				break;
			case "DRG":
				svcLine.setMs_drg_code(col_value);
				break;
			case "APC":
				svcLine.setApr_drg_code(col_value);
				break;
			case "ECodeCleaned":
				if (!col_value.isEmpty())
					svcLine.addECode(col_value);
			default:
				break;	
			}
		}

		return bResult;
	}
	
	
	Provider pv;

	private void setAttributionProvider(String prov_id) {
		pv = providers.get(prov_id);
		if (pv == null)
			log.error("Provider " + prov_id + " used on claim " + svcLine.getClaim_id() + " but not found in provider file");
		else
			if (pv.getProvider_attribution_code().equals("2"))
				svcLine.setFacility_id(prov_id);
			else
				svcLine.setPhysician_id(prov_id);
	}

	/**
	 * check for a header
	 * return true indicates first line is data (not a header)
	 * @param line
	 * @return
	 */
	protected boolean checkForHeader (String line) {
		boolean bReturn = true;
		stats.fileClass = "Claim Service Lines";
		//StringTokenizer in = new StringTokenizer(line,"|,");  
		in = line.split(sDelimiter);
		int i=0;
		for (String lineRead : in)  
		{  
			// set up statistics (imperfect, because I'm stupid enough to try to code for no-header possibility)
			InputStatistics.StatEntry e = stats.column_stats.get(lineRead.toUpperCase());
			if (e == null)
				e =	stats.new StatEntry();
			e.bExpected = false;
	    	e.col_name = lineRead;
	    	// fix common errors
			lineRead = forgiveness(lineRead);
			// look for iterative fields first
			if (lineRead.toUpperCase().startsWith("OtherDiagnosis".toUpperCase()))
	    	{
	    		col_index.put(i, new ColumnFinder(i, lineRead));
	    		bReturn = false;
	    		e.bExpected = true;
	    	}
	    	else if (lineRead.toUpperCase().startsWith("OtherICD9CMProcedureCode".toUpperCase()))
	    	{
	    		col_index.put(i, new ColumnFinder(i, lineRead));
	    		bReturn = false;
	    		e.bExpected = true;
	    	}
	    	else if (lineRead.toUpperCase().startsWith("ProcedureModifier".toUpperCase()))
	    	{
	    		col_index.put(i, new ColumnFinder(i, lineRead));
	    		bReturn = false;
	    		e.bExpected = true;
	    	}
	    	else
	    	// look for everything else
		    for (String s : cols_to_find) {
		    	if (lineRead.equalsIgnoreCase(s))
		    	{
		    		col_index.put(i, new ColumnFinder(i, s));
		    		bReturn = false;
		    		e.bExpected = true;
		    		break;
		    	}
		    }
			if (!bReturn) {						// trying to be sure it's really a header
		    	stats.column_stats.put(lineRead.toUpperCase(), e);
		    }
		    i++;
		}  
		if (bReturn) {
			int x=0;
			for (String s : cols_to_find) {
				col_index.put(x, new ColumnFinder(x, s));
				x++;
			}
		}
		return bReturn;
	}


	// trying to create a default order in absence of a header
	// probably a waste of time given the recurring columns
	String [] cols_to_find = {
		"HashCarrierSpecificUniqueMemberI",
		"NationalServiceProviderIDCleaned",
		"MemberLinkEID",
		"MemberLinkMCL",
		"PayerClaimControlNumber",
		"LineCounter",
		"VersionNumber",
		"AllowedAmountCleaned",
		"TypeofBillOnFacilityClaims",
		"ServiceProviderNumber_Linkage_ID",
		"TypeofBillOnFacilityClaims",
		"DateofServiceFrom",
		"DateofServiceTo",
		"SiteofServiceOnNSFCMS1500ClaimsCleaned",	// PLACE_OF_SRV_CODE
		"FILE_TYPE",
		"PrincipalDiagnosisCleaned",
		"ProcedureCodeCleaned",
		"AdmissionSource",
		"AdmissionType",
		"AdmissionDate",
		"DischargeStatus",
		"DischargeDate",
		"MemberGenderCleaned",
		"MemberDateofBirthYearCleaned",
		"Standardized_MemberZIPCode",
		"Standardized_ServiceProviderZIPCode",
		"NationalBillingProviderIDCleaned",
		"ClaimStatus",
		"AdmittingDiagnosisCleaned",
		"ECodeCleaned",
		"RevenueCodeCleaned",
		"ICD9CMProcedureCodeCleaned",
		"Quantity",
		"CapitatedEncounterFlag",
		"DRG",
		"DRGVersion",
		"APC",
		"APCVersion"
	};
	
	/**
	 * just some bad column headers we've experienced
	 * @param in
	 * @return
	 */
	private String forgiveness (String in) {
		String s = in;
		if (in.equalsIgnoreCase("PLACE_OF_SRV_CODE"))
			s = "PLACE_OF_SVC_CODE";
		return s;
	}
	
	
	
	
	
	@Override
	public ClaimInputCounts getCounts() {
		return counts;
	}
	
	@Override
	public List<String> getErrorMsgs() {
		return errors;
	}


	@Override
	public InputStatistics getInputStatistics() {
		return stats;
	}
	
	
	
	/**
	 * a main strictly for testing
	 * @param args
	 */
	public static void main(String[] args) {
		bDebug = true;
		ClaimDataFromAPCD instance = new ClaimDataFromAPCD(new AllDataAPCD(new HashMap<String, String>()));
		try {
			//instance.addSourceFile(PROF_FILE);
			instance.addSourceFile(STAY_FILE);
			instance.prepareAllServiceLines ();
		} catch (Throwable e) {
			{log.info(e);}	// this main is just for testing - I don't care
		}
		
		// Printing the claim service line list populated. 
		if (bDebug) {
			GenericOutputInterface goif = new GenericOutputCSV();
			for (ClaimServLine clm : instance.svcLines) { 
				log.info(clm);
				goif.write(clm);
			}
			goif.close();
		}
		
		log.info("Found " + instance.getCounts().getRecordsRead() + " claim input records");
		log.info("Accepted " + instance.getCounts().getRecordsAccepted() + " claim input records");
		log.info("Rejected " + instance.getCounts().getRecordsRejected() + " claim input records");
		
	}
	
	//public static final String PROF_FILE = "V:\\72_HCI3_ECR Data\\72_HCL3_ECR_Provider_3";
	public static final String STAY_FILE = "C:\\input\\72_HCL3_ECR_MedicalClaim_2_BCBSMembers.txt";
	//public static final String STAY_FILE = "C:\\input\\72_HCL3_ECR_MedicalClaim_2_Check.txt";
	public static final String PROVIDER_FILE = "C:\\input\\72_HCL3_ECR_Provider_3_BCBSMembers.txt";
	
	private static boolean bDebug = false;
	
	private static org.apache.log4j.Logger log = Logger.getLogger(ClaimDataFromAPCD.class);

}
