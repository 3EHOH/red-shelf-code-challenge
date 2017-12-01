




public class ClaimDataFromHCI3_H extends ClaimAbstract implements ClaimInterface {
	
	
	public String sDelimiter = "\\||;";
	

	/**
	 * Constructor that will provide error message management functionality
	 * @param errMgr
	 */
	public ClaimDataFromHCI3_H(ErrorManager errMgr) {
		this.errMgr = errMgr;
		super.sDelimiter = this.sDelimiter;
	}
	
	/**
	 * Basic constructor without error message management
	 */
	public ClaimDataFromHCI3_H () {
		this.errMgr = null;
		super.sDelimiter = this.sDelimiter;
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
			if (col_clue.col_name.toUpperCase().startsWith("SecondaryDiagnosis".toUpperCase())
					&& !col_value.isEmpty())
	    	{
				svcLine.addDiagCode(col_value);
	    	}
	    	else if (col_clue.col_name.toUpperCase().startsWith("ICDSecondaryProcedureCode".toUpperCase())
	    			&& !col_value.isEmpty())
	    	{
	    		svcLine.addProcCode(col_value);
	    	}
	    	else if (col_clue.col_name.toUpperCase().startsWith("ProcedureModifier".toUpperCase())
	    			&& !col_value.isEmpty())
	    	{
	    		svcLine.addOther_proc_mod_code(col_value);
	    	}
	    	else
			switch (col_clue.col_name) {
			case "UniqueMemberID":
				//log.info("Member Id is: " + col_value);
				svcLine.setMember_id(col_value);
				break;
			case "ClaimNumber":
				svcLine.setClaim_id(col_value);
				break;
			case "LineCounter":
				svcLine.setClaim_line_id(col_value);
				break;
			case "VersionNumber":
				svcLine.setSequence_key(col_value);
				break;
			case "InsuranceProduct":
				svcLine.setInsurance_product(col_value);
				break;
			case "Quantity":
				if (!col_value.isEmpty())
					try {
						svcLine.setQuantity(Integer.parseInt(col_value));
					}
				catch (NumberFormatException e) {
					//log.error("Rejecting claim " + svcLine.getClaim_id() + "|" + svcLine.getClaim_line_id() + " for invalid quantity");
					doErrorReporting("svc115", col_value, svcLine);
					bResult = false;
				}
			break;
			case "AllowedAmount":
				if (!col_value.isEmpty())
					try {
						svcLine.setAllowed_amt(Double.parseDouble(col_value.replace("$", "")));
					}
					catch (NumberFormatException e) {
						//log.error("Rejecting claim " + svcLine.getClaim_id() + "|" + svcLine.getClaim_line_id() + " for invalid allowed amount");
						doErrorReporting("svc104", col_value, svcLine);
						bResult = false;
					};
				break;	
			case "ChargeAmount":
				if (!col_value.isEmpty())
					try {
						svcLine.setCharge_amt(Double.parseDouble(col_value.replace("$", "")));
						if (svcLine.getAllowed_amt() == 0)
							svcLine.setAllowed_amt(svcLine.getCharge_amt());
					}
					catch (NumberFormatException e) {
						//log.error("Rejecting claim " + svcLine.getClaim_id() + "|" + svcLine.getClaim_line_id() + " for invalid charge amount");
						doErrorReporting("svc116", col_value, svcLine);
						bResult = false;
					};
				break;
			case "PaidAmount":
				if (!col_value.isEmpty())
					try {
						svcLine.setPaid_amt(Double.parseDouble(col_value.replace("$", "")));
					}
					catch (NumberFormatException e) {
						//log.error("Rejecting claim " + svcLine.getClaim_id() + "|" + svcLine.getClaim_line_id() + " for invalid paid amount");
						doErrorReporting("svc117", col_value, svcLine);
						bResult = false;
					};
				break;
			case "PrepaidAmount":
				if (!col_value.isEmpty())
					try {
						svcLine.setPrepaid_amt(Double.parseDouble(col_value.replace("$", "")));
					}
					catch (NumberFormatException e) {
						//log.error("Rejecting claim " + svcLine.getClaim_id() + "|" + svcLine.getClaim_line_id() + " for invalid pre-paid amount");
						doErrorReporting("svc121", col_value, svcLine);
						bResult = false;
					};
				break;
			case "CoPayAmount":
				if (!col_value.isEmpty())
					try {
						svcLine.setCopay_amt(Double.parseDouble(col_value.replace("$", "")));
					}
					catch (NumberFormatException e) {
						//log.error("Rejecting claim " + svcLine.getClaim_id() + "|" + svcLine.getClaim_line_id() + " for invalid copay amount");
						doErrorReporting("svc118", col_value, svcLine);
						bResult = false;
					};
				break;
			case "CoinsuranceAmount":
				if (!col_value.isEmpty())
					try {
						svcLine.setCoinsurance_amt(Double.parseDouble(col_value.replace("$", "")));
					}
					catch (NumberFormatException e) {
						//log.error("Rejecting claim " + svcLine.getClaim_id() + "|" + svcLine.getClaim_line_id() + " for invalid coinsurance amount");
						doErrorReporting("svc119", col_value, svcLine);
						bResult = false;
					};
				break;
			case "DeductibleAmount":
				if (!col_value.isEmpty())
					try {
						svcLine.setDeductible_amt(Double.parseDouble(col_value.replace("$", "")));
					}
					catch (NumberFormatException e) {
						//log.error("Rejecting claim " + svcLine.getClaim_id() + "|" + svcLine.getClaim_line_id() + " for invalid deductible amount");
						doErrorReporting("svc120", col_value, svcLine);
						bResult = false;
					};
				break;
			case "PlanServiceProviderNumber":
				svcLine.setProvider_id(col_value);
				break;
			case "NationalServiceProviderID":
				svcLine.setProvider_NPI(col_value);
				break;
			// derive both Facility Code and File Type from TypeOfBill
			case "TypeOfBill":
				svcLine.setType_of_bill(col_value);
				if (col_value != null && col_value.length() > 1) {
					svcLine.setFacility_type_code(BillTypeManager.getFacilityCode(col_value.substring(0,2)));
					svcLine.setClaim_line_type_code(BillTypeManager.getFileType(col_value.substring(0,2)));
					if (svcLine.getClaim_line_type_code() == null  ||  svcLine.getClaim_line_type_code().isEmpty() )
						svcLine.setClaim_line_type_code("PB");
				}
				else
					svcLine.setClaim_line_type_code("PB");
				setAttributionProvider(getColumnValue("NationalServiceProviderID"));
				break;
			case "ServiceFromDate":
				try {
					svcLine.setBegin_date(DateUtil.doParse(col_clue.col_name, col_value));
				} catch (ParseException e) {
					//log.error("Rejecting claim " + svcLine.getClaim_id() + "|" + svcLine.getClaim_line_id() + " for invalid begin date: " + col_value);
					doErrorReporting("svc106", col_value, svcLine);
					bResult = false;
				}
				break;
			case "ServiceThruDate":
				try {
					svcLine.setEnd_date(DateUtil.doParse(col_clue.col_name, col_value));
				} catch (ParseException e) {
					//log.error("Rejecting claim " + svcLine.getClaim_id() + "|" + svcLine.getClaim_line_id() + " for invalid end date: " + col_value);
					doErrorReporting("svc107", col_value, svcLine);
					bResult = false;
				}
				break;
			case "PlaceOfService":
				svcLine.setPlace_of_svc_code(col_value);
				break;
			case "FacilityID":
				svcLine.setFacility_id(col_value);
				break;
			case "PrincipalDiagnosis":
				svcLine.setPrincipal_diag_code(col_value);
				break;
			case "ProcedureCode":
				if (!col_value.isEmpty()) {
					svcLine.addPrinProcCode(col_value);
				}
				break;
			case "ICDPrincipalProcedureCode":
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
					//log.error("Rejecting claim " + svcLine.getClaim_id() + "|" + svcLine.getClaim_line_id() + " for invalid admission date: " + col_value);
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
					//log.error("Rejecting claim " + svcLine.getClaim_id() + "|" + svcLine.getClaim_line_id() + " for invalid discharge date: " + col_value);
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
						//log.error("Rejecting claim " + svcLine.getClaim_id() + "|" + svcLine.getClaim_line_id() + " for invalid admit type code");
						doErrorReporting("svc108", col_value, svcLine);
						bResult = false;
					};
				break;
			case "RevenueCode":
				svcLine.addRevCode(col_value);
				break;
			case "DRGCode":
				svcLine.setMs_drg_code(col_value);
				break;
			case "APCCode":
				svcLine.setApr_drg_code(col_value);
				break;
			case "ECode":
				if (!col_value.isEmpty())
					svcLine.addECode(col_value);
				break;
			case "ClaimStatus":
				svcLine.setClaim_status(col_value);
				break;
			case "CapitatedFlag":
				svcLine.setCapitatedFlag( (col_value.equals("1") ? true : false) ); 
				break;
			default:
				break;	
			}
		}

		return bResult;
	}
	
	
	private void setAttributionProvider(String prov_id) {
		if(svcLine.getClaim_line_type_code().equals("PB") )
			svcLine.setPhysician_id(prov_id);
		else
			svcLine.setFacility_id(prov_id);
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
			//lineRead = forgiveness(lineRead);
			// look for iterative fields first
			if (lineRead.toUpperCase().startsWith("SecondaryDiagnosis".toUpperCase()))
	    	{
	    		col_index.put(i, new ColumnFinder(i, lineRead));
	    		bReturn = false;
	    		e.bExpected = true;
	    	}
	    	else if (lineRead.toUpperCase().startsWith("ICDSecondaryProcedureCode".toUpperCase()))
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
			"NationalPlanID",
			"InsuranceProduct",
			"ClaimNumber",
			"LineCounter",
			"VersionNumber",
			"FinalVersionFlag",
			"UniqueMemberID",
			"MemberGender",
			"MemberYearOfBirth",
			"MemberZipCode",
			"AdmissionDate",
			"DischargeDate",
			"AdmissionType",
			"AdmissionSource",
			"DischargeStatus",
			"PlanServiceProviderNumber",
			"NationalServiceProviderID",
			"ServiceProviderEntityTypeQualifier",
			"ServicingProviderName",
			"ServicingProviderZip",
			"PlanBillingProviderNumber",
			"NationalBillingProviderID",
			"BillingProviderEntityTypeQualifier",
			"BillingProviderName ",
			"BillingProviderZip",
			"FacilityID",
			"TypeOfBill",
			"PlaceOfService",
			"ClaimStatus",
			"AdmittingDiagnosis",
			"ICD9ICD10Flag",
			"ECode",
			"PrincipalDiagnosis",
			"RevenueCode",
			"ProcedureCode",
			"ProcedureModifier1",
			"ProcedureModifierX",
			"ICDPrincipalProcedureCode",
			"ServiceFromDate",
			"ServiceThruDate",
			"Quantity",
			"ChargeAmount",
			"PaidAmount",
			"PrepaidAmount",
			"CoPayAmount",
			"CoinsuranceAmount",
			"DeductibleAmount",
			"AllowedAmount",
			"CapitatedFlag",
			"DRGCode",
			"DRGVersionNumber",
			"APCCode",
			"APCVersionNumber"
	};
	
	/**
	 * just some bad column headers we've experienced
	 * @param in
	 * @return
	 */
	/*
	private String forgiveness (String in) {
		String s = in;
		if (in.equalsIgnoreCase("PLACE_OF_SRV_CODE"))
			s = "PLACE_OF_SVC_CODE";
		return s;
	}
	*/
	
	
	
	
	
	//private static org.apache.log4j.Logger log = Logger.getLogger(ClaimDataFromHCI3_H.class);



}
