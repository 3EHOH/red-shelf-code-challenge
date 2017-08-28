





public class ClaimDataFromWellpoint extends ClaimDataAbstract implements ClaimDataInterface {
	
	
	public String sDelimiter = "\\||;";
	
	/**
	 * Constructor that will provide error message management functionality
	 * @param errMgr
	 */
	public ClaimDataFromWellpoint(ErrorManager errMgr) {
		this.errMgr = errMgr;
		super.sDelimiter = this.sDelimiter;
	}
	
	/**
	 * Basic constructor without error message management
	 */
	public ClaimDataFromWellpoint () {
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
			if (col_clue.col_name.toUpperCase().startsWith("SECDX".toUpperCase())  && 
					!col_clue.col_name.toUpperCase().endsWith("01") 		// 01 is principal
					&& !col_value.isEmpty())
	    	{
				svcLine.addDiagCode(col_value);
	    	}
	    	else if (col_clue.col_name.toUpperCase().startsWith("ICDPROC".toUpperCase()) && 
					!col_clue.col_name.toUpperCase().endsWith("1") 		// 01 is principal
	    			&& !col_value.isEmpty())
	    	{
	    		svcLine.addProcCode(col_value);
	    	}
	    	else
			switch (col_clue.col_name.toUpperCase()) {
			case "MCIDH":
				//log.info("Member Id is: " + col_value);
				svcLine.setMember_id(col_value);
				break;
			case "CLMNBRH":
				svcLine.setClaim_id(svcLine.getMember_id() + col_value);
				break;
			case "LINENBR":
				svcLine.setClaim_line_id(col_value);
				break;
				/*
			case "XG_HDICLAIMSEQKEY":
				svcLine.setSequence_key(col_value);
				break;
				*/
			case "ALOWAMT":
				if (!col_value.isEmpty())
					try {
						svcLine.setAllowed_amt(Double.parseDouble(col_value));
					}
					catch (NumberFormatException e) {
						//log.error("Rejecting claim " + svcLine.getClaim_id() + "|" + svcLine.getClaim_line_id() + " for invalid allowed amount");
						doErrorReporting("svc104", col_value, svcLine);
						bResult = false;
					};
				break;
			case "BILLAMT":
				if (!col_value.isEmpty())
					try {
						svcLine.setCharge_amt(Double.parseDouble(col_value));
					}
					catch (NumberFormatException e) {
						//log.error("Rejecting claim " + svcLine.getClaim_id() + "|" + svcLine.getClaim_line_id() + " for invalid charge amount");
						doErrorReporting("svc116", col_value, svcLine);
						bResult = false;
					};
				break;
			case "PAIDAMT":
				if (!col_value.isEmpty())
					try {
						svcLine.setPaid_amt(Double.parseDouble(col_value));
					}
					catch (NumberFormatException e) {
						//log.error("Rejecting claim " + svcLine.getClaim_id() + "|" + svcLine.getClaim_line_id() + " for invalid paid amount");
						doErrorReporting("svc117", col_value, svcLine);
						bResult = false;
					};
				break;
			case "COPAYAMT":
				if (!col_value.isEmpty())
					try {
						svcLine.setCopay_amt(Double.parseDouble(col_value));
					}
					catch (NumberFormatException e) {
						//log.error("Rejecting claim " + svcLine.getClaim_id() + "|" + svcLine.getClaim_line_id() + " for invalid copay amount");
						doErrorReporting("svc118", col_value, svcLine);
						bResult = false;
					};
				break;
			case "COINAMT":
				if (!col_value.isEmpty())
					try {
						svcLine.setCoinsurance_amt(Double.parseDouble(col_value));
					}
					catch (NumberFormatException e) {
						//log.error("Rejecting claim " + svcLine.getClaim_id() + "|" + svcLine.getClaim_line_id() + " for invalid coinsurance amount");
						doErrorReporting("svc119", col_value, svcLine);
						bResult = false;
					};
				break;
			case "DEDUCT":
				if (!col_value.isEmpty())
					try {
						svcLine.setDeductible_amt(Double.parseDouble(col_value));
					}
					catch (NumberFormatException e) {
						//log.error("Rejecting claim " + svcLine.getClaim_id() + "|" + svcLine.getClaim_line_id() + " for invalid deductible amount");
						doErrorReporting("svc120", col_value, svcLine);
						bResult = false;
					};
				break;
			case "PROVIDH":
				svcLine.setProvider_id(col_value);
				break;
			case "NPI":
				svcLine.setProvider_NPI(col_value);
				break;
			// derive both Facility Code and File Type from TypeOfBill
			case "BILLTYPEH":
				svcLine.setType_of_bill(col_value);
				if (col_value != null && col_value.length() > 3) {
					svcLine.setFacility_type_code(BillTypeManager.getFacilityCode(col_value.substring(1,3)));
					svcLine.setClaim_line_type_code(BillTypeManager.getFileType(col_value.substring(1,3)));
					if (svcLine.getClaim_line_type_code() == null  ||  svcLine.getClaim_line_type_code().isEmpty() )
						svcLine.setClaim_line_type_code("PB");
				}
				else
					svcLine.setClaim_line_type_code("PB");
				//setAttributionProvider(svcLine.getProvider_id());
				break;
			case "BILLUNIT":
				if (!col_value.isEmpty())
					try {
						svcLine.setQuantity(Double.valueOf(col_value).intValue());
					}
					catch (NumberFormatException e) {
						//log.error("Rejecting claim " + svcLine.getClaim_id() + "|" + svcLine.getClaim_line_id() + " for invalid quantity");
						doErrorReporting("svc115", col_value, svcLine);
						bResult = false;
					}
				break;
			case "FROMDT":
				try {
					svcLine.setBegin_date(DateUtil.doParse(col_clue.col_name, col_value));
					svcLine.setAdmission_date(DateUtil.doParse(col_clue.col_name, col_value));
				} catch (ParseException e) {
					//log.error("Rejecting claim " + svcLine.getClaim_id() + "|" + svcLine.getClaim_line_id() + " for invalid begin date: " + col_value);
					doErrorReporting("svc106", col_value, svcLine);
					bResult = false;
				}
				break;
			case "ENDDT":
				try {
					svcLine.setEnd_date(DateUtil.doParse(col_clue.col_name, col_value));
					svcLine.setDischarge_date(DateUtil.doParse(col_clue.col_name, col_value));
				} catch (ParseException e) {
					//log.error("Rejecting claim " + svcLine.getClaim_id() + "|" + svcLine.getClaim_line_id() + " for invalid end date: " + col_value);
					doErrorReporting("svc107", col_value, svcLine);
					bResult = false;
				}
				break;
			case "HCFAPOTD":
				svcLine.setPlace_of_svc_code(col_value);
				break;
			case "PRIMDX":
				svcLine.setPrincipal_diag_code(col_value);
				break;
			case "CPTCDD":
				if (!col_value.isEmpty()) {
					if ( ! svcLine.getClaim_line_type_code().equals("IP")) {
						//svcLine.setPrincipal_proc_code(new ArrayList<String>());
						svcLine.addPrinProcCode(col_value);
					}
				}
				break;
			case "CPTMODD":
				svcLine.setPrincipal_proc_mod_code(col_value);
				break;
			case "ICDPROC1":
				if (!col_value.isEmpty()) {
					//svcLine.setPrincipal_proc_code(new ArrayList<String>());
					svcLine.addPrinProcCode(col_value);
				}
				break;
			case "ADMSRCEH":
				svcLine.setAdmission_src_code(col_value);
				break;
			case "ADMTYPEH":
				try {
					svcLine.setAdmit_type_code(Integer.parseInt(col_value));
				}
				catch (NumberFormatException e) {
					//log.error("Rejecting claim " + svcLine.getClaim_id() + "|" + svcLine.getClaim_line_id() + " for invalid admit type code");
					doErrorReporting("svc108", col_value, svcLine);
					bResult = false;
				};
				break;
			case "DSCHGST":
				svcLine.setDischarge_status_code(col_value);
				break;
			case "REVCDD":
				svcLine.addRevCode(col_value);
				break;
			case "FNLDRGH":
				svcLine.setMs_drg_code(col_value);
				break;
			case "DRGTYPE":
				svcLine.setDrg_version(col_value);
				break;
			case "DRGFAC":
				svcLine.setApr_drg_code(col_value);
				break;
			case "PRDCTSL":
				svcLine.setInsurance_product(col_value);
				break;
			case "CLMKEYH":
				svcLineHdrs.put(col_value, svcLine);
			default:
				break;	
			}
		}

		return bResult;
	}
	
	
	boolean bHeader = true;
	//HashMap<Integer, ClaimServLine> svcLineHdrs = new HashMap<Integer, ClaimServLine>();
	
	@Override
	protected void getServiceLine () {
		if (bHeader)
			svcLine = new ClaimServLine();
		else {
			//builder.setLength(0);
		    //builder.append(getColumnValue("MCIDD"));
		    //builder.append(getColumnValue("CLMNBRD"));
		    //cTemp = svcLineHdrs.get(builder.toString().hashCode());
			cTemp = svcLineHdrs.get(getColumnValue("CLMKEYD"));
			svcLine = new ClaimServLine();
			if (cTemp != null) {
				svcLine.setMember_id(cTemp.getMember_id());
				svcLine.setClaim_id(cTemp.getClaim_id());
				svcLine.setProvider_id(cTemp.getProvider_id());
				svcLine.setProvider_NPI(cTemp.getProvider_NPI());
				svcLine.setPhysician_id(cTemp.getPhysician_id());
				svcLine.setFacility_id(cTemp.getFacility_id());
				svcLine.setAdmission_src_code(cTemp.getAdmission_src_code());
				svcLine.setAdmit_type_code(cTemp.getAdmit_type_code());
				svcLine.setDischarge_status_code(cTemp.getDischarge_status_code());
				svcLine.setType_of_bill(cTemp.getType_of_bill());
				//svcLine.setPrincipal_proc_code(cTemp.getPrincipal_proc_code());
				svcLine.setMed_codes(cTemp.getMed_codes());
				svcLine.setDrg_version(cTemp.getDrg_version());
				svcLine.setMs_drg_code(cTemp.getMs_drg_code());
				svcLine.setApr_drg_code(cTemp.getApr_drg_code());
				svcLine.setFacility_type_code(cTemp.getFacility_type_code());
				svcLine.setClaim_line_type_code(cTemp.getClaim_line_type_code());
				//svcLine.setSecondary_proc_code(cTemp.getSecondary_proc_code());
				//svcLine.setSecondary_diag_code(cTemp.getSecondary_diag_code());  // can't do this because the diag codes are repeated on all lines
			}
			else {
				//log.error("Found a claim detail record with no header: " + getColumnValue("MCIDD"));
				svcLine = null;
			}
		}
	}
	StringBuilder builder = new StringBuilder();
	ClaimServLine cTemp;
	
	
	

	/**
	 * check for a header
	 * return true indicates first line is data (not a header)
	 * @param line
	 * @return
	 */
	protected boolean checkForHeader (String line) {
		boolean bReturn = true;
		stats.fileClass = "Claim Service Lines";

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
	    	
			// look for iterative fields first
			if (lineRead.toUpperCase().startsWith("SECDX".toUpperCase()))
	    	{
	    		col_index.put(i, new ColumnFinder(i, lineRead));
	    		bReturn = false;
	    		e.bExpected = true;
	    	}
	    	else if (lineRead.toUpperCase().startsWith("ICDPROC".toUpperCase()))
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
		
		// are we flipping from header to detail (Wellpoint is first to require this)
		if ( !getColumnValue("MCIDD").isEmpty()) {
			bHeader = false;
			//for (ClaimServLine c : svcLines) {
			//	svcLineHdrs.put(c.hashCode(), c);
			//}
			svcLines = new ArrayList<ClaimServLine>();
		}
		
		return bReturn;
	}
	
	


	// trying to create a default order in absence of a header
	// probably a waste of time given the recurring columns
	String [] cols_to_find = {
		"MCIDH",
		"MCIDD",
		"CLMNBRH",
		"CLMNBRD",
		"LINENBR",
		//"XG_HDICLAIMSEQKEY",
		"ALOWAMT",
		"BILLAMT",
		"PAIDAMT",
		"COPAYAMT",
		"COINAMT",
		"DEDUCT",
		"BILLTYPEH",
		"NPI",
		"PROVIDH",
		"FROMDT",
		"ENDDT",
		"HCFAPOTD",	// PLACE_OF_SRV_CODE
		"ADMSRCEH",
		"DSCHGST",
		"REVCDD",
		"PRIMDX",
		"CPTCDD",
		"CPTMODD",
		"BILLUNIT",
		"FNLDRGH",
		"DRGTYPE",
		"DRGFAC",
		"PRDCTSL",
		"LastAdjstmnt",
		"CLMKEYH",
		"CLMKEYD"
	};
	
	
	@Override
	protected void doRollUp() {
		if (bHeader) {
			svcLines.add(svcLine);
		}
		else
			super.doRollUp();
	}
	
	

	@Override
	protected boolean filterPass() {
		boolean bResult = false;
		if (bHeader) {
			if (getColumnValue("LastAdjstmnt").equals("Y")) {
				bResult = true;
				//claimKeys.add(getColumnValue("CLMKEYH"));
			}
		}
		else {
			//bResult = (claimKeys.contains(getColumnValue("CLMKEYD")));
			bResult = (svcLineHdrs.containsKey(getColumnValue("CLMKEYD")));
		}
		
		return bResult;
	}
	//private Set<String>claimKeys = new HashSet<String>();
	private HashMap<String, ClaimServLine> svcLineHdrs = new HashMap<String, ClaimServLine>();
	
	
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
		ClaimDataFromWellpoint instance = new ClaimDataFromWellpoint();
		try {
			instance.addSourceFile(HDR_FILE);
			instance.addSourceFile(DTL_FILE);
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
			
			
			//common.dao.InputObjectOutputSQL s = new common.dao.InputObjectOutputSQL();
			//s.writeMedicalClaims(instance.svcLines);
			
		}
		
		log.info("Found " + instance.getCounts().getRecordsRead() + " claim input records");
		log.info("Accepted " + instance.getCounts().getRecordsAccepted() + " claim input records");
		log.info("Rejected " + instance.getCounts().getRecordsRejected() + " claim input records");
		
	}
	
	public static final String HDR_FILE = "C:\\input\\Wellpoint\\HCI3_CTM_CLM_HDR_FNL_sampleNegative.txt";
	public static final String DTL_FILE = "C:\\input\\Wellpoint\\HCI3_CTM_CLM_DTL_FNL_sampleNegative.txt";
	//public static final String DTL_FILE = "C:\\input\\Wellpoint\\secondary_diag_test.txt";
	
	private static boolean bDebug = false;
	
	private static org.apache.log4j.Logger log = Logger.getLogger(ClaimDataFromWellpoint.class);


}
