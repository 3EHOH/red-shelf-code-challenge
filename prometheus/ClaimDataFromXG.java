



public class ClaimDataFromXG extends ClaimDataAbstract implements ClaimDataInterface {
	
	
	public String sDelimiter = "\\||;";
	
	/**
	 * Constructor that will provide error message management functionality
	 * @param errMgr
	 */
	public ClaimDataFromXG(ErrorManager errMgr) {
		this.errMgr = errMgr;
		super.sDelimiter = this.sDelimiter;
	}
	
	/**
	 * Basic constructor without error message management
	 */
	public ClaimDataFromXG () {
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
			if (col_clue.col_name.toUpperCase().startsWith("XG_DDIAG".toUpperCase())  && 
					!col_clue.col_name.toUpperCase().endsWith("01") 		// 01 is principal
					&& !col_value.isEmpty())
	    	{
				svcLine.addDiagCode(col_value);
	    	}
			else if (col_clue.col_name.toUpperCase().startsWith("XG_DPROCMOD".toUpperCase()) && 
					!col_clue.col_name.toUpperCase().endsWith("1") 		// 1 is principal
	    			&& !col_value.isEmpty())
	    	{
	    		svcLine.addOther_proc_mod_code(col_value);
	    	}
	    	else if (col_clue.col_name.toUpperCase().startsWith("XG_ICDPROC".toUpperCase()) && 
	    			!col_clue.col_name.toUpperCase().endsWith("TYPE")  &&
					!col_clue.col_name.toUpperCase().endsWith("01")  && 		// 01 is principal
	    			!col_value.isEmpty())
	    	{
	    		svcLine.addProcCode(col_value);
	    	}
	    	else
			switch (col_clue.col_name) {
			case "XG_MASTERPERSONINDEX":
				//log.info("Member Id is: " + col_value);
				svcLine.setMember_id(col_value);
				break;
			case "XG_HDICLAIMHDRKEY":
				svcLine.setClaim_id(col_value);
				break;
			case "XG_HDICLAIMLINEKEY":
				svcLine.setClaim_line_id(col_value);
				break;
			case "XG_HDICLAIMSEQKEY":
				svcLine.setSequence_key(col_value);
				break;
			case "XG_HDICLAIMSTATUS":
				svcLine.setOrig_adj_rev(col_value);
				break;
			case "XG_AMTALLOWED":
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
			case "XG_AMTCHARGE":
				if (!col_value.isEmpty())
					try {
						svcLine.setCharge_amt(Double.parseDouble(col_value));
					}
					catch (NumberFormatException e) {
						log.error("Rejecting claim " + svcLine.getClaim_id() + "|" + svcLine.getClaim_line_id() + " for invalid charge amount");
						doErrorReporting("svc116", col_value, svcLine);
						bResult = false;
					};
				break;
			case "XG_AMTPAY":
				if (!col_value.isEmpty())
					try {
						svcLine.setPaid_amt(Double.parseDouble(col_value));
					}
					catch (NumberFormatException e) {
						log.error("Rejecting claim " + svcLine.getClaim_id() + "|" + svcLine.getClaim_line_id() + " for invalid paid amount");
						doErrorReporting("svc117", col_value, svcLine);
						bResult = false;
					};
				break;
			case "XG_AMTCOPAY":
				if (!col_value.isEmpty())
					try {
						svcLine.setCopay_amt(Double.parseDouble(col_value));
					}
					catch (NumberFormatException e) {
						log.error("Rejecting claim " + svcLine.getClaim_id() + "|" + svcLine.getClaim_line_id() + " for invalid copay amount");
						doErrorReporting("svc118", col_value, svcLine);
						bResult = false;
					};
				break;
			case "XG_AMTCOINS":
				if (!col_value.isEmpty())
					try {
						svcLine.setCoinsurance_amt(Double.parseDouble(col_value));
					}
					catch (NumberFormatException e) {
						log.error("Rejecting claim " + svcLine.getClaim_id() + "|" + svcLine.getClaim_line_id() + " for invalid coinsurance amount");
						doErrorReporting("svc119", col_value, svcLine);
						bResult = false;
					};
				break;
			case "XG_AMTDEDUCTIBLE":
				if (!col_value.isEmpty())
					try {
						svcLine.setDeductible_amt(Double.parseDouble(col_value));
					}
					catch (NumberFormatException e) {
						log.error("Rejecting claim " + svcLine.getClaim_id() + "|" + svcLine.getClaim_line_id() + " for invalid deductible amount");
						doErrorReporting("svc120", col_value, svcLine);
						bResult = false;
					};
				break;
				/*
			case "XG_CMSPROVOSCARNUM":
				svcLine.setFacility_id(col_value);
				break;
				*/
			case "XG_SERVPROV":
				svcLine.setProvider_id(col_value);
				//setAttributionProvider(col_value);
				break;
			case "XG_SERVPROVNPI":
				svcLine.setProvider_NPI(col_value);
				break;
			// derive both Facility Code and File Type from TypeOfBill
			case "XG_TYPEOFBILL":
				svcLine.setType_of_bill(col_value);
				if (col_value != null && col_value.length() > 1) {
					svcLine.setFacility_type_code(BillTypeManager.getFacilityCode(col_value.substring(0,2)));
					svcLine.setClaim_line_type_code(BillTypeManager.getFileType(col_value.substring(0,2)));
					if (svcLine.getClaim_line_type_code() == null  ||  svcLine.getClaim_line_type_code().isEmpty() )
						svcLine.setClaim_line_type_code("PB");
				}
				else
					svcLine.setClaim_line_type_code("PB");
				//setAttributionProvider(svcLine.getProvider_id());
				setPrincProc();
				break;
			case "XG_QUANTITY":
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
			case "XG_YMDEFFECTIVE":
				try {
					svcLine.setBegin_date(DateUtil.doParse(col_clue.col_name, col_value));
				} catch (ParseException e) {
					log.error("Rejecting claim " + svcLine.getClaim_id() + "|" + svcLine.getClaim_line_id() + " for invalid begin date: " + col_value);
					doErrorReporting("svc106", col_value, svcLine);
					bResult = false;
				}
				break;
			case "XG_YMDLASTDOS":
				try {
					svcLine.setEnd_date(DateUtil.doParse(col_clue.col_name, col_value));
				} catch (ParseException e) {
					log.error("Rejecting claim " + svcLine.getClaim_id() + "|" + svcLine.getClaim_line_id() + " for invalid end date: " + col_value);
					doErrorReporting("svc107", col_value, svcLine);
					bResult = false;
				}
				break;
			case "XG_YMDADMIT":
				try {
					svcLine.setAdmission_date(DateUtil.doParse(col_clue.col_name, col_value));
				} catch (ParseException e) {
					log.error("Rejecting claim " + svcLine.getClaim_id() + "|" + svcLine.getClaim_line_id() + " for invalid admission date: " + col_value);
					doErrorReporting("svc113", col_value, svcLine);
					bResult = false;
				}
				break;
			case "XG_YMDDISCH":
				try {
					svcLine.setDischarge_date(DateUtil.doParse(col_clue.col_name, col_value));
				} catch (ParseException e) {
					log.error("Rejecting claim " + svcLine.getClaim_id() + "|" + svcLine.getClaim_line_id() + " for invalid discharge date: " + col_value);
					doErrorReporting("svc114", col_value, svcLine);
					bResult = false;
				}
				break;
			case "XG_POS":
				svcLine.setPlace_of_svc_code(col_value);
				break;
			case "XG_DDIAG01":
				svcLine.setPrincipal_diag_code(col_value);
				break;
			case "XG_DPROC":
				if (!col_value.isEmpty()) {
					sCdType = getColumnValue("XG_DPROCTYPE");
					if (sCdType != null && sCdType.equals("C4")  ||  sCdType.equals("HC"))
						sC4_HC = col_value;
				}
				break;
			case "XG_DPROCMOD1":
				svcLine.setPrincipal_proc_mod_code(col_value);
				break;
			case "XG_ICDPROC01":
				if (!col_value.isEmpty()) {
					sICDProc = col_value;
				}
				break;
			case "XG_CMSADMISSIONSOURCE":
				svcLine.setAdmission_src_code(col_value);
				break;
			case "XG_DISCHARGE":
				svcLine.setDischarge_status_code(col_value);
				break;
			case "XG_REVCODE":
				svcLine.addRevCode(col_value);
				break;
			case "XG_DRG":
				svcLine.setMs_drg_code(col_value);
				break;
			case "XG_DRGVERSION":
				svcLine.setDrg_version(col_value);
				break;
			default:
				break;	
			}
		}

		return bResult;
	}
	
	private String sCdType;
	private String sICDProc;
	private String sC4_HC;
	
	@Override
	protected void doPostProcess() {
		super.doPostProcess();
		setPrincProc();
		sCdType = null;
		sICDProc = null;
		sC4_HC = null;
	}	
	
	private void setPrincProc () {
		ArrayList<String> t = new ArrayList<String>();
		if(svcLine.getClaim_line_type_code().equals("IP") ) {
			if (sICDProc != null)
				t.add(sICDProc.replace(".", ""));
		}
		else {
			if (sC4_HC != null)
				t.add(sC4_HC.replace(".", ""));
		}
		//if (!t.isEmpty()) {
			// svcLine.setPrincipal_proc_code(t);
		for (String s : t) {
			svcLine.addMed_codes("PX", s, "PX", 1);
		}
		
	}
	
	
	@Override
	protected boolean filterPass() {
		return getColumnValue("XG_HDIDUPLICATEINDICATOR").equals("00");
	}
	
	
	//String _riskCat;

	protected void setAttributionProvider() {
		/*
		_riskCat = getColumnValue("XG_RISKCAT");
		if (_riskCat.equals("IP") ||  _riskCat.equals("OP"))
			svcLine.setFacility_id(prov_id);
		else
			svcLine.setPhysician_id(prov_id);
			*/
		if(svcLine.getClaim_line_type_code().equals("PB") )
			svcLine.setPhysician_id(svcLine.getProvider_id());
		else
			svcLine.setFacility_id(svcLine.getProvider_id());
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
			if (lineRead.toUpperCase().startsWith("XG_DDIAG".toUpperCase()))
	    	{
	    		col_index.put(i, new ColumnFinder(i, lineRead));
	    		bReturn = false;
	    		e.bExpected = true;
	    	}
			else if (lineRead.toUpperCase().startsWith("XG_DPROCMOD".toUpperCase()))
	    	{
	    		col_index.put(i, new ColumnFinder(i, lineRead));
	    		bReturn = false;
	    		e.bExpected = true;
	    	}
	    	else if (lineRead.toUpperCase().startsWith("XG_ICDPROC".toUpperCase()))
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
		"XG_MASTERPERSONINDEX",
		"XG_HDICLAIMHDRKEY",
		"XG_HDICLAIMLINEKEY",
		"XG_HDICLAIMSEQKEY",
		"XG_HDICLAIMSTATUS",
		"XG_HDIDUPLICATEINDICATOR",
		"XG_AMTALLOWED",
		"XG_AMTCHARGE",
		"XG_AMTPAY",
		"XG_AMTCOPAY",
		"XG_AMTCOINS",
		"XG_AMTDEDUCTIBLE",
		"XG_TYPEOFBILL",
		"XG_RISKCAT",
		"XG_SERVPROVNPI",
		"XG_SERVPROV",
		"XG_CMSPROVOSCARNUM",
		"XG_YMDEFFECTIVE",
		"XG_YMDLASTDOS",
		"XG_POS",	// PLACE_OF_SRV_CODE
		"XG_CMSADMISSIONSOURCE",
		"XG_YMDADMIT",
		"XG_DISCHARGE",
		"XG_YMDDISCH",
		"XG_REVCODE",
		"XG_DPROC",
		"XG_DPROCTYPE",
		"XG_QUANTITY",
		"XG_DRG",
		"XG_DRGVERSION"
	};
	
	
	
	
	/**
	 * XG roll-up - keep only the claim with the highest sequence key (child of line number)
	 */
	protected void doRollUp() {
		idx = _SvcLines.get(hashSL_ADR());
		if ( idx != null ) {
			try {
			cTarget = svcLines.get(idx);
			if (svcLine.getSequence_key().compareTo(cTarget.getSequence_key()) > 0 )
				cTarget = svcLine;
			}
			catch (IndexOutOfBoundsException e) {
				log.error("Hash error: m-" + svcLine.getMember_id() + " c-" + svcLine.getClaim_id() + 
						" l-" + svcLine.getClaim_line_id() + " s-" + svcLine.getOrig_adj_rev());
			}
		}
		else
		{
			_SvcLines.put(hashSL_ADR(), svcLines.size());
			//svcLines.add(svcLine);
			super.doRollUp();
		}
	}
	
	ClaimServLine cTarget;
	Integer idx;
	
	private HashMap <Integer, Integer> _SvcLines = new HashMap<Integer, Integer>();
	
	private int hashSL_ADR() {
	    StringBuilder builder = new StringBuilder();
	    builder.append(svcLine.getMember_id());
	    builder.append(svcLine.getClaim_id());
	    builder.append(svcLine.getClaim_line_id());
	    builder.append(svcLine.getOrig_adj_rev());
	    return builder.toString().hashCode();
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
		ClaimDataFromXG instance = new ClaimDataFromXG();
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
	//public static final String STAY_FILE = "C:\\input\\XG_MED_11192014.txt";
	//public static final String STAY_FILE = "C:\\input\\HDIV3_MARSHFIELD_MARSHFIELD_MED_0001_TEST_20141205143506_First1000.txt";
	//public static final String STAY_FILE = "C:\\input\\HDIV3_MARSHFIELD_MARSHFIELD_MED_0001_TEST_20141205143506_569916.txt";
	public static final String STAY_FILE = "C:\\input\\HDIV3_MARSHFIELD_MARSHFIELD_MED_0001_TEST_20141205143506_FinalVersionTest.txt";
	
	private static boolean bDebug = false;
	
	private static org.apache.log4j.Logger log = Logger.getLogger(ClaimDataFromXG.class);

}
