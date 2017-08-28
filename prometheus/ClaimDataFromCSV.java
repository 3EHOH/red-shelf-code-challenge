



public class ClaimDataFromCSV extends ClaimDataAbstract implements ClaimDataInterface {
	
	
	

	/**
	 * Constructor that will provide error message management functionality
	 * @param errMgr
	 */
	public ClaimDataFromCSV(ErrorManager errMgr) {
		this.errMgr = errMgr;
		if(RunParameters.parameters.containsKey("DateFormat")  &&
				RunParameters.parameters.get("DateFormat").equalsIgnoreCase("SAS9"))
			bSasDates = true;
	}
	
	/**
	 * Basic constructor without error message management
	 */
	public ClaimDataFromCSV () {
		this.errMgr = null;
		if(RunParameters.parameters.containsKey("DateFormat")  &&
				RunParameters.parameters.get("DateFormat").equalsIgnoreCase("SAS9"))
			bSasDates = true;
	}

	boolean bSasDates = false;
	
	
	protected boolean doMap(int col_ix, String col_value) {
		
		boolean bResult = true;
		
		
		
		ColumnFinder col_clue = col_index.get(col_ix);
		if (col_clue == null)
			{}
		else {
			if (col_value != null  && !(col_value.isEmpty())) {
				//log.info("stats: " + stats);
				//log.info("col_clue: " + col_clue);
				//log.info("stats.column_stats: " + stats.column_stats);
				//log.info("col_clue.col_name: " + col_clue.col_name);
				stats.column_stats.get(col_clue.col_name.toUpperCase()).filled_count++;
			}
			if (col_clue.col_name.toUpperCase().startsWith("SECONDARY_DIAG")
					&& !col_value.isEmpty())
	    	{
				svcLine.addDiagCode(col_value);
	    	}
	    	else if (col_clue.col_name.toUpperCase().startsWith("PROC_ICD9_")
	    			&& !col_value.isEmpty())
	    	{
	    		svcLine.addProcCode(col_value);
	    	}
	    	else if (col_clue.col_name.toUpperCase().startsWith("REV_CPT")
	    			&& !col_value.isEmpty())
	    	{
	    		svcLine.addRevCode(col_value);
	    	}
	    	else if (col_clue.col_name.toUpperCase().startsWith("CPTCODE")  
	    			//&& ! (col_clue.col_name.toUpperCase().equals("CPTCODE1") ) // all are secondary
	    			&& ! col_value.isEmpty())
	    	{
	    		svcLine.addProcCode(col_value);
	    	}
	    	else if (col_clue.col_name.toUpperCase().startsWith("CPTCODE_MOD")  
	    			//&& ! (col_clue.col_name.toUpperCase().equals("CPTCODE_MOD1") ) // all are secondary
	    			&& !col_value.isEmpty())
	    	{
	    		svcLine.addOther_proc_mod_code(col_value);
	    	}
	    	else
			switch (col_clue.col_name) {
			case "CONSISTENT_MEMBER_ID":
				//log.info("Member Id is: " + col_value);
				svcLine.setMember_id(col_value);
				break;
			case "CLAIM_ID":
				svcLine.setClaim_id(col_value);
				break;
			case "LINE_ID":
				svcLine.setClaim_line_id(col_value);
				break;
			case "ALLOWED_AMT":
				if (!col_value.isEmpty())
					try {
						//log.info("Check" + col_value);
						svcLine.setAllowed_amt(Double.parseDouble(col_value.replace("$", "")));
						//log.info(col_value.replace("$", ""));
					}
					catch (NumberFormatException e) {
						//log.error("Rejecting claim " + svcLine.getClaim_id() + "|" + svcLine.getClaim_line_id() + " for invalid allowed amount");
						doErrorReporting("svc104", col_value, svcLine);
						bResult = false;
					};
				break;
			case "STD_AMT":
				if (!col_value.isEmpty())
					try {
						svcLine.setStandard_payment_amt(Double.parseDouble(col_value.replace("$", "")));
					}
					catch (NumberFormatException e) {
						//log.error("Rejecting claim " + svcLine.getClaim_id() + "|" + svcLine.getClaim_line_id() + " for invalid standard amount");
						doErrorReporting("svc105", col_value, svcLine);
						bResult = false;
					};
				break;
			case "PROVIDER_ID":
				svcLine.setProvider_id(col_value);
				break;
			case "FACILITY_TYPE":
				svcLine.setFacility_type_code(col_value);
				break;
			case "FROM_DATE_s":
				if(col_value == null)
					break;
				if (bSasDates)
					svcLine.setBegin_date(DateUtil.getDateFromSAS9(col_value));
				else
					try {
						if (sdf_1 == null) {
							sdf_1 = new SimpleDateFormat(DateUtil.determineDateFormat(col_value));
						}
						svcLine.setBegin_date(sdf_1.parse(col_value));
					} catch (ParseException e) {
						//log.error("Rejecting claim " + svcLine.getClaim_id() + "|" + svcLine.getClaim_line_id() + " for invalid begin date: " + col_value);
						doErrorReporting("svc106", col_value, svcLine);
						bResult = false;
					}
				break;
			case "THRU_DATE_s":
				if(col_value == null)
					break;
				if (bSasDates)
					svcLine.setEnd_date(DateUtil.getDateFromSAS9(col_value));
				else
					try {
						if (sdf_2 == null) {
							sdf_2 = new SimpleDateFormat(DateUtil.determineDateFormat(col_value));
						}
						svcLine.setEnd_date(sdf_2.parse(col_value));
					} catch (ParseException e) {
						//log.error("Rejecting claim " + svcLine.getClaim_id() + "|" + svcLine.getClaim_line_id() + " for invalid end date: " + col_value);
						doErrorReporting("svc107", col_value, svcLine);
						bResult = false;
					}
				break;
			case "PLACE_OF_SVC_CODE":
				svcLine.setPlace_of_svc_code(col_value);
				break;
			case "FILE_TYPE":
				if (col_value.equals("PRO"))
					svcLine.setClaim_line_type_code("PB");
				else
					svcLine.setClaim_line_type_code(col_value);
				break;
			case "HCPCS_PROC_CODE":
				if (!col_value.isEmpty()) {
					if (this.getColumnValue("FILE_TYPE").equalsIgnoreCase("IP"))
						svcLine.addProcCode(col_value);
					else
						svcLine.addPrinProcCode(col_value);
				}
				break;
			case "HCPCS_CPT_MOD":
				svcLine.setPrincipal_proc_mod_code(col_value);
				break;
				/*
				 * Jenna says these are all secondary
			case "CPTCODE_MOD1":
				svcLine.setPrincipal_proc_mod_code(col_value);
				break;
			case "CPTCODE1":
				svcLine.addPrinProcCode(col_value);
				break;
				*/
			case "PRINCIPAL_DIAG_CODE":
				svcLine.setPrincipal_diag_code(col_value);
				break;
			case "PRINCIPAL_PROC_CODE":
				if (!col_value.isEmpty()) {
					svcLine.addPrinProcCode(col_value);
				}
				break;
			case "SRC_ADMS":
				svcLine.setAdmission_src_code(col_value);
				break;
			case "DISCHARGE_STATUS":
				if (col_value != null  && col_value.length() == 1)
					svcLine.setDischarge_status_code("0" + col_value);
				else
					svcLine.setDischarge_status_code(col_value);
				break;
			case "ADMIT_TYPE_CODE":
				try {
					if (!col_value.trim().isEmpty())
						svcLine.setAdmit_type_code(Integer.parseInt(col_value));
				}
				catch (NumberFormatException e) {
					//log.error("Rejecting claim " + svcLine.getClaim_id() + "|" + svcLine.getClaim_line_id() + " for invalid admit type code");
					doErrorReporting("svc108", col_value, svcLine);
					bResult = false;
				};
				break;
			case "MS_DRG_CD":
				svcLine.setMs_drg_code(col_value);
				break;
			case "APR_DRG_CD":
				svcLine.setApr_drg_code(col_value);
				break;
			default:
				break;	
			}
		}

		return bResult;
	}
	

	protected void setAttributionProvider() {
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
		//StringTokenizer in = new StringTokenizer(line,"|,");  
		String[] in = line.split(sDelimiter);
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
			if (lineRead.toUpperCase().startsWith("SECONDARY_DIAG"))
	    	{
	    		col_index.put(i, new ColumnFinder(i, lineRead));
	    		bReturn = false;
	    		e.bExpected = true;
	    	}
	    	else if (lineRead.toUpperCase().startsWith("PROC_ICD9_"))
	    	{
	    		col_index.put(i, new ColumnFinder(i, lineRead));
	    		bReturn = false;
	    		e.bExpected = true;
	    	}
	    	else if (lineRead.toUpperCase().startsWith("REV_CPT"))
	    	{
	    		col_index.put(i, new ColumnFinder(i, lineRead));
	    		bReturn = false;
	    		e.bExpected = true;
	    	}
	    	else if (lineRead.toUpperCase().startsWith("CPTCODE")  
	    			// && ! (lineRead.toUpperCase().equals("CPTCODE1") ) 
	    			)
	    	{
	    		col_index.put(i, new ColumnFinder(i, lineRead));
	    		bReturn = false;
	    		e.bExpected = true;
	    	}
	    	else if (lineRead.toUpperCase().startsWith("CPTCODE_MOD")  
	    			// && ! (lineRead.toUpperCase().equals("CPTCODE_MOD1") ) 
	    			)
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
				//log.info("stats: " + stats);
				//log.info("lineRead.toUpperCase(): " + lineRead.toUpperCase());
				//log.info("stats.column_stats: " + stats.column_stats);
				//log.info("e: " + e);
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
		"CONSISTENT_MEMBER_ID",
		"CLAIM_ID",
		"LINE_ID",
		"ALLOWED_AMT",
		"STD_AMT",
		"PROVIDER_ID",
		"FACILITY_TYPE",
		"FROM_DATE_s",
		"THRU_DATE_s",
		"PLACE_OF_SVC_CODE",	// PLACE_OF_SRV_CODE
		"FILE_TYPE",
		"HCPCS_PROC_CODE",
		"HCPCS_CPT_MOD",
		//"CPTCODE1",
		//"CPTCODE_MOD1",
		"PRINCIPAL_DIAG_CODE",
		"SECONDARY_DIAG1_CODE",
		"PRINCIPAL_PROC_CODE",
		"PROC_ICD9_2ND1",
		"SRC_ADMS",
		"DISCHARGE_STATUS",
		"ADMIT_TYPE_CODE",
		"MS_DRG_CD",
		"APR_DRG_CD"
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
		ClaimDataFromCSV instance = new ClaimDataFromCSV();
		try {
			instance.addSourceFile(STAY_FILE);
			//instance.addSourceFile(PROF_FILE);
			//instance.bSasDates = true;
			instance.prepareAllServiceLines ();
		} catch (Throwable e) {
			{}	// this main is just for testing - I don't care
		}
		
		// Printing the claim service line list populated. 
		if (bDebug) {
			GenericOutputInterface goif = new GenericOutputCSV();
			for (ClaimServLine clm : instance.svcLines) { 
				//log.info(clm);
				goif.write(clm);
			}
			goif.close();
		}
		
		log.info("Found " + instance.getCounts().getRecordsRead() + " claim input records");
		log.info("Accepted " + instance.getCounts().getRecordsAccepted() + " claim input records");
		log.info("Rejected " + instance.getCounts().getRecordsRejected() + " claim input records");
		
	}
	
	//public static final String PROF_FILE = "C:\\input\\UAHN_PROF_UCA.txt";
	public static final String PROF_FILE = "C:\\input\\Superior\\chiprsa\\prof.csv";
	public static final String STAY_FILE = "C:\\input\\Superior\\chiprsa\\stay.csv";
	
	private static boolean bDebug = false;
	
	SimpleDateFormat sdf_1; //  = new SimpleDateFormat("yyyy-MM-dd");
	SimpleDateFormat sdf_2; //  = new SimpleDateFormat("yyyy-MM-dd");
	
	private static org.apache.log4j.Logger log = Logger.getLogger(ClaimDataFromCSV.class);
	
	class col_finder {
		String col_name;
		int col_number;
		col_finder (int i, String s) {
			col_number = i;
			col_name = s;
		}
	}
	
	class slCompare implements Comparator<ClaimServLine> {

	    @Override
	    public int compare(ClaimServLine o1, ClaimServLine o2) {
	    	int c;
	        c = o1.getMember_id().compareTo(o2.getMember_id());
	        if (c == 0)
	        	c = o1.getBegin_date().compareTo(o2.getBegin_date());
	        //if (c == 0)
	        //	c = o1.getMember_id().compareTo(o2.getMember_id());
	        return c;
	    }
	}

	
	protected void doErrorReporting (String id, String errValue, ClaimServLine clm) {
		if(errMgr != null)
			errMgr.issueError(id, errValue, clm);
	}
	
}
