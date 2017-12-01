




public class ClaimDataFromPebtf extends ClaimDataAbstract implements ClaimDataInterface {
	

	
	/**
	 * Constructor that will provide error message management functionality
	 * @param errMgr
	 */
	public ClaimDataFromPebtf (ErrorManager errMgr) {
		this.errMgr = errMgr;
		super.sDelimiter = this.sDelimiter;
		bPreserveQuotes = true;
	}
	
	/**
	 * Basic constructor without error message management
	 */
	public ClaimDataFromPebtf () {
		this.errMgr = null;
		super.sDelimiter = this.sDelimiter;
		bPreserveQuotes = true;
	}

	
	protected boolean doMap(int col_ix, String col_value) {
		
		boolean bResult = true;
		
		
		
		ColumnFinder col_clue = col_index.get(col_ix);
		if (col_clue == null)
			{}
		else {
			
			col_value = col_value.trim();
			
			if (col_value != null  && !(col_value.isEmpty())) {
				stats.column_stats.get(col_clue.col_name.toUpperCase()).filled_count++;
				//col_value = col_value.trim();
			}
			
			if (col_clue.col_name.toUpperCase().startsWith("DIAGCD".toUpperCase())  && 
					(!col_clue.col_name.toUpperCase().endsWith("1")) 	  &&	// 1 is principal
					(!col_clue.col_name.toUpperCase().equals("DIAGCDID"))  &&
					(!col_value.isEmpty()) )
	    	{
				svcLine.addDiagCode(col_value);
	    	}
			else if (col_clue.col_name.toUpperCase().startsWith("PROCM".toUpperCase()) && 
					!col_clue.col_name.toUpperCase().endsWith("1") 		// 1 is principal
	    			&& !col_value.isEmpty())
	    	{
	    		svcLine.addOther_proc_mod_code(col_value);
	    	}
			else if (col_clue.col_name.toUpperCase().equals("PROCTYID"))
			{}
			else if (col_clue.col_name.toUpperCase().equals("PROCPRM"))
			{}
	    	else if (col_clue.col_name.toUpperCase().startsWith("PROC".toUpperCase()) && 
	    			(!col_clue.col_name.toUpperCase().startsWith("PROCM".toUpperCase()))  &&
	    			(!col_clue.col_name.toUpperCase().endsWith("CD"))  &&
	    			(!col_value.isEmpty()) )
	    	{
	    		svcLine.addProcCode(col_value);
	    	}
	    	else
	    		
			switch (col_clue.col_name) {
			case "PLSTNAME":
				//log.info("Member Id is: " + col_value);
				sFN = getColumnValue("PFSTNAME").trim();
				if (sFN.length() > 4)
					iLen = 5;
				else
					iLen = sFN.length();
				svcLine.setMember_id( (col_value + sFN.substring(0, iLen) + getColumnValue("PATDOB")).replace(" ", "") );
				break;
			case "CLAIMNBR":
				svcLine.setClaim_id(getColumnValue("P_SOURCE") + col_value);
				break;
			case "ITMNBR":
				svcLine.setClaim_line_id(col_value);
				break;
			case "CLAIMTYP":
				svcLine.setClaim_line_type_code(setClaimType(col_value));
				//setAttributionProvider(svcLine.getProvider_id());
				break;
				/*
			case "PROVALWD":
				if (!col_value.isEmpty())
					try {
						svcLine.setAllowed_amt(Double.parseDouble(col_value));
					}
					catch (NumberFormatException e) {
						log.error("Rejecting claim " + svcLine.getClaim_id() + "|" + svcLine.getClaim_line_id() + " for invalid allowed amount " + col_value);
						doErrorReporting("svc104", col_value, svcLine);
						bResult = false;
					};
				break;
				*/
			case "SUBCHG":
				if (!col_value.trim().isEmpty())
					try {
						if (col_value.contains(".")) 
							svcLine.setCharge_amt(Double.parseDouble(col_value.replace("$", "")));
						else
							svcLine.setCharge_amt(Integer.parseInt(col_value.replace("$", "")));	
					}
					catch (NumberFormatException e) {
						//log.error("Rejecting claim " + svcLine.getClaim_id() + "|" + svcLine.getClaim_line_id() + " for invalid charge amount " + col_value);
						doErrorReporting("svc116", col_value, svcLine);
						bResult = false;
					};
				break;
			case "AMTPD":
				if (!col_value.isEmpty())
					try {
						svcLine.setPaid_amt(Double.parseDouble(col_value));
						svcLine.setAllowed_amt(svcLine.getAllowed_amt() + Double.parseDouble(col_value));
					}
					catch (NumberFormatException e) {
						log.error("Rejecting claim " + svcLine.getClaim_id() + "|" + svcLine.getClaim_line_id() + " for invalid paid amount " + col_value);
						doErrorReporting("svc117", col_value, svcLine);
						bResult = false;
					};
				break;
			case "COPAY":
				if (!col_value.isEmpty())
					try {
						svcLine.setCopay_amt(Double.parseDouble(col_value));
						svcLine.setAllowed_amt(svcLine.getAllowed_amt() + Double.parseDouble(col_value));
					}
					catch (NumberFormatException e) {
						log.error("Rejecting claim " + svcLine.getClaim_id() + "|" + svcLine.getClaim_line_id() + " for invalid copay amount " + col_value);
						doErrorReporting("svc118", col_value, svcLine);
						bResult = false;
					};
				break;
			case "COINS":
				if (!col_value.isEmpty())
					try {
						svcLine.setCoinsurance_amt(Double.parseDouble(col_value));
						svcLine.setAllowed_amt(svcLine.getAllowed_amt() + Double.parseDouble(col_value));
					}
					catch (NumberFormatException e) {
						log.error("Rejecting claim " + svcLine.getClaim_id() + "|" + svcLine.getClaim_line_id() + " for invalid coinsurance amount " + col_value);
						doErrorReporting("svc119", col_value, svcLine);
						bResult = false;
					};
				break;
			case "DEDUCT":
				if (!col_value.isEmpty())
					try {
						svcLine.setDeductible_amt(Double.parseDouble(col_value));
						svcLine.setAllowed_amt(svcLine.getAllowed_amt() + Double.parseDouble(col_value));
					}
					catch (NumberFormatException e) {
						log.error("Rejecting claim " + svcLine.getClaim_id() + "|" + svcLine.getClaim_line_id() + " for invalid deductible amount " + col_value);
						doErrorReporting("svc120", col_value, svcLine);
						bResult = false;
					};
				break;
			case "BPRVN":
				svcLine.setProvider_id(getColumnValue("P_SOURCE").substring(0, 1) + col_value);
				//setAttributionProvider(col_value);
				if ( (svcLine.getFacility_id() == null  || svcLine.getFacility_id().isEmpty())  && 
						(svcLine.getPhysician_id() == null  || svcLine.getPhysician_id().isEmpty()))	
					setAttributionProvider(svcLine.getProvider_id());
				break;
			case "PROVNPI":
				svcLine.setProvider_NPI(col_value);
				break;
			// derive both Facility Code and File Type from TypeOfBill
			case "FACLTYBIL":
				svcLine.setType_of_bill(col_value);
				if (col_value != null && col_value.length() > 1) {
					svcLine.setFacility_type_code(BillTypeManager.getFacilityCode(col_value.substring(0,2)));
					//svcLine.setClaim_line_type_code(BillTypeManager.getFileType(col_value.substring(0,2)));
				}
				/*
				if (col_value != null && col_value.length() > 1) {
					svcLine.setFacility_type_code(BillTypeManager.getFacilityCode(col_value.substring(0,2)));
					svcLine.setClaim_line_type_code(BillTypeManager.getFileType(col_value.substring(0,2)));
					if (svcLine.getClaim_line_type_code() == null  ||  svcLine.getClaim_line_type_code().isEmpty() )
						svcLine.setClaim_line_type_code("PB");
				}
				else
					svcLine.setClaim_line_type_code("PB");
				setAttributionProvider(svcLine.getProvider_id());
				*/
				break;
			case "DOS":
				try {
					svcLine.setBegin_date(DateUtil.doParse(col_clue.col_name, col_value));
					if(svcLine.getClaim_line_type_code().equals("IP") )
						svcLine.setAdmission_date(DateUtil.doParse(col_clue.col_name, col_value));
					else
						svcLine.setEnd_date(DateUtil.doParse(col_clue.col_name, col_value));
				} catch (ParseException e) {
					log.error("Rejecting claim " + svcLine.getClaim_id() + "|" + svcLine.getClaim_line_id() + " for invalid begin date: " + col_value);
					doErrorReporting("svc106", col_value, svcLine);
					bResult = false;
				}
				break;
			case "DISDT":
				try {
					if(!(col_value == null)  && (col_value.trim().length() > 4) ) {
						svcLine.setEnd_date(DateUtil.doParse(col_clue.col_name, col_value));
						if(svcLine.getClaim_line_type_code().equals("IP") )	
							svcLine.setDischarge_date(DateUtil.doParse(col_clue.col_name, col_value));
					}
				} catch (ParseException e) {
					log.error("Rejecting claim " + svcLine.getClaim_id() + "|" + svcLine.getClaim_line_id() + " for invalid end date: " + col_value);
					doErrorReporting("svc107", col_value, svcLine);
					bResult = false;
				}
				break;
			case "PLACESVC":
				svcLine.setPlace_of_svc_code(lookupPOS(col_value));
				break;
			case "DIAGCD1":
				svcLine.setPrincipal_diag_code(col_value);
				break;
			case "PROCCD":
				svcLine.addPrinProcCode(col_value);
				break;
			case "PROCM1":
				svcLine.setPrincipal_proc_mod_code(col_value);
				break;
			case "ADMSNTYP":
				svcLine.setAdmission_src_code(col_value);
				break;
			case "PATSTATUS":
				svcLine.setDischarge_status_code(col_value);
				break;
			case "REVCDEVAL":
				svcLine.addRevCode(col_value);
				break;
			case "DRGCODE":
				svcLine.setMs_drg_code(col_value);
				break;
			case "P_TYPPLN":
				svcLine.setInsurance_product(col_value);
			default:
				break;	
			}
		}

		return bResult;
	}
	

	String sFN;
	int iLen;
	


	/**
	 * check for a header
	 * return true indicates first line is data (not a header)
	 * PEBTF does not send headers
	 * @param line
	 * @return
	 */
	protected boolean checkForHeader (String line) {
		
		stats.fileClass = "Claim Service Lines";
		
		int i=0;
		for (String col : cols_to_find) {
			InputStatistics.StatEntry e = stats.new StatEntry();
			e.bExpected = true;
	    	e.col_name = col;
		

	    	stats.column_stats.put(col.toUpperCase(), e);
	    	col_index.put(i, new ColumnFinder(i, col));

		    i++;
		}  
		
		return true;		 // means header is never found, so process the first record as data
		
	}
	
	

	
	String [] cols_to_find = {
			"P_SOURCE",						// 0
			"P_TYPPLN",						// 1
			"P_CLMTYP",						// 2
			"P_BENCDE",						// 3
			"PLAN",							// 4
			"P_AR",							// 5
			"CONID",						// 6
			"MHICN",						// 7
			"SUBZP1",						// 8
			"GRPNBR",						// 9
			"PATSSN",						// 10
			"PLSTNAME",						// 11
			"PFSTNAME",						// 12
			"PATSLT",						// 13
			"PATREL",						// 14
			"P_PATGNDR",					// 15
			"PATSTATUS",					// 16 
			"PATDOB",						// 17
			"CLAIMNBR",
			"ITMNBR",
			"CLAIMTYP",
			"CSRCCD",
			"DOS",
			"DISDT",
			"RECDT",
			"P_PDDT",
			"AMTPD",
			"P_INVDAT",
			"BPRVN",
			"BPRONAM",
			"BPROVZIP",
			"BPROVTYP",
			"BPROVSPC",
			"PROVCLAS",
			"PLACESVC",
			"TYPESVC",
			"DIAGCD1",
			"DIAGCD2",
			"DIAGCD3",
			"DIAGCD4",
			"DIAGCD5",
			"DIAGCD6",
			"DIAGCDID",
			"P_DIAGCDID",
			"POA1",
			"POA2",
			"POA3",
			"POA4",
			"POA5",
			"POA6",
			"PROCCD",
			"PROCTYID",
			"P_PROCTYID",
			"PROCM1",
			"PROCM2",
			"NUMSVC",
			"SUBCHG",
			"P_CLMVAL",
			"P_FLXAMT",
			"P_CAPAMT",
			"CVRDCHG",
			"DEDUCT",
			"COINS",
			"COPAY",
			"COINSDY",
			"EXCMAX",
			"ACCFEE",
			"FEEIND",
			"ICENT1",
			"ICENT2",
			"P_NOTCVCD",
			"P_NOTCVAMT",
			"P_EXBENCD",
			"P_EXBENAMT",
			"P_COBCD1",
			"P_COBAMT1",
			"P_COBCD2",
			"P_COBAMT2",
			"P_CNTCD1",
			"P_CNTAMT1",
			"P_CNTCD21",
			"P_CNTAMT2",
			"P_OTHCD1",
			"P_OTHAMT1",
			"P_OTHCD2",
			"P_OTHAMT2",
			"P_DSCCD1",
			"P_DSCAMT1",
			"P_DSCCD2",
			"P_DSCAMT2",
			"DSPCD",
			"P_REJADJCD",
			"P_REJADJTY",
			"CLREMRK1",
			"EMPLNAME",
			"EMPFNAME",
			"P_CRITCODE",
			"P_CKMATCH",
			"P_CLMCONSQ",
			"P_CLMLINSQ",
			"P_SUBNO",
			"P_PERNO",
			"P_MBREDTC",
			"P_PLAN",
			"P_HLTHPL",
			"EMPID",
			"EMPDOB",
			"EMPGNDER",
			"NOUNITS",
			"NOUNITS2",
			"PROCPRM",
			"PROC2",
			"PROC3",
			"PROVTYPE",
			"PROVPAY",
			"DRGCODE",
			"ADMSNTYP",
			"PROVALWD",
			"PROVNPI",
			"PERFNPI",
			"FACLTYBIL",
			"REVCODE#",
			"REVCDEVAL",
			"Filler"
	};
	
	

	private void setAttributionProvider(String prov_id) {
		if(svcLine.getClaim_line_type_code().equals("PB") )
			svcLine.setPhysician_id(prov_id);
		else
			svcLine.setFacility_id(prov_id);
	}
	
	protected void doPostProcess() {
		
		super.doPostProcess();
		
		if (svcLine.getFacility_type_code() == null  ||  svcLine.getFacility_type_code().isEmpty())
			svcLine.setFacility_type_code("ST");
		
	}
	
	
	
	private String lookupPOS(String sIn) {
		sPOS = sIn.length() > 2 ? sIn.substring(sIn.length()-2) : sIn;
		for (int i=0; i < placeOfSvcLookup.length; i++) {
			if (sIn.equals(placeOfSvcLookup[i][0]) ) {
				sPOS = placeOfSvcLookup[i][1];
				break;
			}
		}
		return sPOS;
	}
	private String sPOS;
	
	private String [][] placeOfSvcLookup = {
			
			{"00",	"11"},
			{"02",	"12"},
			{"05",	"21"},
			{"07",	"22"},
			{"08",	"23"},
			{"10",	"24"},
			{"15",	"25"},
			{"20",	"31"},
			{"22",	"32"},
			{"24",	"33"},
			{"25",	"34"},
			{"26",	"34"},
			{"30",	"41"},
			{"31",	"42"},
			{"35",	"51"},
			{"36",	"52"},
			{"37",	"53"},
			{"38",	"54"},
			{"39",	"56"},
			{"40",	"55"},
			{"42",	"57"},
			{"45",	"61"},
			{"47",	"62"},
			{"50",	"65"},
			{"55",	"71"},
			{"60",	"81"},
			{"99",	"99"}

	};
	
	
	private String setClaimType (String sIn) {
		sClmTyp = sIn;
		for (int i=0; i < clmtypLookup.length; i++) {
			if (sIn.equals(clmtypLookup[i][0]) ) {
				sClmTyp = clmtypLookup[i][1];
				break;
			}
		}
		return sClmTyp;
	}
	private String sClmTyp;
	
	private String [][] clmtypLookup = {
			
			{"IP",	"IP"},
			{"HO",	"OP"},
			{"OP",	"PB"},
			{"1",	"IP"},
			{"2",	"OP"},
			{"3",	"PB"},
			{"I",	"IP"},
			{"O",	"OP"},
			{"P",	"PB"},
			{"MM",	"PB"},
			{"V",	"PB"}

	};
	
	/*
	@Override
	protected boolean filterPass() {
		return ! (getColumnValue("NUMSVC").equals("-1"));
	}
	*/
	
	
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
		ClaimDataFromPebtf instance = new ClaimDataFromPebtf();
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
	//public static final String STAY_FILE = "C:\\input\\PEBTF\\HCI_Hmk_LMORGAN_201412011711P1_First1000.CSV";
	//public static final String STAY_FILE = "C:\\input\\PEBTF\\HCI_Hmk_LMORGAN_201412011711P1_sample_without_header.CSV";
	public static final String STAY_FILE = "C:\\input\\PEBTF\\HCI_201504031047P1\\HCI_Hmk_JLATCHFORD_201504031047P1.CSV";
	
	private static boolean bDebug = false;
	
	private static org.apache.log4j.Logger log = Logger.getLogger(ClaimDataFromPebtf.class);

}
