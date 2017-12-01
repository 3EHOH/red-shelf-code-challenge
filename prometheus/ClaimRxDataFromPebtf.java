





public class ClaimRxDataFromPebtf extends ClaimRxDataAbstract implements ClaimRxDataInterface {
	
	
	/**
	 * Constructor that will provide error message management functionality
	 * @param errMgr
	 */
	public ClaimRxDataFromPebtf (ErrorManager errMgr) {
		this.errMgr = errMgr;
		super.sDelimiter = this.sDelimiter;
		bPreserveQuotes = true;
	}
	
	/**
	 * Basic constructor without error message management
	 */
	public ClaimRxDataFromPebtf () {
		this.errMgr = null;
		super.sDelimiter = this.sDelimiter;
		bPreserveQuotes = true;
	}

	
	
	protected boolean doMap(int col_ix, String col_value) {
		
		boolean bResult = true;
		
		ColumnFinder col_clue = col_index.get(col_ix);
		if (col_clue == null)
			{}
		else
		{
			if (col_value != null  && !(col_value.isEmpty())) {
				stats.column_stats.get(col_clue.col_name.toUpperCase()).filled_count++;
			}
			col_value = col_value.trim();
			
			switch (col_clue.col_name) {
			case "PALAST":
				//log.info("Member Id is: " + col_value);
				sFN = getColumnValue("PAFIRS").trim();
				if (sFN.length() > 4)
					iLen = 5;
				else
					iLen = sFN.length();
				Rx.setMember_id(col_value + sFN.substring(0, iLen) + getColumnValue("DOB").trim());
				break;
			case "COPAY":
				if (!col_value.isEmpty())
					try {
						Rx.setCopay_amt(Double.parseDouble(col_value));
					}
					catch (NumberFormatException e) {
						log.error("Rejecting claim Rx of member " + Rx.getMember_id() + " for invalid co-pay amount: " + col_value);
						doErrorReporting("rx106", col_value, Rx);
						bResult = false;
					}
				break;
			case "AMTDUE":
				if (!col_value.isEmpty()) {
					try {
						Rx.setPaid_amt(Double.parseDouble(col_value));
					}
					catch (NumberFormatException e) {
						//log.error("Rejecting claim Rx of member " + Rx.getMember_id() + " for invalid paid amount: " + col_value);
						doErrorReporting("rx110", col_value, Rx);
						bResult = false;
					}
				}
				Rx.setAllowed_amt(Rx.getPaid_amt() + Rx.getCopay_amt());
				break;
			case "DRGCDE":
				Rx.setDrug_code(col_value);
				break;
			case "DRGDSC":
				Rx.setDrug_name(col_value);
				break;
			case "DAYSUP":
				if (!col_value.isEmpty())
					try {
						Rx.setDays_supply_amt(Integer.parseInt(col_value));
					}
					catch (NumberFormatException e) {
						log.error("Rejecting claim Rx of member " + Rx.getMember_id() + " for invalid supply days amount: " + col_value);
						doErrorReporting("rx102", col_value, Rx);
						bResult = false;
					};
				break;
			case "DATSVC":
				if (!col_value.trim().isEmpty()) {
					try {
						Rx.setRx_fill_date(DateUtil.doParse(col_clue.col_name, col_value.trim()));
						//Rx.setRx_fill_date(sdf.parse(col_value));
					} catch (ParseException e) {
						//log.error("Rejecting claim Rx of member " + Rx.getMember_id() + " for invalid fill date: " + col_value);
						doErrorReporting("rx103", col_value, Rx);
						bResult = false;
					}
					catch (NullPointerException e) {
						//log.error("Rejecting claim Rx of member " + Rx.getMember_id() + " for invalid fill date: " + col_value);
						doErrorReporting("rx103", col_value, Rx);
						bResult = false;
					}
				}
				break;
			case "RXNUM":
				Rx.setClaim_id(getColumnValue("BATCH").trim() + getColumnValue("FARMNU").trim() + col_value);
				break;
			case "FORMFL":
				Rx.setGenericDrugIndicator(col_value);
				break;
			case "QTY":
				if (!col_value.isEmpty())
					try {
						Rx.setQuantityDispensed(Integer.parseInt(col_value));
					}
					catch (NumberFormatException e) {
						log.error("Rejecting claim Rx of member " + Rx.getMember_id() + " for invalid dispensed amount: " + col_value);
						doErrorReporting("rx104", col_value, Rx);
						bResult = false;
					};
				break;
			case "PRSCID":
				Rx.setPrescribing_provider_NPI(col_value);
				break;
			case "FARMNU":
				Rx.setNational_pharmacy_Id(col_value);
				break;
			default:
				break;	
			}
			
		}

		return bResult;
	}
	
	String sFN;
	int iLen;
	
	//SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
	
	/**
	 * check for a header
	 * return true indicates first line is data (not a header)
	 * PEBTF never sends a header
	 * @param line
	 * @return
	 */
	protected boolean checkForHeader (String line) {
		
		stats.fileClass = "Rx Claims";
		
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
	

	// trying to create a default order in absence of a header
	// probably a waste of time given the recurring columns
	String [] cols_to_find = {
			"RCDID",
			"PRONUM",
			"BATCH",
			"FARMNUQUA",
			"FARMNU",
			"FARMNUALTQ",
			"FARMNUALT",
			"RXNUM",
			"DATSVC",
			"DRGCDE",
			"DRGDSC",
			"REFILS",
			"QTY",
			"DAYSUP",
			"BASCST",
			"INGCST",
			"DISFEE",
			"COPAY",
			"SALTAX",
			"AMTDUE",
			"PAFIRS",
			"PALAST",
			"DOB",
			"SEX",
			"CARDID",
			"PATREL",
			"GRPNUM",
			"HOMEPL",
			"HOSTPL",
			"PRSCID",
			"DIAGCD",
			"CRDFIR",
			"CRDLST",
			"PREAUT",
			"PAMCSC",
			"CUSTLO",
			"RESUBC",
			"DATEWR",
			"DAWIND",
			"PRSNCD",
			"OTHCOV",
			"ELGCLA",
			"COMPND",
			"NBRRFL",
			"LVLSVC",
			"PRCORI",
			"PRCDEN",
			"PRIPRE",
			"CLINID",
			"DRGTYP",
			"PRLAST",
			"POSTCL",
			"DOSEIN",
			"OTHAMT",
			"BASSUP",
			"STAIND",
			"AWP",
			"ADMCHG",
			"SUBCST",
			"PADDAT",
			"FORMFL",
			"GCNGCL",
			"THERCL",
			"PHRTYP",
			"BILBAS",
			"NPI",
			"LICENSE",
			"P_SUBNO",
			"P_PERNO"
	};
	
	

	
	
	/**
	 * a main strictly for testing
	 * @param args
	 */
	public static void main(String[] args) {
		bDebug = true;
		ClaimRxDataFromPebtf instance = new ClaimRxDataFromPebtf();
		try {
			instance.addSourceFile(RX_FILE);
			instance.prepareAllRxs ();
		} catch (Throwable e) {
			{}	// this main is just for testing - I don't care
		}
		
		// Printing the claim service line list populated. 
		if (bDebug) {
			GenericOutputInterface goif = new GenericOutputCSV();
			for (ClaimRx clm : instance.Rxs) { 
				log.info(clm);
				goif.write(clm);
			}
			goif.close();
		}
		
		log.info("Found " + instance.getCounts().getRecordsRead() + " Rx claim input records");
		log.info("Accepted " + instance.getCounts().getRecordsAccepted() + " Rx claim input records");
		log.info("Rejected " + instance.getCounts().getRecordsRejected() + " Rx claim input records");
		
	}
	
	
	public static final String RX_FILE = "C:\\input\\PEBTF\\HCI_Rx_LMORGAN_201412012101P1_First1000.CSV";

	
	private static boolean bDebug = false;

	
	private static org.apache.log4j.Logger log = Logger.getLogger(ClaimRxDataFromPebtf.class);
	
	
	class rxCompare implements Comparator<ClaimRx> {

	    @Override
	    public int compare(ClaimRx o1, ClaimRx o2) {
	    	int c;
	        c = o1.getMember_id().compareTo(o2.getMember_id());
	        if (c == 0)
	        	c = o1.getRx_fill_date().compareTo(o2.getRx_fill_date());
	        //if (c == 0)
	        //	c = o1.getMember_id().compareTo(o2.getMember_id());
	        return c;
	    }
	}

	@Override
	public ClaimRxInputCounts getCounts() {
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



}
