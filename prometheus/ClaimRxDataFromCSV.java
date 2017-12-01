



public class ClaimRxDataFromCSV extends ClaimRxDataAbstract implements ClaimRxDataInterface {
	
	
	
	private boolean bSasDates;




	/**
	 * Constructor that will provide error message management functionality
	 * @param errMgr
	 */
	public ClaimRxDataFromCSV (ErrorManager errMgr) {
		this.errMgr = errMgr;
		if(RunParameters.parameters.containsKey("DateFormat")  &&
				RunParameters.parameters.get("DateFormat").equalsIgnoreCase("SAS9"))
			bSasDates = true;
	}
	
	/**
	 * Basic constructor without error message management
	 */
	public ClaimRxDataFromCSV () {
		this.errMgr = null;
		if(RunParameters.parameters.containsKey("DateFormat")  &&
				RunParameters.parameters.get("DateFormat").equalsIgnoreCase("SAS9"))
			bSasDates = true;
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
			switch (col_clue.col_name) {
			case "CONSISTENT_MEMBER_ID":
				//log.info("Member Id is: " + col_value);
				Rx.setMember_id(col_value);
				break;
			case "ALLOWED_AMT":
				if (!col_value.isEmpty())
					try {
						Rx.setAllowed_amt(Double.parseDouble(col_value.replace("$", "")));
					}
					catch (NumberFormatException e) {
						//log.error("Rejecting claim Rx of member " + Rx.getMember_id() + " for invalid allowed amount: " + col_value);
						doErrorReporting("rx101", col_value, Rx);
						bResult = false;
					};
				break;
			case "NDC_CODE":
				Rx.setDrug_code(col_value);
				break;
			case "SUPPLY_DAYS_NUM":
				if (!col_value.isEmpty()) {
					try {
						sTemp = col_value;
						if(sTemp.indexOf('.') > -1)
							sTemp = sTemp.substring(0, sTemp.indexOf('.'));
						Rx.setDays_supply_amt(Integer.parseInt(sTemp));
					}
					catch (NumberFormatException e) {
						//log.error("Rejecting claim Rx of member " + Rx.getMember_id() + " for invalid supply days amount: " + col_value);
						doErrorReporting("rx102", col_value, Rx);
						bResult = false;
					}
				}
				break;
			case "PRESCRIPTION_FILLED_DATE_s":
				if (bSasDates)
					Rx.setRx_fill_date(DateUtil.getDateFromSAS9(col_value));
				else
					try {
						if (sdf_1 == null) {
							sdf_1 = new SimpleDateFormat(DateUtil.determineDateFormat(col_value));
						}
						Rx.setRx_fill_date(sdf_1.parse(col_value));
					} catch (NullPointerException | ParseException e) {
						//log.error("Rejecting claim Rx of member " + Rx.getMember_id() + " for invalid fill date: " + col_value);
						doErrorReporting("rx103", col_value, Rx);
						bResult = false;
					}
				break;
			case "CLAIM_ID":
				Rx.setClaim_id(col_value);
				break;
			default:
				break;	
			}
			
		}

		return bResult;
	}
	
	private String sTemp;
	

	/**
	 * check for a header
	 * return true indicates first line is data (not a header)
	 * @param line
	 * @return
	 */
	protected boolean checkForHeader (String line) {
		boolean bReturn = true;
		stats.fileClass = "Rx Claims";
		//StringTokenizer in = new StringTokenizer(line,"|,");  
		String[] in = line.split(sDelimiter);
		int i=0;
		for (String lineRead : in)  
		{  
			// fix common errors
			lineRead = forgiveness(lineRead);
			// set up statistics (imperfect, because I'm stupid enough to try to code for no-header possibility)
			InputStatistics.StatEntry e = stats.column_stats.get(lineRead.toUpperCase());
			if (e == null)
				e =	stats.new StatEntry();
			e.bExpected = false;
			e.col_name = lineRead;
			// look for iterative fields first - none in this file
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
		"CONSISTENT_MEMBER_ID",
		"ALLOWED_AMT",
		"NDC_CODE",
		"SUPPLY_DAYS_NUM",
		"PRESCRIPTION_FILLED_DATE_s",
		"CLAIM_ID"
	};
	

	
	/**
	 * just some bad column headers we've experienced
	 * @param in
	 * @return
	 */
	private String forgiveness (String in) {
		String s = in;
		if (in.equalsIgnoreCase("SUPPLY_DAYS_AMT"))
			s = "SUPPLY_DAYS_NUM";
		return s;
	}
	
	
	/**
	 * a main strictly for testing
	 * @param args
	 */
	public static void main(String[] args) {
		bDebug = true;
		ClaimRxDataFromCSV instance = new ClaimRxDataFromCSV();
		try {
			instance.addSourceFile(RX_FILE);
			instance.prepareAllRxs ();
		} catch (Throwable e) {
			{}	// this main is just for testing - I don't care
		}
		
		// Printing the claim service line list populated. 
		if (bDebug)
			for (ClaimRx clm : instance.Rxs) { 
				log.info(clm);
			}
		
		log.info("Found " + instance.getCounts().getRecordsRead() + " claim input records");
		log.info("Accepted " + instance.getCounts().getRecordsAccepted() + " claim input records");
		log.info("Rejected " + instance.getCounts().getRecordsRejected() + " claim input records");
		
	}
	
	//public static final String RX_FILE = "C:\\input\\ECR_RX_MEDICARE.txt";
	public static final String RX_FILE = "C:\\input\\Pharma.txt.txt";

	
	private static boolean bDebug = false;
	
	SimpleDateFormat sdf_1; // = new SimpleDateFormat("yyyy-MM-dd");
	
	private static org.apache.log4j.Logger log = Logger.getLogger(ClaimRxDataFromCSV.class);
	
	
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
