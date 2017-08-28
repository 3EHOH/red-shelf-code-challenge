





public class ClaimRxDataFromWellpoint extends ClaimRxDataAbstract implements ClaimRxDataInterface {
	
	public String sDelimiter = "\\||;";
	
	
	/**
	 * Constructor that will provide error message management functionality
	 * @param errMgr
	 */
	public ClaimRxDataFromWellpoint (ErrorManager errMgr) {
		this.errMgr = errMgr;
		super.sDelimiter = this.sDelimiter;
	}
	
	/**
	 * Basic constructor without error message management
	 */
	public ClaimRxDataFromWellpoint () {
		this.errMgr = null;
		super.sDelimiter = this.sDelimiter;
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
			case "MCIDP":
				//log.info("Member Id is: " + col_value);
				Rx.setMember_id(col_value);
				break;
			case "ALOWAMT":
				if (!col_value.isEmpty())
					try {
						Rx.setAllowed_amt(Double.parseDouble(col_value));
					}
					catch (NumberFormatException e) {
						//log.error("Rejecting claim Rx of member " + Rx.getMember_id() + " for invalid allowed amount: " + col_value);
						doErrorReporting("rx101", col_value, Rx);
						bResult = false;
					}
				break;
			case "BILLCHG":
				if (!col_value.isEmpty())
					try {
						Rx.setCharge_amt(Double.parseDouble(col_value));
					}
					catch (NumberFormatException e) {
						//log.error("Rejecting claim Rx of member " + Rx.getMember_id() + " for invalid charge amount: " + col_value);
						doErrorReporting("rx107", col_value, Rx);
						bResult = false;
					}
				break;
			case "PAIDAMT":
				if (!col_value.isEmpty())
					try {
						Rx.setPaid_amt(Double.parseDouble(col_value));
					}
					catch (NumberFormatException e) {
						//log.error("Rejecting claim Rx of member " + Rx.getMember_id() + " for invalid co-pay amount: " + col_value);
						doErrorReporting("rx110", col_value, Rx);
						bResult = false;
					}
				break;
			case "COPAY":
				if (!col_value.isEmpty())
					try {
						Rx.setCopay_amt(Double.parseDouble(col_value));
					}
					catch (NumberFormatException e) {
						//log.error("Rejecting claim Rx of member " + Rx.getMember_id() + " for invalid co-pay amount: " + col_value);
						doErrorReporting("rx106", col_value, Rx);
						bResult = false;
					}
				break;
			case "COINS":
				if (!col_value.isEmpty())
					try {
						Rx.setCoinsurance_amt(Double.parseDouble(col_value));
					}
					catch (NumberFormatException e) {
						//log.error("Rejecting claim Rx of member " + Rx.getMember_id() + " for invalid co-insurance amount: " + col_value);
						doErrorReporting("rx107", col_value, Rx);
						bResult = false;
					}
				break;
			case "DEDUCT":
				if (!col_value.isEmpty())
					try {
						Rx.setDeductible_amt(Double.parseDouble(col_value));
					}
					catch (NumberFormatException e) {
						//log.error("Rejecting claim Rx of member " + Rx.getMember_id() + " for invalid deductible amount: " + col_value);
						doErrorReporting("rx108", col_value, Rx);
						bResult = false;
					}
				break;
			case "NDC":
				Rx.setDrug_code(col_value);
				break;
			case "DRUGNM":
				Rx.setDrug_name(col_value);
				break;
			case "DAYSUP":
				if (!col_value.isEmpty())
					try {
						Rx.setDays_supply_amt(Integer.parseInt(col_value));
					}
					catch (NumberFormatException e) {
						//log.error("Rejecting claim Rx of member " + Rx.getMember_id() + " for invalid supply days amount: " + col_value);
						doErrorReporting("rx102", col_value, Rx);
						bResult = false;
					};
				break;
			case "RXFILL":
				try {
					Rx.setRx_fill_date(DateUtil.doParse(col_clue.col_name, col_value));
				} catch (ParseException e) {
					//log.error("Rejecting claim Rx of member " + Rx.getMember_id() + " for invalid fill date: " + col_value);
					doErrorReporting("rx103", col_value, Rx);
					bResult = false;
				}
				break;
			case "CLMNBR":
				Rx.setClaim_id(col_value);
				break;
			case "GENIND":
				Rx.setGenericDrugIndicator(col_value);
				break;
			case "QNTDISP":
				if (!col_value.isEmpty())
					try {
						Rx.setQuantityDispensed(Double.valueOf(col_value).intValue());
					}
					catch (NumberFormatException e) {
						//log.error("Rejecting claim Rx of member " + Rx.getMember_id() + " for invalid dispensed amount: " + col_value);
						doErrorReporting("rx104", col_value, Rx);
						bResult = false;
					};
				break;
			case "NPI":
				Rx.setPrescribing_provider_NPI(col_value);
				break;
			case "DEAP":
				Rx.setPrescribing_provider_id(col_value);
				break;
			default:
				break;	
			}
			
		}

		return bResult;
	}

	/**
	 * check for a header
	 * return true indicates first line is data (not a header)
	 * @param line
	 * @return
	 */
	protected boolean checkForHeader (String line) {
		boolean bReturn = true;
		stats.fileClass = "Rx Claims";
 
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
			// lineRead = forgiveness(lineRead);
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
		"MCIDP",
		"ALOWAMT",
		"BILLCHG",
		"PAIDAMT",
		"COPAY",
		"COINS",
		"DEDUCT",
		"NDC",
		"DRUGNM",
		"DAYSUP",
		"RXFILL",
		"CLMNBR",
		"GENIND",
		"QNTDISP",
		"NPI",
		"DEAP",
		"LastAdjstmnt"
	};
	
	
	

	@Override
	protected boolean filterPass() {
		boolean bResult = true;
		
		sTemp = getColumnValue("LastAdjstmnt");
		if (sTemp != null  && sTemp.equalsIgnoreCase("N"))
			bResult = false;
		
		return bResult;
	}
	String sTemp;
	
	

	@Override
	protected void doRollUp () {	
			
		idx = RxLines.get(hashSL());
		if ( idx != null ) {
			ClaimRx cTarget = Rxs.get(idx);
			combineLines(cTarget);
		}
		else
		{
			RxLines.put(hashSL(), Rxs.size());
			Rxs.add(Rx);
		}
	}
		
	
	
	Integer idx;
	private HashMap <Integer, Integer> RxLines = new HashMap<Integer, Integer>();

	
	

	private int hashSL() {
	    StringBuilder builder = new StringBuilder();
	    builder.append(Rx.getMember_id());
	    builder.append(Rx.getClaim_id());
	    builder.append(Rx.getRx_fill_date());
	    return builder.toString().hashCode();
	}
	

	
	/**
	 * a main strictly for testing
	 * @param args
	 */
	public static void main(String[] args) {
		bDebug = true;
		ClaimRxDataFromWellpoint instance = new ClaimRxDataFromWellpoint();
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
			
			/*
			common.dao.InputObjectOutputSQL s = new common.dao.InputObjectOutputSQL();
			s.writeRxClaims(instance.Rxs);
			*/
		}
		
		log.info("Found " + instance.getCounts().getRecordsRead() + " Rx claim input records");
		log.info("Accepted " + instance.getCounts().getRecordsAccepted() + " Rx claim input records");
		log.info("Rejected " + instance.getCounts().getRecordsRejected() + " Rx claim input records");
		
	}
	
	public static final String RX_FILE = "C:\\input\\Wellpoint\\HCI3_CTM_PHARM_FNL_Sample1000.txt";

	
	private static boolean bDebug = false;

	
	private static org.apache.log4j.Logger log = Logger.getLogger(ClaimRxDataFromWellpoint.class);
	
	
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
