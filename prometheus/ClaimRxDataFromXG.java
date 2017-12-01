





public class ClaimRxDataFromXG extends ClaimRxDataAbstract implements ClaimRxDataInterface {
	
	public String sDelimiter = "\\||;";
	
	
	/**
	 * Constructor that will provide error message management functionality
	 * @param errMgr
	 */
	public ClaimRxDataFromXG (ErrorManager errMgr) {
		this.errMgr = errMgr;
		super.sDelimiter = this.sDelimiter;
	}
	
	/**
	 * Basic constructor without error message management
	 */
	public ClaimRxDataFromXG () {
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
			case "XG_MASTERPERSONINDEX":
				//log.info("Member Id is: " + col_value);
				Rx.setMember_id(col_value);
				break;
			case "XG_AMTALLOWED":
				if (!col_value.isEmpty())
					try {
						Rx.setAllowed_amt(Double.parseDouble(col_value));
					}
					catch (NumberFormatException e) {
						log.error("Rejecting claim Rx of member " + Rx.getMember_id() + " for invalid allowed amount: " + col_value);
						doErrorReporting("rx101", col_value, Rx);
						bResult = false;
					}
				break;
			case "XG_AMTCHARGE":
				if (!col_value.isEmpty())
					try {
						Rx.setCharge_amt(Double.parseDouble(col_value));
					}
					catch (NumberFormatException e) {
						log.error("Rejecting claim Rx of member " + Rx.getMember_id() + " for invalid charge amount: " + col_value);
						doErrorReporting("rx107", col_value, Rx);
						bResult = false;
					}
				break;
			case "XG_AMTCOPAY":
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
			case "XG_AMTCOINS":
				if (!col_value.isEmpty())
					try {
						Rx.setCoinsurance_amt(Double.parseDouble(col_value));
					}
					catch (NumberFormatException e) {
						log.error("Rejecting claim Rx of member " + Rx.getMember_id() + " for invalid co-insurance amount: " + col_value);
						doErrorReporting("rx107", col_value, Rx);
						bResult = false;
					}
				break;
			case "XG_AMTDEDUCT":
				if (!col_value.isEmpty())
					try {
						Rx.setDeductible_amt(Double.parseDouble(col_value));
					}
					catch (NumberFormatException e) {
						log.error("Rejecting claim Rx of member " + Rx.getMember_id() + " for invalid deductible amount: " + col_value);
						doErrorReporting("rx108", col_value, Rx);
						bResult = false;
					}
				break;
			case "XG_NDC":
				Rx.setDrug_code(col_value);
				break;
			case "XG_NDCDESCRIPTION":
				Rx.setDrug_name(col_value);
				break;
			case "XG_DAYSSUPPLY":
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
			case "XG_YMDFILLED":
				try {
					Rx.setRx_fill_date(DateUtil.doParse(col_clue.col_name, col_value));
				} catch (ParseException e) {
					log.error("Rejecting claim Rx of member " + Rx.getMember_id() + " for invalid fill date: " + col_value);
					doErrorReporting("rx103", col_value, Rx);
					bResult = false;
				}
				break;
			case "XG_HDICLAIMHDRKEY":
				Rx.setClaim_id(col_value);
				break;
			case "XG_HDICLAIMLINEKEY":
				if (!col_value.isEmpty())
					try {
						Rx.setLineCounter(Integer.parseInt(col_value));
					}
					catch (NumberFormatException e) {
						log.error("Rejecting claim Rx of member " + Rx.getMember_id() + " for invalid line counter: " + col_value);
						doErrorReporting("rx105", col_value, Rx);
						bResult = false;
					}
				break;
			case "XG_HDICLAIMSEQKEY":
				Rx.setSequence_key(col_value);
				break;
			case "XG_HDICLAIMSTATUS":
				Rx.setOrig_adj_rev(col_value);
				break;
			case "XG_GENERICBRAND":
				Rx.setGenericDrugIndicator(col_value);
				break;
			case "XG_QTY":
				if (!col_value.isEmpty()  &&  getColumnValue("XG_HDICLAIMSTATUS").equals("ORIGINAL"))
					try {
						Rx.setQuantityDispensed(Integer.parseInt(col_value));
					}
					catch (NumberFormatException e) {
						log.error("Rejecting claim Rx of member " + Rx.getMember_id() + " for invalid dispensed amount: " + col_value);
						doErrorReporting("rx104", col_value, Rx);
						bResult = false;
					};
				break;
			case "XG_PHYSICIANNPI1":
				Rx.setPrescribing_provider_NPI(col_value);
				break;
			case "XG_PHYSICIANID":
				Rx.setPrescribing_provider_id(col_value);
				break;
			default:
				break;	
			}
			
		}

		return bResult;
	}
	
	
	
	@Override
	protected boolean filterPass() {
		return getColumnValue("XG_HDIDUPLICATEINDICATOR").equals("00");
	}
	
	@Override
	protected void doRollUp () {
		
		idx = _RxLines.get(hashSL_ADR());
		if ( idx != null ) {
			ClaimRx cTarget = Rxs.get(idx);
			if (Rx.getSequence_key().compareTo(cTarget.getSequence_key()) > 0 )
				cTarget = Rx;
		}
		else
		{
			_RxLines.put(hashSL_ADR(), Rxs.size());
			
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
		
	}
	
	Integer idx;
	private HashMap <Integer, Integer> RxLines = new HashMap<Integer, Integer>();
	private HashMap <Integer, Integer> _RxLines = new HashMap<Integer, Integer>();
	
	private int hashSL_ADR() {
	    StringBuilder builder = new StringBuilder();
	    builder.append(Rx.getMember_id());
	    builder.append(Rx.getClaim_id());
	    builder.append(Rx.getLineCounter());
	    builder.append(Rx.getOrig_adj_rev());
	    return builder.toString().hashCode();
	}

	private int hashSL() {
	    StringBuilder builder = new StringBuilder();
	    builder.append(Rx.getMember_id());
	    builder.append(Rx.getClaim_id());
	    builder.append(Rx.getLineCounter());
	    return builder.toString().hashCode();
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
		"XG_MASTERPERSONINDEX",
		"XG_AMTALLOWED",
		"XG_AMTCHARGE",
		"XG_AMTCOPAY",
		"XG_AMTCOINS",
		"XG_AMTDEDUCT",
		"XG_NDC",
		"XG_NDCDESCRIPTION",
		"XG_DAYSSUPPLY",
		"XG_YMDFILLED",
		"XG_HDICLAIMHDRKEY",
		"XG_HDICLAIMLINEKEY",
		"XG_HDICLAIMSEQKEY",
		"XG_HDICLAIMSTATUS",
		"XG_HDIDUPLICATEINDICATOR",
		"XG_GENERICBRAND",
		"XG_AMTDISPENSE",
		"XG_PHYSICIANNPI1",
		"XG_PHYSICIANID"
	};
	

	
	
	/**
	 * a main strictly for testing
	 * @param args
	 */
	public static void main(String[] args) {
		bDebug = true;
		ClaimRxDataFromXG instance = new ClaimRxDataFromXG();
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
	
	//public static final String RX_FILE = "C:\\input\\ECR_RX_MEDICARE.txt";
	//public static final String RX_FILE = "C:\\input\\XG_RX_11192014.txt";
	//public static final String RX_FILE = "C:\\input\\HDIV3_MARSHFIELD_MARSHFIELD_RX_0001_TEST_20141205145324.dat";
	public static final String RX_FILE = "C:\\input\\HDIV3_MARSHFIELD_MARSHFIELD_RX_0001_TEST_20141205145324_FinalVersionTest.txt";

	
	private static boolean bDebug = false;

	
	private static org.apache.log4j.Logger log = Logger.getLogger(ClaimRxDataFromXG.class);
	
	
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
