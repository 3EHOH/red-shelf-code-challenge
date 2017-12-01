





public class ClaimRxDataFromAPCD extends ClaimRxDataAbstract implements ClaimRxDataInterface {
	
	
	
	/**
	 * Constructor that will provide error message management functionality
	 * @param errMgr
	 */
	public ClaimRxDataFromAPCD (ErrorManager errMgr) {
		this.errMgr = errMgr;
	}
	
	/**
	 * Basic constructor without error message management
	 */
	public ClaimRxDataFromAPCD () {
		this.errMgr = null;
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
			/*
			case "MemberLinkEID":
				//log.info("Member Id is: " + col_value);
				Rx.setMember_id(col_value);
				break;
				*/
			case "HashCarrierSpecificUniqueMemberI":
				//log.info("Member Id is: " + col_value);
				Rx.setMember_id(col_value);
				break;
			case "AllowedAmountCleaned":
				if (!col_value.isEmpty())
					try {
						Rx.setAllowed_amt(Double.parseDouble(col_value));
					}
					catch (NumberFormatException e) {
						log.error("Rejecting claim Rx of member " + Rx.getMember_id() + " for invalid allowed amount: " + col_value);
						doErrorReporting("rx101", col_value, Rx);
						bResult = false;
					};
				break;
			case "DrugCode":
				Rx.setDrug_code(col_value);
				break;
			case "DrugName":
				break;
			case "DaysSupply":
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
			case "DatePrescriptionFilled":
				try {
					if (sdf_1 == null) {
						sdf_1 = new SimpleDateFormat(DateUtil.determineDateFormat(col_value));
					}
					Rx.setRx_fill_date(sdf_1.parse(col_value));
				} catch (ParseException e) {
					log.error("Rejecting claim Rx of member " + Rx.getMember_id() + " for invalid fill date: " + col_value);
					doErrorReporting("rx103", col_value, Rx);
					bResult = false;
				}
				break;
			case "PharmacyClaimID":
				Rx.setClaim_id(col_value);
				break;
			case "LineCounter":
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
			case "VersionNumber":
				break;
			case "GenericDrugIndicatorCleaned":
				Rx.setGenericDrugIndicator(col_value);
				break;
			case "DispenseasWrittenCode":
				break;
			case "QuantityDispensed":
				if (!col_value.isEmpty())
					try {
						Rx.setQuantityDispensed(Integer.parseInt(col_value));
					}
					catch (NumberFormatException e) {
						log.error("Rejecting claim Rx of member " + Rx.getMember_id() + " for invalid dispensed amount: " + col_value);
						doErrorReporting("rx104", col_value, Rx);
						bResult = false;
					}
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
		"HashCarrierSpecificUniqueMemberI",
		"MemberLinkEID",
		"AllowedAmountCleaned",
		"DrugCode",
		"DaysSupply",
		"DatePrescriptionFilled",
		"PharmacyClaimID",
		"LineCounter",
		"VersionNumber",
		"NationalPharmacyIDNumberCleaned",
		"DrugName",
		"GenericDrugIndicatorCleaned",
		"DispenseasWrittenCode",
		"QuantityDispensed"
	};
	

	
	
	/**
	 * a main strictly for testing
	 * @param args
	 */
	public static void main(String[] args) {
		bDebug = true;
		ClaimRxDataFromAPCD instance = new ClaimRxDataFromAPCD();
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
		
		log.info("Found " + instance.getCounts().getRecordsRead() + " Rx claim input records");
		log.info("Accepted " + instance.getCounts().getRecordsAccepted() + " Rx claim input records");
		log.info("Rejected " + instance.getCounts().getRecordsRejected() + " Rx claim input records");
		
	}
	
	//public static final String RX_FILE = "C:\\input\\ECR_RX_MEDICARE.txt";
	public static final String RX_FILE = "C:\\input\\72_HCL3_ECR_PharmacyClaim_First1000.txt";

	
	private static boolean bDebug = false;
	
	SimpleDateFormat sdf_1; // = new SimpleDateFormat("yyyy-MM-dd");
	
	private static org.apache.log4j.Logger log = Logger.getLogger(ClaimRxDataFromAPCD.class);
	
	
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
