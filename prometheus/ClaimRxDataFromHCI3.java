





public class ClaimRxDataFromHCI3 extends ClaimRxDataAbstract implements ClaimRxDataInterface {
	
	

	
	//public String sDelimiter = "\\||;";
	

	
	
	/**
	 * Constructor that will provide error message management functionality
	 * @param errMgr
	 */
	public ClaimRxDataFromHCI3 (ErrorManager errMgr) {
		this.errMgr = errMgr;
		super.sDelimiter = this.sDelimiter;
	}
	
	/**
	 * Basic constructor without error message management
	 */
	public ClaimRxDataFromHCI3 () {
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
			case "NationalPlanID":
				Rx.setPlan_id(col_value);
				break;
			case "InsuranceProduct":
				Rx.setInsurance_product(col_value);
				break;
			case "UniqueMemberID":
				//log.info("Member Id is: " + col_value);
				Rx.setMember_id(col_value);
				break;
			case "AllowedAmount":
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
			case "ChargeAmount":
				if (!col_value.isEmpty())
					try {
						Rx.setCharge_amt(Double.parseDouble(col_value.replace("$", "")));
					}
					catch (NumberFormatException e) {
						//log.error("Rejecting claim Rx of member " + Rx.getMember_id() + " for invalid charge amount: " + col_value);
						doErrorReporting("rx107", col_value, Rx);
						bResult = false;
					}
				break;
			case "PaidAmount":
				if (!col_value.isEmpty())
					try {
						Rx.setPaid_amt(Double.parseDouble(col_value.replace("$", "")));
					}
					catch (NumberFormatException e) {
						doErrorReporting("rx110", col_value, Rx);
						bResult = false;
					}
				break;
			case "PrepaidAmount":
				if (!col_value.isEmpty())
					try {
						Rx.setPrepaid_amt(Double.parseDouble(col_value.replace("$", "")));
					}
					catch (NumberFormatException e) {
						doErrorReporting("rx111", col_value, Rx);
						bResult = false;
					}
				break;
			case "Co-PayAmount":
				if (!col_value.isEmpty())
					try {
						Rx.setCopay_amt(Double.parseDouble(col_value.replace("$", "")));
					}
					catch (NumberFormatException e) {
						//log.error("Rejecting claim Rx of member " + Rx.getMember_id() + " for invalid co-pay amount: " + col_value);
						doErrorReporting("rx106", col_value, Rx);
						bResult = false;
					}
				break;
			case "CoinsuranceAmount":
				if (!col_value.isEmpty())
					try {
						Rx.setCoinsurance_amt(Double.parseDouble(col_value.replace("$", "")));
					}
					catch (NumberFormatException e) {
						//log.error("Rejecting claim Rx of member " + Rx.getMember_id() + " for invalid co-insurance amount: " + col_value);
						doErrorReporting("rx107", col_value, Rx);
						bResult = false;
					}
				break;
			case "DeductibleAmount":
				if (!col_value.isEmpty())
					try {
						Rx.setDeductible_amt(Double.parseDouble(col_value.replace("$", "")));
					}
					catch (NumberFormatException e) {
						//log.error("Rejecting claim Rx of member " + Rx.getMember_id() + " for invalid deductible amount: " + col_value);
						doErrorReporting("rx108", col_value, Rx);
						bResult = false;
					}
				break;
			case "DrugCode":
				Rx.setDrug_code(col_value);
				break;
			case "DrugName":
				Rx.setDrug_name(col_value);
				break;
			case "DaysSupply":
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
			case "DatePrescriptionFilled":
				try {
					Rx.setRx_fill_date(DateUtil.doParse(col_clue.col_name, col_value));
				} catch (ParseException e) {
					//log.error("Rejecting claim Rx of member " + Rx.getMember_id() + " for invalid fill date: " + col_value);
					doErrorReporting("rx103", col_value, Rx);
					bResult = false;
				}
				break;
			case "ClaimNumber":
				Rx.setClaim_id(col_value);
				break;
			case "LineCounter":
				if (!col_value.isEmpty())
					try {
						Rx.setLineCounter(Integer.parseInt(col_value));
					}
					catch (NumberFormatException e) {
						//log.error("Rejecting claim Rx of member " + Rx.getMember_id() + " for invalid line counter: " + col_value);
						doErrorReporting("rx105", col_value, Rx);
						bResult = false;
					}
				break;
			case "VersionNumber":
				break;
			case "GenericDrugIndicator":
				Rx.setGenericDrugIndicator(col_value);
				break;
			case "DispenseAsWritten":
				break;
			case "QuantityDispensed":
				if (!col_value.isEmpty())
					try {
						if (col_value.contains("."))
							Rx.setQuantityDispensed(Double.parseDouble(col_value));
						else
							Rx.setQuantityDispensed(Integer.parseInt(col_value));
					}
					catch (NumberFormatException e) {
						//log.error("Rejecting claim Rx of member " + Rx.getMember_id() + " for invalid dispensed amount: " + col_value);
						doErrorReporting("rx104", col_value, Rx);
						bResult = false;
					}
				break;
			case "NationalPharmacyID":
				Rx.setNational_pharmacy_Id(col_value);
				break;
			case "PharmacyZipCode":
				Rx.setPharmacy_zip_code(col_value);
				break;
			case "NewPrescriptionIndicator":
				//Rx.setNational_pharmacy_Id(col_value);
				break;
			case "PrescribingProviderNPI":
				Rx.setPrescribing_provider_NPI(col_value);
				break;
			case "PrescribingProviderDEA":
				Rx.setPrescribing_provider_DEA(col_value);
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
			"NationalPlanID",
			"InsuranceProduct",
			"ClaimNumber",
			"LineCounter",
			"VersionNumber",
			"FinalVersionFlag",
			"UniqueMemberID",
			"NationalPharmacyID",
			"PharmacyZipCode",
			"DrugCode",
			"DrugName",
			"NewPrescriptionIndicator",
			"GenericDrugIndicator",
			"DispenseAsWritten ",
			"DatePrescriptionFilled",
			"QuantityDispensed",
			"DaysSupply",
			"ChargeAmount",
			"PaidAmount",
			"PrepaidAmount",
			"Co-PayAmount",
			"CoinsuranceAmount",
			"DeductibleAmount",
			"AllowedAmount",
			"PrescribingProviderNPI",
			"PrescribingProviderDEA"
	};
	

	
	
	/**
	 * a main strictly for testing
	 * @param args
	 */
	public static void main(String[] args) {
		bDebug = true;
		ClaimRxDataFromHCI3 instance = new ClaimRxDataFromHCI3();
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

	
	private static org.apache.log4j.Logger log = Logger.getLogger(ClaimRxDataFromHCI3.class);
	
	
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
