





public class ProviderDataFromCSV  extends ProviderDataAbstract  implements ProviderDataInterface {
	
	
	/**
	 * Constructor that will provide error message management functionality
	 * @param errMgr
	 */
	public ProviderDataFromCSV(ErrorManager errMgr) {
		this.errMgr = errMgr;
	}
	
	/**
	 * Basic constructor without error message management
	 */
	public ProviderDataFromCSV () {
		this.errMgr = null;
	}

	

	
	protected boolean doMap(int col_ix, String col_value) {
		
		boolean bResult = true;

		ColumnFinder col_clue = col_index.get(col_ix);
		if (col_clue == null)
			{}
		else
		{
			//log.info("Stats: " + stats );
			//log.info("Clue: " + col_clue.col_name);
			//log.info("Column Stats: " + stats.column_stats );
			if (col_value != null  && !(col_value.isEmpty())) {
				stats.column_stats.get(col_clue.col_name.toUpperCase()).filled_count++;
			}
			
			if (col_clue.col_name.toUpperCase().startsWith("PROVIDER_SPECIALTY_"))
	    	{
				if (col_value != null  && !(col_value.trim().isEmpty()))
					provider.addSpecialty(col_value);
	    	}
	    	else
			switch (col_clue.col_name) {
			case "PROVIDER_ID":
				//log.info("Member Id is: " + col_value);
				provider.setProvider_id(col_value);
				break;
			case "PROVIDER_GROUP":
				provider.setGroup_name(col_value);
				break;
			case "PROVIDER_SYSTEM":
				provider.setSystem_name(col_value);
				break;
			case "PROVIDER_TAX_ID":
				provider.setTax_id(col_value);
				break;
			case "PROVIDER_ZIP_CODE":
				provider.setZipcode(col_value);
				break;
			case "PROVIDER_PILOT":
				provider.setPilot_name(col_value);
				break;
			case "PROVIDER_ACO":
				provider.setAco_name(col_value);
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
		stats.fileClass = "Providers";
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
			//lineRead = forgiveness(lineRead);
			// look for iterative fields first
			if (lineRead.toUpperCase().startsWith("PROVIDER_SPECIALTY_"))
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
	// probably a waste of time given the recurring specialty columns
	String [] cols_to_find = {
		"PROVIDER_ID",
		"PROVIDER_GROUP",
		"PROVIDER_SYSTEM",
		"PROVIDER_TAX_ID",
		"PROVIDER_SPECIALTY_1",
		"PROVIDER_ZIP_CODE",
		"PROVIDER_PILOT",
		"PROVIDER_ACO"
	};
	

	
	
	/**
	 * a main strictly for testing
	 * @param args
	 */
	public static void main(String[] args) {
		bDebug = true;
		ProviderDataFromCSV instance = new ProviderDataFromCSV();
		try {
			instance.addSourceFile(PROVIDER_FILE);
			instance.prepareAllProviders ();
		} catch (Throwable e) {
			{}	// this main is just for testing - I don't care
		}
		
		// Printing the claim service line list populated. 
		if (bDebug)
		{
			Collection<Provider> it = instance.providers.values();
			for (Provider member : it) { 
				log.info(member);
			}
		}
		
		log.info("Found " + instance.getCounts().getRecordsRead() + " provider input records");
		log.info("Accepted " + instance.getCounts().getRecordsAccepted() + " provider input records");
		log.info("Rejected " + instance.getCounts().getRecordsRejected() + " provider input records");
		
	}
	
	public static final String PROVIDER_FILE = "C:\\input\\Horizon\\providers.csv";
	
	private static boolean bDebug = false;
	
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	
	private static org.apache.log4j.Logger log = Logger.getLogger(ProviderDataFromCSV.class);
	

	@Override
	public ProviderInputCounts getCounts() {
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
	 * no errors in here as of yet
	 
	private void doErrorReporting (String id, String errValue, Provider pv) {
		if(errMgr != null)
			errMgr.issueError(id, errValue, pv);
	}
	*/


}
