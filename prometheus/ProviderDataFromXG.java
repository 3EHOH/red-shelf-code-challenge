



public class ProviderDataFromXG extends ProviderDataAbstract implements ProviderDataInterface  {

	public String sDelimiter = "\\||;";

	/**
	 * Constructor that will provide error message management functionality
	 * @param errMgr
	 */
	public ProviderDataFromXG(ErrorManager errMgr) {
		this.errMgr = errMgr;
		super.sDelimiter = this.sDelimiter;
	}
	
	/**
	 * Basic constructor without error message management
	 */
	public ProviderDataFromXG () {
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
			/*
			if (col_clue.col_name.toUpperCase().startsWith("PROVIDER_SPECIALTY_"))
	    	{
				if (col_value != null  && !(col_value.trim().isEmpty()))
					provider.addSpecialty(col_value);
	    	}
	    	else
	    	*/
			if (col_value != null  && !(col_value.trim().isEmpty()))
			switch (col_clue.col_name) {
			case "XG_SERVPROVIDTYPE":
				provider.setProvider_attribution_code(col_value);
				break;
			case "XG_SERVPROV":
				provider.setProvider_id(col_value);
				break;
			case "XG_PROV_NPI":
				provider.setNPI(col_value);
				break;
			case "XG_GROUPPRACTICENAME":
				provider.setGroup_name(col_value);
				break;
			case "XG_PROV_FULL_NAME":
				provider.setProvider_name(col_value);
				break;
			case "XG_TAXID":
				provider.setTax_id(col_value);
				break;
			case "XG_ZIP":
				provider.setZipcode(col_value);
				break;
			case "XG_PROV_TYPE":
				provider.setProvider_type(col_value);
				break;
			case "XG_PROV_SPECIALTY":
				if (col_value != null  && !(col_value.trim().isEmpty()))
					provider.addSpecialty(col_value);
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
			//lineRead = forgiveness(lineRead);
			// look for iterative fields first
			/*
			if (lineRead.toUpperCase().startsWith("PROVIDER_SPECIALTY_"))
	    	{
	    		col_index.put(i, new col_finder(i, lineRead));
	    		bReturn = false;
	    		e.bExpected = true;
	    	}
	    	else
	    	*/
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
		"XG_SERVPROVIDTYPE",
		"XG_SERVPROV",
		"XG_PROV_NPI",
		"XG_GROUPPRACTICENAME",
		"XG_TAXID",
		"XG_ZIP",
		"XG_PROV_TYPE",
		"XG_PROV_SPECIALTY",
		"XG_PROV_FULL_NAME",
		"XG_PROV_LAST_NAME",
		"XG_PROV_FIRST_NAME"
	};
	
	
	@Override
	protected Provider initializeProvider(String [] in) {
		int idx = -1;
		for (Entry<Integer, ColumnFinder> entry : col_index.entrySet()) {
		    if (entry.getValue().col_name.equalsIgnoreCase("XG_SERVPROV")) {
		    	idx = entry.getValue().col_number;
		    	break;
		    }
		}
		if(idx < 0)
			throw new IllegalStateException("Can not find provider key column");
		if(providers.get(in[idx]) != null) {
			log.info("Found prior entry for: " + providers.get(in[idx]).getProvider_name());
			dupesFound++;
		}
		return providers.get(in[idx]) == null ? new Provider() : providers.get(in[idx]);
	}
	
	int dupesFound = 0;
	

	
	
	/**
	 * a main strictly for testing
	 * @param args
	 */
	public static void main(String[] args) {
		bDebug = true;
		ProviderDataFromXG instance = new ProviderDataFromXG();
		try {
			instance.addSourceFile(PROVIDER_FILE);
			instance.prepareAllProviders ();
		} catch (Throwable e) {
			{log.info(e);}	// this main is just for testing - I don't care
		}
		
		// Printing the claim service line list populated. 
		if (bDebug)
		{
			GenericOutputInterface goif = new GenericOutputCSV();
			//GenericOutputInterface goif = new GenericOutputSQL();
			Collection<Provider> it = instance.providers.values();
			for (Provider member : it) { 
				goif.write(member);
				log.info(member);
			}
			goif.close();
		}
		
		log.info("Found " + instance.getCounts().getRecordsRead() + " provider input records");
		log.info("Accepted " + instance.getCounts().getRecordsAccepted() + " provider input records");
		log.info("Rejected " + instance.getCounts().getRecordsRejected() + " provider input records");
		log.info("Dupes found: " + instance.dupesFound);
		
	}
	
	//public static final String PROVIDER_FILE = "C:\\input\\XG_Prov_11192014.txt";
	public static final String PROVIDER_FILE = "C:\\input\\HDIV3_MARSHFIELD_MARSHFIELD_PRV_0001_TEST_20141205145840.dat";
	
	private static boolean bDebug = true;
	
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	
	private static org.apache.log4j.Logger log = Logger.getLogger(ProviderDataFromXG.class);
	

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
