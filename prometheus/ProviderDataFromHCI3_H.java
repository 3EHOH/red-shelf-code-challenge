


public class ProviderDataFromHCI3_H extends ProviderAbstract implements ProviderInterface {


	
	public String sDelimiter = "\\||;";
	

	

	/**
	 * Constructor that will provide error message management functionality
	 * @param errMgr
	 */
	public ProviderDataFromHCI3_H(ErrorManager errMgr) {
		this.errMgr = errMgr;
		super.sDelimiter = this.sDelimiter;
	}
	
	/**
	 * Basic constructor without error message management
	 */
	public ProviderDataFromHCI3_H () {
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
			
			if (col_clue.col_name.toUpperCase().startsWith(("ProviderSpeciality").toUpperCase()))
	    	{
				if (col_value != null  && !(col_value.trim().isEmpty()))
					provider.addSpecialty(col_value);
	    	}
	    	else if (col_clue.col_name.toUpperCase().startsWith(("ProviderTaxonomy").toUpperCase()))
	    	{
				if (col_value != null  && !(col_value.trim().isEmpty()))
					provider.addTaxonomyCode(col_value);
	    	}
	    	else
	    	
			switch (col_clue.col_name) {
			case "PlanProviderID":
				provider.setProvider_id(col_value);
				break;
			case "ProviderNPI":
				provider.setNPI(col_value);
				break;
			case "ProviderDEA":
				provider.setDEA_no(col_value);
				break;	
			case "MedicareID":
				provider.setMedicare_id(col_value);
			case "ProviderTaxID":
				provider.setTax_id(col_value);
				break;
			case "ProviderZipCode":
				provider.setZipcode(col_value);
				break;
			case "ProviderLastName":
				provider.setProvider_name(col_value.trim() + " " + getColumnValue("ProviderFirstName").trim());
				break;
			case "ProviderEntity":
				provider.setProvider_type(col_value);
				break;
			case "FacilityID":
				provider.setFacility_id(col_value);
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
			
			if (lineRead.toUpperCase().startsWith(("ProviderTaxonomy").toUpperCase())) {
	    		col_index.put(i, new ColumnFinder(i, lineRead));
	    		bReturn = false;
	    		e.bExpected = true;
	    	}
	    	else if (lineRead.toUpperCase().startsWith(("ProviderSpeciality").toUpperCase())) {
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
			"PlanProviderID",
			"ProviderNPI",
			"ProviderDEA",
			"ProviderLastName",
			"ProviderFirstName",
			"ProviderZipCode",
			"MedicareID",
			"ProviderTaxID",
			"ProviderEntity",
			"FacilityID"
	};
	


}