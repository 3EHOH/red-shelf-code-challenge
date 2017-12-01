





public class EnrollmentDataFromHCI3_H extends EnrollmentAbstract implements EnrollmentInterface {
	
	
	
	public String sDelimiter = "\\||;";
	

	
	/**
	 * Constructor that will provide error message management functionality
	 * @param errMgr
	 */
	public EnrollmentDataFromHCI3_H(ErrorManager errMgr) {
		this.errMgr = errMgr;
		super.sDelimiter = this.sDelimiter;
	}
	
	/**
	 * Basic constructor without error message management
	 */
	public EnrollmentDataFromHCI3_H () {
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
			case "UniqueMemberID":
				//log.info("Member Id is: " + col_value);
				enrollment.setMember_id(col_value);
				break;
			case "InsuranceProduct":
				enrollment.setInsurance_product(col_value);
				break;
			case "EnrollmentStartDate":
				try {
					enrollment.setBegin_date(DateUtil.doParse(col_clue.col_name, col_value));
				} catch (ParseException e) {
					//log.error("Rejecting enrollment " + enrollment.getMember_id() + " for invalid begin date: " + col_value);
					doErrorReporting("enr101", col_value, enrollment);
					bResult = false;
				}
				break;
			case "EnrollmentEndDate":
				if (col_value != null  && !(col_value.isEmpty())) {
					try {
						enrollment.setEnd_date(DateUtil.doParse(col_clue.col_name, col_value));
					} catch (ParseException e) {
						//log.error("Rejecting enrollment " + enrollment.getMember_id() + " for invalid end date: " + col_value);
						doErrorReporting("enr102", col_value, enrollment);
						bResult = false;
					}
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
		stats.fileClass = "Enrollments";
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
	String [] cols_to_find = {
		"UniqueMemberID",
		"InsuranceProduct",
		"EnrollmentStartDate",
		"EnrollmentEndDate"
	};
	

	
	
	//private static org.apache.log4j.Logger log = Logger.getLogger(EnrollmentDataFromHCI3_H.class);
	
	


}
