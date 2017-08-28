





public class EnrollmentDataFromXG extends EnrollmentDataAbstract implements EnrollmentDataInterface {
	
	public String sDelimiter = "\\||;";
	
	/**
	 * Constructor that will provide error message management functionality
	 * @param errMgr
	 */
	public EnrollmentDataFromXG(ErrorManager errMgr) {
		this.errMgr = errMgr;
		super.sDelimiter = this.sDelimiter;
	}
	
	/**
	 * Basic constructor without error message management
	 */
	public EnrollmentDataFromXG () {
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
				enrollment.setMember_id(col_value);
				break;
			case "XG_LOB":
				enrollment.setInsurance_product(col_value);
				break;
			case "XG_COVERAGETYPE":
				//log.info("Member Id is: " + col_value);
				enrollment.setCoverage_type(col_value);
				break;
			case "XG_YMDENROLLSTART":
				try {
					enrollment.setBegin_date(DateUtil.doParse(col_clue.col_name, col_value));
				} catch (ParseException e) {
					//log.error("Rejecting enrollment " + enrollment.getMember_id() + " for invalid begin date: " + col_value);
					doErrorReporting("enr101", col_value, enrollment);
					bResult = false;
				}
				break;
			case "XG_YMDENROLLEND":
				if (col_value != null  && !(col_value.isEmpty())) {
					try {
						enrollment.setEnd_date(DateUtil.doParse(col_clue.col_name, col_value));
					} catch (ParseException e) {
						log.error("Rejecting enrollment " + enrollment.getMember_id() + " for invalid end date: " + col_value);
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
		"XG_MASTERPERSONINDEX",
		"XG_LOB",
		"XG_COVERAGETYPE",
		"XG_YMDENROLLSTART",
		"XG_YMDENROLLEND"
	};
	

	
	
	/**
	 * a main strictly for testing
	 * @param args
	 */
	public static void main(String[] args) {
		bDebug = true;
		EnrollmentDataFromXG instance = new EnrollmentDataFromXG();
		try {
			instance.addSourceFile(ENROLL_FILE);
			instance.prepareAllEnrollments ();
		} catch (Throwable e) {
			{}	// this main is just for testing - I don't care
		}
		
		// Printing the enrollment list populated. 
		if (bDebug)
		{
			GenericOutputInterface goif = new GenericOutputCSV();
			Collection<List<Enrollment>> it = instance.enrollments.values();
			for (List<Enrollment> enroll : it) { 
				for (Enrollment e : enroll) {
					goif.write(e);
					log.info(e);
				}
			}
			goif.close();
		}
		
		log.info("Found " + instance.getCounts().getRecordsRead() + " enrollment input records");
		log.info("Accepted " + instance.getCounts().getRecordsAccepted() + " enrollment input records");
		log.info("Rejected " + instance.getCounts().getRecordsRejected() + " enrollment input records");
		
	}
	
	public static final String ENROLL_FILE = "C:\\input\\XG_Member11192014.txt";
	
	private static boolean bDebug = false;
	

	private static org.apache.log4j.Logger log = Logger.getLogger(EnrollmentDataFromXG.class);
	
	
	

	@Override
	public EnrollmentInputCounts getCounts() {
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
	
	private void doErrorReporting (String id, String errValue, Enrollment en) {
		if(errMgr != null)
			errMgr.issueError(id, errValue, en);
	}
	
}
