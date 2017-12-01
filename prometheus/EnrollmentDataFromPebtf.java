



public class EnrollmentDataFromPebtf extends EnrollmentDataAbstract implements EnrollmentDataInterface {
	
	
	/**
	 * Constructor that will provide error message management functionality
	 * @param errMgr
	 */
	public EnrollmentDataFromPebtf (ErrorManager errMgr) {
		this.errMgr = errMgr;
		super.sDelimiter = this.sDelimiter;
		bPreserveQuotes = true;
		//seedLastEndDt();
	}
	
	

	/**
	 * Basic constructor without error message management
	 */
	public EnrollmentDataFromPebtf () {
		this.errMgr = null;
		super.sDelimiter = this.sDelimiter;
		//seedLastEndDt();
		bPreserveQuotes = true;
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
			
			col_value = col_value.trim();
			
			
			switch (col_clue.col_name) {
			case "PLSTNAME":
				//log.info("Member Id is: " + col_value);
				sFN = getColumnValue("PFSTNAME").trim();
				if (sFN.length() > 4)
					iLen = 5;
				else
					iLen = sFN.length();
				enrollment.setMember_id(col_value + sFN.substring(0,iLen) + getColumnValue("PATDOB").trim());
				break;
			case "ESTDT":
				try {
					enrollment.setBegin_date(DateUtil.doParse(col_clue.col_name, col_value));
				} catch (ParseException e) {
					//log.error("Rejecting enrollment " + enrollment.getMember_id() + " for invalid begin date: " + col_value);
					doErrorReporting("enr101", col_value, enrollment);
					bResult = false;
				}
				break;
			case "EENDT":
				if (col_value != null  &&  (!(col_value.isEmpty()))  &&   (!(col_value.equals("0")))) {
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
	
	private int iLen;
	private String sFN;

	/**
	 * check for a header
	 * PEBTF doesn't do headers
	 * return true indicates first line is data (not a header)
	 * @param line
	 * @return
	 */
	protected boolean checkForHeader (String line) {

		stats.fileClass = "Enrollments";
		
		int i=0;
		for (String col : cols_to_find) {
			InputStatistics.StatEntry e = stats.new StatEntry();
			e.bExpected = true;
	    	e.col_name = col;
		

	    	stats.column_stats.put(col.toUpperCase(), e);
	    	col_index.put(i, new ColumnFinder(i, col));

		    i++;
		}  
		
		return true;		 // means header is never found, so process the first record as data
		
	}

	
	// columns in absence of a header
	String [] cols_to_find = {
		"SSN",
		"ESTDT",
		"EENDT",
		"GENDER",
		"BRTHYR",
		"UID",
		"SUBSCRIBERSSN",
		"RELCODE",
		"PFSTNAME",
		"PLSTNAME",
		"PATDOB"
		
	};
	

	
	
	/**
	 * a main strictly for testing
	 * @param args
	 */
	public static void main(String[] args) {
		bDebug = true;
		EnrollmentDataFromPebtf instance = new EnrollmentDataFromPebtf();
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
	
	public static final String ENROLL_FILE = "C:\\input\\PEBTF\\HCI_Enrollment_ACORNMAN_201501091553P1.CSV";
	
	private static boolean bDebug = false;
	

	private static org.apache.log4j.Logger log = Logger.getLogger(EnrollmentDataFromPebtf.class);
	
	
	

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
