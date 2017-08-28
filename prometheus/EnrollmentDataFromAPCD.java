



public class EnrollmentDataFromAPCD extends EnrollmentDataAbstract implements EnrollmentDataInterface {
	
	
	/**
	 * Constructor that will provide error message management functionality
	 * @param errMgr
	 */
	public EnrollmentDataFromAPCD(ErrorManager errMgr) {
		this.errMgr = errMgr;
	}
	
	/**
	 * Basic constructor without error message management
	 */
	public EnrollmentDataFromAPCD () {
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
				enrollment.setMember_id(col_value);
				break;
				*/
			case "HashCarrierSpecificUniqueMemberI":
				//log.info("Member Id is: " + col_value);
				enrollment.setMember_id(col_value);
				break;
			case "MedicalCoverage":
				if (col_value != null  && !(col_value.isEmpty()))
					enrollment.setCoverage_type("MED");
				break;	
			case "PrescriptionDrugCoverage":
				if (col_value != null  && !(col_value.isEmpty()))
					enrollment.setCoverage_type("RX");
				break;	
			case "ProductEnrollmentStartDate":
				try {
					if (sdf_1 == null) {
						sdf_1 = new SimpleDateFormat(DateUtil.determineDateFormat(col_value));
					}
					enrollment.setBegin_date(sdf_1.parse(col_value));
				} catch (ParseException e) {
					log.error("Rejecting enrollment " + enrollment.getMember_id() + " for invalid begin date: " + col_value);
					doErrorReporting("enr101", col_value, enrollment);
					bResult = false;
				}
				break;
			case "ProductEnrollmentEndDate":
				if (col_value != null  && !(col_value.isEmpty())) {
					try {
						if (sdf_2 == null) {
							sdf_2 = new SimpleDateFormat(DateUtil.determineDateFormat(col_value));
						}
						enrollment.setEnd_date(sdf_2.parse(col_value));
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
		"HashCarrierSpecificUniqueMemberI",
		"MemberLinkEID",
		"MedicalCoverage",
		"PrescriptionDrugCoverage",
		"ProductEnrollmentStartDate",
		"ProductEnrollmentEndDate"
	};
	

	
	
	/**
	 * a main strictly for testing
	 * @param args
	 */
	public static void main(String[] args) {
		bDebug = true;
		EnrollmentDataFromAPCD instance = new EnrollmentDataFromAPCD();
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
	
	public static final String ENROLL_FILE = "C:\\input\\72_HCL3_ECR_MemberEligibility_BCBS_10K.txt";
	
	private static boolean bDebug = false;
	
	SimpleDateFormat sdf_1; // = new SimpleDateFormat("yyyy-MM-dd");
	SimpleDateFormat sdf_2; // = new SimpleDateFormat("yyyy-MM-dd");
	
	private static org.apache.log4j.Logger log = Logger.getLogger(EnrollmentDataFromAPCD.class);
	
	
	

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
