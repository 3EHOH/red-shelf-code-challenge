



public class PlanMemberDataFromPebtf extends PlanMemberDataAbstract implements PlanMemberDataInterface {
	
	
	/**
	 * Constructor that will provide error message management functionality
	 * @param errMgr
	 */
	public PlanMemberDataFromPebtf(ErrorManager errMgr) {
		this.errMgr = errMgr;
		super.sDelimiter = this.sDelimiter;
		bPreserveQuotes = true;
	}
	
	/**
	 * Basic constructor without error message management
	 */
	public PlanMemberDataFromPebtf () {
		this.errMgr = null;
		super.sDelimiter = this.sDelimiter;
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
				member.setMember_id(col_value + sFN.substring(0,iLen) + getColumnValue("PATDOB").trim());
				break;
			case "GENDER":
				member.setGender_code(col_value);
				break;
			case "BRTHYR":
				if (!col_value.isEmpty())
					try {
						member.setBirth_year(Integer.parseInt(col_value));
					}
					catch (NumberFormatException e) {
						log.error("Rejecting plan member " + member.getMember_id() + " for invalid birth year");
						doErrorReporting("pm101", col_value, member);
						bResult = false;
					};
				break;
			default:
				break;	
			}

		}
		
		return bResult;
	}
	
	String sFN;
	int iLen;


	/**
	 * check for a header
	 * return true indicates first line is data (not a header)
	 * @param line
	 * @return
	 */
	protected boolean checkForHeader (String line) {

		stats.fileClass = "Members";
		
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


	// columns in absence of header
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
	
	
	Calendar c = Calendar.getInstance();
	
	
	
	
	/**
	 * a main strictly for testing
	 * @param args
	 */
	public static void main(String[] args) {
		bDebug = true;
		PlanMemberDataFromPebtf instance = new PlanMemberDataFromPebtf();
		RunParameters.parameters = new HashMap<String, String>();
		RunParameters.parameters.put("runname", "PEBTF_Test");
		//RunParameters.parameters.put("env", "dev");
		RunParameters.parameters.put("env", "ecr");
		try {
			instance.addSourceFile(MEMBER_FILE);
			instance.prepareAllMembers ();
		} catch (Throwable e) {
			{}	// this main is just for testing - I don't care
		}
		
		// Printing the enrollment list populated. 
		if (bDebug)
		{
			GenericOutputInterface goif = new GenericOutputCSV();
			//GenericOutputInterface goif = new GenericOutputSQL();
			Collection<PlanMember> it = instance.members.values();
			for (PlanMember member : it) { 
				goif.write(member);
				log.info(member);
			}
			goif.close();
			
			

			common.dao.InputObjectOutputSQL s = new common.dao.InputObjectOutputSQL();
			s.writeMembers(instance.members.values());
			
		}
		
		log.info("Found " + instance.getCounts().getRecordsRead() + " member input records");
		log.info("Accepted " + instance.getCounts().getRecordsAccepted() + " member input records");
		log.info("Rejected " + instance.getCounts().getRecordsRejected() + " member input records");
		
	}
	
	public static final String MEMBER_FILE = "C:\\input\\PEBTF\\HCI_Enrollment_ACORNMAN_201501091553P1.CSV";
	
	private static boolean bDebug = false;
	
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	
	private static org.apache.log4j.Logger log = Logger.getLogger(PlanMemberDataFromPebtf.class);
	
	

	@Override
	public MemberInputCounts getCounts() {
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
	

	private void doErrorReporting (String id, String errValue, PlanMember pm) {
		if(errMgr != null)
			errMgr.issueError(id, errValue, pm);
	}
	

}
