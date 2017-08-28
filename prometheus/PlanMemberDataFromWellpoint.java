




public class PlanMemberDataFromWellpoint extends PlanMemberDataAbstract implements PlanMemberDataInterface {
	
	public String sDelimiter = "\\||;";
	
	/**
	 * Constructor that will provide error message management functionality
	 * @param errMgr
	 */
	public PlanMemberDataFromWellpoint(ErrorManager errMgr) {
		this.errMgr = errMgr;
		super.sDelimiter = this.sDelimiter;
	}
	
	/**
	 * Basic constructor without error message management
	 */
	public PlanMemberDataFromWellpoint () {
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
			case "MCID":
				//log.info("Member Id is: " + col_value);
				member.setMember_id(col_value);
				break;
			case "GENDER":
				member.setGender_code(col_value);
				break;
			case "DOB":
				if (!col_value.isEmpty())
					try {
						member.setBirth_year(Integer.parseInt(col_value));
					}
					catch (NumberFormatException e) {
						//log.error("Rejecting plan member " + member.getMember_id() + " for invalid birth year");
						doErrorReporting("pm101", col_value, member);
						bResult = false;
					};
				break;
			case "MEMBZIP":
				member.setZip_code(col_value);
				break;
			case "DSDIND":
				if (col_value.equals("YES"))
					try {
						sDD = getColumnValue("ELIGENDDT");
						if(!sDD.isEmpty())
							member.setDate_of_death(DateUtil.doParse("ELIGENDDT", sDD));
					}
					catch (NumberFormatException e) {
						//log.error("Rejecting plan member " + member.getMember_id() + " for invalid PCP effective date");
						doErrorReporting("pm105", col_value, member);
						bResult = false;
					} catch (ParseException e) {
						//log.error("Rejecting plan member " + member.getMember_id() + " for invalid PCP effective date");
						doErrorReporting("pm105", col_value, member);
						bResult = false;
					};
				break;
			case "RENDPRV":
				member.setEnforced_provider_id(col_value);
				break;
			case "PCPID":
				member.setPrimary_care_provider_id(col_value);
				break;
			default:
				break;	
			}

		}
		
		return bResult;
	}
	
	private String sDD;

	/**
	 * check for a header
	 * return true indicates first line is data (not a header)
	 * @param line
	 * @return
	 */
	protected boolean checkForHeader (String line) {
		boolean bReturn = true;
		stats.fileClass = "Members";
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
		"MCID",
		"GENDER",
		"MEMBZIP",
		"DOB",
		"RENDPRV",
		"PCPID",
		"DSDIND",
		"ELIGENDDT"
	};
	
	
	
	Calendar c = Calendar.getInstance();
	
	
	/**
	 * a main strictly for testing
	 * @param args
	 */
	public static void main(String[] args) {
		bDebug = true;
		PlanMemberDataFromWellpoint instance = new PlanMemberDataFromWellpoint();
		RunParameters.parameters = new HashMap<String, String>();
		RunParameters.parameters.put("runname", "Wellpoint");
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
	
	public static final String MEMBER_FILE = "C:\\input\\Wellpoint\\HCI3_CTM_MEMBERS_FNL_First1000.txt";
	
	private static boolean bDebug = false;
	
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	
	private static org.apache.log4j.Logger log = Logger.getLogger(PlanMemberDataFromWellpoint.class);
	

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
