



public class PlanMemberDataFromXG extends PlanMemberDataAbstract implements PlanMemberDataInterface {
	
	public String sDelimiter = "\\||;";
	
	/**
	 * Constructor that will provide error message management functionality
	 * @param errMgr
	 */
	public PlanMemberDataFromXG(ErrorManager errMgr) {
		this.errMgr = errMgr;
		super.sDelimiter = this.sDelimiter;
	}
	
	/**
	 * Basic constructor without error message management
	 */
	public PlanMemberDataFromXG () {
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
				member.setMember_id(col_value);
				break;
			case "XG_GENDER":
				member.setGender_code(col_value);
				break;
			case "XG_DOB":
				if (!col_value.isEmpty())
					try {
						c.setTime(DateUtil.doParse(col_clue.col_name, col_value));
						member.setBirth_year(c.get(Calendar.YEAR));
					}
					catch (NumberFormatException e) {
						log.error("Rejecting plan member " + member.getMember_id() + " for invalid birth year");
						doErrorReporting("pm101", col_value, member);
						bResult = false;
					} catch (ParseException e) {
						log.error("Rejecting plan member " + member.getMember_id() + " for invalid birth year");
						doErrorReporting("pm101", col_value, member);
						bResult = false;
					};
				break;
			case "XG_ZIP":
				member.setZip_code(col_value);
				break;
			case "XG_CMSRACE":
				member.setRace_code(col_value);
				break;
			case "XG_PCPEFFDATE":
				if (!col_value.isEmpty())
					try {
						member.setPcp_effective_date(DateUtil.doParse(col_clue.col_name, col_value));
					}
					catch (NumberFormatException e) {
						log.error("Rejecting plan member " + member.getMember_id() + " for invalid PCP effective date");
						doErrorReporting("pm104", col_value, member);
						bResult = false;
					} catch (ParseException e) {
						log.error("Rejecting plan member " + member.getMember_id() + " for invalid PCP effective date");
						doErrorReporting("pm104", col_value, member);
						bResult = false;
					};
				break;
			case "XG_PCPNPI1":
				member.setPrimary_care_provider_id(col_value);
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
		"XG_MASTERPERSONINDEX",
		"XG_LOB",
		"XG_GENDER",
		"XG_DOB",
		"XG_ZIP",
		"XG_CMSRACE",
		"XG_PCPEFFDATE",
		"XG_PCPNPI1"
	};
	
	
	
	Calendar c = Calendar.getInstance();
	
	
	/**
	 * a main strictly for testing
	 * @param args
	 */
	public static void main(String[] args) {
		bDebug = true;
		PlanMemberDataFromXG instance = new PlanMemberDataFromXG();
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
		}
		
		log.info("Found " + instance.getCounts().getRecordsRead() + " member input records");
		log.info("Accepted " + instance.getCounts().getRecordsAccepted() + " member input records");
		log.info("Rejected " + instance.getCounts().getRecordsRejected() + " member input records");
		
	}
	
	public static final String MEMBER_FILE = "C:\\input\\XG_Member11192014.txt";
	
	private static boolean bDebug = false;
	
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	
	private static org.apache.log4j.Logger log = Logger.getLogger(PlanMemberDataFromXG.class);
	
	class col_finder {
		String col_name;
		int col_number;
		col_finder (int i, String s) {
			col_number = i;
			col_name = s;
		}
	}
	

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
