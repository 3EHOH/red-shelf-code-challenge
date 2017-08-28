





public class PlanMemberDataFromAPCD extends PlanMemberDataAbstract implements PlanMemberDataInterface {
	
	
	/**
	 * Constructor that will provide error message management functionality
	 * @param errMgr
	 */
	public PlanMemberDataFromAPCD(ErrorManager errMgr) {
		this.errMgr = errMgr;
	}
	
	/**
	 * Basic constructor without error message management
	 */
	public PlanMemberDataFromAPCD () {
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
			case "HashCarrierSpecificUniqueMemberI":
				//log.info("Member Id is: " + col_value);
				member.setMember_id(col_value);
				break;
				/*
			case "MemberLinkEID":
				//log.info("Member Id is: " + col_value);
				member.setMember_id(col_value);
				break;
				*/
			//case "HashCarrierSpecificUniqueMemberID":
			//	if(member.getMember_id() == null  ||  member.getMember_id().isEmpty())
			//		member.setMember_id(col_value);
			//	break;
			case "MemberGenderCleaned":
				member.setGender_code(col_value);
				break;
			case "MemberDateofBirthYearCleaned":
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
			case "Standardized_MemberZIPCode":
				member.setZip_code(col_value);
				break;
			case "Race1Cleaned":
				member.setRace_code(col_value);
				break;
			case "Race2Cleaned":
				if(member.getRace_code() == null  ||  member.getRace_code().isEmpty())
					member.setRace_code(col_value);
				break;
			//case "USER_DEFINED_PROVIDER":
			//	member.setEnforced_provider_id(col_value);
			//	break;
			case "MemberPCPID_Linkage_ID":
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
		"HashCarrierSpecificUniqueMemberI",
		"MemberLinkEID",
		"HashCarrierSpecificUniqueMemberID",
		"MemberGenderCleaned",
		"MemberDateofBirthYearCleaned",
		"Standardized_MemberZIPCode",
		"Race1Cleaned",
		"Race2Cleaned",
		//"USER_DEFINED_PROVIDER",
		"MemberPCPID_Linkage_ID"
	};
	

	
	
	/**
	 * a main strictly for testing
	 * @param args
	 */
	public static void main(String[] args) {
		bDebug = true;
		PlanMemberDataFromAPCD instance = new PlanMemberDataFromAPCD();
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
	
	public static final String MEMBER_FILE = "C:\\input\\72_HCL3_ECR_MemberEligibility_BCBSMembers.txt";
	
	private static boolean bDebug = false;
	
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	
	private static org.apache.log4j.Logger log = Logger.getLogger(PlanMemberDataFromAPCD.class);
	
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
