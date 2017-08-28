



public class PlanMemberDataFromHCI3_H extends PlanMemberAbstract implements PlanMemberInterface {
	
	
	

	
	public String sDelimiter = "\\||;";
	

	
	
	/**
	 * Constructor that will provide error message management functionality
	 * @param errMgr
	 */
	public PlanMemberDataFromHCI3_H(ErrorManager errMgr) {
		this.errMgr = errMgr;
		super.sDelimiter = this.sDelimiter;
	}
	
	/**
	 * Basic constructor without error message management
	 */
	public PlanMemberDataFromHCI3_H () {
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
				member.setMember_id(col_value);
				break;
			case "MemberGender":
				member.setGender_code(col_value);
				break;
			case "MemberYearOfBirth":
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
			case "DateOfDeath":
				if (!col_value.isEmpty())
					try {
						if (sdf_1 == null) {
							sdf_1 = new SimpleDateFormat(DateUtil.determineDateFormat(col_value));
						}
						member.setDate_of_death(sdf_1.parse(col_value));
					}
					catch (NumberFormatException e) {
						doErrorReporting("pm105", col_value, member);
						bResult = false;
					} catch (ParseException e) {
						doErrorReporting("pm105", col_value, member);
						bResult = false;
				};
			case "MemberZipCode":
				member.setZip_code(col_value);
				break;
			case "MemberRace1":
				member.setRace_code(col_value);
				break;
			case "MemberRace2":
				if(member.getRace_code() == null  ||  member.getRace_code().isEmpty())
					member.setRace_code(col_value);
				break;
			case "HispanicIndicator":
				member.setHispanic_indicator(col_value.charAt(0));
				break;
			case "PrimaryInsuranceIndicator":
				member.setPrimary_insurance_indicator(col_value.charAt(0));
			case "PlanAssignedProvider":
				member.setEnforced_provider_id(col_value);
				break;
			case "MemberPCPID":
				member.setPrimary_care_provider_id(col_value);
				break;
			case "PCPNationalProviderID":
				member.setPrimary_care_provider_npi(col_value);
				break;
			case "MemberDeductible":
				if (!col_value.isEmpty())
					try {
						member.setMember_deductible(Double.parseDouble(col_value));
					}
					catch (NumberFormatException e) {
						doErrorReporting("pm106", col_value, member);
						bResult = false;
					};
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
			"NationalPlanID",
			"InsuranceProduct",
			"Year",
			"Month",
			"UniqueMemberID",
			"DualEligibilityFlag",
			"MemberGender",
			"MemberYearOfBirth",
			"MemberZipCode",
			"MemberRace1",
			"MemberRace2",
			"HispanicIndicator",
			"PrimaryInsuranceIndicator",
			"MemberPCPID",
			"PCPNationalProviderID",
			"MemberPCPEffectiveDate",
			"MemberDeductible",
			"DisabilityIndicatorFlag",
			"DateOfDeath",
			"PlanAssignedProvider"
	};
	

	SimpleDateFormat sdf_1;
	


}
