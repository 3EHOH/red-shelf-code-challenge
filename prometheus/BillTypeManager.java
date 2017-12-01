

public class BillTypeManager {
	
	
	
	
	static HashMap<String, BillType> btList;
	
	public static String [][] getBtDef () {
		return btDef;
	}
	
	// {"bill_type", "file_type", "facility description", "facility code (IP only)"}
	static final String [][] btDef = {
		{"11x",	"IP",	"Hospital - Inpatient",	"ST"},
		{"12x",	"PB",	"Hospital - Inpatient (Part B only)", ""},	
		{"13x",	"OP",	"Hospital - Outpatient", ""},	
		{"14x",	"OP",	"Hospital - Other/Home Health", ""},	
		{"15x",	"IP",	"Hospital - Nursing Facility Level I",	"LT"},
		{"16x",	"IP",	"Hospital - Nursing Facility Level II",	"LT"},
		{"17x",	"IP",	"Hospital - Intermediate Care - Level III Nursing Facility", "LT"},
		{"18x",	"IP",	"Hospital - Swing Beds",	"LT"},
		{"20x", "PB",	"SNF",	"SNF"},
		{"21x",	"IP",	"SNF - Inpatient",	"SNF"},
		{"22x",	"PB",	"SNF - Inpatient (Part B)", ""},	
		{"23x",	"OP",	"SNF - Outpatient", ""},	
		{"24x",	"OP",	"SNF - Other/Home Health", ""},	
		{"25x",	"IP",	"SNF - Nursing Facility Level I",	"SNF"},
		{"26x",	"IP",	"SNF - Nursing Facility Level II",	"SNF"},
		{"27x",	"IP",	"SNF - Intermediate Care - Level III Nursing Facility",	"SNF"},
		{"28x",	"IP",	"SNF - Swing Beds",	"SNF"},
		{"31x",	"IP",	"Home Health - Inpatient",	"LT"},
		{"32x",	"PB",	"Home Health - Inpatient (Part B)", ""},	
		{"33x",	"OP",	"Home Health - Outpatient",	""},
		{"34x",	"OP",	"Home Health - Other/Home Health",	""},
		{"35x",	"IP",	"Home Health - Nursing Facility Level I",	"LT"},
		{"36x",	"IP",	"Home Health - Nursing Facility Level II",	"LT"},
		{"37x",	"IP",	"Home Health - Intermediate Care - Level III Nursing Facility",	"LT"},
		{"38x",	"IP",	"Home Health - Swing Beds",	"LT"},
		{"41x", "IP",	"Christian Science Hospital - Inpatient",	"ST"},
		{"42x",	"PB",	"Christian Science Hospital - Inpatient (Part B)", ""},	
		{"43x",	"OP",	"Christian Science Hospital - Outpatient", ""},	
		{"44x",	"OP",	"Christian Science Hospital - Other/Home Health", ""},	
		{"45x",	"IP",	"Christian Science Hospital - Nursing Facility Level I",	"LT"},
		{"46x",	"IP",	"Christian Science Hospital - Nursing Facility Level II",	"LT"},
		{"47x",	"IP",	"Christian Science Hospital - Intermediate Care - Level III Nursing Facility",	"LT"},
		{"48x",	"IP",	"Christian Science Hospital - Swing Beds",	"LT"},
		{"51x",	"IP",	"Christian Science Extended Care - Inpatient",	"LT"},
		{"52x",	"PB",	"Christian Science Extended Care - Inpatient (Part B)", ""},	
		{"53x",	"OP",	"Christian Science Extended Care - Outpatient", ""},		
		{"54x",	"OP",	"Christian Science Extended Care - Other/Home Health", ""},	
		{"55x", "IP",	"Christian Science Extended Care - Nursing Facility Level I",	"LT"},
		{"56x",	"IP",	"Christian Science Extended Care - Nursing Facility Level II",	"LT"},
		{"57x",	"IP",	"Christian Science Extended Care - Intermediate Care - Level III Nursing Facility",	"LT"},
		{"58x",	"IP",	"Christian Science Extended Care - Swing Beds",	"LT"},
		{"61x",	"IP",	"Intermediate Care - Inpatient",	"LT"},
		{"62x",	"PB",	"Intermediate Care - Inpatient (Part B)", ""},	
		{"63x",	"OP",	"Intermediate Care - Outpatient", ""},	
		{"64x",	"OP",	"Intermediate Care - Other/Home Health", ""},	
		{"65x",	"IP",	"Intermediate Care - Nursing Facility Level I",	"LT"},
		{"66x",	"IP",	"Intermediate Care - Nursing Facility Level II",	"LT"},
		{"67x",	"IP",	"Intermediate Care - Intermediate Care - Level III Nursing Facility",	"LT"},
		{"68x",	"IP",	"Intermediate Care - Swing Beds",	"LT"},
		{"71x",	"OP",	"Clinic - Rural Health",	""},	
		{"72x",	"OP",	"Clinic - Hospital Based or Independent Renal Dialysis Center",	""},	
		{"73x",	"OP",	"Clinic - Free Standing Outpatient Rehab Facility",	""},	
		{"74x", "OP",	"Clinic - Outpatient Rehab Facility", ""},
		{"75x",	"OP",	"Clinic - Comprehensive Outpatient Rehab Facility",	""},
		{"76x",	"OP",	"Clinic - Community Mental Health Center",	""},
		{"77x", "OP",	"Clinic - Federally Qualified Health Center", ""},
		{"78x", "OP",	"Licensed Free Standing Emergency Medical Facility", ""},	
		{"79x",	"OP",	"Clinic - Other",	""},
		{"81x",	"IP",	"Special Facility - Hospice, Non-hospital based",	"LT"},
		{"82x",	"IP",	"Special Facility - Hospice, Hospital based",	"LT"},
		{"83x",	"OP",	"Special Facility - Ambulatory Surgery Center",	""},	
		{"84x",	"OP",	"Special Facility - Free Standing Birthing Center",	""},	
		{"85x", "OP",	"Special Facility - Critical Access Hospital", ""},
		{"86x", "OP",	"Special Facility - Residential Facility", ""},
		{"87x", "PB",	"Special Facility - Unknown Type", ""},
		{"88x", "PB",	"Special Facility - Unknown Type", ""},
		{"89x",	"OP",	"Special Facility - Other",	""},
		{"90x",	"PB",	"Unknown",	""},
		{"91x",	"PB",	"Unknown",	""},
		{"92x",	"PB",	"Unknown",	""},
		{"93x",	"PB",	"Unknown",	""},
		{"94x",	"PB",	"Unknown",	""},
		{"95x",	"PB",	"Unknown",	""},
		{"96x",	"PB",	"Unknown",	""},
		{"97x",	"PB",	"Unknown",	""},
		{"98x",	"PB",	"Unknown",	""},
		{"99x",	"PB",	"Unknown",	""}
	};
	
	static {
		BillTypeManager btm = new BillTypeManager();
		btList = new HashMap<String, BillType>();
		for (int i=0; i < btDef.length; i++) {
			btList.put(btDef[i][0].substring(0, 2), btm.new BillType(btDef[i][0], btDef[i][1], btDef[i][2], btDef[i][3]) );
		}
	}
	
	
	/**
	 * determine which part of the incoming bill type string is significant and return
	 * @param sIpBT
	 * @return
	 */
	public static String cleanBillType (String sIpBT) {
		
		String sRet = "";
		
		if (sIpBT == null  ||  sIpBT.length() < 4) 
			sRet = sIpBT;
		else {
			sRet = sIpBT.trim();
			while (sRet.length() > 3) {
				// lop off leading zeroes
				if (sRet.startsWith("0"))
					sRet = sRet.substring(1);
				// lop off training digits
				else
					sRet = sRet.substring(0, sRet.length() - 1);
			}
		}
		
		if (sRet != null  && sRet.length() >2)
			sRet = sRet.substring(0, 2);
		
		return sRet;
		
	}
	
	
	public static String getFileType (String sPrefix) {
		BillType bt = btList.get(sPrefix);
		return bt == null ? null : btList.get(sPrefix).sFileType; 
	}
	
	public static String getFacilityDescription (String sPrefix) {
		BillType bt = btList.get(sPrefix);
		return bt == null ? null : btList.get(sPrefix).sFacilityDescription; 
	}
	
	public static String getFacilityCode (String sPrefix) {
		BillType bt = btList.get(sPrefix);
		return bt == null ? null : btList.get(sPrefix).sFacilityCode; 
	}
	
	public class BillType {
		String sBillTypePrefix;
		String sFileType;
		String sFacilityDescription;
		String sFacilityCode;
		BillType (String sBillTypePrefix, String sFileType, String sFacilityDescription, String sFacilityCode) {
			this.sBillTypePrefix = sBillTypePrefix;
			this.sFileType = sFileType;
			this.sFacilityDescription = sFacilityDescription;
			this.sFacilityCode = sFacilityCode;
		}
	}

}


