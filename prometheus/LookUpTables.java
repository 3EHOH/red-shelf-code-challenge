

public class LookUpTables {
	
	public static final String [][] SRC_ADMS = {
		{"1", "Physician Referral"},
		{"2", "Clinic Referral"},
		{"3", "HMO Referral"},
		{"4", "Transfer from Hospital"}, 
		{"5", "Transfer from SNF"},
		{"6", "Transfer From Another Health Care Facility"},
		{"7", "Emergency Room"},
		{"8", "Court/Law Enforcement"},
		{"9", "Information Not Available"}
		/*
		 * In the Case of Newborn
			1 = Normal Delivery
			2 = Premature Delivery
			3 = Sick Baby
			4 = Extramural Birth
		 */
	};
	private static HashMap<String, String> srcAdms = new HashMap<String, String>();
	public static boolean isSrcAdmsValid (String s)
	{
		return srcAdms.containsKey(s);
	}
	public static String getSrcAdmsEquate (String s)
	{
		return srcAdms.get(s);
	}
	
	public static final String [][] RACE = {
		{"0", "UNKNOWN"},
		{"1", "WHITE"},
		{"2", "BLACK"},
		{"3", "OTHER"},
		{"4", "ASIAN"}, 
		{"5", "HISPANIC"},
		{"6", "NORTH AMERICAN NATIVE"}
	};
	private static HashMap<String, String> race = new HashMap<String, String>();
	public static boolean isRaceValid (String s)
	{
		return race.containsKey(s);
	}
	public static String getRaceEquate (String s)
	{
		return race.get(s);
	}
	
	public static final String [][] PROVIDER_SPECIALTY = {
		{"01", "General Practice"}, 
		{"02", "General Surgery"}, 
		{"03", "Allergy Immunology"}, 
		{"04", "Otolaryngology"}, 
		{"05", "Anesthesiology"}, 
		{"06", "Cardiology"}, 
		{"07", "Dermatology"}, 
		{"08", "Family Practice"}, 
		{"09", "Interventional Pain  Management"}, 
		{"10", "Gastroenterology"}, 
		{"11", "Internal Medicine"}, 
		{"12", "Osteopathic Manipulative Therapy (OMM)"}, 
		{"13", "Neurology"}, 
		{"14", "Neurosurgery"}, 
		{"16", "Obstetrics Gynecology"}, 
		{"18", "Ophthalmology"}, 
		{"19", "Oral Surgery (dental only)"}, 
		{"20", "Orthopedic Surgery"}, 
		{"22", "Pathology"}, 
		{"24", "Plastic and Reconstructive Surgery"}, 
		{"25", "Physical Medicine and Rehabilitation"}, 
		{"26", "Psychiatry"}, 
		{"28", "Colorectal Surgery (formerly Proctology)"}, 
		{"29", "Pulmonary Disease"}, 
		{"30", "Diagnostic Radiology"}, 
		{"32", "Anesthesiologist Assistant"}, 
		{"33", "Thoracic Surgery"}, 
		{"34", "Urology"}, 
		{"35", "Chiropractic"}, 
		{"36", "Nuclear Medicine"}, 
		{"37", "Pediatric Medicine"}, 
		{"38", "Geriatric Medicine"}, 
		{"39", "Nephrology"}, 
		{"40", "Hand Surgery"}, 
		{"41", "Optometry"}, 
		{"42", "Certified Nurse Midwife"}, 
		{"43", "Certified Registered Nurse Assistant (CRNA)"}, 
		{"44", "Infectious Disease"}, 
		{"45", "Mammography Screening Center"}, 
		{"46", "Endocrinology"}, 
		{"47", "Independent Diagnostic Testing Facility"}, 
		{"48", "Podiatry"}, 
		{"49", "Ambulatory Surgical Center"}, 
		{"50", "Nurse Practitioner"},
		{"51", "Medical Supply Company with Orthotist"}, 
		{"52", "Medical Supply Company with Prosthetist"}, 
		{"53", "Medical Supply Company with Orthotist-Prosthetist"}, 
		{"54", "Other Medical Supply Company"}, 
		{"55", "Individual Certified Orthotist"}, 
		{"56", "Individual Certified Prosthetist"}, 
		{"57", "Individual Certified Prosthetist-Orthotist"}, 
		{"58", "Medical Supply Company with Pharmacist"}, 
		{"59", "Ambulance Service Provider"}, 
		{"60", "Public Health or Welfare Agency"}, 
		{"61", "Voluntary Health or Charitable Agency"}, 
		{"62", "Psychologist"}, 
		{"63", "Portable X-Ray Supplier"}, 
		{"64", "Audiologist"}, 
		{"65", "Physical Therapist"}, 
		{"66", "Rheumatology"}, 
		{"67", "Occupational Therapist"}, 
		{"68", "Clinical Psychologist"}, 
		{"69", "Clinical Laboratory"}, 
		{"70", "Multi-specialty Clinic or Group Practice/single specialty"}, 
		{"71", "Registered Dietitian/Nutrition Professional"}, 
		{"72", "Pain Management"}, 
		{"73", "Mass Immunization Roster Billers"}, 
		{"74", "Radiation Therapy Center"}, 
		{"75", "Slide Preparation Facilities"}, 
		{"76", "Peripheral Vascular Disease"}, 
		{"77", "Vascular Surgery"}, 
		{"78", "Cardiac Surgery"}, 
		{"79", "Addiction Medicine"}, 
		{"80", "Licensed Clinical Social Worker"}, 
		{"81", "Critical Care (Intensivists)"}, 
		{"82", "Hematology"}, 
		{"83", "Hematology/Oncology"}, 
		{"84", "Preventive Medicine"}, 
		{"85", "Maxillofacial Surgery"}, 
		{"86", "Neuropsychiatry"}, 
		{"87", "All Other Suppliers"}, 
		{"88", "Unknown Supplier/Provider Specialty"}, 
		{"89", "Certified Clinical Nurse Specialist"}, 
		{"90", "Medical Oncology"}, 
		{"91", "Surgical Oncology"}, 
		{"92", "Radiation Oncology"}, 
		{"93", "Emergency Medicine"}, 
		{"94", "Interventional Radiology"}, 
		{"96", "Optician"}, 
		{"97", "Physician Assistant"}, 
		{"98", "Gynecological/Oncology"}, 
		{"99", "Unknown Physician Specialty"}, 
		{"A0", "Hospital"}, 
		{"A1", "Skilled Nursing Facility"}, 
		{"A2", "Intermediate Care Nursing Facility"},  
		{"A3", "Other Nursing Facility"}, 
		{"A4", "Home Health Agency"}, 
		{"A5", "Pharmacy"}, 
		{"A6", "Medical Supply Company with Respiratory Therapist"}, 
		{"A7", "Department Store"}, 
		{"A8", "Grocery Store"}
	};
	private static HashMap<String, String> provider_specialty = new HashMap<String, String>();
	public static boolean isProviderSpecialtyValid (String s)
	{
		return provider_specialty.containsKey(s);
	}
	public static String getProviderSpecialtyEquate (String s)
	{
		return provider_specialty.get(s);
	}
	
	
	public static final String [][] FACILITY_TYPE = {
		{"000", "Unknown"},
		{"ST", "Short term"},
		{"CAH", "Critical access Hospital"},
		{"PSY", "Psych hospital/unit"},
		{"REH", "Rehab hospital/unit"},
		{"LT", "Long term facility"},
		{"SNF", "Skilled nursing facility"}
	};
	private static HashMap<String, String> facility_type = new HashMap<String, String>();
	public static boolean isFacilityTypeValid (String s)
	{
		return facility_type.containsKey(s);
	}
	public static String getFacilityTypeEquate (String s)
	{
		return facility_type.get(s);
	}

	public static final String [][] DISCHARGE_STATUS = {
		{"01", "Home/Self Care"},
		{"02", "Other Acute Care Hospital"},
		{"03", "Skilled Nursing Facility"},
		{"04", "Intermediate Care Facility"},
		{"05", "Other Facility for IP Care"},
		{"06", "Organized Home Care"},
		{"07", "Left Against Advice"},
		{"08", "Home IV Drug Therapy"},
		{"09", "Admitted to this Hospital"},
		{"20", "Expired"},
		{"30", "Still a Patient"},
		{"31", "Still a Patient"},
		{"32", "Still a Patient"},
		{"33", "Still a Patient"},
		{"34", "Still a Patient"},
		{"35", "Still a Patient"},
		{"36", "Still a Patient"},
		{"37", "Still a Patient"},
		{"38", "Still a Patient"},
		{"39", "Still a Patient"},
		{"40", "Expired at Home"},
		{"41", "Expired at Medical Facility"},
		{"42", "Expired at Place Unknown"},
		{"50", "Hospice - Home"},
		{"51", "Hospice - Medical Facility"},
		{"62", "IP Rehab Hospital"},
		{"63", "Long Term Care Hospital"},
		{"65", "Psych Hospital or Distinct Unit"}
	};
	private static HashMap<String, String> discharge_status = new HashMap<String, String>();
	public static boolean isDischargeStatusValid (String s)
	{
		return discharge_status.containsKey(s);
	}
	public static String getDischargeStatusEquate (String s)
	{
		return discharge_status.get(s);
	}
	
	
	public static final String [][] ADMIT_TYPE_CODE = {
		{"1", "Admission through ER"},
		{"2", "Urgent"},
		{"3", "Elective"},
		{"4", "Newborn"},
		{"5", "Trauma"},
		{"9", "Unknown"}
	};
	private static HashMap<String, String> admit_type_code = new HashMap<String, String>();
	public static boolean isAdmitTypeCodeValid (String s)
	{
		return admit_type_code.containsKey(s);
	}
	public static String getAdmitTypeCodeEquate (String s)
	{
		return admit_type_code.get(s);
	}
	
	
	public static final String [][] PLACE_OF_SVC_CODE = {
		{"01", "Pharmacy"},
		{"02", "Unassigned"},
		{"03", "School"},
		{"04", "Homeless Shelter"},
		{"05", "Indian Health Service Free-standing Facility"},
		{"06", "Indian Health Service Provider-based Facility"},
		{"07", "Tribal 638 Free-standing Facility"},
		{"08", "Tribal 638 Provider-based Facility"},
		{"09", "Prison/Correctional Facility"},
		{"10", "Unassigned"},
		{"11", "Office"},
		{"12", "Home"}, 
		{"13", "Assisted Living Facility"},
		{"14", "Group Home"},
		{"15", "Mobile Unit"},
		{"16", "Temporary Lodging"},
		{"17", "Walk-in Retail Health Clinic"},
		{"18", "Place of Employment-Worksite"},
		{"19", "Unassigned"},
		{"20", "Urgent Care Facility"},
		{"21", "Inpatient Hospital"},
		{"22", "Outpatient Hospital"},
		{"23", "Emergency Room - Hospital"},
		{"24", "Ambulatory Surgery Center"},
		{"25", "Birthing Center"},
		{"26", "Military Treatment Facility"},
		{"31", "Skilled Nursing Facility"},
		{"32", "Nursing Facility"},
		{"33", "Custodial Care Facility"},
		{"34", "Hospice"},
		{"41", "Ambulance - Land"},
		{"42", "Ambulance - Air or Water"},
		{"49", "Independent Clinic"},
		{"50", "Federally Qualified Health Center"},
		{"51", "IP Psych Facility"},
		{"52", "Psych Facility - Partial Hosp"},
		{"53", "Community Mental Health Center"},
		{"54", "Intermediate Care Facility/Mentally Retarded"},
		{"55", "Residential Sub Abuse Treat Fac"},
		{"56", "Psych Residential Treat Center"},
		{"57", "Non Res Sub Abuse Treat Fac"}, 
		{"60", "Mass Immunization Center"},
		{"61", "Comprehensive Inpatient Rehabilitation Facility"},
		{"62", "Comprehensive Outpatient Rehabilitation Facility"},
		{"65", "End-Stage Renal Disease Treatment Facility"},
		{"71", "Public Health Clinic"},
		{"72", "Rural Health Clinic"},
		{"81", "Independent Lab"},
		{"99", "Other Place of Service"}
	};
	private static HashMap<String, String>place_of_svc_code = new HashMap<String, String>();
	public static boolean isPlaceOfSvcCodeValid (String s)
	{
		return place_of_svc_code.containsKey(s);
	}
	public static String getPlaceOfSvcCodeEquate (String s)
	{
		return place_of_svc_code.get(s);
	}



	static 
	{
		for (int i=0; i < SRC_ADMS.length; i++)
		{
			srcAdms.put(SRC_ADMS[i][0], SRC_ADMS[i][1]);
		}
		for (int i=0; i < RACE.length; i++)
		{
			race.put(RACE[i][0], RACE[i][1]);
		}
		for (int i=0; i < PROVIDER_SPECIALTY.length; i++)
		{
			provider_specialty.put(PROVIDER_SPECIALTY[i][0], PROVIDER_SPECIALTY[i][1]);
		}
		for (int i=0; i < FACILITY_TYPE.length; i++)
		{
			facility_type.put(FACILITY_TYPE[i][0], FACILITY_TYPE[i][1]);
		}
		for (int i=0; i < DISCHARGE_STATUS.length; i++)
		{
			discharge_status.put(DISCHARGE_STATUS[i][0], DISCHARGE_STATUS[i][1]);
		}
		for (int i=0; i < ADMIT_TYPE_CODE.length; i++)
		{
			admit_type_code.put(ADMIT_TYPE_CODE[i][0], ADMIT_TYPE_CODE[i][1]);
		}
		for (int i=0; i < PLACE_OF_SVC_CODE.length; i++)
		{
			place_of_svc_code.put(PLACE_OF_SVC_CODE[i][0], PLACE_OF_SVC_CODE[i][1]);
		}
	}
	
}
