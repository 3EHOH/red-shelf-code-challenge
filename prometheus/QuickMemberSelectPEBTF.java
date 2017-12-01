




public class QuickMemberSelectPEBTF {
	
	
	public String sDelimiter = "\\*|\\||;|,";
	
	
	/**
	 * REMEMBER - these guys don't have a provider file, so this is a lousy template for anyone else!!
	 */
	

	private int countMedicalClaimRecordsInput = 0;
	private int countMedicalClaimRecordsOutput = 0;
	private int countMemberEligibilityRecordsInput = 0;
	private int countMemberEligibilityRecordsOutput = 0;
	private int countRxClaimRecordsInput = 0;
	private int countRxClaimRecordsOutput = 0;
	
	
	private void process() {
		

		// get a medical claims file
		InputManager medicalClaimFile = new InputManager(parameters.get("claiminput"));
		try {
			medicalClaimFile.openFile();
		} catch (IOException | ZipException e) {
			log.error("An error occurred while opening " + parameters.get("claiminput"));
		}
		
		// get a member eligibility file
		InputManager memberEligibilityFile = new InputManager(parameters.get("memberinput"));
		try {
			memberEligibilityFile.openFile();
		} catch (IOException | ZipException e) {
			log.error("An error occurred while opening " + parameters.get("memberinput"));
		}
		
		// get a Rx claim file
		InputManager RxClaimFile = new InputManager(parameters.get("rxinput"));
		try {
			RxClaimFile.openFile();
		} catch (IOException | ZipException e) {
			log.error("An error occurred while opening " + parameters.get("rxinput"));
		}
				
		
		// define an extract file for the medical claims
		FileWriter outputMedicalClaimFile = null;
		try {
			outputMedicalClaimFile = new FileWriter(parameters.get("claimoutput"));
		} catch (IOException e1) {
			log.error("An error occurred while opening " + parameters.get("claimoutput"));
		}
		BufferedWriter bwMCF = new BufferedWriter(outputMedicalClaimFile);
		
		
		// define an extract file for the member eligibility records
		FileWriter outputMemberEligibilityFile = null;
		try {
			outputMemberEligibilityFile = new FileWriter(parameters.get("memberoutput"));
		} catch (IOException e1) {
			log.error("An error occurred while opening " + parameters.get("memberoutput"));
		}
		BufferedWriter bwMEF = new BufferedWriter(outputMemberEligibilityFile);
		
		// define an extract file for the Rx claim records
		FileWriter outputRxClaimFile = null;
		try {
			outputRxClaimFile = new FileWriter(parameters.get("rxoutput"));
		} catch (IOException e1) {
			log.error("An error occurred while opening " + parameters.get("rxoutput"));
		}
		BufferedWriter bwRXF = new BufferedWriter(outputRxClaimFile);
		
		
		//limit = Integer.parseInt(parameters.get("count"));
		
		
		String line;
		
		
		// read the medical claim records
		try {
			//
			while ((line = medicalClaimFile.readFile()) != null)  {
				countMedicalClaimRecordsInput++;
				if (selectClaim(line)) {
					bwMCF.write(line);
					bwMCF.newLine();
					countMedicalClaimRecordsOutput++;
				}
			}
		} catch (IOException e) {
			log.error("An error occurred while reading " + parameters.get("claiminput"));
		}  
		
		try {
			medicalClaimFile.closeFile();
		} catch (IOException e) {
			log.error("An error occurred while closing file: " + parameters.get("claiminput"));
		}  
		
		try {
			bwMCF.close();
		} catch (IOException e) {
			log.error("An error occurred while closing file: " + parameters.get("claimoutput"));
		}
		
		
		// read the member eligibility records
		try {
			while ((line = memberEligibilityFile.readFile()) != null)  {
				countMemberEligibilityRecordsInput++;
				if (selectMember(line)) {
					bwMEF.write(line);
					bwMEF.newLine();
					countMemberEligibilityRecordsOutput++;
				}
			}
		} catch (IOException e) {
			log.error("An error occurred while reading " + parameters.get("memberinput"));
		}  
		
		try {
			memberEligibilityFile.closeFile();
		} catch (IOException e) {
			log.error("An error occurred while closing file: " + parameters.get("memberinput"));
		}  
		
		try {
			bwMEF.close();
		} catch (IOException e) {
			log.error("An error occurred while closing file: " + parameters.get("memberoutput"));
		}
		
		
		
		
		
		// read the Rx claim records
		try {
			while ((line = RxClaimFile.readFile()) != null)  {
				countRxClaimRecordsInput++;
				if (selectRx(line)) {
					bwRXF.write(line);
					bwRXF.newLine();
					countRxClaimRecordsOutput++;
				}
			}
		} catch (IOException e) {
			log.error("An error occurred while reading " + parameters.get("rxinput"));
		}  
		
		try {
			RxClaimFile.closeFile();
		} catch (IOException e) {
			log.error("An error occurred while closing file: " + parameters.get("rxinput"));
		}  
		
		try {
			bwRXF.close();
		} catch (IOException e) {
			log.error("An error occurred while closing file: " + parameters.get("rxoutput"));
		}
		
		
		
		log.info("Selected " + MemberSelectionList.size() + " Unique Members");
		

		
		log.info("Read " + countMedicalClaimRecordsInput + " Medical Claim Records");
		log.info("Wrote " + countMedicalClaimRecordsOutput + " Medical Claim Records");
		log.info("Read " + countRxClaimRecordsInput + " Rx Claim Records");
		log.info("Wrote " + countRxClaimRecordsOutput + " Rx Claim Records");
		log.info("Read " + countMemberEligibilityRecordsInput + " Member Eligibility Records");
		log.info("Wrote " + countMemberEligibilityRecordsOutput + " Member Eligibility Records");

		
	}
	
	
	
	/**
	 * select a number of members and all of their claim header records
	 * keep track of who is being selected in the MemberSelectionList
	 * @param s
	 * @return
	 */
	private boolean selectClaim (String s) {
		
		boolean bResult = false;
		
		String[] in = HelperPEBTF.splitter(s);
		
		//svcLine.setMember_id( (col_value + sFN.substring(0, iLen) + getColumnValue("PATDOB")).replace(" ", "") );
		StringBuffer sb = new StringBuffer();
		sb.append(in[11] == null ? "" : in[11].trim());
		sb.append(in[12] == null ? "" : in[12].trim());
		sb.append(in[17] == null ? "" : in[17].trim());
		String sMemberId = sb.toString();

		
		
		if (isInMemberList(sMemberId)) {
			bResult = true;
		}
		
		/*
		else if(MemberSelectionList.size() < Integer.parseInt(parameters.get("count"))) {
			MemberSelectionList.add(sMemberId);
			if (!isInProviderList(sProvider))
				ProviderSelectionList.add(sProvider);
			bResult = true;
		}
		*/
		
		else
			bResult = false;
			
		
		return bResult;
		
	}
	
	
	

	/**
	 * select member records for members already placed in MemberSelectionList
	 * @param s
	 * @return
	 */
	private boolean selectMember (String s) {
		
		boolean bResult = false;
		
		String[] in = HelperPEBTF.splitter(s);
		
		StringBuffer sb = new StringBuffer();
		sb.append(in[8] == null ? "" : in[8].trim());
		sb.append(in[9] == null ? "" : in[9].trim());
		sb.append(in[10] == null ? "" : in[10].trim());
		String sMemberId = sb.toString();

		
		if (isInMemberList(sMemberId))
			bResult = true;
		
		return bResult;
		
	}
	
	
	
	private boolean selectRx (String s) {
		
		boolean bResult = false;
		
		String[] in = HelperPEBTF.splitter(s);
		
		StringBuffer sb = new StringBuffer();
		sb.append(in[20] == null ? "" : in[20].trim());
		sb.append(in[21] == null ? "" : in[21].trim());
		sb.append(in[22] == null ? "" : in[22].trim());
		String sMemberId = sb.toString();

		
		if (isInMemberList(sMemberId))
			bResult = true;
		
		return bResult;

	}
	
	
	
	
	
	private boolean isInMemberList (String s) {
		boolean b = false;
		for (String x : MemberList) {
			if (x.equals(s)) {
				b = true;
				break;
			}
		}
		return b;
	}
	
	
	
	/*
	private boolean isInMemberList (String s) {
		return MemberSelectionList.contains(s);
	}
	*/
	

	

	
	public static void main(String[] args) throws IOException {
		
		QuickMemberSelectPEBTF instance = new QuickMemberSelectPEBTF();
		
		// get parameters (if any)
		instance.loadParameters(args);
		
		if (instance.parameters.get("runname") == null)
			instance.parameters.put("runname", "sample");
		
		// process
		instance.process();

	}
	
	HashMap<String, String> parameters = new HashMap<String, String>();
	String [][] parameterDefaults = {
			{"count", "1000"}
	};
	
	/**
	 * load default parameters and 
	 * put any run arguments in the hash map as well
	 * arguments should take the form keyword=value (e.e., studybegin=20140101)
	 * @param args
	 */
	private void loadParameters (String[] args) {
		// load any default parameters from the default parameter array
		for (int i = 0; i < parameterDefaults.length; i++) {
			parameters.put(parameterDefaults[i][0], parameterDefaults[i][1]);
		}
		// overlay or add any incoming parameters
		for (int i = 0; i < args.length; i++) {
			parameters.put(args[i].substring(0, args[i].indexOf('=')), args[i].substring(args[i].indexOf('=')+1)) ;
		}		
	}
	
	
	private static org.apache.log4j.Logger log = Logger.getLogger(QuickMemberSelectPEBTF.class);
	
	HashSet<String> MemberSelectionList = new HashSet<String>();


	// ten members with total cost less than zero
	/*
	private String [] MemberList = {
			"251870340",
			"184085073",
			"245365451",
			"172361806",
			"245343071",
			"246525088",
			"252009695",
			"184085076",
			"244588415",
			"173765857"
	};
	*/
	
	/*  big patient
	private String [] MemberList = {
			"ABARAYDIANA19800805"
	};
	*/
	
	// Diana is a big patient, Dayane has a DEDUCT, DAKOTA has a COPAY, ANN has a COINS
	private String [] MemberList = {
			"ABARAYDIANA19800805",
			"SMITHDAYANE19930305",
			"EVANSDAKOTA19900908",
			"WALSTROMANN19601212",
			"GAILBRAXTON19631207",
			"AUTUMNVOSSLER19930904",
			"TARYNFELSBURG20021125"
	};
	

}
