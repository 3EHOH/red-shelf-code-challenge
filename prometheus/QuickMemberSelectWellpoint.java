





public class QuickMemberSelectWellpoint {
	
	
	public String sDelimiter = "\\|";
	
	
	private int countMedicalClaimHeadersInput = 0;
	private int countMedicalClaimHeadersOutput = 0;
	private int countMedicalClaimRecordsInput = 0;
	private int countMedicalClaimRecordsOutput = 0;
	private int countMemberEligibilityRecordsInput = 0;
	private int countMemberEligibilityRecordsOutput = 0;
	private int countRxClaimRecordsInput = 0;
	private int countRxClaimRecordsOutput = 0;
	private int countProviderRecordsInput = 0;
	private int countProviderRecordsOutput = 0;
	
	
	private void process() {
		
		// get a medical claims file
		InputManager medicalClaimHdrFile = new InputManager(parameters.get("claimhdrinput"));
		try {
			medicalClaimHdrFile.openFile();
		} catch (IOException | ZipException e) {
			log.error("An error occurred while opening " + parameters.get("claimhdrinput"));
		}

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
		
		// get a Provider file
		InputManager ProviderFile = new InputManager(parameters.get("providerinput"));
		try {
			ProviderFile.openFile();
		} catch (IOException | ZipException e) {
			log.error("An error occurred while opening " + parameters.get("providerinput"));
		}
				
		
		
		// define an extract file for the medical claims
		FileWriter outputMedicalHdrClaimFile = null;
		try {
			outputMedicalHdrClaimFile = new FileWriter(parameters.get("claimhdroutput"));
		} catch (IOException e1) {
			log.error("An error occurred while opening " + parameters.get("claimhdroutput"));
		}
		BufferedWriter bwMHF = new BufferedWriter(outputMedicalHdrClaimFile);
		
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
		
		// define an extract file for the provider records
		FileWriter outputProviderFile = null;
		try {
			outputProviderFile = new FileWriter(parameters.get("provideroutput"));
		} catch (IOException e1) {
			log.error("An error occurred while opening " + parameters.get("provideroutput"));
		}
		BufferedWriter bwPRF = new BufferedWriter(outputProviderFile);
		
		//limit = Integer.parseInt(parameters.get("count"));
		
		
		String line;
		
		
		// read the medical claim header records
		try {
			// header
			line = medicalClaimHdrFile.readFile();
			countMedicalClaimHeadersInput++;
			bwMHF.write(line);
			bwMHF.newLine();
			countMedicalClaimHeadersOutput++;
			//
			while ((line = medicalClaimHdrFile.readFile()) != null)  {
				countMedicalClaimHeadersInput++;
				if (selectClaimHdr(line)) {
					bwMHF.write(line);
					bwMHF.newLine();
					countMedicalClaimHeadersOutput++;
				}
			}
		} catch (IOException e) {
			log.error("An error occurred while reading " + parameters.get("claimhdrinput"));
		}
		
		try {
			medicalClaimHdrFile.closeFile();
		} catch (IOException e) {
			log.error("An error occurred while closing file: " + parameters.get("claimhdrinput"));
		}  
		
		try {
			bwMHF.close();
		} catch (IOException e) {
			log.error("An error occurred while closing file: " + parameters.get("claimhdroutput"));
		}
		

		// read the medical claim records
		try {
			// header
			line = medicalClaimFile.readFile();
			countMedicalClaimRecordsInput++;
			bwMCF.write(line);
			bwMCF.newLine();
			countMedicalClaimRecordsOutput++;
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
			// header
			line = memberEligibilityFile.readFile();
			countMemberEligibilityRecordsInput++;
			bwMEF.write(line);
			bwMEF.newLine();
			countMemberEligibilityRecordsOutput++;
			//
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
			// header
			line = RxClaimFile.readFile();
			countRxClaimRecordsInput++;
			bwRXF.write(line);
			bwRXF.newLine();
			countRxClaimRecordsOutput++;
			//
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
		
		// read the Provider claim records
		try {
			// header
			line = ProviderFile.readFile();
			countProviderRecordsInput++;
			bwPRF.write(line);
			bwPRF.newLine();
			countProviderRecordsOutput++;
			//
			while ((line = ProviderFile.readFile()) != null)  {
				countProviderRecordsInput++;
				if (selectProvider(line)) {
					bwPRF.write(line);
					bwPRF.newLine();
					countProviderRecordsOutput++;
				}
			}
		} catch (IOException e) {
			log.error("An error occurred while reading " + parameters.get("providerinput"));
		}  
		
		try {
			ProviderFile.closeFile();
		} catch (IOException e) {
			log.error("An error occurred while closing file: " + parameters.get("providerinput"));
		}  
		
		try {
			bwPRF.close();
		} catch (IOException e) {
			log.error("An error occurred while closing file: " + parameters.get("provideroutput"));
		}
		
		
		log.info("Selected " + MemberSelectionList.size() + " Unique Members");
		
		log.info("Read " + countMedicalClaimHeadersInput + " Medical Header Records");
		log.info("Wrote " + countMedicalClaimHeadersOutput + " Medical Header Records");		
		log.info("Read " + countMedicalClaimRecordsInput + " Medical Claim Records");
		log.info("Wrote " + countMedicalClaimRecordsOutput + " Medical Claim Records");
		log.info("Read " + countRxClaimRecordsInput + " Rx Claim Records");
		log.info("Wrote " + countRxClaimRecordsOutput + " Rx Claim Records");
		log.info("Read " + countMemberEligibilityRecordsInput + " Member Eligibility Records");
		log.info("Wrote " + countMemberEligibilityRecordsOutput + " Member Eligibility Records");
		log.info("Read " + countProviderRecordsInput + " Provider Records");
		log.info("Wrote " + countProviderRecordsOutput + " Provider Records");
		
	}
	
	
	
	/**
	 * select a number of members and all of their claim header records
	 * keep track of who is being selected in the MemberSelectionList
	 * @param s
	 * @return
	 */
	private boolean selectClaimHdr (String s) {
		
		boolean bResult = false;
		
		String[] in = s.split(sDelimiter);
		String sMemberId = in[0];
		String sProvider = in[4];
		
		
		if (isInMemberList(sMemberId)) {
			if (!isInProviderList(sProvider))
				ProviderSelectionList.add(sProvider);
			bResult = true;
		}
		
		else if(MemberSelectionList.size() < Integer.parseInt(parameters.get("count"))) {
			MemberSelectionList.add(sMemberId);
			if (!isInProviderList(sProvider))
				ProviderSelectionList.add(sProvider);
			bResult = true;
		}
		
		else
			bResult = false;
			
		
		return bResult;
		
	}
	
	/**
	 * select claim detail records for members already placed in MemberSelectionList
	 * @param s
	 * @return
	 */
	private boolean selectClaim (String s) {
		
		boolean bResult = false;
		
		String[] in = s.split(sDelimiter);
		String sMemberId = in[0];
		
		if (isInMemberList(sMemberId))
			bResult = true;
		
		return bResult;
		
	}
	
	

	/**
	 * select member records for members already placed in MemberSelectionList
	 * @param s
	 * @return
	 */
	private boolean selectMember (String s) {
		
		boolean bResult = false;
		
		String[] in = s.split(sDelimiter);
		String sMemberId = in[0];
		
		if (isInMemberList(sMemberId))
			bResult = true;
		
		return bResult;
		
	}
	
	
	
	private boolean selectRx (String s) {
		
		boolean bResult = false;
		
		String[] in = s.split(sDelimiter);
		String sMemberId = in[0];
		
		if (isInMemberList(sMemberId))
			bResult = true;
		
		return bResult;

	}
	
	
	private boolean selectProvider (String s) {
		
		boolean bResult = false;
		
		String[] in = s.split(sDelimiter);
		
		String sProviderId = in[0];
		
		// select only providers found in claims
		if (isInProviderList(sProviderId))
			bResult = true;
		
		return bResult;
		
	}
	
	
	/*
	private boolean isInMemberList (String s) {
		boolean b = false;
		for (String x : MemberSelectionList) {
			if (x.equals(s)) {
				b = true;
				break;
			}
		}
		return b;
	}
	*/
	
	

	private boolean isInMemberList (String s) {
		//boolean b = false;
		return MemberSelectionList.contains(s);
		/*
		for (String x : MemberSelectionList) {
			if (x.equals(s)) {
				b = true;
				break;
			}
		}
		return b;
		*/
	}
	

	private boolean isInProviderList (String s) {
		boolean b = false;
		for (String x : ProviderSelectionList) {
			if (x.equals(s)) {
				b = true;
				break;
			}
		}
		return b;
	}
	

	
	public static void main(String[] args) throws IOException {
		
		QuickMemberSelectWellpoint instance = new QuickMemberSelectWellpoint();
		
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
	
	
	private static org.apache.log4j.Logger log = Logger.getLogger(QuickMemberSelectWellpoint.class);
	
	HashSet<String> MemberSelectionList = new HashSet<String>();
	HashSet<String> ProviderSelectionList = new HashSet<String>();

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
	/*
	private String [] MemberList = {
			"100006092"
	};
	*/

}
